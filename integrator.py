import os
import pandas as pd
import datetime
from datetime import date
import random
import config

class Integrator(object):
    """Class that integrates daily scrape jobs to database. Currently the database is just two dataframes containing estates on and off the market."""
    def __init__(self):
        pass

    def integrate(self, integrate_all=False):
        
        if integrate_all:
            self.SCRAPING_JOBS = os.listdir(config.PATH_SCRAPING_JOBS)
            assert self.SCRAPING_JOBS, 'No scraping jobs. Can\'t integrate all.'
            self._integrate_all_jobs()
        else:
            self._check_conditions()
            self._handle_conditions()
        
    
    def _check_conditions(self):

        if not os.path.isdir('./data'): os.mkdir('./data')
        if not os.path.isdir('./data/database'): os.mkdir('./data/database')
        if not os.path.isdir('./data/scraping_jobs'): os.mkdir('./data/scraping_jobs')

        self.SCRAPING_JOBS = os.listdir(config.PATH_SCRAPING_JOBS)
        self.DATABASE = os.listdir(config.PATH_DATABASE)
    
        if not self.DATABASE: self.DATABASE_EXISTS = False
        else: self.DATABASE_EXISTS = True
        
        if not self.SCRAPING_JOBS: self.NO_SCRAPING_JOBS = True
        else: self.NO_SCRAPING_JOBS = False

        if len(self.SCRAPING_JOBS) == 1: self.ONE_SCRAPING_JOB = True
        else: self.ONE_SCRAPING_JOB = False
        
        if len(self.SCRAPING_JOBS) > 1: self.MULTIPLE_SCRAPING_JOBS = True
        else: self.MULTIPLE_SCRAPING_JOBS = False
        

    def _handle_conditions(self):

        # exit conditions        
        assert not (self.DATABASE_EXISTS and self.NO_SCRAPING_JOBS), 'Database exists, but there is no scraping jobs.'
        assert not (self.DATABASE_EXISTS and self.ONE_SCRAPING_JOB), 'Database and only one scraping job exists. Nothing to integrate.'
        assert not (~self.DATABASE_EXISTS and self.NO_SCRAPING_JOBS), 'Neither database nor scraping jobs exist. Nothing to integrate.'

        # action on conditions
        if (self.DATABASE_EXISTS and self.MULTIPLE_SCRAPING_JOBS):
            print('Database and multiple scraping jobs exist. Integrating most recent job.')
            self._integrate_most_recent_job()

        elif (~self.DATABASE_EXISTS and self.ONE_SCRAPING_JOB):
            print('No database and only one scraping job exist. Creating initial database.')
            self._create_initial_database()

        elif (~self.DATABASE_EXISTS and self.MULTIPLE_SCRAPING_JOBS):
            print('No database and multiple scraping job exists. Integrating all jobs.')
            self._integrate_all_jobs()

        # CASES:
        # 1. DATABASE_EXISTS + NO_SCRAPING_JOBS         -----> exit()
        # 2. DATABASE_EXISTS + ONE_SCRAPING_JOB         -----> exit()
        # 3. DATABASE_EXISTS + MULTIPLE_SCRAPING_JOBS   -----> _integrate_latest()
        # 4. ~DATABASE_EXISTS + NO_SCRAPING_JOBS        -----> exit()
        # 5. ~DATABASE_EXISTS + ONE_SCRAPING_JOB        -----> _create_initial_database()
        # 6. ~DATABASE_EXISTS + MULTIPLE_SCRAPING_JOBS  -----> _integrate_all()

    def _create_initial_database(self):
        df = pd.read_pickle(config.PATH_SCRAPING_JOBS + '/' + self.SCRAPING_JOBS[0])
        df.to_pickle(config.PATH_DATABASE + '/' + config.NAME_DATABASE_ON_MARKET)


    def _integrate_most_recent_job(self):
        self.SCRAPING_JOBS = sorted(self.SCRAPING_JOBS)
        df_mr = pd.read_pickle(config.PATH_SCRAPING_JOBS + '/' + self.SCRAPING_JOBS[-1]) # most recent job
        df_2mr = pd.read_pickle(config.PATH_SCRAPING_JOBS + '/' + self.SCRAPING_JOBS[-2]) # second most recent job
        df_off, df_new = self._detect_new_and_off_market_listings(df_mr, df_2mr) # get off market items
        
        # Add dateDetected column
        df_off['dateLastSeen'] = self.SCRAPING_JOBS[-2]
        df_off['dateLastSeen'] = pd.to_datetime(df_off['dateLastSeen'])
        df_new['dateLastSeen'] = self.SCRAPING_JOBS[-2]
        df_new['dateLastSeen'] = pd.to_datetime(df_new['dateLastSeen'])
        

        # Save on-market databse
        df_mr.to_pickle(config.PATH_DATABASE_ON_MARKET)

        # Append to or initiate to off-market database
        if not os.path.isfile(config.PATH_DATABASE_OFF_MARKET):
            df_off.to_pickle(config.PATH_DATABASE_OFF_MARKET)
        else:
            df_db_off = pd.read_pickle(config.PATH_DATABASE_OFF_MARKET)
            df_db_off = pd.concat([df_db_off, df_off])
            # drop duplicates (fixes case where same date is integrated multiple times.. hacky..)
            columns = list(df_db_off.columns)
            columns.remove('rating')
            df_db_off = df_db_off.drop_duplicates(subset=columns)
            df_db_off.to_pickle(config.PATH_DATABASE_OFF_MARKET)

        # Append to or initiate new-on-market database
        if not os.path.isfile(config.PATH_DATABASE_NEW_ON_MARKET):
            df_new.to_pickle(config.PATH_DATABASE_NEW_ON_MARKET)
        else:
            df_db_new = pd.read_pickle(config.PATH_DATABASE_NEW_ON_MARKET)
            df_db_new = pd.concat([df_db_new, df_new])
            columns = list(df_db_new.columns)
            columns.remove('rating')        
            df_db_new = df_db_new.drop_duplicates(subset=columns)    
            df_db_new.to_pickle(config.PATH_DATABASE_NEW_ON_MARKET)


    def _detect_new_and_off_market_listings(self, df_mr, df_2mr):
        df_diff = pd.concat([df_mr, df_2mr]).drop_duplicates(subset=['city', 'address', 'postal'], keep=False)
        dates = list(df_diff.dateScraped.unique())
        dates = sorted(dates)
        df_new = df_diff[df_diff['dateScraped'] == dates[-1]].copy()
        df_off = df_diff[df_diff['dateScraped'] == dates[0]].copy()
        return df_off, df_new
   
    def _integrate_all_jobs(self):

        for i in range(len(self.SCRAPING_JOBS)):
            print('Integrating: {}'.format(self.SCRAPING_JOBS[i]))
            if i == 0:
                df = pd.read_pickle(config.PATH_SCRAPING_JOBS + '/' + self.SCRAPING_JOBS[i])
                df.to_pickle(config.PATH_DATABASE_ON_MARKET)
            else:
                df_mr = pd.read_pickle(config.PATH_SCRAPING_JOBS + '/' + self.SCRAPING_JOBS[i])
                df_2mr = pd.read_pickle(config.PATH_SCRAPING_JOBS + '/' + self.SCRAPING_JOBS[i-1])
                df_off, df_new = self._detect_new_and_off_market_listings(df_mr, df_2mr)
            
                # Add dateDetected column
                df_off['dateDetected'] = date.today()
                df_off['dateDetected'] = pd.to_datetime(df_off['dateDetected'])
                df_new['dateDetected'] = date.today()
                df_new['dateDetected'] = pd.to_datetime(df_new['dateDetected'])
                

                # Save on-market datbase
                df_mr.to_pickle(config.PATH_DATABASE_ON_MARKET)

                # Append to or initiate to off-market database
                if not os.path.isfile(config.PATH_DATABASE_OFF_MARKET):
                    df_off.to_pickle(config.PATH_DATABASE_OFF_MARKET)
                else:
                    df_db_off = pd.read_pickle(config.PATH_DATABASE_OFF_MARKET)
                    df_db_off = pd.concat([df_db_off, df_off])
                    # drop duplicates (fixes case where same date is integrated multiple times.. hacky..)
                    columns = list(df_db_off.columns)
                    columns.remove('rating')
                    df_db_off = df_db_off.drop_duplicates(subset=columns)
                    df_db_off.to_pickle(config.PATH_DATABASE_OFF_MARKET)

                # Append to or initiate new-on-market database
                if not os.path.isfile(config.PATH_DATABASE_NEW_ON_MARKET):
                    df_new.to_pickle(config.PATH_DATABASE_NEW_ON_MARKET)
                else:
                    df_db_new = pd.read_pickle(config.PATH_DATABASE_NEW_ON_MARKET)
                    df_db_new = pd.concat([df_db_new, df_new])
                    columns = list(df_db_new.columns)
                    columns.remove('rating')        
                    df_db_new = df_db_new.drop_duplicates(subset=columns)    
                    df_db_new.to_pickle(config.PATH_DATABASE_NEW_ON_MARKET)

