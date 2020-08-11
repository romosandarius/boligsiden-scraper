import os
import pandas as pd
import datetime
from datetime import date
import random
import config

class Integrator(object):
    """Class that integrates daily scrape jobs to database. Currently the database is just two dataframes containing estates on and off the market."""
    def __init__(self):
        self.SCRAPING_JOBS = os.listdir(config.PATH_SCRAPING_JOBS)
        self.DATABASE = os.listdir(config.PATH_DATABASE)
        assert self.SCRAPING_JOBS, 'No scraping jobs. Can\'t integrate.'


    def integrate(self):
        
        self._check_conditions()
        self._handle_conditions()


    def integrate_all_jobs(self):

        for i in range(len(self.SCRAPING_JOBS)):
            print('Integrating: {}'.format(self.SCRAPING_JOBS[i]))
            if i == 0:
                df = pd.read_pickle(config.PATH_SCRAPING_JOBS + '/' + self.SCRAPING_JOBS[i])
                df.to_pickle(config.PATH_DATABASE_ON_MARKET)
            else:
                df_mr = pd.read_pickle(config.PATH_SCRAPING_JOBS + '/' + self.SCRAPING_JOBS[i])
                df_2mr = pd.read_pickle(config.PATH_SCRAPING_JOBS + '/' + self.SCRAPING_JOBS[i-1])
                df_off, df_new = self._detect_new_and_off_market_listings(df_mr, df_2mr)
            
                # Add dateLastSeen column
                df_off['dateOff'] = self.SCRAPING_JOBS[i][:-4]
                df_off['dateOff'] = pd.to_datetime(df_off['dateOff'])
                df_new['dateOn'] = self.SCRAPING_JOBS[i][:-4]
                df_new['dateOn'] = pd.to_datetime(df_new['dateOn'])      

                # Save on-market datbase
                df_mr.to_pickle(config.PATH_DATABASE_ON_MARKET)
                
                # Append off-market to db
                self._append_to_db(df=df_off, path_db=config.PATH_DATABASE_OFF_MARKET)
                
                # Append new-in-market to db
                self._append_to_db(df=df_new, path_db=config.PATH_DATABASE_NEW_ON_MARKET)
        
    
    def _check_conditions(self):

        if not os.path.isdir('./data'): os.mkdir('./data')
        if not os.path.isdir('./data/database'): os.mkdir('./data/database')
        if not os.path.isdir('./data/scraping_jobs'): os.mkdir('./data/scraping_jobs')

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
            self.integrate_all_jobs()

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


    def _append_to_db(self, df, path_db):
        # Append to or initiate to off-market database  
        if not os.path.isfile(path_db):
            df.to_pickle(path_db)
        else:
            df_db = pd.read_pickle(path_db)
            # print(df.columns)
            # print(df_db.columns)
            df_db = pd.concat([df_db, df])
            columns = list(df_db.columns)
            columns.remove('rating')
            df_db = df_db.drop_duplicates(subset=columns)
            df_db.to_pickle(path_db)


    def _detect_new_and_off_market_listings(self, df_mr, df_2mr):
        df_diff = pd.concat([df_mr, df_2mr]).drop_duplicates(subset=['city', 'address', 'postal'], keep=False)
        dates = list(df_diff.dateScraped.unique())
        dates = sorted(dates)
        df_new = df_diff[df_diff['dateScraped'] == dates[-1]].copy()
        df_off = df_diff[df_diff['dateScraped'] == dates[0]].copy()
        return df_off, df_new


    def _integrate_most_recent_job(self):
        self.SCRAPING_JOBS = sorted(self.SCRAPING_JOBS)
        df_mr = pd.read_pickle(config.PATH_SCRAPING_JOBS + '/' + self.SCRAPING_JOBS[-1]) # most recent job
        df_2mr = pd.read_pickle(config.PATH_SCRAPING_JOBS + '/' + self.SCRAPING_JOBS[-2]) # second most recent job
        df_off, df_new = self._detect_new_and_off_market_listings(df_mr, df_2mr) # get off market items
        
        # Add dateLastSeen column
        df_off['dateOff'] = self.SCRAPING_JOBS[-i][:-4]
        df_off['dateOff'] = pd.to_datetime(df_off['dateOff'])
        df_new['dateOn'] = self.SCRAPING_JOBS[-1][:-4]
        df_new['dateOn'] = pd.to_datetime(df_new['dateOn'])
        
        # Save on-market databse
        df_mr.to_pickle(config.PATH_DATABASE_ON_MARKET)

        # Append off-market to db
        self._append_to_db(df=df_off, path_db=config.PATH_DATABASE_OFF_MARKET)
        
        # Append new-in-market to db
        self._append_to_db(df=df_new, path_db=config.PATH_DATABASE_NEW_ON_MARKET)

   
    



