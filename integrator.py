import os
import pandas as pd
import datetime
import random
import config

class Integrator(object):
    """Class that integrates daily scrape jobs to database. Currently the database is just two dataframes containing estates on and off the market."""
    def __init__(self):
        pass

    def integrate(self):
        
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
            print(' CASE 3. DATABASE_EXISTS + MULTIPLE_SCRAPING_JOBS   -----> _integrate_most_recent_job()')
            self._integrate_most_recent_job()

        elif (~self.DATABASE_EXISTS and self.ONE_SCRAPING_JOB):
            print('CASE 5. ~DATABASE_EXISTS + ONE_SCRAPING_JOB        ----->_create_initial_database()')
            self._create_initial_database()

        elif (~self.DATABASE_EXISTS and self.MULTIPLE_SCRAPING_JOBS):
            print('CASE 6. ~DATABASE_EXISTS + MULTIPLE_SCRAPING_JOBS  -----> _integrate_all_jobs()')
            self._integrate_all_jobs()
            print('Case handling not implemented!')

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
        df_off = self._detect_off_market_listings(df_mr, df_2mr) # get off market items
        #df_off['offMarket'] = pd.Series(len(df_off) * [True])
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


    def _detect_off_market_listings(self, df_mr, df_2mr):
        mask = ~(df_2mr.fullAddress.isin(df_mr.fullAddress))
        df_off = df_2mr[mask]
        return df_off
   

