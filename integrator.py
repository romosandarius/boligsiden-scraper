import os
import pandas as pd
import datetime
import random
import config

class Integrator(object):

    def __init__(self, date, backfill=False):
        self.date = date
        self.backfill = backfill


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

        # action conditions
        if (self.DATABASE_EXISTS and self.MULTIPLE_SCRAPING_JOBS):
            print(' CASE 3. DATABASE_EXISTS + MULTIPLE_SCRAPING_JOBS   -----> _integrate_latest()')
            self._integrate_latest()

        elif (~self.DATABASE_EXISTS and self.ONE_SCRAPING_JOB):
            print('CASE 5. ~DATABASE_EXISTS + ONE_SCRAPING_JOB        ----->_create_initial_database()')
            self._create_initial_database()

        elif (~self.DATABASE_EXISTS and self.MULTIPLE_SCRAPING_JOBS):
            print('CASE 6. ~DATABASE_EXISTS + MULTIPLE_SCRAPING_JOBS  -----> _integrate_all()')

        # CASES:
        # 1. DATABASE_EXISTS + NO_SCRAPING_JOBS         -----> exit()
        # 2. DATABASE_EXISTS + ONE_SCRAPING_JOB         -----> exit()
        # 3. DATABASE_EXISTS + MULTIPLE_SCRAPING_JOBS   -----> _integrate_latest()
        # 4. ~DATABASE_EXISTS + NO_SCRAPING_JOBS        -----> exit()
        # 5. ~DATABASE_EXISTS + ONE_SCRAPING_JOB        -----> _create_initial_database()
        # 6. ~DATABASE_EXISTS + MULTIPLE_SCRAPING_JOBS  -----> _integrate_all()

    def _create_initial_database(self):
        df = pd.read_pickle(config.PATH_SCRAPING_JOBS + '/' + self.SCRAPING_JOBS[0])
        df.to_pickle(config.PATH_DATABASE + '/' + config.NAME_DATABASE)

    def _integrate_latest(self):
        # Find integrated dates
        database = pd.read_pickle(config.PATH_DATABASE + '/' + config.NAME_DATABASE)
        dates_integrated = database.scrapeDate.unique()
        dates_integrated = [str(date)[:10] for date in dates_integrated]
        
        # Find scraped dates
        dates_scraped = os.listdir(config.PATH_SCRAPING_JOBS)
        dates_scraped = [date[:-4] for date in dates_scraped]
        
        # Get difference between integrated and scraped! NEXT UP!
        date_to_integrate = list(set(dates_scraped) - set(dates_integrated))
        print(date_to_integrate)



    
