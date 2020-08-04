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

        #self.SCRAPING_JOBS = sorted([file[:-4] for file in self.SCRAPING_JOBS]) # remove .pkl extension + sort
        
        
    
    def _check_conditions(self):
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
        elif (~self.DATABASE_EXISTS and self.ONE_SCRAPING_JOB):
            print('CASE 5. ~DATABASE_EXISTS + ONE_SCRAPING_JOB        -----> _create_initial_database()')
        elif (~self.DATABASE_EXISTS and self.MULTIPLE_SCRAPING_JOBS):
            print('CASE 6. ~DATABASE_EXISTS + MULTIPLE_SCRAPING_JOBS  -----> _integrate_all()')

        # CASES:
        # 1. DATABASE_EXISTS + NO_SCRAPING_JOBS         -----> exit()
        # 2. DATABASE_EXISTS + ONE_SCRAPING_JOB         -----> exit()
        # 3. DATABASE_EXISTS + MULTIPLE_SCRAPING_JOBS   -----> _integrate_latest()
        # 4. ~DATABASE_EXISTS + NO_SCRAPING_JOBS        -----> exit()
        # 5. ~DATABASE_EXISTS + ONE_SCRAPING_JOB        -----> _create_initial_database()
        # 6. ~DATABASE_EXISTS + MULTIPLE_SCRAPING_JOBS  -----> _integrate_all()


    # def _get_two_latest_scrapes(self):
    #     # Return boligsiden data from a given date and the day before.
    #     print(f'Integrating: {self.date}')
    #     yesterday = self.date + datetime.timedelta(days=-1)
    #     path_date = f'./data/boligsiden/{self.date}.pkl'
    #     path_yesterday = f'./data/boligsiden/{yesterday}.pkl'
    #     assert os.path.isfile(path_date), f'{path_date} does not exist'
    #     assert os.path.isfile(path_yesterday), f'{path_yesterday} does not exist'
    #     df_date = pd.read_pickle(path_date)
    #     df_yesterday = pd.read_pickle(path_yesterday)
    #     return df_date, df_yesterday


    # def _return_off_market_estates(self, df_date, df_yesterday):
    #     # Return dataframe with estates that is taken off the market
    #     df_yesterday['fullAddress'] = df_yesterday['address'] + ' ' + df_yesterday['city']
    #     df_date['fullAddress'] = df_date['address'] + ' ' + df_date['city']        
    #     mask = ~df_yesterday.fullAddress.isin(df_date.fullAddress)
    #     df_off_market = df_yesterday[mask].copy()
    #     df_off_market['offMarketDate'] = self.date
    #     df_off_market['offMarketDate'] = pd.to_datetime(df_off_market['offMarketDate'])
    #     df_off_market['liggetid'] = df_off_market['dateAnnounced'].apply(lambda x: (self.date.today() - x.date()).days)
    #     df_off_market['saleConfirmed'] = False
    
    #     df_off_market = df_off_market.drop('fullAddress', axis=1)
        
    #     return df_off_market

    
