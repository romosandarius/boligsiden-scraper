import os
import pandas as pd
import datetime


class Integrator(object):

    def __init__(self, date, backfill=False):
        self.date = date
        self.backfill = backfill

    def integrate(self):
        
        # if backfill false: integrate latest scraped data
        # if backfill true: integrate all scraped data

        
        df, df_yd = self._get_two_latest_scrapings() # return todays df + yesterdays df
        # df_off_market_new = self._return_off_market_estates(df_date, df_yesterday)
        # df_off_market = pd.read_pickle('./data/off-market.pkl')
        # df_off_market = pd.concat([df_off_market, df_off_market_new])
        # df_date.to_pickle('./data/on-market.pkl')
        # df_off_market.to_pickle('./data/off-market.pkl')
        

    def _get_two_latest_scrapes(self):
        # Return boligsiden data from a given date and the day before.
        print(f'Integrating: {self.date}')
        yesterday = self.date + datetime.timedelta(days=-1)
        path_date = f'./data/boligsiden/{self.date}.pkl'
        path_yesterday = f'./data/boligsiden/{yesterday}.pkl'
        assert os.path.isfile(path_date), f'{path_date} does not exist'
        assert os.path.isfile(path_yesterday), f'{path_yesterday} does not exist'
        df_date = pd.read_pickle(path_date)
        df_yesterday = pd.read_pickle(path_yesterday)
        return df_date, df_yesterday


    def _return_off_market_estates(self, df_date, df_yesterday):
        # Return dataframe with estates that is taken off the market
        df_yesterday['fullAddress'] = df_yesterday['address'] + ' ' + df_yesterday['city']
        df_date['fullAddress'] = df_date['address'] + ' ' + df_date['city']        
        mask = ~df_yesterday.fullAddress.isin(df_date.fullAddress)
        df_off_market = df_yesterday[mask].copy()
        df_off_market['offMarketDate'] = self.date
        df_off_market['offMarketDate'] = pd.to_datetime(df_off_market['offMarketDate'])
        df_off_market['liggetid'] = df_off_market['dateAnnounced'].apply(lambda x: (self.date.today() - x.date()).days)
        df_off_market['saleConfirmed'] = False
    
        df_off_market = df_off_market.drop('fullAddress', axis=1)
        
        return df_off_market

    
