import os
import pandas as pd
import datetime

class Integrator(object):

    def integrate(self, date=None):
        df_date, df_yesterday = self._get_dfs(date)
        df_off_market_new = self._return_off_market_estates(df_date, df_yesterday)
        df_on_market_new = df_date

        # Next time: update on + off market df

        

        return df_on_market, df_off_market

    def _get_dfs(self, date):
        if date == None:
            date = datetime.date.today() 
        print(f'Date: {date}')
        yesterday = date + datetime.timedelta(days=-1)
        path_date = f'./data/boligsiden/{date}.pkl'
        path_yesterday = f'./data/boligsiden/{yesterday}.pkl'
        assert os.path.isfile(path_date), f'{path_date} does not exist'
        assert os.path.isfile(path_yesterday), f'{path_yesterday} does not exist'
        df_date = pd.read_pickle(path_date)
        df_yesterday = pd.read_pickle(path_yesterday)
        return df_date, df_yesterday


    def _return_off_market_estates(self, df_date, df_yesterday):
        
        # Create full address col, to compare dfs
        df_yesterday['fullAddress'] = df_yesterday['address'] + ' ' + df_yesterday['city']
        df_date['fullAddress'] = df_date['address'] + ' ' + df_date['city']
        
        # Identify off market estates
        mask = ~df_yesterday.fullAddress.isin(df_date.fullAddress)
        df_off_market = df_yesterday[mask].copy()
        
        # Add offMarketDate column
        df_off_market['offMarketDate'] = datetime.date.today().strftime('%d-%m-%Y')
        
        # Add saleConfirmed column (for later use)
        df_off_market['saleConfirmed'] = False
        
        # Drop fullAddress column
        df_off_market = df_off_market.drop('fullAddress', axis=1)
        
        return df_off_market

    
