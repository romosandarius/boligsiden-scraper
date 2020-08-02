import os
import pandas as pd
from datetime import datetime

class Integrator(object):
    
    def integrate(self, df_on_market):

        if os.path.isfile('./data/off-market.pkl'):
            df_off_market = pd.read_pickle('./data/off-market.pkl')
        else:
            df_off_market = self._return_off_market_estates(df_on_market)
            df_off_market.to_pickle('./data/off-market.pkl')
            


        return

    def _return_off_market_estates(df_on_market):
        

        # Create full address col, to compare dfs
        df_db['fullAddress'] = df_db['address'] + ' ' + df_db['city']
        df_on_market['fullAddress'] = df_on_market['address'] + ' ' + df_on_market['city']
        
        # Identify off market estates
        mask = ~df_db.fullAddress.isin(df_on_market.fullAddress)
        df_off_market = df_db[mask].copy()
        
        # Add offMarketDate column
        df_off_market['offMarketDate'] = datetime.date.today().strftime('%d-%m-%Y')
        
        # Add saleConfirmed column (for later use)
        df['saleConfirmed'] = False
        
        # Drop fullAddress column
        df_off_market = df_off_market.drop('fullAddress', axis=1)
        
        return df_off_market

    
