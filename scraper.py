import json
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date

class BoligScraper(object):
    r"""Class for scraping www.boligsiden.dk"""
    
    def __init__(self, num_listings_per_page=5000):
        self.num_listings_per_page = num_listings_per_page
        self.base_url = 'https://www.boligsiden.dk/resultat/1f923c02d4bf4c0ca6b0e7320ee8daee?s=12&sd=false&d=1&p={}&i={}'
    
    def scrape(self):
        print('Scraping..')      
        dfs = []
        for i in range(10000):
            print(f'Scraping page {i+1}')
            # Get url
            url = self.base_url.format(i, self.num_listings_per_page)
            df = self._get_listing_page_df(url)
            if df.empty:
                break
            else:
                dfs.append(df)      
        self.df = pd.concat(dfs)
        self.df['scrapeDate'] = date.today()
        self.df['scrapeDate'] = pd.to_datetime(self.df['scrapeDate'])
        self.df['dateAnnounced'] = pd.to_datetime(self.df['dateAnnounced'])
        self.df['liggetid'] = self.df['dateAnnounced'].apply(lambda x: (date.today() - x.date()).days) # Add liggetid column
        self._save_df()        
        print('Scraping finished!')            
        return self.df

    def _get_listing_page_df(self, url):
        
        # Get all script tags
        html = requests.get(url).content
        soup = BeautifulSoup(html, 'html.parser')
        scripts = soup.find_all('script')
        
        # Find script tag with json string
        for s in scripts:
            if '__bs_propertylist_result__ ' in str(s):
                script = str(s)

        # Locate JSON string (find better solution)
        script = script[45:]
        script = script[:-16]
        
        # Get df
        data =json.loads(script)['result']['properties'] # convert string to json
        df = pd.DataFrame(data) # convert to pandas df
                
        return df
        

    def _save_df(self):
        self.df.to_pickle(f'./data/boligsiden/{date.today()}.pkl')


    def _clean_cols(self):
        cols = ['paymentCash', 'downPayment', 'paymentExpenses', 'paymentGross','paymentNet', 'areaResidential', 
                'numberOfRooms', 'areaParcel', 'salesPeriod', 'areaPaymentCash', 'areaWeighted', 'salesPeriodTotal']
        def if_dash(x):
            if not x.isnumeric():
                x = 0
            return x
        for col in cols:
            self.df[col] = self.df[col].apply(lambda x: x.replace('.', ''))
            self.df[col] = self.df[col].apply(lambda x: if_dash(x))
            self.df[col] = self.df[col].astype(float)
        
    