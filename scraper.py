import os
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date
import regex
import config

class BoligScraper(object):
    
    def __init__(self, num_listings_per_page=5000, save_df=True):
        self.num_listings_per_page = num_listings_per_page
        self.base_url = 'https://www.boligsiden.dk/resultat/1f923c02d4bf4c0ca6b0e7320ee8daee?s=12&sd=false&d=1&p={}&i={}' # will this url always work?
        self.save_df = save_df
        if not os.path.isdir('./data'): os.mkdir('./data')
        if not os.path.isdir('./data/database'): os.mkdir('./data/database')
        if not os.path.isdir('./data/scraping_jobs'): os.mkdir('./data/scraping_jobs')


    def scrape_listings(self):
        print('Scraping boligsiden.dk ..') 
        self._collect_listed_items() 
        self._add_timestamp_column()
        self._drop_unused_columns()
        self._drop_duplicates()
        if self.save_df: self._df_to_pickle()        
        print('Scraping finished!\n')            
        return self.df
        

    def _collect_listed_items(self):
        dfs = []
        for i in range(10000):
            print(f'Fetching page {i+1}')
            url = self.base_url.format(i, self.num_listings_per_page) 
            df = self._get_listing_page_df(url)
            if df.empty: break
            else: dfs.append(df)      
        
        self.df = pd.concat(dfs)
                    

    def _get_listing_page_df(self, url):
        html = requests.get(url).content
        soup = BeautifulSoup(html, 'html.parser')
        scripts = soup.find_all('script')
        for s in scripts:
            if '__bs_propertylist_result__ ' in str(s):
                script = str(s)
        pattern = regex.compile(r'\{(?:[^{}]|(?R))*\}') 
        json_string = pattern.findall(script)[0]            
        data =json.loads(json_string)['result']['properties'] 
        df = pd.DataFrame(data) 
                
        return df
        

    def _add_timestamp_column(self):
        self.df['dateScraped'] = date.today()
        self.df['dateScraped'] = pd.to_datetime(self.df['dateScraped'])

    def _drop_unused_columns(self):
        self.df = self.df.drop(columns=['isFavorite', 'isArchive', 'hasOpenHouse', 
                                        'nextOpenHouse', 'nextOpenHouseSignup',
                                        'energyMarkLink', 'openHouseRedirectLink', 
                                        'agentsLogoLink', 'financing', 'calculateLoanAgentChain'])

    def _drop_duplicates(self):
        columns = list(self.df.columns)
        columns.remove('rating')
        self.df = self.df.drop_duplicates(subset=columns)

    def _df_to_pickle(self):
        self.df.to_pickle(f'./data/{config.DIR_SCRAPING_JOBS}/{date.today()}.pkl')
