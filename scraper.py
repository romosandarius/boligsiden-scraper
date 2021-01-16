import json
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date
import regex

class BoligScraper(object):
    
    def __init__(self, num_listings_per_page=1000):
        self.num_listings_per_page = num_listings_per_page
        self.base_url = 'https://www.boligsiden.dk/resultat/976e8dd6ca274cd0b18ac7ed7fbe4703?s=12&sd=false&d=1&p={}&i={}' 

    def scrape_listings(self):
        print('Scraping boligsiden.dk ..') 
        self._collect_listed_items() 
        self._add_timestamp_column()
        self._drop_unused_columns()
        self._drop_duplicates()
        self._clean_columns()
        self.df.to_pickle(f'boliger_{date.today()}.pkl')  
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

    def _clean_columns(self):
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


if __name__ == "__main__":
    scraper = BoligScraper()
    df = scraper.scrape_listings()
