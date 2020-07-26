import requests
import json
import pandas as pd
from bs4 import BeautifulSoup


class BoligsidenScraper(object):

    def __init__(self):
        pass

    def scrape(self):
        """ Scrapes boligsiden.dk for all estates on the site. """
        
        dataframes = []
        base_url = 'https://www.boligsiden.dk/resultat/1f923c02d4bf4c0ca6b0e7320ee8daee?s=12&sd=false&d=1&p={}&i=5000'
        pages = list(range(1,20))
        
        for page in pages:
            print(f'Scraping page: {page}')

            # Get response
            url = base_url.format(page)
            html = requests.get(url).content

            # Soup it!
            soup = BeautifulSoup(html, 'html.parser')
            scripts = soup.find_all('script')

            # Find script tag w. json data
            for s in scripts:
                if '__bs_propertylist_result__ ' in str(s):
                    script = str(s)

            # Locate JSON string (find better solution)
            script = script[45:]
            script = script[:-16]

            # Convert to json
            data =json.loads(script)['result']['properties']
            
            # Convert to Pandas Dataframe
            df  = pd.DataFrame(data)
            dataframes.append(df)
        

        

        return None

    
scraper = BoligsidenScraper()
scraper.scrape()