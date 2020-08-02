import datetime
from scraper import BoligScraper
from preprocessor import Preprocessor
from integrator import Integrator

#~~~~~~~~~~~~~~#
#   Pipeline   #
#~~~~~~~~~~~~~~#

# ## scrape
# scraper = BoligScraper()
# df = scraper.scrape()

# ## preprocess
# preprocessor = Preprocessor()   
# df = preprocessor.process()

# import pandas as pd
# df_on_market = pd.read_pickle('./data/boligsiden_2020-08-01.pkl')
# df_on_market.to_pickle('./data/on-market.pkl')

## integrate data
integrator = Integrator()
integrator.integrate()

# 1. update off-market df
# 2. update on-market df


