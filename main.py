from scraper import BoligScraper
from preprocessor import Preprocessor
from integrator import Integrator

#~~~~~~~~~~~~~~#
#   Pipeline   #
#~~~~~~~~~~~~~~#

# ## scrape
# scraper = BoligScraper()
# df_today = scraper.scrape()

# ## preprocess
# preprocessor = Preprocessor()   
# df_on_market = preprocessor.process(df_on_market)

import pandas as pd
df_on_market = pd.read_pickle('./data/boligsiden_2020-08-01.pkl')
df_on_market.to_pickle('./data/on-market.pkl')

## integrate data
integrator = Integrator()
integrator.integrate(df_on_market)

# 1. update off-market df
# 2. update on-market df


