import pandas as pd
from scraper import BoligScraper
from integrator import Integrator
from preprocessor import Preprocessor

#~~~~~~~~~~~~~~#
#   Pipeline   #
#~~~~~~~~~~~~~~#

## scrape
scraper = BoligScraper()
df = scraper.scrape_listings()
 
## integrate data
integrator = Integrator()
integrator.integrate()

# ## Load dfs from db
# df_off = pd.read_pickle('./data/database/db_off_market.pkl')
# df_new = pd.read_pickle('./data/database/db_new_on_market.pkl')
# df_on = pd.read_pickle('./data/database/db_on_market.pkl')

# # Preprocess dataframes
# preprocessor = Preprocessor()
# df_off = preprocessor.process(df_off)
# df_new = preprocessor.process(df_new)
# df_on = preprocessor.process(df_on)
