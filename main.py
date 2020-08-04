import datetime
from scraper import BoligScraper
from preprocessor import Preprocessor
from integrator import Integrator

#~~~~~~~~~~~~~~#
#   Pipeline   #
#~~~~~~~~~~~~~~#

## scrape
scraper = BoligScraper()
df = scraper.scrape()

# ## preprocess
# preprocessor = Preprocessor() 
# df = preprocessor.process(df)

# ## integrate data
# integrator = Integrator(datetime.date.today())
# integrator.integrate()


## To do:
# 1. Refactor
# 2. clean_cols not converting to float. Fix!
# 3. Research possibilities for automatically running scrape daily 
# 4.