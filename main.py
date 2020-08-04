import datetime
from scraper import BoligScraper
from preprocessor import Preprocessor
from integrator import Integrator

#~~~~~~~~~~~~~~#
#   Pipeline   #
#~~~~~~~~~~~~~~#

## scrape
# scraper = BoligScraper()
# df = scraper.scrape_listings()


## integrate data
integrator = Integrator(datetime.date.today())
integrator.integrate()


# ## preprocess
# preprocessor = Preprocessor() 
# df = preprocessor.process(df)



## To do:
# 1. Integrator
# 2. Preprocessor
# 3. Research possibilities for automatically running scrape daily 
# .