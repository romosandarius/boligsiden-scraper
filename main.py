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

## integrate data
integrator = Integrator(date=datetime.date.today() + datetime.timedelta(days=-1))
integrator.integrate()

#print(datetime.date.today() + datetime.timedelta(days=-3))


