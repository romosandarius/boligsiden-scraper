import datetime
from scraper import BoligScraper
from integrator import Integrator

#~~~~~~~~~~~~~~#
#   Pipeline   #
#~~~~~~~~~~~~~~#

## scrape
scraper = BoligScraper()
df = scraper.scrape_listings()


## integrate data
integrator = Integrator(datetime.date.today())
integrator.integrate()

