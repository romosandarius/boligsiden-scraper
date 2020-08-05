import datetime
from scraper import BoligScraper
from integrator import Integrator

#~~~~~~~~~~~~~~#
#   Pipeline   #
#~~~~~~~~~~~~~~#

## scrape
# scraper = BoligScraper()
# df = scraper.scrape_listings()
 
## integrate data
integrator = Integrator()
integrator.integrate()



# TO-DO
# - Clean scrapeDate column (tjek)
# - Remove offMarket column (tjek)
# - Fix _detect_off_market_listings without using fullAddress colum
# - Remove fullAddress column 
# - Implement integrate_all method
# - Clean columns (preprocessor)
