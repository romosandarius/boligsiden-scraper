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
integrator = Integrator()
integrator.integrate()



# TO-DO 
# - dateLastSeen (instead of dateDetected), should be last df (tjek)
# - refactor integrator. Add _append_to_or_init_db() method
# - Include automatic backup of scraping job when ever a job is done
# - Clean columns (preprocessor)
