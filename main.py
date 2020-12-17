import pandas as pd
from scraper import BoligScraper
from preprocessor import Preprocessor

#~~~~~~~~~~~~~~#
#   Pipeline   #
#~~~~~~~~~~~~~~#

# scrape
scraper = BoligScraper()
df = scraper.scrape_listings()

# Preprocess dataframes
preprocessor = Preprocessor()
df = preprocessor.process(df)
