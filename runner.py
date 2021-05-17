from helpers.scraper import BoligScraper
import logging


logger = logging.getLogger(__file__)
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


if __name__ == "__main__":
    scraper = BoligScraper()
    df = scraper.scrape()

    print(len(df.index))