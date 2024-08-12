import os
from scraper import SobaScraper

if __name__ == "__main__":
    ss = SobaScraper(os.path.dirname(__file__) + '\\config.json', 10)
    ss.scrape_all()