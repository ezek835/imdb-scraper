# src/scrapers/scrapy_scraper.py
from src.factory import Scraper
from src.config import ScraperConfig
from src.utils.logger import get_logger


class ScrapyScraper(Scraper):
    """Scraper -- Solo para la parte de factory"""
    
    def __init__(self, config: ScraperConfig):        
        self.config = config
        self.logger = get_logger(self.__class__.__name__)

