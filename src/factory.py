# src/factory.py
"""
Scraper factory implementation
"""

from enum import Enum
from typing import Protocol, Optional
from dataclasses import dataclass
from datetime import datetime
from src.config import ScraperConfig

class ScraperType(Enum):
    BEAUTIFULSOUP = "beautifulsoup"
    SCRAPY = "scrapy"

@dataclass
class MovieData:
    title: str
    year: int
    rating: float
    duration_minutes: Optional[int]
    metascore: Optional[int]
    actors: list[str]
    imdb_id: str
    quality_score: float
    scraped_at: datetime
    
    def to_dict(self) -> dict:
        return {
            "title": self.title,
            "year": self.year,
            "rating": self.rating,
            "duration_minutes": self.duration_minutes,
            "metascore": self.metascore,
            "actors": self.actors,
            "imdb_id": self.imdb_id,
            "quality_score": self.quality_score,
            "scraped_at": self.scraped_at.isoformat()
        }

class Scraper(Protocol):
    def scrape(self, num_movies: int) -> list[MovieData]:
        ...
        
    def get_scraper_info(self) -> dict:
        ...
        
    def get_metrics(self) -> dict:
        ...

class ScraperFactory:
    @staticmethod
    def create_scraper(scraper_type: ScraperType, config: ScraperConfig) -> Scraper:
        if scraper_type == ScraperType.BEAUTIFULSOUP:
            from src.scrapers.bs4_scraper import BeautifulSoupScraper
            return BeautifulSoupScraper(config)
        elif scraper_type == ScraperType.SCRAPY:
            from src.scrapers.scrapy_scraper import ScrapyScraper
            return ScrapyScraper(config)
        raise ValueError(f"Unknown scraper type: {scraper_type}")