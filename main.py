# main.py
"""
Main entry point for IMDB Top Movies Scraper
Executes the configured scraper and orchestrates the full flow
"""

import argparse
import asyncio
from pathlib import Path
from src.config import ScraperConfig
from src.factory import ScraperType
from src.app import IMDBScraperApp
from src.utils.logger import setup_logger

if __name__ == "__main__":
    setup_logger(
        name="imdb_scraper",  # Nombre raíz
        level="DEBUG",
        log_dir=Path("logs"),
    )
    parser = argparse.ArgumentParser(description='IMDB Top Movies Scraper')
    parser.add_argument('--scraper', choices=['beautifulsoup', 'scrapy'], default='beautifulsoup', help='Scraper type')
    parser.add_argument('--movies', type=int, default=50, help='Number of movies to scrape (default: 50)')
    parser.add_argument('--no-proxy', action='store_true', help='Disable proxy usage')

    args = parser.parse_args()

    config = ScraperConfig(num_movies=args.movies, use_proxies=not args.no_proxy)

    scraper_type = ScraperType[args.scraper.upper()]
    app = IMDBScraperApp(config)
    asyncio.run(app.run(scraper_type))
