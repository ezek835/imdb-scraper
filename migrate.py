# create_tables.py
import asyncio
from pathlib import Path
from src.config import ScraperConfig
from src.database import Database
from src.utils.logger import setup_logger, get_logger

async def main():
    setup_logger(name="imdb_scraper", level="DEBUG", log_dir=Path("logs"))
    logger = get_logger("imdb_scraper.create_tables")
    
    try:
        config = ScraperConfig.from_env()
        db = Database(config)
        
        logger.info("Initializing database...")
        await db.initialize()
        
        logger.info("Creating tables...")
        await db.create_tables()
        
        logger.info("Tables created successfully!")
        
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())