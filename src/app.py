# src/app.py
"""
Main application class that orchestrates the entire scraping process
"""

import sys
import os
from pathlib import Path
from datetime import datetime
import json
import signal
from typing import List, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.config import ScraperConfig
from src.factory import ScraperFactory, ScraperType, MovieData
from src.database import Database
from src.metrics import MetricsCollector, MetricsDashboard
from src.utils.logger import get_logger
from src.exporters import CSVExporter, JSONExporter


class IMDBScraperApp:
    """Main application class that orchestrates the scraping process"""

    def __init__(self, config: Optional[ScraperConfig] = None):
        self.config = config or ScraperConfig.from_env()
        self.logger = get_logger('imdb_scraper.app')
        # CAMBIO: Usar la nueva clase Database que maneja PostgreSQL
        self.db = Database(self.config)  # Pasar config completo, no solo URL
        self.metrics = MetricsCollector()
        self._shutdown = False

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.warning(f"Received signal {signum}. Shutting down gracefully...")
        self._shutdown = True

    async def run(self, scraper_type: ScraperType = ScraperType.BEAUTIFULSOUP):
        """
        Main execution method

        Args:
            scraper_type: Type of scraper to use
        """
        start_time = datetime.now()
        movies_scraped = []
        scraper = None  # CAMBIO: Inicializar la variable

        try:
            self.logger.info("=" * 60)
            self.logger.info("IMDB TOP MOVIES SCRAPER - STARTING")
            self.logger.info("=" * 60)
            self.logger.info(f"Configuration: {json.dumps(self.config.to_dict(), indent=2)}")

            self.logger.info("Initializing PostgreSQL database...")
            await self.db.initialize()

            db_info = await self.db.get_database_info()
            self.logger.info(f"Connected to PostgreSQL: {db_info.get('current_database')} - {db_info.get('version')}")

            self.logger.info(f"Creating {scraper_type.name} scraper...")
            scraper = ScraperFactory.create_scraper(scraper_type, self.config)

            # Register metrics callbacks
            if hasattr(scraper, 'register_callback'):
                scraper.register_callback('on_movie_scraped', self.metrics.record_movie)
                scraper.register_callback('on_error', self.metrics.record_error)

            # Step 3: Initialize proxy manager if using proxies
            if self.config.use_proxies and hasattr(scraper, 'proxy_manager'):
                self.logger.info("Initializing proxy manager...")
                await scraper.proxy_manager.initialize()

            # Step 4: Start scraping
            self.logger.info(f"Starting to scrape {self.config.num_movies} movies...")

            # CAMBIO: Mejor manejo del context manager
            if hasattr(scraper, '__aenter__'):
                async with scraper:
                    movies_scraped = scraper.scrape(self.config.num_movies)
            else:
                movies_scraped = scraper.scrape(self.config.num_movies)

            # Check if we got any movies
            if not movies_scraped:
                self.logger.error("No movies were scraped!")
                return

            self.logger.info(f"Successfully scraped {len(movies_scraped)} movies")

            self.logger.info("Saving movies to PostgreSQL...")
            saved_count = await self._save_movies_to_db(movies_scraped)
            self.logger.info(f"Saved {saved_count} movies to database")
            
            # Step 6: Export to files
            await self._export_results(movies_scraped)

            # Step 7: Generate metrics
            metrics_data = {
                'scraper_metrics': scraper.get_metrics() if hasattr(scraper, 'get_metrics') else {},
                'app_metrics': self.metrics.get_summary(),
                'proxy_stats': scraper.proxy_manager.get_statistics() 
                              if hasattr(scraper, 'proxy_manager') and scraper.proxy_manager else None
            }

            # Step 8: Generate dashboard (si existe la clase)
            if 'MetricsDashboard' in globals():
                dashboard = MetricsDashboard(metrics_data)
                dashboard_path = await dashboard.generate()
                self.logger.info(f"Metrics dashboard generated: {dashboard_path}")

            # Step 9: Log final summary
            self._log_summary(movies_scraped, start_time)

        except Exception as e:
            self.logger.error(f"Fatal error: {str(e)}", exc_info=True)
            raise

        finally:
            try:
                if scraper is not None:
                    if hasattr(scraper, 'proxy_manager') and scraper.proxy_manager:
                        await scraper.proxy_manager.close()
                    if hasattr(scraper, 'close'):
                        await scraper.close()
                        
                await self.db.close()
                self.logger.info("Cleanup completed")
            except Exception as e:
                self.logger.error(f"Error during cleanup: {e}")

    async def _save_movies_to_db(self, movies: List[MovieData]) -> int:
        """
        Save movies to PostgreSQL database using the upsert_pelicula stored procedure
        
        Args:
            movies: List of MovieData objects
            
        Returns:
            Number of movies saved
        """
        from src.models import ScrapingSession, Pelicula, Actor, PeliculaActor, PeliculaAudit
        from sqlalchemy.dialects.postgresql import insert
        from datetime import datetime
        from sqlalchemy.dialects.postgresql import JSONB
        import uuid

        saved_count = 0

        async with self.db.get_session() as session:
            try:
                scraping_session = ScrapingSession(
                    session_id=uuid.uuid4(),
                    scraper_type=self.config.scraper_type if hasattr(self.config, 'scraper_type') else 'unknown',
                    movies_scraped=len(movies),
                    status='completed',
                    config_data=self.config.to_dict() if hasattr(self.config, 'to_dict') else {}
                )
                session.add(scraping_session)
                await session.flush()

                # Guardar películas usando el procedimiento almacenado
                for movie_data in movies:
                    try:
                        if not movie_data.title or not movie_data.year:
                            raise ValueError("Título y año son requeridos")
                        if movie_data.rating is not None and (movie_data.rating < 0 or movie_data.rating > 10):
                            raise ValueError("Calificación debe estar entre 0 y 10")
                        if movie_data.duration_minutes is not None and movie_data.duration_minutes <= 0:
                            raise ValueError("Duración debe ser mayor a 0")
                        # Insertar o actualizar película
                        pelicula_stmt = insert(Pelicula).values(
                            titulo=movie_data.title,
                            anio=movie_data.year,
                            calificacion=movie_data.rating,
                            duracion=movie_data.duration_minutes,
                            metascore=movie_data.metascore
                        ).on_conflict_do_update(
                            index_elements=['titulo', 'anio'],
                            set_={
                                'calificacion': movie_data.rating,
                                'duracion': movie_data.duration_minutes,
                                'metascore': movie_data.metascore,
                                'updated_at': datetime.utcnow()
                            }
                        ).returning(Pelicula.id)
                        result = await session.execute(pelicula_stmt)
                        pelicula_id = result.scalar_one()

                        # Registrar en auditoría
                        audit_data = {
                            'titulo': movie_data.title,
                            'anio': movie_data.year,
                            'calificacion': movie_data.rating,
                            'duracion': movie_data.duration_minutes,
                            'metascore': movie_data.metascore
                        }
                        audit_stmt = insert(PeliculaAudit).values(
                            pelicula_id=pelicula_id,
                            operation='INSERT',
                            new_data=audit_data
                        )
                        await session.execute(audit_stmt)

                        # Procesar actores
                        actores = movie_data.actors if hasattr(movie_data, 'actors') and movie_data.actors else []
                        actores_principales = [True if i < min(3, len(actores)) else False for i in range(len(actores))]
                        for actor_name, es_principal in zip(actores, actores_principales):
                            # Insertar o actualizar actor
                            actor_stmt = insert(Actor).values(
                                nombre=actor_name
                            ).on_conflict_do_update(
                                index_elements=['nombre'],
                                set_={'nombre': actor_name}
                            ).returning(Actor.id)

                            result = await session.execute(actor_stmt)
                            actor_id = result.scalar_one()

                            # Insertar relación película-actor
                            pelicula_actor_stmt = insert(PeliculaActor).values(
                                pelicula_id=pelicula_id,
                                actor_id=actor_id,
                                es_principal=es_principal
                            ).on_conflict_do_update(
                                index_elements=['pelicula_id', 'actor_id'],
                                set_={'es_principal': es_principal}
                            )
                            await session.execute(pelicula_actor_stmt)
                        saved_count += 1
                        self.logger.success(f"Película '{movie_data.title}' procesada correctamente")

                    except Exception as e:
                        self.logger.error(f"Error saving movie '{movie_data.title}': {e}")
                        self.metrics.record_error(f"Database save error for {movie_data.title}")
                        continue

                await session.commit()
                self.logger.success(f"Database transaction completed. {saved_count} movies saved.")

            except Exception as e:
                await session.rollback()
                self.logger.error(f"Error during database transaction: {e}")
                raise

        return saved_count

    async def _export_results(self, movies: List[MovieData]):
        """Export results to multiple formats"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        data_dir = getattr(self.config, 'data_dir', Path('./data'))
        data_dir = Path(data_dir)
        data_dir.mkdir(exist_ok=True)

        csv_path = data_dir / f"movies_{timestamp}.csv"
        if 'CSVExporter' in globals():
            csv_exporter = CSVExporter(csv_path)
            await csv_exporter.export(movies)
            self.logger.info(f"Exported to CSV: {csv_path}")

        json_path = data_dir / f"movies_{timestamp}.json"
        if 'JSONExporter' in globals():
            json_exporter = JSONExporter(json_path)
            await json_exporter.export(movies)
            self.logger.info(f"Exported to JSON: {json_path}")
        else:
            movies_dict = [movie.to_dict() for movie in movies]
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(movies_dict, f, indent=2, ensure_ascii=False, default=str)
            self.logger.info(f"Exported to JSON: {json_path}")

    def _log_summary(self, movies: List[MovieData], start_time: datetime):
        """Log comprehensive summary of the scraping session"""
        duration = (datetime.now() - start_time).total_seconds()

        avg_quality = sum(m.quality_score for m in movies) / len(movies) if movies else 0
        complete_movies = sum(1 for m in movies if m.quality_score == 1.0)
        movies_with_metascore = sum(1 for m in movies if m.metascore is not None)

        self.logger.info("=" * 60)
        self.logger.info("SCRAPING SESSION SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"Total Duration: {duration:.2f} seconds ({duration/60:.1f} minutes)")
        self.logger.info(f"Movies Scraped: {len(movies)}/{self.config.num_movies}")
        self.logger.info(f"Success Rate: {len(movies)/self.config.num_movies*100:.1f}%")
        self.logger.info(f"Average Quality Score: {avg_quality:.1%}")
        self.logger.info(f"Complete Data: {complete_movies}/{len(movies)}")
        self.logger.info(f"Movies with Metascore: {movies_with_metascore}")
        self.logger.info(f"Average Time per Movie: {duration/len(movies):.2f}s" if movies else "N/A")
        
        if movies:
            self.logger.info("Top 5 Movies by Rating:")
            top_movies = sorted(movies, key=lambda m: m.rating or 0, reverse=True)[:5]
            for i, movie in enumerate(top_movies, 1):
                self.logger.info(f"  {i}. {movie.title} ({movie.year}) - {movie.rating or 'N/A'}/10")
                
        self.logger.info("=" * 60)

    async def health_check(self):
        """Check application health status"""
        try:
            # Check database connection
            db_healthy = await self.db.health_check()
            
            return {
                "status": "healthy" if db_healthy else "unhealthy",
                "database": "connected" if db_healthy else "disconnected",
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }