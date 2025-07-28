# src/scrapers/bs4_scraper.py
import requests
import time
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Optional, Callable, Dict
import random
import re
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.config import ScraperConfig
from src.factory import MovieData, Scraper
from src.utils.cookie_manager import CookieManager
from src.utils.validators import MovieDataValidator
from src.utils.logger import get_logger
import re

class BeautifulSoupScraper(Scraper):
    BASE_URL = "https://www.imdb.com"
    TOP_MOVIES_URL = f"{BASE_URL}/chart/top/"
    
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.imdb.com/",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Cache-Control": "max-age=0"
    }

    def __init__(self, config: ScraperConfig):
        self.config = config
        self.cookie_manager = CookieManager()
        self.logger = get_logger('imdb_scraper.app')
        self.callbacks: Dict[str, List[Callable]] = {"on_movie_scraped": [], "on_error": []}
        self.movies_scraped = 0
        self.errors_count = 0
        if config.use_proxies:
            from src.utils.proxy_manager import ProxyManager
            self.proxy_manager = ProxyManager(config)
        else:
            self.proxy_manager = None


    def register_callback(self, name: str, callback: Callable):
        """Registrar callback para eventos"""
        if name in self.callbacks:
            self.callbacks[name].append(callback)


    def scrape(self, num_movies: int) -> List[MovieData]:
        """Scraper principal"""
        try:
            self.logger.info(f"Iniciando scraping de {num_movies} películas de IMDb...")
            
            # Lista de géneros a consultar
            genres = ['drama', 'adventure', 'thriller', 'crime']
            all_movies = []

            # Paso 1: Obtener página principal con reintentos
            for genre in genres:
                url = f"{self.TOP_MOVIES_URL}?genres={genre}"
                html = self._fetch_with_retry(url)
                if not html:
                    self.logger.error("No se pudo obtener la página principal")
                    continue
                soup = BeautifulSoup(html, 'html.parser')
                movies_containers = soup.select('li.ipc-metadata-list-summary-item')
                self.logger.info(f"Encontrados {len(movies_containers)} contenedores de películas para {genre}")
                if not movies_containers:
                    self.logger.error("No se encontraron contenedores de películas. HTML estructura cambió.")
                    self.logger.debug(f"HTML preview: {str(soup)[:1000]}...")
                    continue
                all_movies.extend(movies_containers[:num_movies // len(genres) + 5])
            
            movies = []
            for i, container in enumerate(all_movies, 1):
                time.sleep(random.uniform(1.5, 3.0))
                
                try:
                    self.logger.info(f"Procesando película {i}/{len(movies_containers)}...")
                    movie = self._parse_movie_container(container)
                    
                    if movie:
                        movies.append(movie)
                        self.movies_scraped += 1
                        self._emit("on_movie_scraped", movie)
                        self.logger.info(f"{movie.title} ({movie.year}) - Rating: {movie.rating}")
                    else:
                        self.logger.warning(f"No se pudo procesar película {i}")
                        
                except Exception as e:
                    self.errors_count += 1
                    self.logger.error(f"Error procesando película {i}: {e}")
                    self._emit("on_error", e)

            self.logger.info(f"Scraping completado: {len(movies)} películas exitosas")
            return movies
        except Exception as e:
            self.logger.error(f"Error fatal en scraping: {e}", exc_info=True)
            self._emit("on_error", e)
            return []

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type((requests.RequestException, requests.Timeout))
    )
    def _fetch_with_retry(self, url: str) -> Optional[str]:
        """Fetch URL con reintentos y manejo de errores"""
        try:
            self.logger.debug(f"Fetching: {url}")
            session = self.cookie_manager.get_session()
            proxies = None
            if self.config.use_proxies and self.proxy_manager:
                proxies = self.proxy_manager.get_proxy()

            response = session.get(
                url,
                headers=self.HEADERS,
                timeout=30,
                proxies=proxies
            )

            self.cookie_manager.update_cookies_from_response(response)

            if response.status_code == 403:
                self.logger.warning(f"Acceso prohibido (403) para: {url}")
                raise Exception("Access forbidden - might need proxy")
            elif response.status_code == 429:
                self.logger.warning(f"Rate limited (429) para: {url}")
                time.sleep(random.uniform(5, 10))
                raise Exception("Rate limited")
            elif response.status_code != 200:
                self.logger.error(f"HTTP {response.status_code} para: {url}")
                return None

            self.logger.debug(f"Página obtenida: {len(response.text)} caracteres")
            return response.text                

        except Exception as e:
            self.logger.warning(f"Error en fetch {url}: {e}")
            raise

    def _parse_movie_container(self, container) -> Optional[MovieData]:
        """Parsear contenedor de película con selectores actualizados"""
        try:
            title_element = (
                container.select_one('h3.ipc-title__text') or
                container.select_one('.cli-title') or 
                container.select_one('.titleColumn h3 a') or
                container.select_one('a.ipc-title-link-wrapper h3')
            )
            
            if not title_element:
                self.logger.warning("No se encontró título")
                return None
                
            title_text = title_element.get_text().strip()
            # Remover numeración
            title = re.sub(r'^\d+\.\s*', '', title_text)

            # YEAR
            year = self._extract_year(container)
            
            # RATING  
            rating = self._extract_rating(container)
            
            # MOVIE LINK - Para obtener detalles
            link_element = (
                container.select_one('a.ipc-title-link-wrapper') or
                container.select_one('.cli-title-link') or
                container.select_one('.titleColumn a')
            )
            
            if not link_element:
                self.logger.warning(f"No se encontró link para: {title}")
                return None
                
            relative_url = link_element.get('href')
            imdb_id = self._extract_imdb_id(relative_url)
            
            if not imdb_id:
                self.logger.warning(f"No se pudo extraer IMDB ID de: {relative_url}")
                return None

            # Obtener detalles de la página individual
            detail_url = f"{self.BASE_URL}{relative_url}"
            duration, metascore, actors = self._get_movie_details(detail_url)

            raw_data = {
                'title': title,
                'year': year,
                'rating': rating,
                'duration_minutes': duration,
                'metascore': metascore,
                'actors': actors,
                'imdb_id': imdb_id
            }

            validated = MovieDataValidator.validate_movie_data(raw_data)
            if not MovieDataValidator.is_valid_movie(validated):
                self.logger.warning(f"❌ Datos inválidos para película: {title}")
                return None

            return MovieData(
                title=validated['title'],
                year=validated['year'] or 0,
                rating=validated['rating'] or 0.0,
                duration_minutes=validated['duration_minutes'],
                metascore=validated['metascore'],
                actors=validated['actors'],
                imdb_id=validated['imdb_id'],
                quality_score=self._calculate_quality_score(
                    validated['rating'], 
                    validated['duration_minutes'], 
                    validated['metascore'], 
                    validated['actors']
                ),
                scraped_at=datetime.utcnow()
            )

        except Exception as e:
            self.logger.error(f"Error parseando contenedor: {e}", exc_info=True)
            return None

    def _extract_year(self, container) -> Optional[int]:
        """Extraer año con múltiples selectores"""
        try:
            year_selectors = [
                'span.cli-title-metadata-item',
                '.secondaryInfo',
                '[data-testid="title-metadata"] span',
                '.titleColumn .secondaryInfo'
            ]

            for selector in year_selectors:
                year_element = container.select_one(selector)
                if year_element:
                    year_text = year_element.get_text().strip('() ')
                    match = re.search(r'(\d{4})', year_text)
                    if match:
                        return int(match.group(1))

            return None
        except (AttributeError, ValueError, TypeError) as e:
            self.logger.warning(f"Error extrayendo año: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error inesperado extrayendo año: {e}")
            return None


    def _extract_rating(self, container) -> Optional[float]:
        """Extraer rating con múltiples selectores"""
        rating_selectors = [
            'span.ipc-rating-star--rating',
            '.ratingColumn strong',
            '[data-testid="rating"] span',
            '.imdbRating strong'
        ]

        for selector in rating_selectors:
            rating_element = container.select_one(selector)
            if rating_element:
                rating_text = rating_element.get_text().strip()
                try:
                    return float(rating_text)
                except ValueError:
                    continue
        
        return None

    def _extract_imdb_id(self, relative_url: str) -> Optional[str]:
        """Extraer IMDB ID de URL"""
        if not relative_url:
            return None
            
        match = re.search(r'/title/(tt\d+)/', relative_url)
        return match.group(1) if match else None

    def _get_movie_details(self, detail_url: str) -> tuple:
        """Obtener detalles adicionales de la página de película"""
        try:
            detail_html = self._fetch_with_retry(detail_url)
            if not detail_html:
                return None, None, []

            soup = BeautifulSoup(detail_html, 'html.parser')
            
            duration = self._parse_duration(soup)
            
            metascore = self._parse_metascore(soup)
            
            # ACTORS - Top 3 actores principales
            actors = self._parse_actors(soup)

            return duration, metascore, actors

        except Exception as e:
            self.logger.warning(f"Error obteniendo detalles de {detail_url}: {e}")
            return None, None, []

    def _parse_duration(self, soup) -> Optional[int]:
        """Parsear duración con selectores actualizados"""
        duration_selectors = [
            'h1 ~ ul[role="presentation"].ipc-inline-list.ipc-inline-list--show-dividers',
            'h1 + ul[role="presentation"].ipc-inline-list.ipc-inline-list--show-dividers',
            'h1 + div + ul[role="presentation"].ipc-inline-list',
        ]

        duration_text = None
        for selector in duration_selectors:
            duration_elements = soup.select(selector)
            if duration_elements:
                for element in duration_elements:
                    li_items = element.find_all('li')
                    if len(li_items) >= 3:
                        duration_li = li_items[2].get_text(strip=True)
                        if any(x in duration_li for x in ['h', 'min', 'm']):
                            duration_text = duration_li
                if duration_text:
                    break
        if not duration_text:
            return None
    
        try:
            hours_match = re.search(r'(\d+)\s*h(?:oras|rs|r|)?', duration_text, re.IGNORECASE)
            minutes_match = re.search(r'(\d+)\s*m(?:inutos|in|)?', duration_text, re.IGNORECASE)
            hours = int(hours_match.group(1)) if hours_match else 0
            minutes = int(minutes_match.group(1)) if minutes_match else 0
            return hours * 60 + minutes
        except (ValueError, AttributeError):
            self.logger.warning(f"No se pudo parsear duración: {duration_text}")
            return None

    def _parse_metascore(self, soup) -> Optional[int]:
        """Parsear Metascore con selectores actualizados"""
        metascore_selectors = [
            '[data-testid="metacritic-score-box"] span',
            '.metacritic-score-box',
            '.score-meta',
            'span.metacritic-score'
        ]

        for selector in metascore_selectors:
            meta_element = soup.select_one(selector)
            if meta_element:
                meta_text = meta_element.get_text().strip()
                match = re.search(r'(\d+)', meta_text)
                if match:
                    return int(match.group(1))
        
        return None

    def _parse_actors(self, soup) -> List[str]:
        """Parsear actores principales con selectores actualizados"""
        actors = []

        actor_selectors = [
            '[data-testid="title-cast-item__actor"]',
            '[data-testid="cast-item-characters-link"]',
            '.cast_list .primary_photo + td a',
            '.cast .actor a'
        ]
        
        for selector in actor_selectors:
            actor_elements = soup.select(selector)[:3]  # Top 3
            if actor_elements:
                for element in actor_elements:
                    actor_name = element.get_text().strip()
                    if actor_name and actor_name not in actors:
                        actors.append(actor_name)
                break
        
        return actors[:3]  # Máximo 3 actores

    def _calculate_quality_score(self, rating, duration, metascore, actors) -> float:
        """Calcular score de calidad basado en datos disponibles"""
        total_fields = 4
        available_fields = 0
        
        if rating is not None: available_fields += 1
        if duration is not None: available_fields += 1  
        if metascore is not None: available_fields += 1
        if actors: available_fields += 1
        
        return available_fields / total_fields

    def _emit(self, event_name: str, *args, **kwargs):
        """Emitir evento a callbacks registrados"""
        for callback in self.callbacks.get(event_name, []):
            try:
                callback(*args, **kwargs)
            except Exception as e:
                self.logger.warning(f"Error en callback {event_name}: {e}")

    def get_metrics(self) -> Dict:
        """Obtener métricas del scraper"""
        metrics = {
            "scraper_type": "BeautifulSoup",
            "movies_scraped": self.movies_scraped,
            "errors_count": self.errors_count,
            "success_rate": self.movies_scraped / (self.movies_scraped + self.errors_count) if (self.movies_scraped + self.errors_count) > 0 else 0,
            "config": self.config.to_dict()
        }

        if self.proxy_manager:
            metrics["proxy_stats"] = self.proxy_manager.get_statistics()

        return metrics


    def get_scraper_info(self) -> Dict:
        """Información del scraper"""
        return {
            "name": "BeautifulSoupScraper",
            "engine": "aiohttp + BeautifulSoup4",
            "supports_async": True,
            "supports_proxies": True,
            "version": "2025.1"
        }