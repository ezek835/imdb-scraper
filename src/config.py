# src/config.py
import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class ScraperConfig:
    # Configuración de base de datos PostgreSQL
    database_url: str = "postgresql+asyncpg://postgres:w6qaZ1Qk4rR7DQtyCD5TVQr48@172.17.0.1:5432/scraper"
    
    # Configuración de pool (solo para bases de datos que lo soporten)
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30
    pool_recycle: int = 3600
    
    # Configuración de directorios
    data_dir: str = "./data"
    
    # Configuración general
    num_movies: int = 100
    debug_mode: bool = False
    
    # Configuración de scraping
    delay_between_requests: float = 1.0
    max_retries: int = 3
    timeout: int = 30
    
    # Configuración de proxy
    use_proxies: bool = False
    proxy_list: Optional[list] = None
    proxy_rotation_strategy: str = "random"
    proxy_max_failures: int = 3
    
    @classmethod
    def from_env(cls):
        """Crear configuración desde variables de entorno"""
        proxy_list = os.getenv("PROXY_LIST")
        if proxy_list:
            proxy_list = [p.strip() for p in proxy_list.split(",")]

        return cls(
            database_url=os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:w6qaZ1Qk4rR7DQtyCD5TVQr48@172.17.0.1:5432/scraper"),
            num_movies=int(os.getenv("NUM_MOVIES", "100")),
            debug_mode=os.getenv("DEBUG", "false").lower() == "true",
            delay_between_requests=float(os.getenv("DELAY_BETWEEN_REQUESTS", "1.0")),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            timeout=int(os.getenv("TIMEOUT", "30")),
            use_proxies=os.getenv("USE_PROXIES", "false").lower() == "true",
            proxy_list=proxy_list,
            proxy_rotation_strategy=os.getenv("PROXY_ROTATION_STRATEGY", "random"),
            proxy_max_failures=int(os.getenv("PROXY_MAX_FAILURES", "3")),
        )
    
    def is_postgresql(self) -> bool:
        """Verificar si se está usando PostgreSQL"""
        return self.database_url.startswith(("postgresql", "postgresql+asyncpg"))
    
    def is_sqlite(self) -> bool:
        """Verificar si se está usando SQLite"""
        return self.database_url.startswith(("sqlite", "sqlite+aiosqlite"))
    
    def get_database_engine_kwargs(self) -> dict:
        """Obtener argumentos apropiados para create_async_engine"""
        kwargs = {
            'echo': self.debug_mode,
        }
        
        if self.is_sqlite():
            kwargs['connect_args'] = {"check_same_thread": False}
        elif self.is_postgresql():
            # PostgreSQL configuración
            kwargs.update({
                'pool_size': self.pool_size,
                'max_overflow': self.max_overflow,
                'pool_timeout': self.pool_timeout,
                'pool_recycle': self.pool_recycle,
                'pool_pre_ping': True,
            })
        else:
            kwargs.update({
                'pool_size': self.pool_size,
                'max_overflow': self.max_overflow,
                'pool_timeout': self.pool_timeout,
                'pool_recycle': self.pool_recycle,
            })
        
        return kwargs
    
    def to_dict(self) -> dict:
        """Convertir configuración a diccionario"""
        return {
            'database_url': self.database_url,
            'num_movies': self.num_movies,
            'debug_mode': self.debug_mode,
            'delay_between_requests': self.delay_between_requests,
            'max_retries': self.max_retries,
            'timeout': self.timeout,
            'use_proxies': self.use_proxies,
            'data_dir': self.data_dir,
        }