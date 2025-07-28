# src/utils/validators.py
from typing import Optional, List, Any
import re
from datetime import datetime

class MovieDataValidator:
    """Validador de datos de películas"""

    @staticmethod
    def validate_title(title: Any) -> Optional[str]:
        """Validar título"""
        if not title or not isinstance(title, str):
            return None

        title = title.strip()
        if len(title) < 1 or len(title) > 500:
            return None

        # Remover caracteres no válidos
        title = re.sub(r'[^\w\s\-.,!?():\'"&]', '', title)
        return title if title else None

    @staticmethod
    def validate_year(year: Any) -> Optional[int]:
        """Validar año"""
        if year is None:
            return None

        try:
            year_int = int(year)
            current_year = datetime.now().year

            if 1888 <= year_int <= current_year + 5:
                return year_int
        except (ValueError, TypeError):
            pass

        return None
    
    @staticmethod
    def validate_rating(rating: Any) -> Optional[float]:
        """Validar calificación"""
        if rating is None:
            return None

        try:
            rating_float = float(rating)
            # IMDb ratings van de 1.0 a 10.0
            if 1.0 <= rating_float <= 10.0:
                return round(rating_float, 1)
        except (ValueError, TypeError):
            pass

        return None
    
    @staticmethod
    def validate_duration(duration: Any) -> Optional[int]:
        """Validar duración"""
        if duration is None:
            return None

        try:
            duration_int = int(duration)
            if 1 <= duration_int <= 1000:
                return duration_int
        except (ValueError, TypeError):
            pass

        return None
    
    @staticmethod
    def validate_metascore(metascore: Any) -> Optional[int]:
        """Validar Metascore"""
        if metascore is None:
            return None
            
        try:
            metascore_int = int(metascore)
            # Metascore va de 0 a 100
            if 0 <= metascore_int <= 100:
                return metascore_int
        except (ValueError, TypeError):
            pass
            
        return None
    
    @staticmethod
    def validate_actors(actors: Any) -> List[str]:
        """Validar lista de actores"""
        if not actors:
            return []
            
        if isinstance(actors, str):
            actors = [actors]
        
        if not isinstance(actors, list):
            return []
        
        validated_actors = []
        for actor in actors[:3]:  # Máximo 3
            if isinstance(actor, str):
                actor_clean = actor.strip()
                if len(actor_clean) > 0 and len(actor_clean) <= 255:
                    validated_actors.append(actor_clean)
        
        return validated_actors
    
    @staticmethod
    def validate_imdb_id(imdb_id: Any) -> Optional[str]:
        """Validar IMDb ID"""
        if not imdb_id or not isinstance(imdb_id, str):
            return None
            
        # Formato: tt seguido de números
        if re.match(r'^tt\d{7,8}$', imdb_id.strip()):
            return imdb_id.strip()
            
        return None
    
    @staticmethod
    def validate_movie_data(data: dict) -> dict:
        """Validar todos los campos de una película"""
        validated = {}
        
        # Campos requeridos
        validated['title'] = MovieDataValidator.validate_title(data.get('title'))
        validated['year'] = MovieDataValidator.validate_year(data.get('year'))
        validated['rating'] = MovieDataValidator.validate_rating(data.get('rating'))
        validated['imdb_id'] = MovieDataValidator.validate_imdb_id(data.get('imdb_id'))
        
        # Campos opcionales
        validated['duration_minutes'] = MovieDataValidator.validate_duration(data.get('duration_minutes'))
        validated['metascore'] = MovieDataValidator.validate_metascore(data.get('metascore'))
        validated['actors'] = MovieDataValidator.validate_actors(data.get('actors'))
        
        return validated
    
    @staticmethod
    def is_valid_movie(data: dict) -> bool:
        """Verificar si los datos mínimos están presentes"""
        validated = MovieDataValidator.validate_movie_data(data)
        
        # Campos requeridos deben estar presentes
        required_fields = ['title', 'imdb_id']
        return all(validated.get(field) is not None for field in required_fields)