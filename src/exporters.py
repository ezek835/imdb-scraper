# src/exporters.py
"""
Data exporters for various formats
"""

import csv
import json
from pathlib import Path
from typing import List
from datetime import datetime

from src.factory import MovieData


class BaseExporter:
    """Base class for data exporters"""
    
    def __init__(self, output_path: Path):
        self.output_path = Path(output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        
    async def export(self, movies: List[MovieData]) -> Path:
        """Export movies to file"""
        raise NotImplementedError


class CSVExporter(BaseExporter):
    """Export movies to CSV format"""
    
    async def export(self, movies: List[MovieData]) -> Path:
        """Export movies to CSV file"""
        with open(self.output_path, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'title', 'year', 'rating', 'duration_minutes', 
                'metascore', 'actors', 'imdb_id', 'scraped_at'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            writer.writeheader()
            for movie in movies:
                writer.writerow({
                    'title': movie.title,
                    'year': movie.year,
                    'rating': movie.rating,
                    'duration_minutes': movie.duration_minutes or '',
                    'metascore': movie.metascore or '',
                    'actors': '|'.join(movie.actors[:3]),  # Top 3 actors
                    'imdb_id': movie.imdb_id,
                    'scraped_at': movie.scraped_at.isoformat()
                })
                
        return self.output_path


class JSONExporter(BaseExporter):
    """Export movies to JSON format"""
    
    async def export(self, movies: List[MovieData]) -> Path:
        """Export movies to JSON file"""
        data = {
            'metadata': {
                'export_date': datetime.now().isoformat(),
                'total_movies': len(movies),
                'version': '1.0.0'
            },
            'movies': [movie.to_dict() for movie in movies]
        }
        
        with open(self.output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            
        return self.output_path
