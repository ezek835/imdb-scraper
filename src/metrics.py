# src/metrics.py
"""
Metrics collection and dashboard generation
"""

from typing import Dict, Any, List
from datetime import datetime
from pathlib import Path
import json

from src.factory import MovieData


class MetricsCollector:
    """Collects metrics during scraping"""
    
    def __init__(self):
        self.movies_scraped = 0
        self.errors = []
        self.start_time = datetime.now()
        
    def record_movie(self, movie: MovieData):
        """Record successful movie scrape"""
        self.movies_scraped += 1
        
    def record_error(self, error: Exception):
        """Record scraping error"""
        self.errors.append({
            'error': str(error),
            'timestamp': datetime.now().isoformat()
        })
        
    def get_summary(self) -> Dict[str, Any]:
        """Get metrics summary"""
        duration = (datetime.now() - self.start_time).total_seconds()
        
        return {
            'movies_scraped': self.movies_scraped,
            'errors_count': len(self.errors),
            'duration_seconds': duration,
            'success_rate': self.movies_scraped / (self.movies_scraped + len(self.errors))
                           if (self.movies_scraped + len(self.errors)) > 0 else 0,
            'errors': self.errors[-10:]  # Last 10 errors
        }


class MetricsDashboard:
    """Generate HTML metrics dashboard"""
    
    def __init__(self, metrics_data: Dict[str, Any]):
        self.metrics_data = metrics_data
        
    async def generate(self) -> Path:
        """Generate HTML dashboard"""
        output_path = Path('data/metrics.html')
        
        html_content = self._generate_html()
        
        with open(output_path, 'w') as f:
            f.write(html_content)
            
        return output_path
        
    def _generate_html(self) -> str:
        """Generate HTML content"""
        return f"""
<!DOCTYPE html>
<html>
<head>
    <title>IMDB Scraper Metrics Dashboard</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .metric {{ background: #f0f0f0; padding: 15px; margin: 10px 0; border-radius: 5px; }}
        .success {{ color: green; }}
        .error {{ color: red; }}
        h1 {{ color: #333; }}
        pre {{ background: #f5f5f5; padding: 10px; overflow-x: auto; }}
    </style>
</head>
<body>
    <h1>IMDB Scraper Metrics Dashboard</h1>
    <div class="metric">
        <h2>Scraping Summary</h2>
        <pre>{json.dumps(self.metrics_data.get('app_metrics', {}), indent=2)}</pre>
    </div>
    <div class="metric">
        <h2>Scraper Performance</h2>
        <pre>{json.dumps(self.metrics_data.get('scraper_metrics', {}), indent=2)}</pre>
    </div>
    <div class="metric">
        <h2>Proxy Statistics</h2>
        <pre>{json.dumps(self.metrics_data.get('proxy_stats', {}), indent=2)}</pre>
    </div>
    <p>Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
</body>
</html>
"""