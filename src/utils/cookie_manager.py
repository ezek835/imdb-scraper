# src/utils/cookie_manager.py
import requests
from typing import Dict, Optional
import json
from pathlib import Path

class CookieManager:
    """Manejo de cookies personalizados para evitar bloqueos"""
    
    def __init__(self, cookie_file: Optional[Path] = None):

        self.data_dir = Path("./data")
        self.data_dir.mkdir(exist_ok=True)

        self.cookie_file = cookie_file or self.data_dir / "cookies.json"
        self.session = requests.Session()
        self.load_cookies()
    
    def get_default_cookies(self) -> Dict[str, str]:
        """Cookies por defecto para IMDb"""
        return {
            'session-id': '000-0000000-0000000',
            'session-id-time': '2082787201l',
            'session-token': 'example-token',
            'csm-hit': 'tb:example+b-example',
            'ubid-main': '000-0000000-0000000',
            'at-main': 'example-at-main',
            'sess-at-main': 'example-sess',
            'lc-main': 'en_US',
            'skin': 'imdb',
            'consumer-id': 'imdb-consumer',
            'ad-oo': '0'
        }
    
    def load_cookies(self):
        """Cargar cookies desde archivo o usar por defecto"""
        try:
            if self.cookie_file.exists():
                with open(self.cookie_file, 'r') as f:
                    cookies = json.load(f)
                    for name, value in cookies.items():
                        self.session.cookies.set(name, value)
            else:
                # Usar cookies por defecto
                for name, value in self.get_default_cookies().items():
                    self.session.cookies.set(name, value)
        except Exception as e:
            print(f"Error loading cookies: {e}")
    
    def save_cookies(self):
        """Guardar cookies actuales"""
        try:
            cookies_dict = dict(self.session.cookies)
            with open(self.cookie_file, 'w') as f:
                json.dump(cookies_dict, f, indent=2)
        except Exception as e:
            print(f"Error saving cookies: {e}")
    
    def get_session(self) -> requests.Session:
        """Obtener sesión con cookies"""
        return self.session
    
    def update_cookies_from_response(self, response):
        """Actualizar cookies desde respuesta"""
        # Los cookies se actualizan automáticamente en la sesión
        self.save_cookies()