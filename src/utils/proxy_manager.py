# src/utils/proxy_manager.py
import os
from dotenv import load_dotenv
import random
import requests
from typing import List, Dict, Optional
from tenacity import retry, stop_after_attempt, wait_exponential
from datetime import datetime
from src.utils.logger import get_logger

load_dotenv()


class ProxyManager:
    def __init__(self, config):
        self.config = config
        self.logger = get_logger('imdb_scraper.app')
        self.current_proxy = None
        self.proxy_list = self._build_proxy_list()
        self.health_checked = False
        self.stats = {
            'total_requests': 0,
            'failed_requests': 0,
            'rotations': 0,
            'usage_by_country': {},
            'ip_history': [],
            'last_used': None
        }

    def _build_proxy_list(self) -> List[Dict]:
        """Construye lista dinámica de proxies por país"""
        base_credential = os.getenv('PROXY_USER')
        password = os.getenv('PROXY_PASS')
        gateway = os.getenv('PROXY_GATEWAY')

        if not all([base_credential, password, gateway]):
             raise ValueError("Faltan credenciales de proxy en el archivo .env")
        
        # Configuración de países disponibles
        countries = [
            {'code': 'mx', 'name': 'México', 'region': 'mx'},
            {'code': 'ar', 'name': 'Argentina', 'region': 'ar'},
            {'code': 'bo', 'name': 'Bolivia', 'region': 'bo'}
        ]
        
        proxies = []
        for country in countries:
            # http://95533cdcdac76f2a8d8c__cr.mx:30c4cc87da609d99@gw.dataimpulse.com:823
            proxy_str = f"http://{base_credential}__cr.{country['region']}:{password}@{gateway}"
            hash_str = str(hash(proxy_str))[-4:]
            
            proxies.append({
                'http': proxy_str,
                'https': proxy_str,
                'country_code': country['code'],
                'country_name': country['name'],
                'proxy_id': f"{country['code']}-{hash_str}",
                'region': country['region']
            })
        
        self.logger.info(f"Configurados {len(proxies)} proxies por país")
        return proxies

    async def initialize(self):
        """Verifica que todos los proxies estén funcionando"""
        self.logger.info("Inicializando ProxyManager")
        
        working_proxies = []
        for proxy in self.proxy_list:
            if await self._verify_proxy(proxy):
                working_proxies.append(proxy)
        
        if not working_proxies:
            self.logger.error("No hay proxies funcionando!")
            self.proxy_list = None
        else:
            self.proxy_list = working_proxies
            self.logger.info(f"{len(working_proxies)} proxies verificados")
        
        self.health_checked = True

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=2, max=5))
    async def _verify_proxy(self, proxy: Dict) -> bool:
        """Verifica que el proxy funcione y esté en el país correcto"""
        try:
            # Primero verificamos conexión básica
            ip_response = requests.get(
                "https://api.ipify.org?format=json",
                proxies={'http': proxy['http'], 'https': proxy['https']},
                timeout=10
            )
            
            if ip_response.status_code != 200:
                return False
            
            ip_data = ip_response.json()
            current_ip = ip_data.get('ip', '')
            
            # Luego verificamos geolocalización
            geo_response = requests.get(
                f"http://ip-api.com/json/{current_ip}",
                timeout=8
            )
            
            if geo_response.status_code == 200:
                geo_data = geo_response.json()
                actual_country = geo_data.get('countryCode', '').lower()
                
                if actual_country != proxy['country_code']:
                    self.logger.warning(
                        f"Proxy {proxy['proxy_id']} está en país incorrecto: "
                        f"{actual_country.upper()} (debería ser {proxy['country_code'].upper()})"
                    )
                    return False
                
                self.logger.info(
                    f"Proxy {proxy['proxy_id']} verificado | "
                    f"IP: {current_ip} | País: {actual_country.upper()}"
                )
                return True
        
        except Exception as e:
            self.logger.warning(f"Proxy {proxy['proxy_id']} falló verificación: {str(e)}")
            return False

    def get_proxy(self) -> Optional[Dict]:
        """Obtiene un proxy aleatorio y registra su uso"""
        if not self.proxy_list:
            return None
        
        # Estrategia de rotación: aleatoria pero evitando repetir el último usado
        available = [p for p in self.proxy_list if p['proxy_id'] != self.stats.get('last_used')]
        self.current_proxy = random.choice(available or self.proxy_list)
        
        # Registro de uso
        self._record_proxy_usage()
        
        return {
            'http': self.current_proxy['http'],
            'https': self.current_proxy['https']
        }

    def _record_proxy_usage(self, response: Optional[requests.Response] = None):
        """Registra el uso del proxy"""
        self.stats['total_requests'] += 1
        self.stats['rotations'] += 1
        proxy_id = self.current_proxy['proxy_id']
        country = self.current_proxy['country_code']

        current_ip = 'unknown'

        # Intentar obtener IP de los headers de respuesta primero
        if response and 'X-Forwarded-For' in response.headers:
            current_ip = response.headers['X-Forwarded-For'].split(',')[0]
        elif response and 'X-Real-IP' in response.headers:
            current_ip = response.headers['X-Real-IP']
        else:
            try:
                ip_response = requests.get(
                    "https://api.ipify.org?format=json",
                    proxies={'http': self.current_proxy['http'], 'https': self.current_proxy['https']},
                    timeout=2
                )
                current_ip = ip_response.json().get('ip', 'unknown')
                self.logger.info('IP de : ipify')
            except Exception:
                pass

        # Registrar el uso
        self.stats['usage_by_country'][country] = self.stats['usage_by_country'].get(country, 0) + 1
        self.stats['ip_history'].append({
            'timestamp': datetime.now().isoformat(),
            'proxy_id': proxy_id,
            'country': country,
            'ip': current_ip,
            'region': self.current_proxy['region'],
            'source': 'response_headers' if current_ip != 'unknown' and response else 'ipify'
        })
        self.stats['last_used'] = proxy_id

        self.logger.info(
            f"Proxy: {proxy_id} | IP: {current_ip} | "
            f"País: {country.upper()} | Región: {self.current_proxy['region']}"
        )


    def mark_failed(self, proxy_url: str):
        """Marca un proxy como fallido basado en su URL"""
        if not self.proxy_list:
            return
            
        self.stats['failed_requests'] += 1
        
        # Encontrar el proxy fallado
        failed_proxy = next(
            (p for p in self.proxy_list if p['http'] == proxy_url or p['https'] == proxy_url),
            None
        )
        
        if failed_proxy:
            self.logger.warning(
                f"🔴 Proxy fallado: {failed_proxy['proxy_id']} | "
                f"País: {failed_proxy['country_code'].upper()}"
            )
            # Rotar a nuevo proxy
            return self.get_proxy()
        
        return None

    def get_statistics(self) -> Dict:
        """Obtiene estadísticas detalladas del uso de proxies"""
        return {
            'status': 'active' if self.proxy_list else 'inactive',
            'total_proxies': len(self.proxy_list) if self.proxy_list else 0,
            'total_requests': self.stats['total_requests'],
            'failed_requests': self.stats['failed_requests'],
            'success_rate': (
                (self.stats['total_requests'] - self.stats['failed_requests']) / 
                self.stats['total_requests']
            ) if self.stats['total_requests'] > 0 else 0,
            'usage_by_country': self.stats['usage_by_country'],
            'last_10_ips': self.stats['ip_history'][-10:],
            'current_proxy': self.stats.get('last_used')
        }

    async def close(self):
        """Genera reporte final"""
        self.logger.info("Reporte final de proxies:")
        stats = self.get_statistics()
        
        self.logger.info(f"Total requests: {stats['total_requests']}")
        self.logger.info(f"Failed requests: {stats['failed_requests']}")
        self.logger.info(f"Success rate: {stats['success_rate']:.2%}")
        
        self.logger.info("Uso por país:")
        for country, count in stats['usage_by_country'].items():
            self.logger.info(f"  {country.upper()}: {count} requests")
        
        if stats['last_10_ips']:
            self.logger.info("Últimas IPs utilizadas:")
            for ip_record in stats['last_10_ips']:
                self.logger.info(
                    f"  [{ip_record['timestamp']}] {ip_record['ip']} "
                    f"({ip_record['country'].upper()})"
                )