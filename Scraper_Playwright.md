# Comparación Técnica: Implementación de Scraper con Playwright

## 1. Elección de Playwright

### Justificación Técnica
Basándome en mi experiencia desarrollando un sistema de scraping para monitorear los medios de comunicación (prensa, redes sociales, etc) en México, **Playwright** y otras herramientas, fue una de las herrmaientas clave debido a:
- **Velocidad de ejecución**: Playwright es 30-50% más rápido que Selenium
- **Soporte nativo multi-browser**: Chrome, Firefox, Safari con una sola API
- **Auto-wait inteligente**: Manejo automático de elementos dinámicos sin esperas explícitas adicionales
- **Network interception**: Control granular de requests/responses
- **Mejor evasión de detección**: Headers y fingerprinting


## 2. Configuración
### Configuración Anti-Detección
```python
# Configuración para evasión de webdriver
async def create_stealth_browser():
    browser = await playwright.chromium.launch(
        headless=True,
        args=[
            '--no-sandbox',
            '--disable-blink-features=AutomationControlled',
            '--disable-web-security',
            '--disable-features=VizDisplayCompositor',
            '--disable-dev-shm-usage',
            '--no-first-run',
            '--disable-extensions',
            '--disable-default-apps'
        ],
        executable_path='/usr/bin/chromium-browser'
    )
    
    context = await browser.new_context(
        viewport={'width': 1920, 'height': 1080},
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        extra_http_headers={
            'Accept-Language': 'es-MX,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }
    )
    
    # Inyección de playwright-stealth y evasión de WebGL/Canvas
    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined,
        });
        
        // Desactivar WebGL Fingerprinting
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(parameter) {
            if (parameter === 37445) return 'Intel Inc.';
            if (parameter === 37446) return 'Intel Iris OpenGL Engine';
            return getParameter(parameter);
        };
    """)
    
    return browser, context
```

### Headers Dinámicos con Random.org
```python
class DynamicHeadersManager:
    def __init__(self):
        self.user_agents_pool = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59"
        ]
        self.random_api_key = "tu-api-key-random-org"
    
    async def get_random_headers(self):
        # Integración con Random.org para comportamiento humano
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f'https://api.random.org/json-rpc/2/invoke',
                json={
                    "jsonrpc": "2.0",
                    "method": "generateSignedIntegers",
                    "params": {
                        "apiKey": self.random_api_key,
                        "n": 1,  # Solo necesitamos un valor para seleccionar el User-Agent
                        "min": 0,  # Índice mínimo
                        "max": len(self.user_agents_pool) - 1  # Índice máximo
                    },
                    "id": 42
                }
            ) as response:
                random_data = await response.json()
                random_index = random_data['result']['random']['data'][0]
        
        # Seleccionar User-Agent basado en el índice aleatorio
        selected_user_agent = self.user_agents_pool[random_index]
        
        # Generar headers basados en valores random
        return {
            'User-Agent': selected_user_agent,
            'Accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            'Accept-Language': 'es-MX,es;q=0.8,en-US;q=0.5,en;q=0.3',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Sec-Gpc': '1'
        }
```

## 3. Selectores Dinámicos con Espera Explícita
### Manejo de elementos dinámicos
```python
class SmartSelectorManager:
    def __init__(self, page):
        self.page = page
    
    async def wait_for_dynamic_content(self, selectors_fallback):
        """Espera por múltiples selectores con fallback"""
        for selector in selectors_fallback:
            try:
                await self.page.wait_for_selector(
                    selector, 
                    timeout=15000,
                    state='visible'
                )
                return selector
            except Exception:
                continue
        raise Exception("Ningún selector funcionó")
    
    async def extract_with_retry(self, selectors, max_retries=3):
        """Extracción con reintentos inteligentes"""
        for attempt in range(max_retries):
            try:
                active_selector = await self.wait_for_dynamic_content(selectors)
                elements = await self.page.query_selector_all(active_selector)
                return [await el.inner_text() for el in elements]
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                await self.page.wait_for_timeout(2000)
```

## 4. Manejo de CAPTCHA y JavaScript Rendering

### JavaScript Rendering con Module Raid
```python
async def inject_module_raid(page):
    """Inyección de Module Raid para acceso a módulos internos"""
    module_raid_script = """
    (function() {
        let modules = {};
        let cache = {};
        
        function getModules() {
            if (window.webpackJsonp) {
                window.webpackJsonp.forEach(function(item) {
                    item[1].forEach(function(module, id) {
                        modules[id] = module;
                    });
                });
            }
            return modules;
        }
        
        window.moduleRaid = {
            get: function(id) {
                if (!cache[id]) {
                    let module = modules[id] || getModules()[id];
                    if (module) {
                        cache[id] = {};
                        module(cache[id], {}, function(req) {
                            return cache[req] || window.moduleRaid.get(req);
                        });
                    }
                }
                return cache[id];
            },
            getAllModules: getModules
        };
    })();
    """
    
    await page.add_script_tag(content=module_raid_script)
    
    # Acceso a sistemas internos de React/Angular
    internal_data = await page.evaluate("""
        () => {
            // Acceso a store de Redux o estado interno
            let store = window.moduleRaid?.get('store');
            let apiCache = window.moduleRaid?.get('apiCache');
            
            return {
                store: store?.getState?.() || null,
                cache: apiCache || null,
                modules: Object.keys(window.moduleRaid?.getAllModules() || {})
            };
        }
    """)
    
    return internal_data

### Manejo de CAPTCHA
class CaptchaSolver:
    async def detect_and_solve(self, page):
        # Detección de diferentes tipos de CAPTCHA
        captcha_selectors = [
            'iframe[src*="recaptcha"]',
            '.captcha-challenge',
            '#captcha',
            '[data-testid="captcha"]'
        ]
        
        for selector in captcha_selectors:
            if await page.query_selector(selector):
                return await self.solve_captcha(page, selector)
        
        return True  # No hay CAPTCHA
    
    async def solve_captcha(self, page, selector):
        # Integración con servicios como 2captcha o death-by-captcha
        # Simulación de comportamiento humano mientras se resuelve
        await page.mouse.move(
            random.randint(100, 500), 
            random.randint(100, 500)
        )
        await page.wait_for_timeout(random.randint(2000, 5000))
        return True
```

## 5. Control de Concurrencia con Celery y Redis

### Arquitectura Distribuida
```python
from celery import Celery
from celery.result import AsyncResult
import redis

# Configuración de Celery con Redis como broker
app = Celery('media_scraper')
app.config_from_object({
    'broker_url': 'redis://localhost:6379/0',
    'result_backend': 'redis://localhost:6379/0',
    'task_routes': {
        'scraper.tasks.scrape_news_site': {'queue': 'high_priority'},
        'scraper.tasks.scrape_blog': {'queue': 'normal_priority'},
        'scraper.tasks.scrape_pdf': {'queue': 'heavy_processing'}
    },
    'worker_prefetch_multiplier': 1,
    'task_acks_late': True,
    'worker_max_tasks_per_child': 50
})

@app.task(bind=True, max_retries=3)
def scrape_with_playwright(self, url, site_config):
    """Task distribuida para scraping con Playwright"""
    try:
        # Proxy residencial dinámico
        proxy_config = get_residential_proxy()  # DataImpulse integration
        
        result = asyncio.run(
            perform_scraping(url, site_config, proxy_config)
        )
        
        return {
            'status': 'success',
            'data': result,
            'proxy_used': proxy_config['ip']
        }
        
    except Exception as exc:
        self.retry(countdown=60 * (2 ** self.request.retries))

class ScrapingOrchestrator:
    def __init__(self):
        self.redis_client = redis.Redis(host='localhost', port=6379, db=1)
        self.task_monitor = {}
    
    async def distribute_scraping_tasks(self, urls_batch):
        """Distribución inteligente de tareas"""
        tasks = []
        
        for url in urls_batch:
            # Clasificación automática del sitio
            site_type = self.classify_site(url)
            queue_name = self.get_optimal_queue(site_type)
            
            # Envío a cola específica
            task = scrape_with_playwright.apply_async(
                args=[url, self.get_site_config(site_type)],
                queue=queue_name
            )
            
            tasks.append(task)
            self.task_monitor[task.id] = {
                'url': url,
                'started_at': datetime.now(),
                'status': 'pending'
            }
        
        return tasks
    
    def monitor_tasks(self):
        """Monitoreo en tiempo real con Celery Flower integration"""
        for task_id, info in self.task_monitor.items():
            result = AsyncResult(task_id, app=app)
            
            if result.ready():
                self.task_monitor[task_id]['status'] = 'completed'
                self.task_monitor[task_id]['result'] = result.result
```

## 6. Justificación: Playwright vs Scrapy
### Ventajas de Playwright sobre Scrapy

**Casos donde Playwright es mejor:**
1. **JavaScript-Heavy Sites**: SPAs, React/Angular/Vue applications
   - Scrapy requiere Splash o middleware adicional
   - Playwright maneja JS nativamente

2. **Anti-Bot Sophistication**: 
   - Fingerprinting detection
   - Behavioral analysis
   - Playwright + stealth plugins > Scrapy + middleware

3. **Real-time Interaction**:
   - Forms complejos
   - Multi-step processes
   - Session management avanzado

4. **Network Control**:
   - Request/Response interception
   - Network throttling
   - Cache management

### Cuendo he usado Playwright:

**Cuando:**
- Sitios con heavy JavaScript
- Anti-bot detection avanzado
- Interacciones complejas requeridas
- Budget de recursos suficiente

### Cuendo he usado Scrapy:
**Cuando:**
- Sitios principalmente estáticos
- Volúmenes masivos (>10M páginas/día)
- Equipo con experiencia limitada en automation
- Recursos computacionales limitados

En prtoyectos en dodne tenia una instancia con Playwright, para el proyecto de **monitoreo de medios en México**,  se eligio en:
- Sitios de noticias modernos con JS rendering
- Necesidad de evasión anti-bot
- Ventana crítica de scraping (4-8 AM)
- Requerimiento de alta fidelidad de datos