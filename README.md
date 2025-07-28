## Descripción del Proyecto
Este proyecto es un sistema de scraping para extraer información de películas del Top 250 de IMDB, con capacidades de:

- Extracción de datos usando BeautifulSoup
- Persistencia en PostgreSQL con modelo relacional
- Estrategias anti-bloqueo con rotación de proxies
- Análisis de datos con SQL
- Comparación técnica con alternativas como Playwright

## Instalación
1. Requisitos previos:
    - Python 3.10+
    - PostgreSQL 14+

2. Configuración:
```bash
git clone https://github.com/tu-usuario/imdb-scraper.git
cd imdb-scraper
python -m venv venv
source venv/bin/activate  # Linux/Mac
# o venv\Scripts\activate en Windows
pip install -r requirements.txt
```

3. Variables de entorno:
Crear un archivo .env con:
```ini
DATABASE_URL=postgresql+asyncpg://usuario:contraseña@localhost:5432/imdb_scraper
PROXY_LIST=proxy1,proxy2,proxy3
PROXY_USER=tu_usuario
PROXY_PASS=tu_contraseña
```

## Uso
1. Ejecutar el scraper
```bash
python main.py --scraper beautifulsoup --movies 50
```
2. Configurar la base de datos
```bash
python migrate.py
```
3. Exportar datos
Los resultados se guardan automáticamente en:
   - data/movies_<timestamp>.csv
   - data/movies_<timestamp>.json

## Consultas SQL de Ejemplo
Ejecutar en tu cliente PostgreSQL:

```sql
-- 1. Top 5 películas por duración por década
SELECT * FROM imdb.v_duracion_por_decada LIMIT 20;

-- 2. Años con mayor variabilidad en calificaciones
SELECT * FROM imdb.v_estadisticas_por_anio 
WHERE desviacion_estandar IS NOT NULL 
ORDER BY desviacion_estandar DESC 
LIMIT 10;

-- 3. Películas con mayores diferencias de calificación
SELECT * FROM imdb.v_diferencias_calificacion LIMIT 10;

-- 4. Buscar películas por actor principal
SELECT titulo, anio, calificacion, actores_principales_lista
FROM imdb.v_peliculas_actores_completa 
WHERE actores_principales_lista ILIKE '%Morgan Freeman%'
ORDER BY calificacion DESC;

-- 5. Estadísticas de actor
SELECT * FROM imdb.obtener_estadisticas_actor('Morgan Freeman');

-- 6. Análisis de tendencias por década
SELECT * FROM imdb.analisis_tendencias_decada();

```

## Comparación Técnica: BeautifulSoup vs Playwright
> Esta en el archivo: Scraper_Playwright.md

# Estructura del Proyecto
```text
imdb-scraper/
├── data/                   # Datos exportados
├── logs/                   # Logs de ejecución
├── src/
│   ├── config.py           # Configuración
│   ├── factory.py          # Patrón Factory
│   ├── models.py           # Modelos SQLAlchemy
│   ├── scrapers/
│   │   ├── bs4_scraper.py  # Scraper principal
│   │   └── scrapy_scraper.py
│   ├── utils/              # Utilidades
│   └── database.py         # Conexión PostgreSQL
├── main.py                 # Punto de entrada
├── migrate.py              # Migraciones DB
├── script.sql              # Esquema SQL completo
└── requirements.txt
```