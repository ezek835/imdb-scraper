-- =============================================
-- SCRIPT SQL - IMDB
-- Proyecto: IMDB Scraper
-- Base de Datos: PostgreSQL
-- =============================================

-- Crear esquema si no existe
CREATE SCHEMA IF NOT EXISTS imdb;

-- =============================================
-- 1. CREACIÓN DE TABLAS PRINCIPALES
-- =============================================

-- Tabla de películas
CREATE TABLE IF NOT EXISTS imdb.peliculas (
    id SERIAL PRIMARY KEY,
    titulo VARCHAR(255) NOT NULL,
    anio INTEGER NOT NULL,
    calificacion NUMERIC(3,1) NOT NULL,
    duracion INTEGER NOT NULL, -- en minutos
    metascore INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints de validación
    CONSTRAINT check_anio CHECK (anio >= 1888 AND anio <= EXTRACT(YEAR FROM CURRENT_DATE)),
    CONSTRAINT check_calificacion CHECK (calificacion >= 0 AND calificacion <= 10),
    CONSTRAINT check_duracion CHECK (duracion > 0),
    CONSTRAINT check_metascore CHECK (metascore >= 0 AND metascore <= 100),
    CONSTRAINT uq_pelicula_titulo_anio UNIQUE (titulo, anio)
);

-- Tabla de actores
CREATE TABLE IF NOT EXISTS imdb.actores (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL UNIQUE
);

-- Tabla de relación películas-actores (muchos a muchos)
CREATE TABLE IF NOT EXISTS imdb.pelicula_actor (
    pelicula_id INTEGER REFERENCES imdb.peliculas(id) ON DELETE CASCADE,
    actor_id INTEGER REFERENCES imdb.actores(id) ON DELETE CASCADE,
    es_principal BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (pelicula_id, actor_id)
);

-- =============================================
-- 2. ÍNDICES PARA OPTIMIZACIÓN
-- =============================================

-- Índices principales para consultas frecuentes
CREATE INDEX IF NOT EXISTS idx_peliculas_anio ON imdb.peliculas(anio);
CREATE INDEX IF NOT EXISTS idx_peliculas_calificacion ON imdb.peliculas(calificacion DESC);
CREATE INDEX IF NOT EXISTS idx_peliculas_duracion ON imdb.peliculas(duracion);
CREATE INDEX IF NOT EXISTS idx_peliculas_metascore ON imdb.peliculas(metascore) WHERE metascore IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_actores_nombre ON imdb.actores(nombre);
CREATE INDEX IF NOT EXISTS idx_pelicula_actor_actor_id ON imdb.pelicula_actor(actor_id);
CREATE INDEX IF NOT EXISTS idx_pelicula_actor_principal ON imdb.pelicula_actor(es_principal) WHERE es_principal = TRUE;

-- Índice compuesto para consultas por década
CREATE INDEX IF NOT EXISTS idx_peliculas_decada_duracion ON imdb.peliculas(
    ((anio / 10) * 10), duracion
);

-- =============================================
-- 3. VISTA PRINCIPAL - PELÍCULAS Y ACTORES
-- =============================================

CREATE OR REPLACE VIEW imdb.v_peliculas_actores AS
SELECT 
    p.id as pelicula_id,
    p.titulo,
    p.anio,
    p.calificacion,
    p.duracion,
    p.metascore,
    a.id as actor_id,
    a.nombre as actor_nombre,
    pa.es_principal,
    ((p.anio / 10) * 10) as decada,
    CASE 
        WHEN p.metascore IS NOT NULL THEN (p.metascore::NUMERIC / 10.0)
        ELSE NULL 
    END as metascore_normalizado
FROM imdb.peliculas p
LEFT JOIN imdb.pelicula_actor pa ON p.id = pa.pelicula_id
LEFT JOIN imdb.actores a ON pa.actor_id = a.id;

-- =============================================
-- 4. CONSULTAS SQL
-- =============================================

-- =============================================
-- 4.1 Las 5 películas con mayor promedio de duración por década
-- =============================================

CREATE OR REPLACE VIEW imdb.v_duracion_por_decada AS
WITH duracion_decadas AS (
    SELECT 
        ((anio / 10) * 10) as decada,
        AVG(duracion) as promedio_duracion,
        COUNT(*) as total_peliculas,
        STDDEV(duracion) as desviacion_duracion
    FROM imdb.peliculas
    WHERE duracion IS NOT NULL
    GROUP BY ((anio / 10) * 10)
),
peliculas_ranking AS (
    SELECT 
        p.*,
        ((p.anio / 10) * 10) as decada,
        dd.promedio_duracion,
        ROW_NUMBER() OVER (
            PARTITION BY ((p.anio / 10) * 10) 
            ORDER BY p.duracion DESC
        ) as ranking_decada
    FROM imdb.peliculas p
    JOIN duracion_decadas dd ON ((p.anio / 10) * 10) = dd.decada
)
SELECT 
    decada,
    titulo,
    anio,
    duracion,
    calificacion,
    ROUND(promedio_duracion, 2) as promedio_decada,
    ranking_decada
FROM peliculas_ranking
WHERE ranking_decada <= 5
ORDER BY decada DESC, ranking_decada;

-- =============================================
-- 4.2 Desviación estándar de calificaciones por año
-- =============================================

CREATE OR REPLACE VIEW imdb.v_estadisticas_por_anio AS
SELECT 
    anio,
    COUNT(*) as total_peliculas,
    ROUND(AVG(calificacion)::numeric, 2) as promedio_calificacion,
    ROUND(STDDEV(calificacion)::numeric, 4) as desviacion_estandar,
    ROUND(VARIANCE(calificacion)::numeric, 4) as varianza,
    MIN(calificacion) as min_calificacion,
    MAX(calificacion) as max_calificacion,
    -- Percentiles
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY calificacion)::numeric, 2) as percentil_25,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY calificacion)::numeric, 2) as mediana,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY calificacion)::numeric, 2) as percentil_75
FROM imdb.peliculas
GROUP BY anio
HAVING COUNT(*) >= 2
ORDER BY anio DESC;

-- =============================================
-- 4.3 Películas con >20% diferencia entre IMDB y Metascore
-- =============================================

CREATE OR REPLACE VIEW imdb.v_diferencias_calificacion AS
WITH calificaciones_normalizadas AS (
    SELECT 
        id,
        titulo,
        anio,
        calificacion as imdb_score,
        metascore,
        (metascore::NUMERIC / 10.0) as metascore_normalizado,
        -- Calcular diferencia porcentual
        CASE 
            WHEN metascore IS NOT NULL AND metascore > 0 THEN
                ABS(calificacion - (metascore::NUMERIC / 10.0)) / calificacion * 100
            ELSE NULL
        END as diferencia_porcentual,
        -- Determinar cuál es mayor
        CASE 
            WHEN metascore IS NOT NULL THEN
                CASE 
                    WHEN calificacion > (metascore::NUMERIC / 10.0) THEN 'IMDB_MAYOR'
                    WHEN calificacion < (metascore::NUMERIC / 10.0) THEN 'METASCORE_MAYOR'
                    ELSE 'SIMILARES'
                END
            ELSE 'SIN_METASCORE'
        END as comparacion
    FROM imdb.peliculas
    WHERE metascore IS NOT NULL
)
SELECT 
    titulo,
    anio,
    ROUND(imdb_score, 1) as calificacion_imdb,
    metascore,
    ROUND(metascore_normalizado, 1) as metascore_normalizado,
    ROUND(diferencia_porcentual, 2) as diferencia_porcentual,
    comparacion,
    -- Categorizar la diferencia
    CASE 
        WHEN diferencia_porcentual >= 50 THEN 'EXTREMA'
        WHEN diferencia_porcentual >= 30 THEN 'ALTA'
        WHEN diferencia_porcentual >= 20 THEN 'SIGNIFICATIVA'
        ELSE 'MODERADA'
    END as categoria_diferencia
FROM calificaciones_normalizadas
WHERE diferencia_porcentual >= 20
ORDER BY diferencia_porcentual DESC;

-- =============================================
-- 4.4 Vista películas-actores con filtros
-- =============================================

CREATE OR REPLACE VIEW imdb.v_peliculas_actores_completa AS
WITH actores_por_pelicula AS (
    SELECT 
        pelicula_id,
        COUNT(*) as total_actores,
        COUNT(*) FILTER (WHERE es_principal = TRUE) as actores_principales,
        STRING_AGG(
            CASE WHEN es_principal THEN nombre ELSE NULL END, 
            ', ' ORDER BY nombre
        ) as actores_principales_lista,
        STRING_AGG(nombre, ', ' ORDER BY nombre) as todos_actores
    FROM imdb.v_peliculas_actores
    WHERE actor_nombre IS NOT NULL
    GROUP BY pelicula_id
)
SELECT 
    p.id,
    p.titulo,
    p.anio,
    p.calificacion,
    p.duracion,
    p.metascore,
    COALESCE(app.total_actores, 0) as total_actores,
    COALESCE(app.actores_principales, 0) as actores_principales,
    app.actores_principales_lista,
    app.todos_actores,
    -- Métricas adicionales
    CASE 
        WHEN p.metascore IS NOT NULL THEN 
            ROUND(ABS(p.calificacion - (p.metascore::NUMERIC / 10.0)), 2)
        ELSE NULL 
    END as diferencia_scores
FROM imdb.peliculas p
LEFT JOIN actores_por_pelicula app ON p.id = app.pelicula_id;

-- =============================================
-- 5. FUNCIONES Y PROCEDIMIENTOS
-- =============================================

-- =============================================
-- 5.1 Función para obtener estadísticas de actor
-- =============================================

CREATE OR REPLACE FUNCTION imdb.obtener_estadisticas_actor(nombre_actor VARCHAR)
RETURNS TABLE (
    actor VARCHAR,
    total_peliculas INTEGER,
    peliculas_principales INTEGER,
    promedio_calificacion NUMERIC,
    mejor_pelicula VARCHAR,
    peor_pelicula VARCHAR,
    decadas_activas TEXT
) AS $$
BEGIN
    RETURN QUERY
    WITH estadisticas AS (
        SELECT 
            a.nombre,
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE pa.es_principal = TRUE) as principales,
            ROUND(AVG(p.calificacion), 2) as promedio,
            STRING_AGG(DISTINCT ((p.anio / 10) * 10)::TEXT, ', ' ORDER BY ((p.anio / 10) * 10)::TEXT) as decadas
        FROM imdb.actores a
        JOIN imdb.pelicula_actor pa ON a.id = pa.actor_id
        JOIN imdb.peliculas p ON pa.pelicula_id = p.id
        WHERE LOWER(a.nombre) LIKE LOWER('%' || nombre_actor || '%')
        GROUP BY a.nombre
    ),
    mejor_peor AS (
        SELECT 
            a.nombre,
            (SELECT p2.titulo FROM imdb.peliculas p2 
             JOIN imdb.pelicula_actor pa2 ON p2.id = pa2.pelicula_id
             WHERE pa2.actor_id = a.id
             ORDER BY p2.calificacion DESC LIMIT 1) as mejor,
            (SELECT p3.titulo FROM imdb.peliculas p3 
             JOIN imdb.pelicula_actor pa3 ON p3.id = pa3.pelicula_id
             WHERE pa3.actor_id = a.id
             ORDER BY p3.calificacion ASC LIMIT 1) as peor
        FROM imdb.actores a
        WHERE LOWER(a.nombre) LIKE LOWER('%' || nombre_actor || '%')
    )
    SELECT 
        e.nombre::VARCHAR,
        e.total::INTEGER,
        e.principales::INTEGER,
        e.promedio::NUMERIC,
        mp.mejor::VARCHAR,
        mp.peor::VARCHAR,
        e.decadas::TEXT
    FROM estadisticas e
    JOIN mejor_peor mp ON e.nombre = mp.nombre;
END;
$$ LANGUAGE plpgsql;

-- =============================================
-- 5.2 Procedimiento para análisis de tendencias por década
-- =============================================

CREATE OR REPLACE FUNCTION imdb.analisis_tendencias_decada()
RETURNS TABLE (
    decada INTEGER,
    total_peliculas INTEGER,
    promedio_calificacion NUMERIC,
    promedio_duracion NUMERIC,
    promedio_metascore NUMERIC,
    tendencia_calificacion TEXT,
    pelicula_destacada VARCHAR
) AS $$
BEGIN
    RETURN QUERY
    WITH datos_decada AS (
        SELECT 
            ((anio / 10) * 10) as dec,
            COUNT(*) as total,
            ROUND(AVG(calificacion), 2) as avg_calif,
            ROUND(AVG(duracion), 0) as avg_dur,
            ROUND(AVG(metascore), 0) as avg_meta,
            -- Calcular tendencia comparando con década anterior
            LAG(ROUND(AVG(calificacion), 2)) OVER (ORDER BY ((anio / 10) * 10)) as calif_anterior
        FROM imdb.peliculas
        GROUP BY ((anio / 10) * 10)
    ),
    peliculas_destacadas AS (
        SELECT DISTINCT
            ((anio / 10) * 10) as dec,
            FIRST_VALUE(titulo) OVER (
                PARTITION BY ((anio / 10) * 10) 
                ORDER BY calificacion DESC, metascore DESC NULLS LAST
            ) as destacada
        FROM imdb.peliculas
    )
    SELECT 
        dd.dec::INTEGER,
        dd.total::INTEGER,
        dd.avg_calif::NUMERIC,
        dd.avg_dur::NUMERIC,
        dd.avg_meta::NUMERIC,
        CASE 
            WHEN dd.calif_anterior IS NULL THEN 'BASE'
            WHEN dd.avg_calif > dd.calif_anterior + 0.2 THEN 'MEJORANDO'
            WHEN dd.avg_calif < dd.calif_anterior - 0.2 THEN 'EMPEORANDO'
            ELSE 'ESTABLE'
        END::TEXT as tendencia_calificacion,
        pd.destacada::VARCHAR
    FROM datos_decada dd
    LEFT JOIN peliculas_destacadas pd ON dd.dec = pd.dec
    ORDER BY dd.dec DESC;
END;
$$ LANGUAGE plpgsql;