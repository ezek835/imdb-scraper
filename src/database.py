# src/database.py
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text
from typing import AsyncGenerator, Dict, Any
from src.models import Base
from src.utils.logger import get_logger


class Database:
    def __init__(self, config):
        self.config = config
        self.engine = None
        self.async_session_factory = None
        self.logger = get_logger('imdb_scraper.app')
    
    async def initialize(self):
        """Inicializar la conexión a PostgreSQL"""
        database_url = self.config.database_url
        
        # Convertir postgres:// a postgresql+asyncpg://
        if database_url.startswith("postgres://"):
            database_url = database_url.replace("postgres://", "postgresql+asyncpg://", 1)
        elif not database_url.startswith("postgresql+asyncpg://"):
            database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        
        # Configuración optimizada para PostgreSQL
        engine_kwargs = {
            'echo': self.config.debug_mode,
            'pool_size': getattr(self.config, 'pool_size', 10),
            'max_overflow': getattr(self.config, 'max_overflow', 20),
            'pool_timeout': getattr(self.config, 'pool_timeout', 30),
            'pool_recycle': getattr(self.config, 'pool_recycle', 3600),
            'pool_pre_ping': True,
            'connect_args': {
                'server_settings': {
                    'application_name': 'imdb_scraper',
                }
            }
        }
        
        try:
            self.engine = create_async_engine(database_url, **engine_kwargs)
            
            self.async_session_factory = async_sessionmaker(
                bind=self.engine,
                class_=AsyncSession,
                expire_on_commit=False
            )
            
            await self.test_connection()
            self.logger.success("Conexión a PostgreSQL establecida correctamente")
            
        except Exception as e:
            self.logger.error(f"Error al conectar con PostgreSQL: {e}")
            raise
    
    async def test_connection(self):
        """Probar la conexión a la base de datos"""
        async with self.engine.begin() as conn:
            result = await conn.execute(text("SELECT version()"))
            version = result.scalar()
            self.logger.info(f"PostgreSQL version: {version}")
    
    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Context manager para sesiones"""
        if not self.async_session_factory:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        
        async with self.async_session_factory() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()
    
    async def create_tables(self):
        """Crear el esquema, tablas, procedimiento y trigger en la base de datos"""
        async with self.engine.begin() as conn:
            # Crear el esquema imdb
            await conn.execute(text("CREATE SCHEMA IF NOT EXISTS imdb"))
            self.logger.info("Esquema imdb creado/verificado")

            # Crear las tablas usando SQLAlchemy
            await conn.run_sync(Base.metadata.create_all)
            self.logger.success(" - Tablas creadas/verificados correctamente")

        # Crear la función y el trigger
        async with self.engine.begin() as conn:
            function_sql = """
            CREATE OR REPLACE FUNCTION imdb.log_pelicula_changes()
            RETURNS TRIGGER AS $$
            BEGIN
                IF TG_OP = 'UPDATE' THEN
                    INSERT INTO imdb.pelicula_audit (
                        pelicula_id, 
                        operation, 
                        old_data, 
                        new_data,
                        changed_at,
                        changed_by
                    )
                    VALUES (
                        NEW.id, 
                        'UPDATE',
                        jsonb_build_object(
                            'titulo', OLD.titulo, 
                            'anio', OLD.anio, 
                            'calificacion', OLD.calificacion,
                            'duracion', OLD.duracion, 
                            'metascore', OLD.metascore
                        ),
                        jsonb_build_object(
                            'titulo', NEW.titulo, 
                            'anio', NEW.anio, 
                            'calificacion', NEW.calificacion,
                            'duracion', NEW.duracion, 
                            'metascore', NEW.metascore
                        ),
                        NOW(),
                        current_user
                    );
                ELSIF TG_OP = 'DELETE' THEN
                    INSERT INTO imdb.pelicula_audit (
                        pelicula_id, 
                        operation, 
                        old_data,
                        changed_at,
                        changed_by
                    )
                    VALUES (
                        OLD.id, 
                        'DELETE',
                        jsonb_build_object(
                            'titulo', OLD.titulo, 
                            'anio', OLD.anio, 
                            'calificacion', OLD.calificacion,
                            'duracion', OLD.duracion, 
                            'metascore', OLD.metascore
                        ),
                        NOW(),
                        current_user
                    );
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """
            await conn.execute(text(function_sql))
            self.logger.success(" - Función log_pelicula_changes creada/verificada")

        async with self.engine.begin() as conn:
            trigger_sql = """
            CREATE TRIGGER pelicula_audit_trigger
            AFTER UPDATE OR DELETE ON imdb.peliculas
            FOR EACH ROW EXECUTE FUNCTION imdb.log_pelicula_changes();
            """
            await conn.execute(text(trigger_sql))
            self.logger.success(" - Trigger pelicula_audit_trigger creado/verificado")

        async with self.engine.begin() as conn:
            view_sql_pa = """
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
            """
            await conn.execute(text(view_sql_pa))
            self.logger.success(" - VIEW v_peliculas_actores creado/verificado")

        async with self.engine.begin() as conn:
            view_sql_dpd = """
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
            """
            await conn.execute(text(view_sql_dpd))
            self.logger.success(" - VIEW v_duracion_por_decada creado/verificado")

        async with self.engine.begin() as conn:
            view_sql_de = """
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
            """
            await conn.execute(text(view_sql_de))
            self.logger.success(" - VIEW v_estadisticas_por_anio creado/verificado")

        async with self.engine.begin() as conn:
            view_sql_dc = """
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
            """
            await conn.execute(text(view_sql_dc))
            self.logger.success(" - VIEW v_diferencias_calificacion creado/verificado")
    

        async with self.engine.begin() as conn:
            view_sql_pac = """
            CREATE OR REPLACE VIEW imdb.v_peliculas_actores_completa AS
                WITH actores_por_pelicula AS (
                    SELECT 
                        pelicula_id,
                        COUNT(*) as total_actores,
                        COUNT(*) FILTER (WHERE es_principal = TRUE) as actores_principales,
                        STRING_AGG(
                            CASE WHEN es_principal THEN actor_nombre ELSE NULL END, 
                            ', ' ORDER BY actor_nombre
                        ) as actores_principales_lista,
                        STRING_AGG(actor_nombre, ', ' ORDER BY actor_nombre) as todos_actores
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
            """
            await conn.execute(text(view_sql_pac))
            self.logger.success(" - VIEW v_peliculas_actores_completa creado/verificado")

        async with self.engine.begin() as conn:
            procedure_sql_eu = """
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
            """
            await conn.execute(text(procedure_sql_eu))
            self.logger.success(" - FUNCTION obtener_estadisticas_actor creado/verificado")

        async with self.engine.begin() as conn:
            procedure_sql_atd = """
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
            """
            await conn.execute(text(procedure_sql_atd))
            self.logger.success(" - FUNCTION analisis_tendencias_decada creado/verificado")
    
    async def drop_tables(self):
        """Eliminar todas las tablas (usar con cuidado)"""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.execute(text("DROP SCHEMA IF EXISTS imdb CASCADE"))
            self.logger.info("Tablas y esquema eliminados")
    
    async def execute_raw(self, query: str, parameters: dict = None):
        """Ejecutar consulta SQL raw"""
        async with self.get_session() as session:
            result = await session.execute(text(query), parameters or {})
            await session.commit()
            return result
    
    async def get_database_info(self) -> Dict[str, Any]:
        """Obtener información de la base de datos"""
        async with self.get_session() as session:
            queries = {
                'version': "SELECT version()",
                'current_database': "SELECT current_database()",
                'current_user': "SELECT current_user",
                'connection_count': """
                    SELECT count(*) as active_connections 
                    FROM pg_stat_activity 
                    WHERE state = 'active'
                """,
                'database_size': """
                    SELECT pg_size_pretty(pg_database_size(current_database())) as size
                """
            }
            
            info = {}
            for key, query in queries.items():
                try:
                    result = await session.execute(text(query))
                    info[key] = result.scalar()
                except Exception as e:
                    info[key] = f"Error: {e}"
            
            return info
    
    async def close(self):
        """Cerrar la conexión a la base de datos"""
        if self.engine:
            await self.engine.dispose()
            self.logger.info("Conexión a PostgreSQL cerrada")
    
    async def health_check(self) -> bool:
        """Verificar si la base de datos está disponible"""
        try:
            async with self.engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return False