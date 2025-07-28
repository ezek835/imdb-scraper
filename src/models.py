# src/models.py
from sqlalchemy import Column, Integer, String, Numeric, DateTime, Boolean, ForeignKey, Index, CheckConstraint, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
import uuid
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func, text

Base = declarative_base()

class Pelicula(Base):
    __tablename__ = "peliculas"
    __table_args__ = (
        # Constraints
        CheckConstraint('anio >= 1888 AND anio <= EXTRACT(YEAR FROM CURRENT_DATE)', name='check_anio'),
        CheckConstraint('calificacion >= 0 AND calificacion <= 10', name='check_calificacion'),
        CheckConstraint('duracion > 0', name='check_duracion'),
        CheckConstraint('metascore >= 0 AND metascore <= 100', name='check_metascore'),
        UniqueConstraint('titulo', 'anio', name='uq_pelicula_titulo_anio'),
        # Índices
        Index('idx_peliculas_anio', 'anio'),
        Index('idx_peliculas_calificacion', text('calificacion DESC')),
        Index('idx_peliculas_duracion', 'duracion'),
        Index('idx_peliculas_metascore', 'metascore', postgresql_where=text('metascore IS NOT NULL')),
        Index('idx_peliculas_decada_duracion', text('((anio / 10) * 10), duracion')),
        # Esquema
        {'schema': 'imdb'}
    )

    id = Column(Integer, primary_key=True)
    titulo = Column(String(255), nullable=False)
    anio = Column(Integer, nullable=False)
    calificacion = Column(Numeric(3, 1), nullable=False)
    duracion = Column(Integer, nullable=False)  # En minutos
    metascore = Column(Integer)
    created_at = Column(DateTime(timezone=True), default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())

    def to_dict(self):
        return {
            "id": self.id,
            "titulo": self.titulo,
            "anio": self.anio,
            "calificacion": float(self.calificacion),
            "duracion": self.duracion,
            "metascore": self.metascore,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }

class Actor(Base):
    __tablename__ = "actores"
    __table_args__ = (
        Index('idx_actores_nombre', 'nombre'),
        {'schema': 'imdb'}
    )

    id = Column(Integer, primary_key=True)
    nombre = Column(String(100), nullable=False, unique=True)

    def to_dict(self):
        return {
            "id": self.id,
            "nombre": self.nombre
        }

class PeliculaActor(Base):
    __tablename__ = "pelicula_actor"
    __table_args__ = (
        Index('idx_pelicula_actor_actor_id', 'actor_id'),
        Index('idx_pelicula_actor_principal', 'es_principal', postgresql_where=text('es_principal = TRUE')),
        {'schema': 'imdb'}
    )

    pelicula_id = Column(Integer, ForeignKey('imdb.peliculas.id', ondelete='CASCADE'), primary_key=True)
    actor_id = Column(Integer, ForeignKey('imdb.actores.id', ondelete='CASCADE'), primary_key=True)
    es_principal = Column(Boolean, default=False)

    def to_dict(self):
        return {
            "pelicula_id": self.pelicula_id,
            "actor_id": self.actor_id,
            "es_principal": self.es_principal
        }

class PeliculaAudit(Base):
    __tablename__ = "pelicula_audit"
    __table_args__ = {'schema': 'imdb'}

    audit_id = Column(Integer, primary_key=True)
    pelicula_id = Column(Integer, ForeignKey('imdb.peliculas.id'))
    operation = Column(String(10), nullable=False)
    old_data = Column(JSONB)
    new_data = Column(JSONB)
    changed_at = Column(DateTime(timezone=True), default=func.now(), nullable=False)
    changed_by = Column(String(50), default='imdb_scraper')

    def to_dict(self):
        return {
            "audit_id": self.audit_id,
            "pelicula_id": self.pelicula_id,
            "operation": self.operation,
            "old_data": self.old_data,
            "new_data": self.new_data,
            "changed_at": self.changed_at.isoformat() if self.changed_at else None,
            "changed_by": self.changed_by
        }

class ScrapingSession(Base):
    __tablename__ = "scraping_sessions"
    __table_args__ = (
        Index('idx_scraping_sessions_status', 'status'),
        {'schema': 'imdb'}
    )

    id = Column(Integer, primary_key=True)
    session_id = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, index=True)
    scraper_type = Column(String(50), nullable=False)
    movies_scraped = Column(Integer, default=0)
    movies_failed = Column(Integer, default=0)
    start_time = Column(DateTime(timezone=True), default=func.now(), nullable=False)
    end_time = Column(DateTime(timezone=True))
    status = Column(String(20), default="running")
    error_message = Column(String)
    config_data = Column(JSONB)
    total_requests = Column(Integer, default=0)
    failed_requests = Column(Integer, default=0)
    avg_response_time = Column(Numeric)

    def to_dict(self):
        return {
            "id": self.id,
            "session_id": self.session_id,
            "scraper_type": self.scraper_type,
            "movies_scraped": self.movies_scraped,
            "movies_failed": self.movies_failed,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "status": self.status,
            "config_data": self.config_data
        }