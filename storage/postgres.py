import psycopg2
import psycopg2.extras
import logging
import os

logger = logging.getLogger(__name__)


class PostgresStorage:
    """
    Storage genérico para PostgreSQL.
    Crea las tablas automáticamente si no existen.
    Usa upsert para evitar duplicados.
    """

    def __init__(self, db_url: str = None):
        self.db_url = db_url or os.getenv("DATABASE_URL")
        self.conn = None
        self._connect()
        self._ensure_tables()

    def _connect(self):
        self.conn = psycopg2.connect(self.db_url)
        self.conn.autocommit = False

    def _ensure_tables(self):
        """Crea tablas si no existen — genérico para cualquier tipo de scraping."""
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS scrape_jobs (
                    id SERIAL PRIMARY KEY,
                    source VARCHAR(100) NOT NULL,
                    search_url TEXT,
                    status VARCHAR(50) DEFAULT 'running',
                    total_results INTEGER DEFAULT 0,
                    started_at TIMESTAMP DEFAULT NOW(),
                    finished_at TIMESTAMP,
                    error TEXT
                );

                CREATE TABLE IF NOT EXISTS properties (
                    id SERIAL PRIMARY KEY,
                    source VARCHAR(100) NOT NULL,
                    external_id VARCHAR(200),
                    titulo TEXT,
                    descripcion TEXT,
                    precio NUMERIC,
                    moneda VARCHAR(10),
                    operacion VARCHAR(50),
                    tipo_propiedad VARCHAR(100),
                    m2_total VARCHAR(50),
                    m2_cubiertos VARCHAR(50),
                    ambientes VARCHAR(20),
                    dormitorios VARCHAR(20),
                    banos VARCHAR(20),
                    direccion TEXT,
                    barrio VARCHAR(200),
                    partido VARCHAR(200),
                    latitud FLOAT,
                    longitud FLOAT,
                    whatsapp VARCHAR(50),
                    telefono VARCHAR(50),
                    inmobiliaria TEXT,
                    url TEXT,
                    fecha_publicacion VARCHAR(50),
                    expensas NUMERIC,
                    raw_data JSONB,
                    scraped_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(source, external_id)
                );

                CREATE INDEX IF NOT EXISTS idx_properties_source ON properties(source);
                CREATE INDEX IF NOT EXISTS idx_properties_barrio ON properties(barrio);
                CREATE INDEX IF NOT EXISTS idx_properties_precio ON properties(precio);
            """)
            self.conn.commit()

    def start_job(self, source: str, search_url: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO scrape_jobs (source, search_url) VALUES (%s, %s) RETURNING id",
                (source, search_url)
            )
            job_id = cur.fetchone()[0]
            self.conn.commit()
            return job_id

    def finish_job(self, job_id: int, total: int, error: str = None):
        with self.conn.cursor() as cur:
            cur.execute(
                """UPDATE scrape_jobs SET 
                    status = %s, total_results = %s, 
                    finished_at = NOW(), error = %s
                WHERE id = %s""",
                ("error" if error else "done", total, error, job_id)
            )
            self.conn.commit()

    def save_batch(self, records: list[dict], job_id: int = None) -> int:
        """
        Guarda un batch de registros con upsert.
        Retorna cuántos se insertaron/actualizaron.
        """
        if not records:
            return 0

        saved = 0
        with self.conn.cursor() as cur:
            for record in records:
                try:
                    cur.execute("""
                        INSERT INTO properties (
                            source, external_id, titulo, descripcion,
                            precio, moneda, operacion, tipo_propiedad,
                            m2_total, m2_cubiertos, ambientes, dormitorios, banos,
                            direccion, barrio, partido, latitud, longitud,
                            whatsapp, telefono, inmobiliaria, url,
                            fecha_publicacion, expensas, raw_data
                        ) VALUES (
                            %(source)s, %(external_id)s, %(titulo)s, %(descripcion)s,
                            %(precio)s, %(moneda)s, %(operacion)s, %(tipo_propiedad)s,
                            %(m2_total)s, %(m2_cubiertos)s, %(ambientes)s, %(dormitorios)s, %(banos)s,
                            %(direccion)s, %(barrio)s, %(partido)s, %(latitud)s, %(longitud)s,
                            %(whatsapp)s, %(telefono)s, %(inmobiliaria)s, %(url)s,
                            %(fecha_publicacion)s, %(expensas)s, %(raw_data)s
                        )
                        ON CONFLICT (source, external_id) DO UPDATE SET
                            precio = EXCLUDED.precio,
                            whatsapp = EXCLUDED.whatsapp,
                            telefono = EXCLUDED.telefono,
                            updated_at = NOW()
                    """, {**record, "raw_data": psycopg2.extras.Json(record)})
                    saved += 1
                except Exception as e:
                    logger.warning(f"Error saving record {record.get('external_id')}: {e}")
                    self.conn.rollback()
                    continue

            self.conn.commit()

        return saved

    def close(self):
        if self.conn:
            self.conn.close()
