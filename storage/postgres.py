import psycopg2
import psycopg2.extras
import logging
import os

logger = logging.getLogger(__name__)


class PostgresStorage:
    """
    Storage genérico para cualquier tipo de dato scrapeado.
    Una sola tabla con JSONB — sin migraciones al agregar nuevos sitios.
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
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS scrape_jobs (
                    id SERIAL PRIMARY KEY,
                    source VARCHAR(100) NOT NULL,
                    record_type VARCHAR(100),
                    search_url TEXT,
                    status VARCHAR(50) DEFAULT 'running',
                    total_results INTEGER DEFAULT 0,
                    started_at TIMESTAMP DEFAULT NOW(),
                    finished_at TIMESTAMP,
                    error TEXT
                );

                CREATE TABLE IF NOT EXISTS scrape_records (
                    id SERIAL PRIMARY KEY,
                    source VARCHAR(100) NOT NULL,
                    record_type VARCHAR(100) NOT NULL,
                    external_id VARCHAR(500),
                    data JSONB NOT NULL,
                    scraped_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(source, external_id)
                );

                ALTER TABLE scrape_jobs ADD COLUMN IF NOT EXISTS record_type VARCHAR(100);
                CREATE INDEX IF NOT EXISTS idx_records_source ON scrape_records(source);
                CREATE INDEX IF NOT EXISTS idx_records_type ON scrape_records(record_type);
                CREATE INDEX IF NOT EXISTS idx_records_source_type ON scrape_records(source, record_type);
                CREATE INDEX IF NOT EXISTS idx_records_data ON scrape_records USING gin(data);
                CREATE INDEX IF NOT EXISTS idx_records_scraped ON scrape_records(scraped_at);
            """)
            self.conn.commit()
        logger.info("Tables ready")

    def start_job(self, source: str, record_type: str, search_url: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO scrape_jobs (source, record_type, search_url) VALUES (%s, %s, %s) RETURNING id",
                (source, record_type, search_url)
            )
            job_id = cur.fetchone()[0]
            self.conn.commit()
            return job_id

    def finish_job(self, job_id: int, total: int, error: str = None):
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE scrape_jobs SET status=%s, total_results=%s, finished_at=NOW(), error=%s WHERE id=%s",
                ("error" if error else "done", total, error, job_id)
            )
            self.conn.commit()

    def save_batch(self, records: list[dict], source: str, record_type: str) -> int:
        if not records:
            return 0

        saved = 0
        with self.conn.cursor() as cur:
            for record in records:
                try:
                    external_id = str(record.get("external_id", ""))
                    cur.execute("""
                        INSERT INTO scrape_records (source, record_type, external_id, data)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (source, external_id) DO UPDATE SET
                            data = EXCLUDED.data,
                            updated_at = NOW()
                    """, (source, record_type, external_id, psycopg2.extras.Json(record)))
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

def migrate(self):
    """Migraciones para tablas existentes."""
    with self.conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE scrape_jobs ADD COLUMN IF NOT EXISTS record_type VARCHAR(100);
        """)
        self.conn.commit()
