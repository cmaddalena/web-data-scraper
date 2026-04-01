import psycopg2
import psycopg2.extras
import logging
import os

logger = logging.getLogger(__name__)

DEFAULT_USER_ID = 1  # Charly Maddalena


class PostgresStorage:
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
                    user_id INTEGER,
                    recipe_id INTEGER,
                    source VARCHAR(100) NOT NULL,
                    record_type VARCHAR(100) DEFAULT 'generic',
                    search_url TEXT,
                    scraper_type VARCHAR(50),
                    status VARCHAR(50) DEFAULT 'running',
                    total_results INTEGER DEFAULT 0,
                    started_at TIMESTAMP DEFAULT NOW(),
                    finished_at TIMESTAMP,
                    duration_seconds FLOAT,
                    error TEXT
                );

                CREATE TABLE IF NOT EXISTS scrape_records (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER,
                    job_id INTEGER,
                    source VARCHAR(100) NOT NULL,
                    record_type VARCHAR(100) NOT NULL,
                    external_id VARCHAR(500),
                    data JSONB NOT NULL,
                    scraped_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(source, external_id)
                );

                CREATE INDEX IF NOT EXISTS idx_records_source ON scrape_records(source);
                CREATE INDEX IF NOT EXISTS idx_records_type ON scrape_records(record_type);
                CREATE INDEX IF NOT EXISTS idx_records_user ON scrape_records(user_id);
                CREATE INDEX IF NOT EXISTS idx_records_data ON scrape_records USING gin(data);
                CREATE INDEX IF NOT EXISTS idx_jobs_status ON scrape_jobs(status);
            """)
            self.conn.commit()
        logger.info("Tables ready")

    def start_job(self, source: str, record_type: str, search_url: str,
                  scraper_type: str = "requests", user_id: int = DEFAULT_USER_ID,
                  recipe_id: int = None) -> int:
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO scrape_jobs
                    (user_id, recipe_id, source, record_type, search_url, scraper_type)
                VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
            """, (user_id, recipe_id, source, record_type, search_url, scraper_type))
            job_id = cur.fetchone()[0]
            self.conn.commit()
            return job_id

    def finish_job(self, job_id: int, total: int, error: str = None):
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE scrape_jobs SET
                    status = %s, total_results = %s,
                    finished_at = NOW(),
                    duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at)),
                    error = %s
                WHERE id = %s
            """, ("error" if error else "done", total, error, job_id))
            self.conn.commit()

    def save_batch(self, records: list, source: str, record_type: str,
                   user_id: int = DEFAULT_USER_ID, job_id: int = None) -> int:
        if not records:
            return 0
        saved = 0
        with self.conn.cursor() as cur:
            for record in records:
                try:
                    external_id = str(record.get("external_id", ""))
                    cur.execute("""
                        INSERT INTO scrape_records
                            (user_id, job_id, source, record_type, external_id, data)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (source, external_id) DO UPDATE SET
                            data = EXCLUDED.data, updated_at = NOW()
                    """, (user_id, job_id, source, record_type, external_id,
                          psycopg2.extras.Json(record)))
                    saved += 1
                except Exception as e:
                    logger.warning(f"Error saving record: {e}")
                    self.conn.rollback()
            self.conn.commit()
        return saved

    def get_recipe(self, site: str, task_name: str = "scraping_lista") -> dict | None:
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT r.* FROM site_recipes r
                JOIN tasks t ON t.id = r.task_id
                WHERE r.site = %s AND t.name = %s AND r.success_rate > 0.3
                ORDER BY r.success_rate DESC LIMIT 1
            """, (site, task_name))
            row = cur.fetchone()
            return dict(row) if row else None

    def save_recipe(self, site: str, task_name: str, scraper_type: str,
                    steps: list, extraction_prompt: str = None,
                    has_cloudflare: bool = False, pagination_pattern: str = None):
        with self.conn.cursor() as cur:
            cur.execute("SELECT id FROM tasks WHERE name = %s", (task_name,))
            task = cur.fetchone()
            if not task:
                return
            cur.execute("""
                INSERT INTO site_recipes
                    (task_id, site, scraper_type, steps, extraction_prompt,
                     has_cloudflare, pagination_pattern, learned_from_ai, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, true, NOW())
                ON CONFLICT (task_id, site) DO UPDATE SET
                    scraper_type = EXCLUDED.scraper_type,
                    steps = EXCLUDED.steps,
                    extraction_prompt = EXCLUDED.extraction_prompt,
                    updated_at = NOW()
            """, (task[0], site, scraper_type, psycopg2.extras.Json(steps),
                  extraction_prompt, has_cloudflare, pagination_pattern))
            self.conn.commit()

    def update_recipe_stats(self, site: str, success: bool):
        with self.conn.cursor() as cur:
            if success:
                cur.execute("""
                    UPDATE site_recipes SET times_used = times_used + 1,
                    last_success = NOW(),
                    success_rate = LEAST(1.0, (success_rate * times_used + 1.0) / (times_used + 1))
                    WHERE site = %s
                """, (site,))
            else:
                cur.execute("""
                    UPDATE site_recipes SET fail_count = fail_count + 1,
                    last_failed = NOW(),
                    success_rate = GREATEST(0.0, (success_rate * times_used) / NULLIF(times_used + 1, 0))
                    WHERE site = %s
                """, (site,))
            self.conn.commit()

    def close(self):
        if self.conn:
            self.conn.close()
