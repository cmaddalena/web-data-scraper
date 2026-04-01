#!/usr/bin/env python3
import json
import logging
import os
import re
from urllib.parse import urlparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


def load_site_config(site: str) -> dict:
    config_path = os.path.join(os.path.dirname(__file__), "config", "sites.json")
    with open(config_path) as f:
        all_sites = json.load(f)
    return all_sites.get(site, {})


def extract_domain(url: str) -> str:
    return urlparse(url).netloc.replace("www.", "")


def create_app():
    from fastapi import FastAPI
    from pydantic import BaseModel
    from typing import Optional

    app = FastAPI(title="web-data-scraper")

    class ScrapeRequest(BaseModel):
        url: str
        site: Optional[str] = None        # si no viene, se detecta del dominio
        search: Optional[str] = None      # key del config/sites.json (opcional)
        prompt: Optional[str] = None      # qué extraer (para sitios nuevos)
        max_pages: int = 10
        user_id: int = 1
        job_id: Optional[int] = None
        recipe_id: Optional[int] = None

    class RecipeRequest(BaseModel):
        site: str
        task_name: str = "scraping_lista"

    class SaveRecipeRequest(BaseModel):
        site: str
        task_name: str = "scraping_lista"
        scraper_type: str
        steps: list = []
        extraction_prompt: Optional[str] = None
        has_cloudflare: bool = False
        pagination_pattern: Optional[str] = None

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/sites")
    def list_sites():
        config_path = os.path.join(os.path.dirname(__file__), "config", "sites.json")
        with open(config_path) as f:
            sites = json.load(f)
        return {site: list(cfg["searches"].keys()) for site, cfg in sites.items()}

    # ── RECIPE ENDPOINTS ─────────────────────────────────────────────────────

    @app.post("/recipe/get")
    def get_recipe(req: RecipeRequest):
        from storage.postgres import PostgresStorage
        db = PostgresStorage()
        recipe = db.get_recipe(req.site, req.task_name)
        db.close()
        if recipe:
            return {"found": True, "recipe": recipe}
        return {"found": False}

    @app.post("/recipe/save")
    def save_recipe(req: SaveRecipeRequest):
        from storage.postgres import PostgresStorage
        db = PostgresStorage()
        db.save_recipe(
            req.site, req.task_name, req.scraper_type,
            req.steps, req.extraction_prompt,
            req.has_cloudflare, req.pagination_pattern
        )
        db.close()
        return {"success": True}

    # ── SCRAPE ENDPOINTS ─────────────────────────────────────────────────────

    @app.post("/scrape/requests")
    def scrape_requests(req: ScrapeRequest):
        """HTTP simple — para sitios sin Cloudflare ni JS dinámico"""
        from storage.postgres import PostgresStorage

        domain = extract_domain(req.url)
        site = req.site or domain
        config = load_site_config(site) or {"delay_between_requests": 2, "retries": 3}

        try:
            from scrapers.argenprop import ArgenpropScraper
            scraper = ArgenpropScraper(config)
            results = scraper.scrape(req.url, max_pages=req.max_pages)

            if results:
                db = PostgresStorage()
                job_id = req.job_id or db.start_job(
                    domain, "generic", req.url, "requests", req.user_id, req.recipe_id
                )
                saved = db.save_batch(results, domain, "generic", req.user_id, job_id)
                db.finish_job(job_id, saved)
                db.update_recipe_stats(domain, True)
                db.close()
                return {"success": True, "scraped": len(results), "saved": saved,
                        "method": "requests", "job_id": job_id}
            else:
                return {"success": False, "error": "No results — site may need JS rendering",
                        "method": "requests"}

        except Exception as e:
            return {"success": False, "error": str(e), "method": "requests"}

    @app.post("/scrape/playwright")
    def scrape_playwright(req: ScrapeRequest):
        """Playwright headless — para sitios con JS pero sin Cloudflare duro"""
        # Este endpoint delega al VPS via CDP si está disponible
        # Por ahora retorna not_supported para que n8n escale al siguiente
        return {
            "success": False,
            "error": "Playwright requiere VPS — usar endpoint del agente",
            "method": "playwright",
            "delegate_to": "http://91.98.95.90:8001/scrape/playwright"
        }

    @app.post("/scrape")
    def scrape_legacy(req: ScrapeRequest):
        """Endpoint legacy — usa /scrape/requests por defecto"""
        return scrape_requests(req)

    return app


if __name__ == "__main__":
    import uvicorn
    app = create_app()
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
