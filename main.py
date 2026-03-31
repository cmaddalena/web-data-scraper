#!/usr/bin/env python3
import argparse
import json
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


def load_config(site: str) -> dict:
    config_path = os.path.join(os.path.dirname(__file__), "config", "sites.json")
    with open(config_path) as f:
        all_sites = json.load(f)
    if site not in all_sites:
        raise ValueError(f"Site '{site}' not found. Available: {list(all_sites.keys())}")
    return all_sites[site]


def get_scraper(site: str, config: dict, proxy_url: str = None):
    if site == "zonaprop":
        from scrapers.zonaprop import ZonaPropScraper
        return ZonaPropScraper(config, proxy_url)
    elif site == "argenprop":
        from scrapers.argenprop import ArgenpropScraper
        return ArgenpropScraper(config, proxy_url)
    else:
        raise ValueError(f"No scraper for '{site}'")


def run_scrape(site: str, search: str, max_pages: int = None, output: str = "postgres"):
    config = load_config(site)
    search_url = config["base_url"] + config["searches"].get(search, search)

    proxy_url = None
    if config.get("requires_proxy"):
        proxy_url = os.getenv("PROXY_URL")

    scraper = get_scraper(site, config, proxy_url)

    if output == "postgres":
        from storage.postgres import PostgresStorage
        storage = PostgresStorage()
        job_id = storage.start_job(site, "property", search_url)
        logger.info(f"Job ID: {job_id}")
    else:
        storage = None
        job_id = None

    try:
        results = scraper.scrape(search_url, max_pages=max_pages)

        if storage:
            saved = storage.save_batch(results, site, "property")
            storage.finish_job(job_id, saved)
            logger.info(f"Saved {saved}/{len(results)} records to PostgreSQL")
            storage.close()
            return {"scraped": len(results), "saved": saved, "job_id": job_id}
        else:
            output_file = f"{site}_{search}.csv"
            if results:
                import csv
                with open(output_file, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=results[0].keys())
                    writer.writeheader()
                    writer.writerows(results)
            logger.info(f"Saved {len(results)} records to {output_file}")
            return {"scraped": len(results), "file": output_file}

    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        if storage and job_id:
            storage.finish_job(job_id, 0, str(e))
            storage.close()
        raise


# ── FastAPI server mode ──────────────────────────────────────────────────────

def create_app():
    from fastapi import FastAPI
    from pydantic import BaseModel

    app = FastAPI(title="web-data-scraper")

    class ScrapeRequest(BaseModel):
        site: str = "argenprop"
        search: str = "departamentos_venta_caba"
        max_pages: int = 10

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.post("/scrape")
    def scrape(req: ScrapeRequest):
        try:
            result = run_scrape(req.site, req.search, req.max_pages)
            return {"success": True, **result}
        except Exception as e:
            return {"success": False, "error": str(e)}

    @app.get("/sites")
    def list_sites():
        config_path = os.path.join(os.path.dirname(__file__), "config", "sites.json")
        with open(config_path) as f:
            sites = json.load(f)
        return {
            site: list(cfg["searches"].keys())
            for site, cfg in sites.items()
        }

    return app


# ── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Si se pasan argumentos CLI → modo batch
    if len(sys.argv) > 1:
        parser = argparse.ArgumentParser()
        parser.add_argument("--site", required=True)
        parser.add_argument("--search", required=True)
        parser.add_argument("--max-pages", type=int, default=None)
        parser.add_argument("--output", default="postgres", choices=["postgres", "csv"])
        args = parser.parse_args()
        run_scrape(args.site, args.search, args.max_pages, args.output)
    else:
        # Sin argumentos → modo servidor HTTP
        import uvicorn
        app = create_app()
        uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
