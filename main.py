#!/usr/bin/env python3
"""
web-data-scraper - Entry point genérico
Uso: python main.py --site zonaprop --search departamentos_venta_caba --max-pages 10
"""

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
        raise ValueError(f"Site '{site}' not found in config. Available: {list(all_sites.keys())}")
    return all_sites[site]


def get_scraper(site: str, config: dict, proxy_url: str):
    """Factory de scrapers — agregar un nuevo sitio = agregar un import aquí."""
    if site == "zonaprop":
        from scrapers.zonaprop import ZonaPropScraper
        return ZonaPropScraper(config, proxy_url)
    elif site == "argenprop":
        from scrapers.argenprop import ArgenpropScraper
        return ArgenpropScraper(config, proxy_url)
    elif site == "mercadolibre":
        from scrapers.mercadolibre import MercadoLibreScraper
        return MercadoLibreScraper(config, proxy_url)
    else:
        raise ValueError(f"No scraper implemented for '{site}'")


def main():
    parser = argparse.ArgumentParser(description="Generic web data scraper")
    parser.add_argument("--site", required=True, help="Site to scrape (zonaprop, argenprop, mercadolibre)")
    parser.add_argument("--search", required=True, help="Search key from sites.json")
    parser.add_argument("--max-pages", type=int, default=None, help="Max pages to scrape")
    parser.add_argument("--output", default="postgres", choices=["postgres", "csv"], help="Output storage")
    args = parser.parse_args()

    # Cargar config del sitio
    config = load_config(args.site)
    search_url = config["base_url"] + config["searches"].get(args.search, args.search)

    # Proxy solo si el sitio lo requiere
    proxy_url = None
    if config.get("requires_proxy"):
        proxy_url = os.getenv("PROXY_URL")
        if not proxy_url:
            logger.warning("Site requires proxy but PROXY_URL not set")

    # Inicializar scraper
    scraper = get_scraper(args.site, config, proxy_url)

    # Storage
    if args.output == "postgres":
        from storage.postgres import PostgresStorage
        storage = PostgresStorage()
        job_id = storage.start_job(args.site, "property", search_url)
        logger.info(f"Job ID: {job_id}")
    else:
        storage = None
        job_id = None

    # Ejecutar scraping
    try:
        results = scraper.scrape(search_url, max_pages=args.max_pages)

        if storage:
            saved = storage.save_batch(results, args.site, "property")
            storage.finish_job(job_id, saved)
            logger.info(f"Saved {saved}/{len(results)} records to PostgreSQL")
            storage.close()
        else:
            # CSV fallback
            import csv
            output_file = f"{args.site}_{args.search}.csv"
            if results:
                with open(output_file, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=results[0].keys())
                    writer.writeheader()
                    writer.writerows(results)
                logger.info(f"Saved {len(results)} records to {output_file}")

    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        if storage and job_id:
            storage.finish_job(job_id, 0, str(e))
            storage.close()
        sys.exit(1)


if __name__ == "__main__":
    main()
