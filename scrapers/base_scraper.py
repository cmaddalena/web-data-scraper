import cloudscraper
import requests
from bs4 import BeautifulSoup
import time
import random
import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """
    Clase base genérica para todos los scrapers.
    Cada sitio hereda de esta clase e implementa sus métodos específicos.
    """

    def __init__(self, config: dict, proxy_url: str = None):
        self.config = config
        self.proxy_url = proxy_url
        self.session = self._create_session()

    def _create_session(self):
        """Crea sesión con bypass de Cloudflare y proxy si corresponde."""
        scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True}
        )
        if self.proxy_url:
            scraper.proxies = {"http": self.proxy_url, "https": self.proxy_url}
        return scraper

    def get(self, url: str) -> BeautifulSoup | None:
        """GET con reintentos, delays y manejo de errores."""
        delay = self.config.get("delay_between_requests", 2)
        retries = self.config.get("retries", 3)

        for attempt in range(retries):
            try:
                time.sleep(delay + random.uniform(0.5, 1.5))
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return BeautifulSoup(response.text, "html.parser")
            except Exception as e:
                logger.warning(f"Attempt {attempt+1}/{retries} failed for {url}: {e}")
                if attempt < retries - 1:
                    time.sleep(delay * (attempt + 2))
        return None

    def paginate_url(self, base_url: str, page: int) -> str:
        """Genera URL paginada según el patrón del sitio."""
        pattern = self.config.get("pagination_pattern", "{base_url}-pagina-{page}.html")
        base = base_url.replace(".html", "")
        return pattern.format(base_url=base, page=page)

    @abstractmethod
    def parse_listing_page(self, soup: BeautifulSoup) -> list[dict]:
        """Extrae listado de propiedades de una página de resultados."""
        pass

    @abstractmethod
    def parse_detail_page(self, soup: BeautifulSoup, url: str) -> dict:
        """Extrae detalle completo de una propiedad individual."""
        pass

    @abstractmethod
    def get_total_pages(self, soup: BeautifulSoup) -> int:
        """Detecta el total de páginas disponibles."""
        pass

    def scrape(self, search_url: str, max_pages: int = None) -> list[dict]:
        """
        Entry point principal. Pagina y extrae todos los resultados.
        Genérico — funciona para cualquier sitio que herede esta clase.
        """
        all_results = []
        page = 1

        logger.info(f"Starting scrape: {search_url}")

        while True:
            url = self.paginate_url(search_url, page) if page > 1 else search_url
            logger.info(f"Scraping page {page}: {url}")

            soup = self.get(url)
            if not soup:
                logger.error(f"Failed to fetch page {page}, stopping.")
                break

            # Detectar total de páginas en primera iteración
            if page == 1:
                total_pages = self.get_total_pages(soup)
                if max_pages:
                    total_pages = min(total_pages, max_pages)
                logger.info(f"Total pages: {total_pages}")

            results = self.parse_listing_page(soup)
            if not results:
                logger.info(f"No results on page {page}, stopping.")
                break

            all_results.extend(results)
            logger.info(f"Page {page}: {len(results)} results (total: {len(all_results)})")

            if page >= total_pages:
                break

            page += 1

        logger.info(f"Scrape complete: {len(all_results)} total results")
        return all_results
