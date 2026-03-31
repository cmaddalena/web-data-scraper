import requests
from bs4 import BeautifulSoup
import logging
import time
import random
import re

logger = logging.getLogger(__name__)


class ArgenpropScraper:
    def __init__(self, config: dict, proxy_url: str = None):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "es-AR,es;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    def scrape(self, search_url: str, max_pages: int = None) -> list[dict]:
        all_results = []
        page = 1

        logger.info(f"Starting scrape: {search_url}")

        while True:
            url = self._paginate(search_url, page)
            logger.info(f"Scraping page {page}: {url}")

            soup = self._get(url)
            if not soup:
                break

            results = self._parse_page(soup)
            if not results:
                logger.info(f"No results on page {page}, stopping")
                break

            all_results.extend(results)
            logger.info(f"Page {page}: {len(results)} results (total: {len(all_results)})")

            if max_pages and page >= max_pages:
                break

            next_btn = soup.select_one('a[data-page-go="next"], .pagination__page--next, [class*="pag"][class*="next"]')
            if not next_btn:
                break

            page += 1
            time.sleep(self.config.get("delay_between_requests", 2) + random.uniform(0.5, 1.5))

        logger.info(f"Scrape complete: {len(all_results)} total")
        return all_results

    def _get(self, url: str) -> BeautifulSoup | None:
        retries = self.config.get("retries", 3)
        for attempt in range(retries):
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                return BeautifulSoup(resp.text, "html.parser")
            except Exception as e:
                logger.warning(f"Attempt {attempt+1}/{retries} failed: {e}")
                time.sleep(3 * (attempt + 1))
        return None

    def _paginate(self, base_url: str, page: int) -> str:
        if page == 1:
            return base_url
        return f"{base_url}--pagina-{page}"

    def _parse_page(self, soup: BeautifulSoup) -> list[dict]:
        listings = soup.select('.listing__item, .posting-card, [class*="listing-card"]')
        results = []
        for item in listings:
            try:
                result = self._parse_item(item)
                if result:
                    results.append(result)
            except Exception as e:
                logger.warning(f"Error parsing item: {e}")
        return results

    def _parse_item(self, item) -> dict | None:
        # Link y ID
        link = item.select_one('a[href*="argenprop.com"], a[href^="/"]')
        if not link:
            return None
        href = link.get('href', '')
        url = f"https://www.argenprop.com{href}" if href.startswith('/') else href
        external_id = href.split('--')[-1].replace('.html', '') if '--' in href else href.split('/')[-1]

        # WhatsApp — directo del atributo data-href
        whatsapp = ''
        wa_el = item.select_one('[data-whatsapp-target], [data-href*="wa.me"]')
        if wa_el:
            wa_href = wa_el.get('data-href', '')
            match = re.search(r'wa\.me/(\d+)', wa_href)
            if match:
                whatsapp = '+' + match.group(1)

        # Precio y moneda desde atributos
        precio = ''
        moneda = ''
        contact_el = item.select_one('[data-precio]')
        if contact_el:
            precio = contact_el.get('data-precio', '')
            moneda = contact_el.get('data-moneda', '')

        # ID anunciante
        anunciante_id = ''
        if contact_el:
            anunciante_id = contact_el.get('data-anunciante-id', '')

        # Precio texto fallback
        price_el = item.select_one('.card__price, [class*="price"]')
        precio_texto = price_el.get_text(strip=True) if price_el else f"{moneda} {precio}".strip()

        # Título
        title_el = item.select_one('.card__title, h2, h3')
        titulo = title_el.get_text(strip=True) if title_el else ''

        # Dirección
        loc_el = item.select_one('.card__address, [class*="address"], [class*="location"]')
        direccion = loc_el.get_text(strip=True) if loc_el else ''

        # Features
        feat_els = item.select('.card__common-data span, [class*="feature"] span')
        features = [f.get_text(strip=True) for f in feat_els if f.get_text(strip=True)]

        # Descripción
        desc_el = item.select_one('.card__description, [class*="description"] p')
        descripcion = desc_el.get_text(strip=True)[:500] if desc_el else ''

        # Inmobiliaria — del texto de descripción o elemento específico
        inmobiliaria = ''
        agency_el = item.select_one('[class*="agency"], [class*="real-estate"], [class*="inmobiliaria"]')
        if agency_el:
            inmobiliaria = agency_el.get_text(strip=True)

        if not titulo and not precio_texto:
            return None

        return {
            "external_id": external_id,
            "source": "argenprop",
            "record_type": "property",
            "titulo": titulo,
            "precio": precio,
            "moneda": moneda,
            "precio_texto": precio_texto,
            "direccion": direccion,
            "features": features,
            "descripcion": descripcion,
            "whatsapp": whatsapp,
            "inmobiliaria": inmobiliaria,
            "anunciante_id": anunciante_id,
            "url": url,
        }
