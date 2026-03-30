import json
import re
import logging
from bs4 import BeautifulSoup
from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class ZonaPropScraper(BaseScraper):
    """
    Spider para ZonaProp.
    ZonaProp embede todos los datos como JSON en el HTML — no necesitamos
    parsear selectores CSS frágiles.
    """

    def get_total_pages(self, soup: BeautifulSoup) -> int:
        try:
            data = self._extract_json(soup)
            if data:
                total = data.get("totalCount", 0)
                per_page = self.config.get("results_per_page", 20)
                return max(1, (total + per_page - 1) // per_page)
        except Exception as e:
            logger.warning(f"Could not get total pages: {e}")
        return 1

    def parse_listing_page(self, soup: BeautifulSoup) -> list[dict]:
        """Extrae propiedades del JSON embebido en el HTML."""
        data = self._extract_json(soup)
        if not data:
            return []

        postings = data.get("listPostings", [])
        results = []

        for p in postings:
            try:
                result = self._parse_posting(p)
                if result:
                    results.append(result)
            except Exception as e:
                logger.warning(f"Error parsing posting: {e}")

        return results

    def _extract_json(self, soup: BeautifulSoup) -> dict | None:
        """Extrae el JSON con todos los datos embebido en el HTML."""
        scripts = soup.find_all("script")
        for script in scripts:
            if script.string and "listPostings" in script.string:
                # Buscar el objeto JSON
                match = re.search(r"window\.__INITIAL_DATA__\s*=\s*({.*?});", script.string, re.DOTALL)
                if match:
                    try:
                        return json.loads(match.group(1))
                    except json.JSONDecodeError:
                        pass

                # Alternativa: buscar directamente el listado
                match = re.search(r'"listPostings"\s*:\s*(\[.*?\])\s*,\s*"', script.string, re.DOTALL)
                if match:
                    try:
                        return {"listPostings": json.loads(match.group(1))}
                    except json.JSONDecodeError:
                        pass
        return None

    def _parse_posting(self, p: dict) -> dict | None:
        """Normaliza un posting de ZonaProp al formato estándar."""
        try:
            # Precio
            price_data = p.get("price", {})
            price = price_data.get("amount")
            currency = price_data.get("currency", "USD")

            # Ubicación
            geo = p.get("posting_location", {})
            location = geo.get("location", {})
            address = location.get("address", {})
            barrio = location.get("name", "")
            partido = location.get("parent", {}).get("name", "")

            # Geolocalización
            geoloc = geo.get("geolocation", {})
            lat = geoloc.get("latitude")
            lon = geoloc.get("longitude")

            # Características
            features = {}
            for attr in p.get("main_features", []):
                features[attr.get("key", "")] = attr.get("value")

            m2_total = features.get("CFT100", features.get("mt2", ""))
            m2_cubiertos = features.get("CFT101", "")
            ambientes = features.get("rooms", "")
            dormitorios = features.get("bedrooms", "")
            banos = features.get("bathrooms", "")

            # Contacto
            whatsapp = p.get("whatsapp", "")
            publisher = p.get("publisher", {})
            inmobiliaria = publisher.get("name", "")
            telefono = publisher.get("phone", "")

            # URL
            url_rel = p.get("url", "")
            url = f"https://www.zonaprop.com.ar{url_rel}" if url_rel else ""

            return {
                "source": "zonaprop",
                "external_id": str(p.get("posting_id", "")),
                "titulo": p.get("title", ""),
                "descripcion": p.get("description", "")[:500] if p.get("description") else "",
                "precio": price,
                "moneda": currency,
                "operacion": p.get("operation", {}).get("name", ""),
                "tipo_propiedad": p.get("real_estate_type", {}).get("name", ""),
                "m2_total": m2_total,
                "m2_cubiertos": m2_cubiertos,
                "ambientes": ambientes,
                "dormitorios": dormitorios,
                "banos": banos,
                "direccion": address.get("name", ""),
                "barrio": barrio,
                "partido": partido,
                "latitud": lat,
                "longitud": lon,
                "whatsapp": whatsapp,
                "telefono": telefono,
                "inmobiliaria": inmobiliaria,
                "url": url,
                "fecha_publicacion": p.get("publication_date", ""),
                "expensas": p.get("expenses", {}).get("amount") if p.get("expenses") else None,
            }
        except Exception as e:
            logger.warning(f"Error parsing posting data: {e}")
            return None

    def parse_detail_page(self, soup: BeautifulSoup, url: str) -> dict:
        """
        Detalle individual de una propiedad.
        Usado para obtener el teléfono completo que requiere click.
        Por ahora devuelve vacío — se implementa con Playwright si hace falta.
        """
        return {}
