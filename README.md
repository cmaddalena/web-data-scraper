# web-data-scraper

Scraper genérico y modular para extracción masiva de datos de múltiples sitios web.

## Sitios soportados
- ZonaProp (con bypass Cloudflare)
- Argenprop (próximamente)
- MercadoLibre inmuebles (próximamente)

## Uso

```bash
# Instalar
pip install -r requirements.txt

# Scraping ZonaProp CABA
python main.py --site zonaprop --search departamentos_venta_caba

# Con límite de páginas (para testing)
python main.py --site zonaprop --search departamentos_venta_caba --max-pages 5

# Output CSV en lugar de PostgreSQL
python main.py --site zonaprop --search departamentos_venta_caba --output csv
```

## Variables de entorno

```
DATABASE_URL=postgresql://user:pass@host:5432/dbname
PROXY_URL=http://user:pass@proxy.host:port
```

## Agregar un nuevo sitio

1. Agregar config en `config/sites.json`
2. Crear `scrapers/nuevo_sitio.py` heredando de `BaseScraper`
3. Agregar el import en `main.py`

## Estructura

```
scrapers/
  base_scraper.py   # Clase base genérica
  zonaprop.py       # Spider ZonaProp
config/
  sites.json        # Config por sitio (URLs, delays, paginación)
storage/
  postgres.py       # Guardar en PostgreSQL con upsert
main.py             # Entry point
```
