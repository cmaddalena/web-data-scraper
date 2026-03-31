import asyncio
import logging
from playwright.async_api import async_playwright

logger = logging.getLogger(__name__)


class ZonaPropScraper:
    """
    Spider para ZonaProp usando Playwright.
    ZonaProp renderiza con React — necesitamos browser real.
    """

    def __init__(self, config: dict, proxy_url: str = None):
        self.config = config
        self.proxy_url = proxy_url
        self.cdp_url = "http://localhost:9222"

    def scrape(self, search_url: str, max_pages: int = None) -> list[dict]:
        return asyncio.run(self._scrape_async(search_url, max_pages))

    async def _scrape_async(self, search_url: str, max_pages: int = None) -> list[dict]:
        async with async_playwright() as p:
            try:
                browser = await p.chromium.connect_over_cdp(self.cdp_url)
                ctx = browser.contexts[0]
                own_browser = False
                logger.info("Connected via CDP")
            except Exception:
                launch_args = ["--no-sandbox", "--disable-dev-shm-usage"]
                if self.proxy_url:
                    launch_args.append(f"--proxy-server={self.proxy_url}")
                ctx = await p.chromium.launch_persistent_context(
                    "/tmp/zonaprop-profile",
                    headless=True,
                    args=launch_args
                )
                own_browser = True
                logger.info("Launched new browser")

            all_results = []
            page = await ctx.new_page()
            page_num = 1

            try:
                while True:
                    url = self._paginate(search_url, page_num)
                    logger.info(f"Scraping page {page_num}: {url}")

                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    await asyncio.sleep(self.config.get("delay_between_requests", 5))

                    results = await self._extract_page(page)
                    if not results:
                        logger.info(f"No results on page {page_num}, stopping")
                        break

                    all_results.extend(results)
                    logger.info(f"Page {page_num}: {len(results)} results (total: {len(all_results)})")

                    if max_pages and page_num >= max_pages:
                        break

                    has_next = await page.query_selector('[data-qa="PAGING_NEXT"]')
                    if not has_next:
                        break

                    page_num += 1

            finally:
                await page.close()
                if own_browser:
                    await ctx.close()
                else:
                    await browser.close()

            logger.info(f"Scrape complete: {len(all_results)} total")
            return all_results

    def _paginate(self, base_url: str, page: int) -> str:
        if page == 1:
            return base_url
        base = base_url.replace(".html", "")
        return f"{base}-pagina-{page}.html"

    async def _extract_page(self, page) -> list[dict]:
        try:
            data = await page.evaluate("""() => {
                const cards = document.querySelectorAll('[data-posting-id]');
                const results = [];
                cards.forEach(card => {
                    try {
                        const id = card.getAttribute('data-posting-id');
                        if (!id) return;
                        const priceEl = card.querySelector('[data-qa="POSTING_CARD_PRICE"]');
                        const expEl = card.querySelector('[data-qa="expensas"]');
                        const addrEl = card.querySelector('[data-qa="POSTING_CARD_LOCATION"]');
                        const featEls = card.querySelectorAll('[data-qa="POSTING_CARD_FEATURES"] span');
                        const descEl = card.querySelector('[data-qa="POSTING_CARD_DESCRIPTION"]');
                        const linkEl = card.querySelector('a[href*="/propiedades/"]');
                        const tagEls = card.querySelectorAll('[data-qa="POSTING_CARD_TAGS"] span');
                        results.push({
                            external_id: id,
                            source: 'zonaprop',
                            record_type: 'property',
                            precio_texto: priceEl ? priceEl.innerText.trim() : '',
                            expensas_texto: expEl ? expEl.innerText.trim() : '',
                            direccion: addrEl ? addrEl.innerText.trim() : '',
                            features: Array.from(featEls).map(el => el.innerText.trim()).filter(t => t),
                            descripcion: descEl ? descEl.innerText.trim().substring(0, 300) : '',
                            url: linkEl ? 'https://www.zonaprop.com.ar' + linkEl.getAttribute('href') : '',
                            tags: Array.from(tagEls).map(el => el.innerText.trim()).filter(t => t)
                        });
                    } catch(e) {}
                });
                return results;
            }""")
            return data or []
        except Exception as e:
            logger.error(f"Error extracting page: {e}")
            return []
