"""
extractors/product_extractor.py — Scrapes product listings from e-commerce pages.
"""

import logging
import re
import time
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import ProductSource
from utils.http import build_session, get

logger = logging.getLogger(__name__)


class ProductExtractor:
    """
    Scrapes product cards from listing pages, following pagination up to `max_pages`.

    Extracted fields per product:
        name, price_raw, price_numeric, currency, image_url, product_url, page_number
    """

    # Common currency symbols → ISO codes
    CURRENCY_MAP = {
        "$": "USD",
        "€": "EUR",
        "£": "GBP",
        "¥": "JPY",
        "₹": "INR",
    }
    PRICE_RE = re.compile(r"([€$£¥₹]?)\s*([\d,\.]+)")

    def __init__(self, delay: float = 1.0, timeout: int = 15, max_retries: int = 3):
        self.delay = delay
        self.timeout = timeout
        self.session = build_session(max_retries=max_retries)

    def extract(self, source: ProductSource) -> pd.DataFrame:
        logger.info("[ProductExtractor] Scraping '%s' from %s", source.name, source.url)
        all_products: list[dict] = []
        url = source.url
        page = 1

        while url and page <= source.max_pages:
            logger.debug("[ProductExtractor] Page %d: %s", page, url)
            try:
                resp = get(self.session, url, delay=self.delay, timeout=self.timeout)
                soup = BeautifulSoup(resp.text, "html.parser")
            except requests.RequestException as exc:
                logger.error("[ProductExtractor] HTTP error on page %d for '%s': %s", page, source.name, exc)
                break

            products = soup.select(source.product_selector)
            if not products:
                logger.warning(
                    "[ProductExtractor] No products matched selector '%s' on page %d of '%s'",
                    source.product_selector, page, source.name,
                )
                break

            for card in products:
                record = self._parse_card(card, source, base_url=url)
                record["page_number"] = page
                all_products.append(record)

            # Pagination
            next_el = soup.select_one(source.next_page_selector)
            if next_el and page < source.max_pages:
                href = next_el.get("href", "")
                url = urljoin(url, href) if href else None
            else:
                url = None
            page += 1

        if not all_products:
            logger.warning("[ProductExtractor] No products found for '%s'", source.name)
            return pd.DataFrame()

        df = pd.DataFrame(all_products)
        df["_source"] = source.name
        df["_source_url"] = source.url
        logger.info("[ProductExtractor] '%s' → %d products", source.name, len(df))
        return df

    def _parse_card(self, card: BeautifulSoup, source: ProductSource, base_url: str) -> dict:
        name = self._text(card, source.name_selector)
        price_raw = self._text(card, source.price_selector)
        price_numeric, currency = self._parse_price(price_raw)
        image_url = self._attr(card, source.image_selector, "src") or \
                    self._attr(card, source.image_selector, "data-src")
        link = self._attr(card, source.link_selector, "href")
        product_url = urljoin(base_url, link) if link else ""
        return {
            "name": name,
            "price_raw": price_raw,
            "price_numeric": price_numeric,
            "currency": currency,
            "image_url": image_url,
            "product_url": product_url,
        }

    @staticmethod
    def _text(element: BeautifulSoup, selector: str) -> str:
        """Try multiple comma-separated CSS selectors; return first match text."""
        for sel in selector.split(","):
            el = element.select_one(sel.strip())
            if el:
                return el.get_text(separator=" ", strip=True)
        return ""

    @staticmethod
    def _attr(element: BeautifulSoup, selector: str, attr: str) -> str:
        for sel in selector.split(","):
            el = element.select_one(sel.strip())
            if el and el.get(attr):
                return el[attr].strip()
        return ""

    def _parse_price(self, raw: str) -> tuple[float | None, str]:
        if not raw:
            return None, ""
        m = self.PRICE_RE.search(raw)
        if not m:
            return None, ""
        symbol, digits = m.group(1), m.group(2)
        try:
            numeric = float(digits.replace(",", ""))
        except ValueError:
            numeric = None
        currency = self.CURRENCY_MAP.get(symbol, symbol or "")
        return numeric, currency
