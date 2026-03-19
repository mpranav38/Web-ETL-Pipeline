"""
extractors/article_extractor.py — Scrapes article/blog content from listing pages.
"""

import logging
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import ArticleSource
from utils.http import build_session, get

logger = logging.getLogger(__name__)


class ArticleExtractor:
    """
    Scrapes a listing page and optionally follows each article link to retrieve
    the full body text.

    Extracted fields: title, date, author, body_preview, full_url, word_count
    """

    def __init__(self, delay: float = 1.0, timeout: int = 15, max_retries: int = 3):
        self.delay = delay
        self.timeout = timeout
        self.session = build_session(max_retries=max_retries)

    def extract(self, source: ArticleSource) -> pd.DataFrame:
        logger.info("[ArticleExtractor] Scraping '%s' from %s", source.name, source.url)
        try:
            resp = get(self.session, source.url, delay=self.delay, timeout=self.timeout)
            soup = BeautifulSoup(resp.text, "html.parser")
        except requests.RequestException as exc:
            logger.error("[ArticleExtractor] HTTP error for '%s': %s", source.name, exc)
            return pd.DataFrame()

        article_els = soup.select(source.article_list_selector)[: source.max_articles]

        if not article_els:
            logger.warning(
                "[ArticleExtractor] No elements matched '%s' on %s",
                source.article_list_selector, source.url,
            )
            return pd.DataFrame()

        records = []
        for el in article_els:
            record = self._parse_article_element(el, source, base_url=source.url)
            records.append(record)

        df = pd.DataFrame(records)
        df["_source"] = source.name
        df["_source_url"] = source.url
        logger.info("[ArticleExtractor] '%s' → %d articles", source.name, len(df))
        return df

    def _parse_article_element(self, el: BeautifulSoup, source: ArticleSource, base_url: str) -> dict:
        title = self._text(el, source.title_selector)
        date = self._text(el, source.date_selector)
        author = self._text(el, source.author_selector)
        body = self._text(el, source.body_selector, join_all=True)
        link = self._attr(el, source.link_selector, "href")
        full_url = urljoin(base_url, link) if link else ""
        word_count = len(body.split()) if body else 0
        body_preview = body[:500] + "…" if len(body) > 500 else body

        return {
            "title": title,
            "date": date,
            "author": author,
            "body_preview": body_preview,
            "word_count": word_count,
            "full_url": full_url,
        }

    @staticmethod
    def _text(element: BeautifulSoup, selector: str, join_all: bool = False) -> str:
        """Return text from the first (or all, if join_all) matched elements."""
        for sel in selector.split(","):
            sel = sel.strip()
            if join_all:
                els = element.select(sel)
                if els:
                    return " ".join(e.get_text(separator=" ", strip=True) for e in els)
            else:
                el = element.select_one(sel)
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
