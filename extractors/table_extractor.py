"""
extractors/table_extractor.py — Scrapes HTML <table> elements into DataFrames.
"""

import logging
import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import TableSource
from utils.http import build_session, get

logger = logging.getLogger(__name__)


class TableExtractor:
    """
    Extracts structured data from HTML tables on a web page.

    Usage:
        extractor = TableExtractor()
        df = extractor.extract(source)
    """

    def __init__(self, delay: float = 1.0, timeout: int = 15, max_retries: int = 3):
        self.delay = delay
        self.timeout = timeout
        self.session = build_session(max_retries=max_retries)

    def extract(self, source: TableSource) -> pd.DataFrame:
        """
        Fetch `source.url` and parse the table at `source.table_index`.
        Returns an empty DataFrame on failure (never raises).
        """
        logger.info("[TableExtractor] Extracting '%s' from %s", source.name, source.url)
        try:
            resp = get(self.session, source.url, delay=self.delay, timeout=self.timeout)
            soup = BeautifulSoup(resp.text, "html.parser")
            tables = soup.find_all("table")

            if not tables:
                logger.warning("[TableExtractor] No <table> elements found at %s", source.url)
                return pd.DataFrame()

            if source.table_index >= len(tables):
                logger.warning(
                    "[TableExtractor] table_index=%d but only %d tables found at %s",
                    source.table_index, len(tables), source.url,
                )
                return pd.DataFrame()

            # Use pandas read_html for robust table parsing
            dfs = pd.read_html(str(tables[source.table_index]))
            df = dfs[0]

            if source.headers:
                df.columns = source.headers[: len(df.columns)]

            df["_source"] = source.name
            df["_source_url"] = source.url
            logger.info("[TableExtractor] '%s' → %d rows, %d columns", source.name, len(df), len(df.columns))
            return df

        except requests.RequestException as exc:
            logger.error("[TableExtractor] HTTP error for '%s': %s", source.name, exc)
        except Exception as exc:  # noqa: BLE001
            logger.error("[TableExtractor] Unexpected error for '%s': %s", source.name, exc)

        return pd.DataFrame()
