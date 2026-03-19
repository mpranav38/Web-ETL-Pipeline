"""
extractors/api_extractor.py — Fetches JSON from REST API endpoints into DataFrames.
"""

import logging
import pandas as pd
import requests

from config import APISource
from utils.http import build_session, get

logger = logging.getLogger(__name__)


class APIExtractor:
    """
    Extracts records from a JSON REST API response.

    Handles both top-level list responses and nested key responses:
        { "results": [...] }   →  set source.data_key = "results"
        [...]                  →  set source.data_key = None
    """

    def __init__(self, delay: float = 1.0, timeout: int = 15, max_retries: int = 3):
        self.delay = delay
        self.timeout = timeout
        self.session = build_session(max_retries=max_retries)

    def extract(self, source: APISource) -> pd.DataFrame:
        logger.info("[APIExtractor] Fetching '%s' from %s", source.name, source.url)
        try:
            resp = get(
                self.session,
                source.url,
                delay=self.delay,
                timeout=self.timeout,
                params=source.params,
                headers=source.headers,
            )
            data = resp.json()

            # Unwrap nested key if specified
            if source.data_key:
                if not isinstance(data, dict) or source.data_key not in data:
                    logger.error(
                        "[APIExtractor] data_key '%s' not found in response for '%s'",
                        source.data_key, source.name,
                    )
                    return pd.DataFrame()
                data = data[source.data_key]

            if not isinstance(data, list):
                logger.warning(
                    "[APIExtractor] Expected a list, got %s for '%s'. Wrapping in list.",
                    type(data).__name__, source.name,
                )
                data = [data]

            if not data:
                logger.warning("[APIExtractor] Empty response for '%s'", source.name)
                return pd.DataFrame()

            df = pd.json_normalize(data)
            df["_source"] = source.name
            df["_source_url"] = source.url
            logger.info("[APIExtractor] '%s' → %d rows, %d columns", source.name, len(df), len(df.columns))
            return df

        except requests.RequestException as exc:
            logger.error("[APIExtractor] HTTP error for '%s': %s", source.name, exc)
        except ValueError as exc:
            logger.error("[APIExtractor] JSON decode error for '%s': %s", source.name, exc)
        except Exception as exc:  # noqa: BLE001
            logger.error("[APIExtractor] Unexpected error for '%s': %s", source.name, exc)

        return pd.DataFrame()
