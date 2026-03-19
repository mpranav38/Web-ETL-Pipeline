"""
utils/http.py — Shared HTTP session with retry, backoff, and polite delays.
"""

import time
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; ETLPipeline/1.0; "
        "+https://github.com/your-repo/etl-pipeline)"
    ),
    "Accept": "text/html,application/json,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def build_session(max_retries: int = 3, backoff_factor: float = 0.5) -> requests.Session:
    """Return a requests.Session with automatic retry + backoff."""
    session = requests.Session()
    session.headers.update(HEADERS)

    retry = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get(session: requests.Session, url: str, delay: float = 1.0,
        timeout: int = 15, **kwargs) -> requests.Response:
    """
    Polite GET: waits `delay` seconds before each request and logs the call.
    Raises requests.HTTPError for 4xx/5xx responses.
    """
    time.sleep(delay)
    logger.debug("GET %s", url)
    resp = session.get(url, timeout=timeout, **kwargs)
    resp.raise_for_status()
    return resp
