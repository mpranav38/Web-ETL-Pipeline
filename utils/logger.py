"""
utils/logger.py — Structured file + console logging for the ETL pipeline.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path


def setup_logger(log_dir: str = "logs", level: int = logging.INFO) -> logging.Logger:
    """
    Configure the root logger with:
      - a rotating timestamped file handler
      - a console (stdout) handler
    Returns the root logger so callers can use logging.getLogger(__name__).
    """
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = Path(log_dir) / f"etl_run_{timestamp}.log"

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(level)

    # File handler — full detail
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    root.addHandler(fh)

    # Console handler — INFO and above
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    root.addHandler(ch)

    root.info("Logging initialised → %s", log_file)
    return root
