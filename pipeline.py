"""
pipeline.py — Orchestrates the full Extract → Transform → Load sequence.

Run once:
    python pipeline.py

Run on a schedule (every N minutes):
    python pipeline.py --schedule 30
"""

import argparse
import logging
import sys
from datetime import datetime

import config
from extractors.api_extractor import APIExtractor
from extractors.article_extractor import ArticleExtractor
from extractors.product_extractor import ProductExtractor
from extractors.table_extractor import TableExtractor
from loaders.db_loader import DBLoader
from loaders.file_loader import FileLoader
from transformers.cleaner import DataCleaner
from utils.logger import setup_logger
from utils.notifier import EmailConfig, Notifier, SlackConfig

logger = logging.getLogger(__name__)


# ─── Pipeline ────────────────────────────────────────────────────────────────

def run_pipeline() -> dict:
    """
    Execute one full ETL run.
    Returns a dict summarising results per source.
    """
    start = datetime.now()
    logger.info("=" * 60)
    logger.info("ETL Pipeline run started: %s", start.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 60)

    # ── Instantiate extractors ─────────────────────────────────────────────
    table_ext = TableExtractor(
        delay=config.REQUEST_DELAY,
        timeout=config.REQUEST_TIMEOUT,
        max_retries=config.MAX_RETRIES,
    )
    api_ext = APIExtractor(
        delay=config.REQUEST_DELAY,
        timeout=config.REQUEST_TIMEOUT,
        max_retries=config.MAX_RETRIES,
    )
    product_ext = ProductExtractor(
        delay=config.REQUEST_DELAY,
        timeout=config.REQUEST_TIMEOUT,
        max_retries=config.MAX_RETRIES,
    )
    article_ext = ArticleExtractor(
        delay=config.REQUEST_DELAY,
        timeout=config.REQUEST_TIMEOUT,
        max_retries=config.MAX_RETRIES,
    )
    cleaner = DataCleaner()
    loader = FileLoader(
        output_dir=config.OUTPUT_DIR,
        save_csv=config.SAVE_CSV,
        save_excel=config.SAVE_EXCEL,
        timestamp=config.TIMESTAMP_FILES,
    )

    raw_dataframes: dict = {}

    # ── Extract ────────────────────────────────────────────────────────────
    logger.info("--- EXTRACT ---")

    for src in config.TABLE_SOURCES:
        df = table_ext.extract(src)
        raw_dataframes[src.name] = df

    for src in config.API_SOURCES:
        df = api_ext.extract(src)
        raw_dataframes[src.name] = df

    for src in config.PRODUCT_SOURCES:
        df = product_ext.extract(src)
        raw_dataframes[src.name] = df

    for src in config.ARTICLE_SOURCES:
        df = article_ext.extract(src)
        raw_dataframes[src.name] = df

    total_extracted = sum(len(df) for df in raw_dataframes.values())
    logger.info("Extracted %d total rows across %d sources", total_extracted, len(raw_dataframes))

    # ── Transform ──────────────────────────────────────────────────────────
    logger.info("--- TRANSFORM ---")
    clean_dataframes: dict = {}
    for name, df in raw_dataframes.items():
        clean_dataframes[name] = cleaner.clean(df, source_name=name)

    total_clean = sum(len(df) for df in clean_dataframes.values())
    logger.info("After cleaning: %d rows remain", total_clean)

    # ── Load — Files ──────────────────────────────────────────────
    logger.info("--- LOAD ---")
    output_paths = loader.save(clean_dataframes)

    # ── Load — Database (optional) ──────────────────────────────────────
    db_results: dict = {}
    if getattr(config, "DB_ENABLED", False):
        try:
            if config.DB_BACKEND == "postgres":
                db_loader = DBLoader.postgres(
                    host=config.POSTGRES_HOST,
                    port=config.POSTGRES_PORT,
                    dbname=config.POSTGRES_DB,
                    user=config.POSTGRES_USER,
                    password=config.POSTGRES_PASS,
                    if_exists=config.DB_IF_EXISTS,
                )
            else:
                db_loader = DBLoader.sqlite(
                    db_path=config.SQLITE_PATH,
                    if_exists=config.DB_IF_EXISTS,
                )
            db_results = db_loader.save(clean_dataframes)
            logger.info("DB write complete: %s", db_results)
        except Exception as exc:
            logger.error("DB write failed: %s", exc)

    elapsed = (datetime.now() - start).total_seconds()
    logger.info("=" * 60)
    logger.info("Pipeline complete in %.1fs", elapsed)
    logger.info("CSV files:   %s", output_paths.get("csv_files", []))
    logger.info("Excel files: %s", output_paths.get("excel_files", []))
    if db_results:
        logger.info("DB tables:   %s", db_results)
    logger.info("=" * 60)

    result = {
        "sources": list(raw_dataframes.keys()),
        "rows_extracted": total_extracted,
        "rows_after_cleaning": total_clean,
        "outputs": output_paths,
        "db_tables": db_results,
        "elapsed_seconds": elapsed,
    }

    # ── Notifications (optional) ───────────────────────────────────────────
    if getattr(config, "NOTIFICATIONS_ENABLED", False):
        try:
            email_cfg = None
            if getattr(config, "EMAIL_ENABLED", False):
                email_cfg = EmailConfig(
                    smtp_host=config.EMAIL_SMTP_HOST,
                    smtp_port=config.EMAIL_SMTP_PORT,
                    username=config.EMAIL_USERNAME,
                    password=config.EMAIL_PASSWORD,
                    from_addr=config.EMAIL_FROM or config.EMAIL_USERNAME,
                    to_addrs=config.EMAIL_TO,
                )
            slack_cfg = None
            if getattr(config, "SLACK_ENABLED", False):
                slack_cfg = SlackConfig(
                    webhook_url=config.SLACK_WEBHOOK_URL,
                    channel=config.SLACK_CHANNEL,
                )
            notifier = Notifier(email_cfg=email_cfg, slack_cfg=slack_cfg)
            notifier.notify(result)
        except Exception as exc:
            logger.error("Notification failed: %s", exc)

    return result


# ─── Scheduler ───────────────────────────────────────────────────────────────

def run_scheduled(interval_minutes: int) -> None:
    """Run the pipeline on a recurring schedule using APScheduler."""
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
    except ImportError:
        logger.error(
            "APScheduler is not installed. Run: pip install apscheduler\n"
            "Or use your OS cron/task scheduler with: python pipeline.py"
        )
        sys.exit(1)

    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_pipeline,
        trigger="interval",
        minutes=interval_minutes,
        id="etl_job",
        max_instances=1,
        coalesce=True,
    )

    logger.info("Scheduler started — running every %d minute(s). Press Ctrl+C to stop.", interval_minutes)
    run_pipeline()  # Run immediately on startup

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the web ETL pipeline.")
    parser.add_argument(
        "--schedule",
        type=int,
        metavar="MINUTES",
        default=None,
        help="Run on a recurring schedule every N minutes. Omit for a single run.",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Console log level (default: INFO).",
    )
    args = parser.parse_args()

    import logging as _logging
    setup_logger(log_dir=config.LOG_DIR, level=getattr(_logging, args.log_level))

    if args.schedule:
        run_scheduled(args.schedule)
    else:
        result = run_pipeline()
        sys.exit(0 if result["rows_extracted"] >= 0 else 1)
