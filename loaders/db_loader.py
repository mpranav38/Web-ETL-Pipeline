"""
loaders/db_loader.py — Load cleaned DataFrames into SQLite or PostgreSQL.

SQLite  (zero config, local file):
    loader = DBLoader.sqlite("etl_data.db")
    loader.save(dataframes)

PostgreSQL (requires psycopg2-binary):
    loader = DBLoader.postgres(
        host="localhost", port=5432,
        dbname="mydb", user="etl", password="secret"
    )
    loader.save(dataframes)
"""

import logging
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


class DBLoader:
    """
    Persist DataFrames to a relational database via SQLAlchemy.

    Parameters
    ----------
    engine : sqlalchemy.Engine
        Pre-built engine. Use the class-methods `sqlite()` or `postgres()` for convenience.
    if_exists : str
        One of 'replace', 'append', 'fail' (pandas to_sql behaviour).
        'append'  — add rows to an existing table (safe for incremental runs)
        'replace' — drop and recreate the table each run
    """

    def __init__(self, engine: Engine, if_exists: str = "append"):
        self.engine = engine
        self.if_exists = if_exists

    # ── Convenience constructors ───────────────────────────────────────────

    @classmethod
    def sqlite(cls, db_path: str = "etl_data.db", if_exists: str = "append") -> "DBLoader":
        """Create a loader backed by a local SQLite file."""
        engine = create_engine(f"sqlite:///{db_path}", echo=False)
        logger.info("[DBLoader] SQLite engine → %s", db_path)
        return cls(engine, if_exists=if_exists)

    @classmethod
    def postgres(
        cls,
        host: str = "localhost",
        port: int = 5432,
        dbname: str = "etl",
        user: str = "postgres",
        password: str = "",
        schema: str = "public",
        if_exists: str = "append",
    ) -> "DBLoader":
        """Create a loader backed by PostgreSQL (requires psycopg2-binary)."""
        url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        engine = create_engine(url, echo=False)
        logger.info("[DBLoader] PostgreSQL engine → %s@%s:%s/%s", user, host, port, dbname)
        return cls(engine, if_exists=if_exists)

    # ── Public API ─────────────────────────────────────────────────────────

    def save(self, dataframes: dict[str, pd.DataFrame]) -> dict[str, int]:
        """
        Write each DataFrame to a database table named after its source key.

        Returns a mapping of {table_name: rows_written}.
        """
        results: dict[str, int] = {}

        for name, df in dataframes.items():
            if df.empty:
                logger.warning("[DBLoader] Skipping empty DataFrame '%s'", name)
                continue

            table = self._safe_table_name(name)
            df_clean = self._prepare(df)

            try:
                df_clean.to_sql(
                    table,
                    con=self.engine,
                    if_exists=self.if_exists,
                    index=False,
                    chunksize=500,
                    method="multi",
                )
                rows = len(df_clean)
                results[table] = rows
                logger.info("[DBLoader] '%s' → table '%s': %d rows written", name, table, rows)
            except Exception as exc:  # noqa: BLE001
                logger.error("[DBLoader] Failed to write '%s' to table '%s': %s", name, table, exc)

        return results

    def query(self, sql: str) -> pd.DataFrame:
        """Run an arbitrary SQL query and return the result as a DataFrame."""
        with self.engine.connect() as conn:
            return pd.read_sql(text(sql), conn)

    def table_info(self) -> pd.DataFrame:
        """Return a summary of all tables currently in the database."""
        dialect = self.engine.dialect.name
        if dialect == "sqlite":
            sql = "SELECT name AS table_name FROM sqlite_master WHERE type='table' ORDER BY name"
        else:
            sql = (
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' ORDER BY table_name"
            )
        return self.query(sql)

    # ── Helpers ────────────────────────────────────────────────────────────

    @staticmethod
    def _safe_table_name(name: str) -> str:
        import re
        return re.sub(r"[^\w]", "_", name.lower()).strip("_") or "etl_table"

    @staticmethod
    def _prepare(df: pd.DataFrame) -> pd.DataFrame:
        """Strip internal metadata columns before writing."""
        return df.drop(columns=[c for c in df.columns if c.startswith("_")], errors="ignore")
