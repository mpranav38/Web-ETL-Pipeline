"""
transformers/cleaner.py — Universal data cleaning pipeline for all source types.

Cleaning steps applied (in order):
  1. Strip whitespace from all string columns
  2. Standardise column names (lowercase + underscores)
  3. Drop fully-duplicate rows
  4. Drop columns that are entirely empty
  5. Remove internal metadata columns (_source, _source_url) before further ops
  6. Coerce obvious numeric columns
  7. Parse date-like columns to datetime
  8. Fill remaining NaN strings with empty string (numeric NaN left as-is)
  9. Normalise price columns (strip symbols, convert to float)
  10. Deduplicate by key columns if provided
"""

import logging
import re
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Regex: matches strings that look like prices, e.g. "$1,234.56" or "1 234,99 €"
_PRICE_LIKE = re.compile(r"^[\$£€¥₹]?\s*[\d,\. ]+[\$£€¥₹]?$")
_STRIP_PRICE = re.compile(r"[^\d\.]")


class DataCleaner:
    """
    Apply a configurable sequence of cleaning operations to a DataFrame.

    Parameters
    ----------
    dedupe_keys : list[str] | None
        Column names to use when deduplicating. If None, entire rows are compared.
    date_columns : list[str] | None
        Columns to explicitly parse as dates.  Auto-detection also runs on all
        columns with "date", "time", or "published" in their name.
    """

    def __init__(
        self,
        dedupe_keys: Optional[list[str]] = None,
        date_columns: Optional[list[str]] = None,
    ):
        self.dedupe_keys = dedupe_keys
        self.date_columns = date_columns or []

    def clean(self, df: pd.DataFrame, source_name: str = "") -> pd.DataFrame:
        if df.empty:
            logger.warning("[Cleaner] Received empty DataFrame for '%s' — skipping.", source_name)
            return df

        tag = f"[Cleaner:{source_name}]" if source_name else "[Cleaner]"
        original_rows = len(df)
        logger.info("%s Starting clean: %d rows × %d columns", tag, original_rows, len(df.columns))

        df = df.copy()

        df = self._standardise_columns(df, tag)
        df = self._strip_strings(df, tag)
        df = self._drop_empty_columns(df, tag)
        df = self._drop_duplicates(df, tag)
        df = self._coerce_numerics(df, tag)
        df = self._parse_dates(df, tag)
        df = self._clean_prices(df, tag)
        df = self._fill_string_nulls(df, tag)

        logger.info(
            "%s Done: %d → %d rows (removed %d), %d columns",
            tag, original_rows, len(df), original_rows - len(df), len(df.columns),
        )
        return df

    # ── Step 1: Standardise column names ───────────────────────────────────

    @staticmethod
    def _standardise_columns(df: pd.DataFrame, tag: str) -> pd.DataFrame:
        def norm(col: str) -> str:
            col = str(col).strip().lower()
            col = re.sub(r"[\s\-/\.]+", "_", col)
            col = re.sub(r"[^\w]", "", col)
            col = re.sub(r"_+", "_", col).strip("_")
            return col or "column"

        df.columns = [norm(c) for c in df.columns]
        # Handle duplicate column names after normalisation
        seen: dict[str, int] = {}
        new_cols = []
        for col in df.columns:
            if col in seen:
                seen[col] += 1
                new_cols.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                new_cols.append(col)
        df.columns = new_cols
        return df

    # ── Step 2: Strip leading/trailing whitespace ───────────────────────────

    @staticmethod
    def _strip_strings(df: pd.DataFrame, tag: str) -> pd.DataFrame:
        str_cols = df.select_dtypes(include="object").columns
        df[str_cols] = df[str_cols].apply(lambda s: s.str.strip())
        # Replace empty strings with NaN for consistent null handling downstream
        df[str_cols] = df[str_cols].replace("", pd.NA)
        return df

    # ── Step 3: Drop columns that are entirely null ─────────────────────────

    @staticmethod
    def _drop_empty_columns(df: pd.DataFrame, tag: str) -> pd.DataFrame:
        empty_cols = [c for c in df.columns if df[c].isna().all()]
        if empty_cols:
            logger.debug("%s Dropping empty columns: %s", tag, empty_cols)
            df = df.drop(columns=empty_cols)
        return df

    # ── Step 4: Deduplicate ─────────────────────────────────────────────────

    def _drop_duplicates(self, df: pd.DataFrame, tag: str) -> pd.DataFrame:
        before = len(df)
        subset = [k for k in (self.dedupe_keys or []) if k in df.columns] or None
        df = df.drop_duplicates(subset=subset, keep="first")
        removed = before - len(df)
        if removed:
            logger.debug("%s Removed %d duplicate rows", tag, removed)
        return df

    # ── Step 5: Coerce numeric columns ─────────────────────────────────────

    @staticmethod
    def _coerce_numerics(df: pd.DataFrame, tag: str) -> pd.DataFrame:
        for col in df.select_dtypes(include="object").columns:
            if col.startswith("_"):
                continue
            converted = pd.to_numeric(df[col], errors="coerce")
            # Only convert if >60% of non-null values parsed successfully
            non_null = df[col].notna().sum()
            parsed = converted.notna().sum()
            if non_null > 0 and (parsed / non_null) >= 0.6:
                df[col] = converted
                logger.debug("%s Coerced '%s' to numeric", tag, col)
        return df

    # ── Step 6: Parse date columns ─────────────────────────────────────────

    def _parse_dates(self, df: pd.DataFrame, tag: str) -> pd.DataFrame:
        date_hints = {"date", "time", "published", "created", "updated", "timestamp"}
        auto_cols = [
            c for c in df.select_dtypes(include="object").columns
            if any(hint in c.lower() for hint in date_hints)
        ]
        target_cols = list(set(auto_cols + [c for c in self.date_columns if c in df.columns]))

        for col in target_cols:
            try:
                parsed = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True)
                if parsed.notna().sum() > 0:
                    df[col] = parsed
                    logger.debug("%s Parsed '%s' as datetime", tag, col)
            except Exception:  # noqa: BLE001
                pass
        return df

    # ── Step 7: Clean price columns ────────────────────────────────────────

    @staticmethod
    def _clean_prices(df: pd.DataFrame, tag: str) -> pd.DataFrame:
        price_hints = {"price", "cost", "amount", "fee", "rate"}
        for col in df.select_dtypes(include="object").columns:
            if not any(hint in col.lower() for hint in price_hints):
                continue
            sample = df[col].dropna().head(20)
            if sample.empty:
                continue
            price_matches = sample.apply(lambda v: bool(_PRICE_LIKE.match(str(v).strip())))
            if price_matches.mean() >= 0.5:
                cleaned = df[col].astype(str).apply(
                    lambda v: _STRIP_PRICE.sub("", v).replace(",", "") if v != "nan" else ""
                )
                df[col + "_numeric"] = pd.to_numeric(cleaned, errors="coerce")
                logger.debug("%s Extracted numeric price column '%s_numeric'", tag, col)
        return df

    # ── Step 8: Fill string NaN with empty string ───────────────────────────

    @staticmethod
    def _fill_string_nulls(df: pd.DataFrame, tag: str) -> pd.DataFrame:
        str_cols = df.select_dtypes(include="object").columns
        df[str_cols] = df[str_cols].fillna("")
        return df
