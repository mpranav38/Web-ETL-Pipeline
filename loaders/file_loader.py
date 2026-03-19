"""
loaders/file_loader.py — Save cleaned DataFrames to CSV and/or Excel files.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


class FileLoader:
    """
    Saves DataFrames to the output directory as CSV and/or Excel files.

    Parameters
    ----------
    output_dir : str
        Directory path where output files will be written.
    save_csv : bool
        Write a .csv file for each DataFrame.
    save_excel : bool
        Write a single multi-sheet .xlsx file containing all DataFrames as separate sheets.
    timestamp : bool
        Append a timestamp suffix to filenames to avoid overwriting previous runs.
    """

    def __init__(
        self,
        output_dir: str = "output",
        save_csv: bool = True,
        save_excel: bool = True,
        timestamp: bool = True,
    ):
        self.output_dir = Path(output_dir)
        self.save_csv = save_csv
        self.save_excel = save_excel
        self.timestamp = timestamp
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._ts = datetime.now().strftime("%Y%m%d_%H%M%S") if timestamp else ""

    def _filename(self, base: str, ext: str) -> Path:
        suffix = f"_{self._ts}" if self._ts else ""
        return self.output_dir / f"{base}{suffix}.{ext}"

    def save(self, dataframes: dict[str, pd.DataFrame]) -> dict[str, list[str]]:
        """
        Persist all DataFrames.

        Parameters
        ----------
        dataframes : dict[str, pd.DataFrame]
            Mapping of {source_name: DataFrame}.

        Returns
        -------
        dict with keys "csv_files" and "excel_files" listing paths written.
        """
        outputs: dict[str, list[str]] = {"csv_files": [], "excel_files": []}

        if not dataframes:
            logger.warning("[FileLoader] No DataFrames to save.")
            return outputs

        # ── CSV — one file per source ─────────────────────────────────────
        if self.save_csv:
            for name, df in dataframes.items():
                if df.empty:
                    logger.warning("[FileLoader] Skipping empty DataFrame '%s'", name)
                    continue
                path = self._filename(name, "csv")
                df = self._prepare(df)
                df.to_csv(path, index=False, encoding="utf-8-sig")
                outputs["csv_files"].append(str(path))
                logger.info("[FileLoader] Saved CSV → %s (%d rows)", path, len(df))

        # ── Excel — all sources in one workbook, one sheet each ──────────
        if self.save_excel:
            non_empty = {k: v for k, v in dataframes.items() if not v.empty}
            if non_empty:
                path = self._filename("etl_output", "xlsx")
                with pd.ExcelWriter(path, engine="openpyxl") as writer:
                    for name, df in non_empty.items():
                        sheet = name[:31]  # Excel sheet name limit
                        self._prepare(df).to_excel(writer, sheet_name=sheet, index=False)
                        self._auto_width(writer, sheet, df)
                outputs["excel_files"].append(str(path))
                logger.info("[FileLoader] Saved Excel → %s (%d sheets)", path, len(non_empty))

        # ── Run summary ───────────────────────────────────────────────────
        summary = {
            "run_timestamp": self._ts or datetime.now().isoformat(),
            "sources": {
                name: {
                    "rows": len(df),
                    "columns": list(df.columns),
                    "empty": df.empty,
                }
                for name, df in dataframes.items()
            },
            "outputs": outputs,
        }
        summary_path = self._filename("run_summary", "json")
        summary_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
        logger.info("[FileLoader] Run summary → %s", summary_path)

        return outputs

    @staticmethod
    def _prepare(df: pd.DataFrame) -> pd.DataFrame:
        """Remove internal metadata columns before saving."""
        return df.drop(columns=[c for c in df.columns if c.startswith("_")], errors="ignore")

    @staticmethod
    def _auto_width(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame) -> None:
        """Auto-fit column widths in the Excel sheet."""
        try:
            ws = writer.sheets[sheet_name]
            for i, col in enumerate(df.columns, start=1):
                max_len = max(
                    len(str(col)),
                    df[col].astype(str).str.len().max() if not df[col].empty else 0,
                )
                ws.column_dimensions[ws.cell(1, i).column_letter].width = min(max_len + 4, 60)
        except Exception:  # noqa: BLE001
            pass  # Non-critical — column widths are cosmetic
