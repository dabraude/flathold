"""Build and maintain a ledger Delta table from the bank table, with stable IDs per transaction."""

import hashlib
import shutil
from dataclasses import dataclass

import polars as pl
from deltalake import write_deltalake

from flathold.bank_delta import read_existing_table
from flathold.constants import LEDGER_TABLE


@dataclass(frozen=True, slots=True)
class UpdateLedgerResult:
    success: bool
    message: str


def read_ledger_table() -> pl.DataFrame | None:
    """Read the ledger Delta table if it exists. Returns None if missing."""
    if not LEDGER_TABLE.exists():
        return None
    try:
        return pl.read_delta(str(LEDGER_TABLE))
    except Exception:
        return None


def ensure_ledger_dir() -> None:
    """Create the ledger directory if it does not exist."""
    LEDGER_TABLE.parent.mkdir(parents=True, exist_ok=True)


def _row_sha256(rows: pl.Series) -> pl.Series:
    """Hash each 'col1=val1|col2=val2|...' string with SHA-256 (for map_batches)."""
    return pl.Series([hashlib.sha256(s.encode("utf-8")).hexdigest() for s in rows])


def _build_ledger_from_bank_df(bank: pl.DataFrame) -> pl.DataFrame:
    """Build ledger from bank table; year, month, day from Transaction Date."""
    ordered = bank.sort(["Transaction Date", "Transaction Counter"])
    if "month" in ordered.columns:
        ordered = ordered.drop("month")
    date_parsed = pl.col("Transaction Date").str.to_date("%d/%m/%Y")
    ordered = ordered.with_columns(
        date_parsed.dt.year().alias("year"),
        date_parsed.dt.month().alias("month"),
        date_parsed.dt.day().alias("day"),
    )
    sorted_cols = sorted(ordered.columns)
    row_str = pl.concat_str(
        [
            pl.concat_str([pl.lit(f"{c}="), pl.col(c).cast(pl.Utf8).fill_null("")])
            for c in sorted_cols
        ],
        separator="|",
    )
    ids = row_str.map_batches(_row_sha256)
    ledger = ordered.with_columns(ids.alias("id")).with_columns(
        pl.lit("").alias("Counter Party"),
        pl.lit("").alias("Item"),
        pl.Series("tags", [[] for _ in range(ordered.height)]).cast(pl.List(pl.Utf8)),
    )
    return ledger


def update_ledger_from_bank() -> UpdateLedgerResult:
    """
    Rebuild the ledger table from the current bank table, assigning a stable id per
    transaction: SHA-256 of the entire row with columns in sorted order.
    """
    ensure_ledger_dir()
    bank = read_existing_table()
    if bank is None or len(bank) == 0:
        return UpdateLedgerResult(
            success=False,
            message="No bank data to build ledger from. Upload statements first.",
        )
    ledger = _build_ledger_from_bank_df(bank)
    write_deltalake(
        str(LEDGER_TABLE),
        ledger.to_arrow(),
        mode="overwrite",
        partition_by=["year", "month"],
    )
    return UpdateLedgerResult(
        success=True,
        message=f"Ledger updated: {len(ledger)} transactions with stable SHA-256 ids.",
    )


def recreate_ledger_from_bank() -> UpdateLedgerResult:
    """
    Delete the ledger table completely and recreate it from the current bank table.
    Use when you want a fresh ledger (e.g. after schema or id logic changes).
    """
    if LEDGER_TABLE.exists():
        shutil.rmtree(LEDGER_TABLE)
    return update_ledger_from_bank()
