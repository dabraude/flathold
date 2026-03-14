"""Build and maintain a ledger Delta table from the bank table, with stable IDs per transaction."""

import hashlib
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
    ordered = bank.sort(["Transaction Date", "Transaction Counter"])
    # Canonical row string: col1=val1|col2=val2|... with columns in sorted order, nulls as ""
    sorted_cols = sorted(ordered.columns)
    row_str = pl.concat_str(
        [
            pl.concat_str([pl.lit(f"{c}="), pl.col(c).cast(pl.Utf8).fill_null("")])
            for c in sorted_cols
        ],
        separator="|",
    )
    ids = row_str.map_batches(_row_sha256)
    ledger = ordered.with_columns(ids.alias("id"))
    write_deltalake(
        str(LEDGER_TABLE),
        ledger.to_arrow(),
        mode="overwrite",
        partition_by=["month"],
    )
    return UpdateLedgerResult(
        success=True,
        message=f"Ledger updated: {len(ledger)} transactions with stable SHA-256 ids.",
    )
