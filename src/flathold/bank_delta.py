"""Load bank statement CSV into a Delta table."""

from pathlib import Path

import polars as pl
from deltalake import write_deltalake

from flathold.constants import BANK_TABLE

# Columns that identify a transaction (used to avoid re-adding ones already in the table)
_DEDUP_COLS = [
    "Transaction Counter",
    "Transaction Date",
    "Transaction Type",
    "Sort Code",
    "Account Number",
    "Transaction Description",
    "Debit Amount",
    "Credit Amount",
]


def _add_transaction_counter(df: pl.DataFrame) -> pl.DataFrame:
    """Add Transaction Counter column (1, 2, 3, … per day in row order)."""
    return (
        df.with_columns(pl.int_range(0, pl.len()).alias("_rn"))
        .with_columns(
            pl.col("_rn").rank(method="ordinal").over("Transaction Date").cast(pl.Int64).alias("Transaction Counter")
        )
        .drop("_rn")
    )


def _normalize(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize column names, fix Sort Code quoting, and add per-day transaction counter."""
    # Strip whitespace from column names
    df = df.rename({c: c.strip() for c in df.columns})
    # Sort Code sometimes has a leading single quote in the CSV
    if "Sort Code" in df.columns:
        df = df.with_columns(
            pl.col("Sort Code").str.strip_chars("'").alias("Sort Code")
        )
    return _add_transaction_counter(df)


def load_csv_to_dataframe(csv_path: str | Path) -> pl.DataFrame:
    """Load bank statement CSV into a Polars DataFrame with normalized columns."""
    df = pl.read_csv(csv_path)
    return _normalize(df)


def load_csv_bytes_to_dataframe(csv_bytes: bytes) -> pl.DataFrame:
    """Load bank statement CSV from bytes (e.g. uploaded file) into a Polars DataFrame."""
    df = pl.read_csv(csv_bytes)
    return _normalize(df)


def ensure_db_dir() -> None:
    """Create the database directory if it does not exist."""
    BANK_TABLE.parent.mkdir(parents=True, exist_ok=True)


def read_existing_table() -> pl.DataFrame | None:
    """Read existing Delta table if it exists. Returns None if the table is missing."""
    if not BANK_TABLE.exists():
        return None
    try:
        return pl.read_delta(str(BANK_TABLE))
    except Exception:
        return None


def _add_month_partition(df: pl.DataFrame) -> pl.DataFrame:
    """Add month column (YYYY-MM) from Transaction Date for partitioning."""
    return df.with_columns(
        pl.col("Transaction Date").str.to_date("%d/%m/%Y").dt.strftime("%Y-%m").alias("month")
    )


def save_to_delta(df: pl.DataFrame) -> int:
    """Merge upload with existing table, skip transactions already present. Returns total row count."""
    ensure_db_dir()
    existing = read_existing_table()
    df = df.with_columns(pl.col("Transaction Counter").cast(pl.Int64))
    df = _add_month_partition(df)
    if existing is not None:
        existing = existing.with_columns(pl.col("Transaction Counter").cast(pl.Int64))
    combined = pl.concat([existing, df]) if existing is not None else df
    combined = combined.unique(subset=_DEDUP_COLS, keep="first")
    write_deltalake(str(BANK_TABLE), combined.to_arrow(), mode="overwrite", partition_by=["month"])
    return len(combined)
