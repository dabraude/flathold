"""Load bank statement CSV into a Delta table."""

from dataclasses import dataclass
from pathlib import Path

import polars as pl
from deltalake import write_deltalake

from flathold.constants import BANK_TABLE


@dataclass(frozen=True, slots=True)
class SaveResult:
    total: int
    new_rows: int
    duplicated: int


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
            pl.col("_rn")
            .rank(method="ordinal")
            .over("Transaction Date")
            .cast(pl.Int64)
            .alias("Transaction Counter")
        )
        .drop("_rn")
    )


def _ensure_float_debit_credit(df: pl.DataFrame) -> pl.DataFrame:
    """Cast Debit Amount and Credit Amount to Float64 and fill nulls with 0.0."""
    updates = []
    if "Debit Amount" in df.columns:
        updates.append(pl.col("Debit Amount").cast(pl.Float64).fill_null(0.0).alias("Debit Amount"))
    if "Credit Amount" in df.columns:
        updates.append(
            pl.col("Credit Amount").cast(pl.Float64).fill_null(0.0).alias("Credit Amount")
        )
    if not updates:
        return df
    return df.with_columns(updates)


def _normalize(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize column names, Sort Code quoting, debit/credit as float, per-day counter."""
    # Strip whitespace from column names
    df = df.rename({c: c.strip() for c in df.columns})
    # Sort Code sometimes has a leading single quote in the CSV
    if "Sort Code" in df.columns:
        df = df.with_columns(pl.col("Sort Code").str.strip_chars("'").alias("Sort Code"))
    df = _ensure_float_debit_credit(df)
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
        df = pl.read_delta(str(BANK_TABLE))
        return _ensure_float_debit_credit(df)
    except Exception:
        return None


def _add_month_partition(df: pl.DataFrame) -> pl.DataFrame:
    """Add month column (YYYY-MM) from Transaction Date for partitioning."""
    return df.with_columns(
        pl.col("Transaction Date").str.to_date("%d/%m/%Y").dt.strftime("%Y-%m").alias("month")
    )


def save_to_delta(df: pl.DataFrame) -> SaveResult:
    """Merge upload with existing table, skip transactions already present."""
    ensure_db_dir()
    existing = read_existing_table()
    existing_count = len(existing) if existing is not None else 0
    df = df.with_columns(pl.col("Transaction Counter").cast(pl.Int64))
    df = _add_month_partition(df)
    if existing is not None:
        existing = existing.with_columns(pl.col("Transaction Counter").cast(pl.Int64))
    combined = pl.concat([existing, df]) if existing is not None else df
    combined = _ensure_float_debit_credit(combined)
    combined = combined.unique(subset=_DEDUP_COLS, keep="first")
    total = len(combined)
    new_rows = total - existing_count
    duplicated = len(df) - new_rows
    write_deltalake(str(BANK_TABLE), combined.to_arrow(), mode="overwrite", partition_by=["month"])
    return SaveResult(total=total, new_rows=new_rows, duplicated=duplicated)
