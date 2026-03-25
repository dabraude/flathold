"""Load bank statement CSV into a Delta table."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import polars as pl
from deltalake import write_deltalake

from flathold.data.paths import BANK_TABLE
from flathold.data.schemas import BankSchema


@dataclass(frozen=True, slots=True)
class SaveResult:
    total: int
    new_rows: int
    duplicated: int


def _row_sha256(rows: pl.Series) -> pl.Series:
    return pl.Series([hashlib.sha256(s.encode("utf-8")).hexdigest() for s in rows])


def compute_bank_transaction_ids(df: pl.DataFrame) -> pl.DataFrame:
    """Deterministic ``id`` per row (SHA-256 of canonical row string).

    Matches the historical ledger formula: sort by date/counter, drop partition ``month``,
    add calendar ``year``/``month``/``day`` for hashing, then drop those parts and keep ``id``.
    """
    ordered = df.sort(["Transaction Date", "Transaction Counter"])
    if "month" in ordered.columns:
        ordered = ordered.drop("month")
    if "id" in ordered.columns:
        ordered = ordered.drop("id")
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
    return ordered.with_columns(ids.alias("id")).drop("year", "month", "day")


def _add_transaction_counter(df: pl.DataFrame) -> pl.DataFrame:
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
    df = df.rename({c: c.strip() for c in df.columns})
    if "Sort Code" in df.columns:
        df = df.with_columns(pl.col("Sort Code").str.strip_chars("'").alias("Sort Code"))
    if "Account Number" in df.columns:
        df = df.with_columns(pl.col("Account Number").cast(pl.Utf8).alias("Account Number"))
    df = _ensure_float_debit_credit(df)
    if "Balance" in df.columns:
        df = df.with_columns(pl.col("Balance").cast(pl.Utf8).alias("Balance"))
    return _add_transaction_counter(df)


def load_csv_to_dataframe(csv_path: str | Path) -> pl.DataFrame:
    df = pl.read_csv(csv_path)
    return _normalize(df)


def load_csv_bytes_to_dataframe(csv_bytes: bytes) -> pl.DataFrame:
    df = pl.read_csv(csv_bytes)
    return _normalize(df)


def ensure_db_dir() -> None:
    BANK_TABLE.parent.mkdir(parents=True, exist_ok=True)


def read_existing_table() -> pl.DataFrame | None:
    if not BANK_TABLE.exists():
        return None
    try:
        df = pl.read_delta(str(BANK_TABLE))
        return _ensure_float_debit_credit(df)
    except Exception:
        return None


def _add_month_partition(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("Transaction Date").str.to_date("%d/%m/%Y").dt.strftime("%Y-%m").alias("month")
    )


def save_to_delta(df: pl.DataFrame) -> SaveResult:
    ensure_db_dir()
    existing = read_existing_table()
    existing_count = len(existing) if existing is not None else 0
    df = df.with_columns(pl.col("Transaction Counter").cast(pl.Int64))
    df = compute_bank_transaction_ids(df)
    df = _add_month_partition(df)
    combined = pl.concat([existing, df]) if existing is not None else df
    combined = _ensure_float_debit_credit(combined)
    combined = combined.unique(subset=["id"], keep="first")
    BankSchema.validate(combined)
    total = len(combined)
    new_rows = total - existing_count
    duplicated = len(df) - new_rows
    write_deltalake(
        str(BANK_TABLE),
        combined.to_arrow(),
        mode="overwrite",
        schema_mode="overwrite",
        partition_by=["month"],
    )
    return SaveResult(total=total, new_rows=new_rows, duplicated=duplicated)
