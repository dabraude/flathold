#!/usr/bin/env python3
"""Rewrite ``db/bank`` to the current bank schema (run once after schema changes).

Alternative: delete the ``db/bank`` directory and upload CSVs again.

Usage: ``uv run python scripts/migrate_bank_table.py``
"""

from __future__ import annotations

import sys

import polars as pl
from deltalake import DeltaTable, write_deltalake

from flathold.data.paths import BANK_TABLE
from flathold.data.schemas import BankSchema
from flathold.data.tables.bank_table import compute_bank_transaction_ids


def _expected_bank_columns() -> list[str]:
    return sorted(BankSchema.to_schema().columns.keys())


def _print_migration_write_error(df: pl.DataFrame, err: Exception) -> None:
    print(f"Migration write failed: {err}", file=sys.stderr)
    print("\n--- DataFrame to write ---", file=sys.stderr)
    print(f"  column count: {len(df.columns)}", file=sys.stderr)
    for name, dtype in zip(df.columns, df.dtypes, strict=True):
        print(f"  {name}: {dtype}", file=sys.stderr)
    print("\n--- Arrow schema (to write) ---", file=sys.stderr)
    try:
        print(df.to_arrow().schema, file=sys.stderr)
    except Exception as arrow_err:
        print(f"  (could not build Arrow schema: {arrow_err})", file=sys.stderr)
    print("\n--- Expected columns (BankSchema) ---", file=sys.stderr)
    print(f"  column count: {len(_expected_bank_columns())}", file=sys.stderr)
    for c in _expected_bank_columns():
        print(f"  {c}", file=sys.stderr)
    expected = set(_expected_bank_columns())
    df_cols = set(df.columns)
    only_in_df = sorted(df_cols - expected)
    only_in_schema = sorted(expected - df_cols)
    if only_in_df or only_in_schema:
        print("\n--- Column name mismatch ---", file=sys.stderr)
        if only_in_df:
            print(f"  only in dataframe (extra): {only_in_df}", file=sys.stderr)
        if only_in_schema:
            print(f"  only in BankSchema (missing): {only_in_schema}", file=sys.stderr)
    print("\n--- Existing Delta table schema (before overwrite) ---", file=sys.stderr)
    try:
        dt = DeltaTable(str(BANK_TABLE))
        print(dt.schema(), file=sys.stderr)
    except Exception as delta_err:
        print(f"  (could not read: {delta_err})", file=sys.stderr)


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


def _add_month_partition(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("Transaction Date").str.to_date("%d/%m/%Y").dt.strftime("%Y-%m").alias("month")
    )


def _assert_all_rows_have_assigned_ids(df: pl.DataFrame) -> None:
    """Every row must have a non-null, non-empty ``id`` before write."""
    if "id" not in df.columns:
        msg = "Migration aborted: expected an 'id' column after processing"
        raise ValueError(msg)
    ids = pl.col("id").cast(pl.Utf8, strict=False)
    missing = df.filter(ids.is_null() | (ids.str.strip_chars() == ""))
    if missing.height > 0:
        msg = (
            f"Migration aborted: {missing.height} row(s) have a null or empty id "
            "(not all ids assigned)"
        )
        raise ValueError(msg)


def _canonicalize_bank_for_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast columns to match :class:`BankSchema` (migration only)."""
    out = df
    if "Transaction Date" in out.columns:
        td = out["Transaction Date"].dtype
        if td == pl.Date or isinstance(td, pl.Datetime):
            out = out.with_columns(
                pl.col("Transaction Date").dt.strftime("%d/%m/%Y").alias("Transaction Date")
            )
        else:
            out = out.with_columns(
                pl.col("Transaction Date").cast(pl.Utf8).alias("Transaction Date")
            )
    for col in (
        "Transaction Type",
        "Sort Code",
        "Account Number",
        "Transaction Description",
        "Balance",
    ):
        if col in out.columns:
            out = out.with_columns(pl.col(col).cast(pl.Utf8).alias(col))
    if "Sort Code" in out.columns:
        out = out.with_columns(pl.col("Sort Code").str.strip_chars("'").alias("Sort Code"))
    if "id" in out.columns:
        out = out.with_columns(pl.col("id").cast(pl.Utf8).alias("id"))
    if "month" in out.columns:
        out = out.with_columns(pl.col("month").cast(pl.Utf8).alias("month"))
    if "Transaction Counter" in out.columns:
        out = out.with_columns(
            pl.col("Transaction Counter").cast(pl.Int64).alias("Transaction Counter")
        )
    out = _ensure_float_debit_credit(out)
    return out


def migrate_bank_table() -> None:
    """Rewrite ``db/bank`` in place to current :class:`BankSchema` (dtypes, ``id``, ``month``).

    Run once if uploads fail after a schema change. Or delete ``db/bank`` and re-import CSVs.
    """
    if not BANK_TABLE.exists():
        return
    df = pl.read_delta(str(BANK_TABLE))
    df = _ensure_float_debit_credit(df)
    df = _canonicalize_bank_for_schema(df)
    if "id" not in df.columns:
        if "month" in df.columns:
            df = df.drop("month")
        df = compute_bank_transaction_ids(df)
    if "month" not in df.columns:
        df = _add_month_partition(df)
    _assert_all_rows_have_assigned_ids(df)
    df = df.unique(subset=["id"], keep="first")
    BankSchema.validate(df)
    try:
        write_deltalake(
            str(BANK_TABLE),
            df.to_arrow(),
            mode="overwrite",
            schema_mode="overwrite",
            partition_by=["month"],
        )
    except Exception as e:
        _print_migration_write_error(df, e)
        raise


if __name__ == "__main__":
    migrate_bank_table()
