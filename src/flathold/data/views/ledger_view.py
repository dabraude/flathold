"""Base ledger: bank union manual, stable ids, optional ``ledger_source``, tags list."""

from __future__ import annotations

import polars as pl

from flathold.data.schemas import LEDGER_COLUMN_NAMES
from flathold.data.tables.bank_table import compute_bank_transaction_ids, read_existing_table
from flathold.data.tables.manual_ledger_table import read_manual_ledger_table
from flathold.data.tables.tag_definitions_table import ensure_tag_definitions_table
from flathold.data.tables.transaction_tags_table import read_transaction_tags_table


def build_ledger_from_bank_df(bank: pl.DataFrame) -> pl.DataFrame:
    """Build ledger from bank table; year, month, day from Transaction Date; uses stored ``id``."""
    ordered = bank.sort(["Transaction Date", "Transaction Counter"])
    if "month" in ordered.columns:
        ordered = ordered.drop("month")
    if "id" not in ordered.columns:
        ordered = compute_bank_transaction_ids(ordered)
    date_parsed = pl.col("Transaction Date").str.to_date("%d/%m/%Y")
    ordered = ordered.with_columns(
        date_parsed.dt.year().alias("year"),
        date_parsed.dt.month().alias("month"),
        date_parsed.dt.day().alias("day"),
    )
    return ordered.drop("Balance")


def _align_ledger_columns(df: pl.DataFrame) -> pl.DataFrame:
    aligned = df.select(LEDGER_COLUMN_NAMES)
    return aligned.with_columns(
        pl.col("Transaction Counter").cast(pl.Int64, strict=False),
        pl.col("Transaction Date").cast(pl.Utf8, strict=False),
        pl.col("Transaction Type").cast(pl.Utf8, strict=False),
        pl.col("Sort Code").cast(pl.Utf8, strict=False),
        pl.col("Account Number").cast(pl.Utf8, strict=False),
        pl.col("Transaction Description").cast(pl.Utf8, strict=False),
        pl.col("Debit Amount").cast(pl.Float64, strict=False),
        pl.col("Credit Amount").cast(pl.Float64, strict=False),
        pl.col("id").cast(pl.Utf8, strict=False),
        pl.col("year").cast(pl.Int64, strict=False),
        pl.col("month").cast(pl.Int64, strict=False),
        pl.col("day").cast(pl.Int64, strict=False),
    )


def combine_bank_and_manual_ledger(
    bank_ledger: pl.DataFrame | None,
    manual_ledger: pl.DataFrame | None,
) -> pl.DataFrame | None:
    """Union bank-derived and manual rows (no ``ledger_source``)."""
    parts: list[pl.DataFrame] = []
    if bank_ledger is not None and len(bank_ledger) > 0:
        parts.append(_align_ledger_columns(bank_ledger))
    if manual_ledger is not None and len(manual_ledger) > 0:
        parts.append(_align_ledger_columns(manual_ledger))
    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]
    return pl.concat(parts, how="vertical")


def compute_bank_ledger() -> pl.DataFrame | None:
    """Build ledger frame from bank Delta table only, or None if no bank data."""
    bank = read_existing_table()
    if bank is None or len(bank) == 0:
        return None
    return _align_ledger_columns(build_ledger_from_bank_df(bank))


def _ledger_with_tags_left_join(ledger: pl.DataFrame) -> pl.DataFrame:
    if "tags" in ledger.columns:
        ledger = ledger.drop("tags")
    tags_df = read_transaction_tags_table()
    if tags_df is None or len(tags_df) == 0:
        return ledger.with_columns(
            pl.Series("tags", [[] for _ in range(ledger.height)]).cast(pl.List(pl.Utf8))
        )
    tags_agg = tags_df.group_by("id").agg(pl.col("tag").implode().alias("tags"))
    joined = ledger.join(tags_agg, on="id", how="left")
    return joined.with_columns(
        pl.when(pl.col("tags").is_null())
        .then(pl.lit([]).cast(pl.List(pl.Utf8)))
        .otherwise(pl.col("tags"))
        .alias("tags")
    )


def _ledger_with_source(
    bank_ledger: pl.DataFrame | None,
    manual_ledger: pl.DataFrame | None,
) -> pl.DataFrame | None:
    parts: list[pl.DataFrame] = []
    if bank_ledger is not None and len(bank_ledger) > 0:
        parts.append(
            _align_ledger_columns(bank_ledger).with_columns(pl.lit("bank").alias("ledger_source"))
        )
    if manual_ledger is not None and len(manual_ledger) > 0:
        parts.append(
            _align_ledger_columns(manual_ledger).with_columns(
                pl.lit("manual").alias("ledger_source")
            )
        )
    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]
    return pl.concat(parts, how="vertical")


def read_ledger_view() -> pl.DataFrame | None:
    """Bank + manual ledger with ``ledger_source`` and ``tags`` list from ``transaction_tags``."""
    ensure_tag_definitions_table()
    bank_ledger = compute_bank_ledger()
    manual = read_manual_ledger_table()
    manual_df = manual if manual is not None and len(manual) > 0 else None
    ledger = _ledger_with_source(bank_ledger, manual_df)
    if ledger is None:
        return None
    return _ledger_with_tags_left_join(ledger)
