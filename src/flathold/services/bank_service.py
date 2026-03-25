"""Bank statement table reads (orchestration boundary for UI)."""

from __future__ import annotations

import polars as pl

from flathold.data.tables.bank_table import (
    SaveResult,
    load_csv_bytes_to_dataframe,
    read_existing_table,
    save_to_delta,
)


def read_bank_table() -> pl.DataFrame | None:
    return read_existing_table()


def load_csv_bytes(csv_bytes: bytes) -> pl.DataFrame:
    return load_csv_bytes_to_dataframe(csv_bytes)


def save_bank_to_delta(df: pl.DataFrame) -> SaveResult:
    return save_to_delta(df)
