"""Manual ledger reads and appends (orchestration boundary for UI)."""

from __future__ import annotations

import polars as pl

from flathold.data.tables.manual_ledger_table import (
    AppendManualLedgerResult,
    ManualLedgerAppendInput,
    append_manual_ledger_row,
    read_manual_ledger_table,
)

__all__ = [
    "AppendManualLedgerResult",
    "ManualLedgerAppendInput",
    "append_manual_row",
    "read_manual_ledger",
]


def read_manual_ledger() -> pl.DataFrame | None:
    return read_manual_ledger_table()


def append_manual_row(inp: ManualLedgerAppendInput) -> AppendManualLedgerResult:
    return append_manual_ledger_row(inp)
