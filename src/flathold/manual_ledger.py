"""Non-bank ledger rows in ``db/manual_ledger`` (``LedgerSchema``; ids ``manual-*``)."""

from __future__ import annotations

import shutil
import uuid
from dataclasses import dataclass
from datetime import datetime

import polars as pl
from deltalake import write_deltalake

from flathold.constants import MANUAL_LEDGER_TABLE
from flathold.schemas import LedgerSchema

_MANUAL_ID_PREFIX = "manual-"


@dataclass(frozen=True, slots=True)
class AppendManualLedgerResult:
    success: bool
    message: str
    id: str | None


@dataclass(frozen=True, slots=True)
class ManualLedgerAppendInput:
    """Fields for a new manual row (date DD/MM/YYYY)."""

    transaction_date: str
    transaction_description: str
    debit_amount: float = 0.0
    credit_amount: float = 0.0
    transaction_type: str = "MANUAL"
    sort_code: str = ""
    account_number: str = ""


def read_manual_ledger_table() -> pl.DataFrame | None:
    """Read ``db/manual_ledger`` if present; otherwise ``None``."""
    if not MANUAL_LEDGER_TABLE.exists():
        return None
    try:
        return pl.read_delta(str(MANUAL_LEDGER_TABLE))
    except Exception:
        return None


def write_manual_ledger_table(df: pl.DataFrame) -> None:
    """Replace manual ledger; empty input removes the table. Rows must satisfy ``LedgerSchema``."""
    if len(df) == 0:
        if MANUAL_LEDGER_TABLE.exists():
            shutil.rmtree(MANUAL_LEDGER_TABLE)
        return
    if len(df) != len(df["id"].unique()):
        msg = "manual_ledger rows must have unique id"
        raise ValueError(msg)
    if df.filter(~pl.col("id").str.starts_with(_MANUAL_ID_PREFIX)).height > 0:
        msg = f"manual_ledger ids must start with {_MANUAL_ID_PREFIX!r}"
        raise ValueError(msg)
    LedgerSchema.validate(df)
    MANUAL_LEDGER_TABLE.parent.mkdir(parents=True, exist_ok=True)
    write_deltalake(
        str(MANUAL_LEDGER_TABLE),
        df.to_arrow(),
        mode="overwrite",
        schema_mode="overwrite",
    )


def _next_manual_transaction_counter(existing: pl.DataFrame | None, transaction_date: str) -> int:
    if existing is None or len(existing) == 0:
        return 1
    same = existing.filter(pl.col("Transaction Date") == transaction_date)
    if len(same) == 0:
        return 1
    vals = same.get_column("Transaction Counter").to_list()
    return max(vals) + 1


def append_manual_ledger_row(inp: ManualLedgerAppendInput) -> AppendManualLedgerResult:
    """Append one row with a new ``manual-…`` id; parses ``inp.transaction_date`` as DD/MM/YYYY."""
    try:
        dt = datetime.strptime(inp.transaction_date.strip(), "%d/%m/%Y")
    except ValueError as e:
        return AppendManualLedgerResult(
            success=False,
            message=f"Invalid transaction_date (use DD/MM/YYYY): {e}",
            id=None,
        )
    y, m, day = dt.year, dt.month, dt.day
    existing = read_manual_ledger_table()
    counter = _next_manual_transaction_counter(existing, inp.transaction_date.strip())
    new_id = f"{_MANUAL_ID_PREFIX}{uuid.uuid4().hex}"
    row = {
        "Transaction Counter": counter,
        "Transaction Date": inp.transaction_date.strip(),
        "Transaction Type": inp.transaction_type,
        "Sort Code": inp.sort_code,
        "Account Number": inp.account_number,
        "Transaction Description": inp.transaction_description,
        "Debit Amount": float(inp.debit_amount),
        "Credit Amount": float(inp.credit_amount),
        "id": new_id,
        "year": y,
        "month": m,
        "day": day,
    }
    new_df = pl.DataFrame([row])
    combined = pl.concat([existing, new_df], how="vertical") if existing is not None else new_df
    try:
        write_manual_ledger_table(combined)
    except ValueError as e:
        return AppendManualLedgerResult(success=False, message=str(e), id=None)
    return AppendManualLedgerResult(
        success=True,
        message="Manual transaction saved.",
        id=new_id,
    )
