"""Orchestration: prune tags, reapply rules, legacy ledger cleanup."""

import shutil
from dataclasses import dataclass

from flathold.data.paths import LEDGER_TABLE
from flathold.data.tables import transaction_tags_table as tags_table
from flathold.data.tables.bank_table import read_existing_table
from flathold.data.tables.manual_ledger_table import read_manual_ledger_table
from flathold.data.views.ledger_view import (
    build_ledger_from_bank_df,
    combine_bank_and_manual_ledger,
    compute_bank_ledger,
)
from flathold.tag_rules import apply_tag_rules


@dataclass(frozen=True, slots=True)
class UpdateLedgerResult:
    success: bool
    message: str


@dataclass(frozen=True, slots=True)
class UpdateTagsResult:
    success: bool
    message: str


def _remove_legacy_ledger_table() -> None:
    """Remove persisted ``db/ledger`` if present (older versions wrote a Delta ledger)."""
    if LEDGER_TABLE.exists():
        shutil.rmtree(LEDGER_TABLE)


def update_transaction_tags_from_rules() -> UpdateTagsResult:
    """Replace transaction tags with tags derived from `tag_rules.rules.TAG_RULES`."""
    bank_ledger = compute_bank_ledger()
    manual = read_manual_ledger_table()
    manual_df = manual if manual is not None and len(manual) > 0 else None
    ledger = combine_bank_and_manual_ledger(bank_ledger, manual_df)
    if ledger is None or len(ledger) == 0:
        return UpdateTagsResult(
            success=False,
            message="No ledger data. Upload bank CSV and/or add manual entries.",
        )
    try:
        tags_df = apply_tag_rules(ledger)
    except ValueError as e:
        return UpdateTagsResult(success=False, message=str(e))
    tags_table.write_transaction_tags_table(tags_df)
    n = len(tags_df)
    return UpdateTagsResult(
        success=True,
        message=f"Tags updated: {n} tag row(s) from rules.",
    )


def clear_transaction_tags() -> UpdateTagsResult:
    """Remove all rows from the transaction tags table."""
    tags_table.clear_transaction_tags_table()
    return UpdateTagsResult(success=True, message="All transaction tags cleared.")


def update_ledger_from_bank() -> UpdateLedgerResult:
    """
    Prune stored tags to ids present in the current bank + manual ledger,
    and remove any legacy persisted ledger Delta directory.
    """
    bank = read_existing_table()
    manual = read_manual_ledger_table()
    bank_ledger = build_ledger_from_bank_df(bank) if bank is not None and len(bank) > 0 else None
    manual_df = manual if manual is not None and len(manual) > 0 else None
    ledger = combine_bank_and_manual_ledger(bank_ledger, manual_df)
    if ledger is None or len(ledger) == 0:
        return UpdateLedgerResult(
            success=False,
            message="No bank or manual ledger data.",
        )
    n_bank = len(bank_ledger) if bank_ledger is not None else 0
    n_manual = len(manual_df) if manual_df is not None else 0
    _remove_legacy_ledger_table()
    tags_table.prune_transaction_tags_to_ledger_ids(ledger["id"])
    msg = (
        f"Tags pruned to current ledger ({len(ledger)} rows: {n_bank} bank, {n_manual} manual). "
        "Ledger is computed on read."
    )
    return UpdateLedgerResult(success=True, message=msg)


def recreate_ledger_from_bank() -> UpdateLedgerResult:
    """
    Clear all transaction tags and remove any legacy persisted ledger table.
    The ledger view is derived from bank + manual tables when the app reads data.
    """
    _remove_legacy_ledger_table()
    tags_table.clear_transaction_tags_table()
    msg = (
        "All transaction tags cleared. "
        "Ledger rows come from bank and manual data when you view the app."
    )
    return UpdateLedgerResult(success=True, message=msg)


def refresh_ledger_and_tags() -> UpdateTagsResult:
    """Prune tags to bank + manual ledger, drop legacy ledger files, reapply tag rules."""
    prune = update_ledger_from_bank()
    if not prune.success:
        return UpdateTagsResult(success=False, message=prune.message)
    rules = update_transaction_tags_from_rules()
    if not rules.success:
        return UpdateTagsResult(success=False, message=rules.message)
    return UpdateTagsResult(success=True, message=f"{prune.message}\n\n{rules.message}")
