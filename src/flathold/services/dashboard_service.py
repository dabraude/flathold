"""Dashboard inputs: base ledger, tags, metadata (orchestration boundary for UI)."""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from flathold.core.tag_rule_metadata import TagRuleMetadata
from flathold.data.tables.bank_table import read_existing_table
from flathold.data.tables.tag_definitions_table import read_tag_rule_metadata_map
from flathold.data.tables.transaction_tags_table import read_transaction_tags_table
from flathold.data.views.ledger_view import read_ledger_view


@dataclass(frozen=True, slots=True)
class DashboardInputs:
    ledger: pl.DataFrame
    tags_df: pl.DataFrame | None
    tag_meta: dict[str, TagRuleMetadata]


def get_dashboard_inputs() -> DashboardInputs | None:
    ledger = read_ledger_view()
    if ledger is None or len(ledger) == 0:
        return None
    tags_df = read_transaction_tags_table()
    tag_meta = read_tag_rule_metadata_map()
    return DashboardInputs(ledger=ledger, tags_df=tags_df, tag_meta=tag_meta)


def bank_statement_row_count() -> int | None:
    bank = read_existing_table()
    if bank is None or len(bank) == 0:
        return None
    return len(bank)
