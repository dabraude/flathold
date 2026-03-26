"""Read-only access to the base ledger view (orchestration boundary for UI)."""

from __future__ import annotations

import polars as pl

from flathold.analytics.enhanced_ledger import build_enhanced_ledger
from flathold.data.tables.tag_definitions_table import read_tag_rule_metadata_map
from flathold.data.tables.transaction_tags_table import read_transaction_tags_table
from flathold.data.views.ledger_view import read_ledger_view


def get_ledger_view() -> pl.DataFrame | None:
    """Return bank + manual ledger with ``ledger_source`` and ``tags`` list, or None."""
    return read_ledger_view()


def get_enhanced_ledger() -> pl.DataFrame | None:
    """Base ledger plus analytics columns (e.g. ``calculated_tags`` per row), or None."""
    base = read_ledger_view()
    if base is None or base.is_empty():
        return None
    tags_df = read_transaction_tags_table()
    tag_meta = read_tag_rule_metadata_map()
    return build_enhanced_ledger(base, tags_df, tag_meta)
