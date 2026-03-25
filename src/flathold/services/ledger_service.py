"""Read-only access to the base ledger view (orchestration boundary for UI)."""

from __future__ import annotations

import polars as pl

from flathold.data.views.ledger_view import read_ledger_view


def get_ledger_view() -> pl.DataFrame | None:
    """Return bank + manual ledger with ``ledger_source`` and ``tags`` list, or None."""
    return read_ledger_view()
