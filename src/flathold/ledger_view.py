"""Ledger rows prepared for the View ledger page (split tags + display styling)."""

from collections.abc import Sequence

import pandas as pd
import polars as pl
from pandas.io.formats.style import Styler

from flathold.tag_rules import TagGroup, tag_groups

LEDGER_VIEW_COUNTER_PARTY_COLUMN = "Counter Party"
LEDGER_VIEW_TAGS_COLUMN = "Tags"


def _non_counterparty_tag_count_for_cell(cell: object) -> int:
    """How many tags on this row are not in the ``counter_party`` group (same split as the view)."""
    if cell is None or isinstance(cell, (str, bytes)):
        return 0
    if not isinstance(cell, Sequence):
        return 0
    return sum(1 for t in cell if TagGroup.COUNTER_PARTY not in tag_groups(str(t)))


def ledger_non_counterparty_tag_count_expr(tags_column: str = "tags") -> pl.Expr:
    """Polars expression: count of tags per row that belong in ``Tags``, not ``Counter Party``."""
    return pl.col(tags_column).map_elements(
        _non_counterparty_tag_count_for_cell,
        return_dtype=pl.UInt32,
    )


def ledger_to_ledger_view(ledger: pl.DataFrame) -> pl.DataFrame:
    """Split ledger ``tags`` into ``Tags`` and ``Counter Party`` columns using rule ``groups``.

    Tags whose rule includes ``TagGroup.COUNTER_PARTY`` (including when added in
    ``TagRule.__post_init__``) go to the counter party column.
    """
    if "tags" not in ledger.columns:
        return ledger

    raw = ledger["tags"].to_list()
    other: list[list[str]] = []
    counter_party: list[list[str]] = []
    for cell in raw:
        if cell is None:
            other.append([])
            counter_party.append([])
            continue
        tags = [str(t) for t in cell]
        cp = [t for t in tags if TagGroup.COUNTER_PARTY in tag_groups(t)]
        rest = [t for t in tags if TagGroup.COUNTER_PARTY not in tag_groups(t)]
        other.append(rest)
        counter_party.append(cp)

    out = ledger.drop("tags").with_columns(
        pl.Series(LEDGER_VIEW_TAGS_COLUMN, other).cast(pl.List(pl.Utf8)),
        pl.Series(LEDGER_VIEW_COUNTER_PARTY_COLUMN, counter_party).cast(pl.List(pl.Utf8)),
    )
    return reorder_ledger_view_columns(out)


def reorder_ledger_view_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Place ``id`` first, then counter party column, then other tags, then remaining columns."""
    cols = df.columns
    if "id" not in cols:
        return df
    cp = LEDGER_VIEW_COUNTER_PARTY_COLUMN
    tags_col = LEDGER_VIEW_TAGS_COLUMN
    rest = [c for c in cols if c not in ("id", tags_col, cp)]
    mid = [c for c in (cp, tags_col) if c in cols]
    return df.select(["id", *mid, *rest])


def style_ledger_view_pandas(pdf: pd.DataFrame) -> Styler:
    """Highlight the counter party column for ``st.dataframe`` (Pandas Styler)."""
    if LEDGER_VIEW_COUNTER_PARTY_COLUMN not in pdf.columns:
        return pdf.style
    return pdf.style.set_properties(
        subset=[LEDGER_VIEW_COUNTER_PARTY_COLUMN],
        **{
            "background-color": "#e3f2fd",
            "color": "#0d47a1",
        },
    )
