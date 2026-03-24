"""Ledger rows prepared for the View ledger page (split tags + display styling)."""

import pandas as pd
import polars as pl
from pandas.io.formats.style import Styler

from flathold.tag_definitions_store import read_tag_rule_metadata_map
from flathold.tag_rules import TagGroup, tag_groups

LEDGER_VIEW_COUNTER_PARTY_COLUMN = "Counter Party"
LEDGER_VIEW_TAGS_COLUMN = "Tags"
LEDGER_SOURCE_COLUMN = "ledger_source"


def _counterparty_column_tag_names() -> frozenset[str]:
    """Names that map to the Counter Party column (``ledger_to_ledger_view`` split)."""
    meta = read_tag_rule_metadata_map()
    return frozenset({name for name, m in meta.items() if TagGroup.COUNTER_PARTY in m.groups})


def ledger_non_counterparty_tag_count_expr(tags_column: str = "tags") -> pl.Expr:
    """Polars expression: count of tags per row that belong in ``Tags``, not ``Counter Party``."""
    cp = _counterparty_column_tag_names()
    inner = pl.element().is_in(list(cp)).not_().cast(pl.UInt32)
    return pl.col(tags_column).list.eval(inner).list.sum()


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
    """Place ``id`` first, optional ``ledger_source``, then counter party / tags, then the rest."""
    cols = df.columns
    if "id" not in cols:
        return df
    cp = LEDGER_VIEW_COUNTER_PARTY_COLUMN
    tags_col = LEDGER_VIEW_TAGS_COLUMN
    src = LEDGER_SOURCE_COLUMN
    head: list[str] = ["id"]
    if src in cols:
        head.append(src)
    mid = [c for c in (cp, tags_col) if c in cols]
    rest = [c for c in cols if c not in (*head, *mid)]
    return df.select([*head, *mid, *rest])


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
