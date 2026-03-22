"""Ledger rows prepared for the View ledger page (split tags + display styling)."""

import pandas as pd
import polars as pl
from pandas.io.formats.style import Styler

from flathold.tag_rules import tag_groups


def ledger_to_ledger_view(ledger: pl.DataFrame) -> pl.DataFrame:
    """Split ``tags`` into ``tags`` and ``counter_party_tags`` using rule ``groups``.

    Tags whose rule includes the ``"counter_party"`` group (including when added in
    ``TagRule.__post_init__``) go to ``counter_party_tags``.
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
        cp = [t for t in tags if "counter_party" in tag_groups(t)]
        rest = [t for t in tags if "counter_party" not in tag_groups(t)]
        other.append(rest)
        counter_party.append(cp)

    out = ledger.drop("tags").with_columns(
        pl.Series("tags", other).cast(pl.List(pl.Utf8)),
        pl.Series("counter_party_tags", counter_party).cast(pl.List(pl.Utf8)),
    )
    return reorder_ledger_view_columns(out)


def reorder_ledger_view_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Place ``id`` first, then counterparty tags, then other tags, then remaining columns."""
    cols = df.columns
    if "id" not in cols:
        return df
    rest = [c for c in cols if c not in ("id", "tags", "counter_party_tags")]
    mid = [c for c in ("counter_party_tags", "tags") if c in cols]
    return df.select(["id", *mid, *rest])


def style_ledger_view_pandas(pdf: pd.DataFrame) -> Styler:
    """Highlight the counterparty-tags column for ``st.dataframe`` (Pandas Styler)."""
    if "counter_party_tags" not in pdf.columns:
        return pdf.style
    return pdf.style.set_properties(
        subset=["counter_party_tags"],
        **{
            "background-color": "#e3f2fd",
            "color": "#0d47a1",
        },
    )
