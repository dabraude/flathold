"""View ledger: enhanced ledger (base + calculated-tag hints) through the viewable presenter."""

import polars as pl
import streamlit as st

from flathold.services.ledger_service import get_enhanced_ledger
from flathold.services.tag_definitions_service import get_tag_rule_metadata_map
from flathold.services.tagging_service import refresh_ledger_and_tags
from flathold.ui.presenters.ledger_presenter import (
    LEDGER_VIEW_COUNTER_PARTY_COLUMN,
    ledger_non_counterparty_tag_count_expr,
    ledger_to_ledger_view,
    style_ledger_view_pandas,
)

st.set_page_config(page_title="View ledger", page_icon="📋", layout="wide")

with st.sidebar:
    if st.button(
        "Update",
        help=(
            "Sync stored tags with current bank data (prune orphans, remove legacy ledger files), "
            "then reapply tag rules from tag_rules"
        ),
        key="view_refresh_ledger_tags",
        width="stretch",
    ):
        with st.spinner("Updating…"):
            result = refresh_ledger_and_tags()
        if result.success:
            st.success(result.message)
        else:
            st.error(result.message)

MONTH_NAMES = (
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
)


def _month_label(month_num: int) -> str:
    return MONTH_NAMES[month_num - 1]


st.title("📋 View ledger")
tag_meta = get_tag_rule_metadata_map()
existing = get_enhanced_ledger()

if existing is None or len(existing) == 0:
    st.info(
        "No ledger data yet. Upload a CSV on **Upload statements** and/or add rows on "
        "**Manual entries**."
    )
    st.stop()

only_untagged = st.checkbox(
    "Only transactions without tags",
    help=(
        "Hide rows that have at least one tag outside Counter Party "
        "(counterparty-only rows count as untagged)"
    ),
    key="view_ledger_only_untagged",
)
only_without_counter_party = st.checkbox(
    "Only transactions without counterparty tags",
    help="Hide rows where at least one tag is a counterparty (person or institution)",
    key="view_ledger_only_without_counter_party",
)

year_month = existing.select(["year", "month"]).unique().sort(["year", "month"])
years_sorted = year_month["year"].unique().to_list()

year_tabs = st.tabs([str(y) for y in years_sorted])
for i, year in enumerate(years_sorted):
    with year_tabs[i]:
        months_in_year = year_month.filter(pl.col("year") == year)["month"].to_list()
        month_tabs = st.tabs([_month_label(m) for m in months_in_year])
        for j, month_num in enumerate(months_in_year):
            with month_tabs[j]:
                subset = existing.filter((pl.col("year") == year) & (pl.col("month") == month_num))
                if only_untagged:
                    subset = subset.filter(ledger_non_counterparty_tag_count_expr(tag_meta) == 0)
                subset = subset.sort(["Transaction Date", "Transaction Counter"])
                view_df = ledger_to_ledger_view(subset, tag_meta=tag_meta)
                if only_without_counter_party:
                    view_df = view_df.filter(
                        pl.col(LEDGER_VIEW_COUNTER_PARTY_COLUMN).list.len() == 0
                    )
                st.dataframe(
                    style_ledger_view_pandas(view_df.to_pandas()),
                    width="stretch",
                    height="stretch",
                )
