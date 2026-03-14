"""View ledger data from the Delta table (bank data + id, Counter Party, Item, tags)."""

import polars as pl
import streamlit as st

from flathold.ledger_delta import (
    read_ledger_table,
    recreate_ledger_from_bank,
    update_ledger_from_bank,
)

with st.sidebar:
    st.caption("Ledger")
    if st.button(
        "Update ledger",
        help="Rebuild the ledger table from bank data (adds ids to each transaction)",
        key="view_update_ledger",
    ):
        with st.spinner("Updating ledger…"):
            result = update_ledger_from_bank()
        if result.success:
            st.success(result.message)
        else:
            st.error(result.message)
    if st.button(
        "Recreate ledger",
        help="Delete the ledger table and rebuild it from scratch from bank data",
        key="view_recreate_ledger",
    ):
        with st.spinner("Recreating ledger…"):
            result = recreate_ledger_from_bank()
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
    """Turn month number (1-12) into e.g. 'Jan'."""
    return MONTH_NAMES[month_num - 1]


st.title("📋 View ledger")
existing = read_ledger_table()

if existing is None or len(existing) == 0:
    st.info(
        "No ledger data yet. Upload a CSV on **Upload statements**, then use "
        "**Update ledger** in the sidebar to build the ledger."
    )
    st.stop()

# Tabs: year → month (ledger has year, month columns)
year_month = (
    existing.select(["year", "month"]).unique().sort(["year", "month"])
)
years_sorted = year_month["year"].unique().to_list()

year_tabs = st.tabs([str(y) for y in years_sorted])
for i, year in enumerate(years_sorted):
    with year_tabs[i]:
        months_in_year = year_month.filter(pl.col("year") == year)["month"].to_list()
        month_tabs = st.tabs([_month_label(m) for m in months_in_year])
        for j, month_num in enumerate(months_in_year):
            with month_tabs[j]:
                subset = existing.filter(
                    (pl.col("year") == year) & (pl.col("month") == month_num)
                ).sort(["Transaction Date", "Transaction Counter"])
                st.dataframe(subset, width="stretch", height=400)
