"""View bank statement data from the Delta table."""

import polars as pl
import streamlit as st

from flathold.bank_delta import read_existing_table

MONTH_NAMES = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


def _month_label(ym: str) -> str:
    """Turn YYYY-MM into e.g. 'Jan'."""
    _, m = ym.split("-")
    return MONTH_NAMES[int(m) - 1]


st.title("📋 View statements")
existing = read_existing_table()

if existing is None or len(existing) == 0:
    st.info("No data in the Delta table yet. Upload a CSV on the **Upload statements** page.")
    st.stop()

# Tabs: year → month
months_sorted = sorted(existing["month"].unique().to_list())
years_sorted = sorted({m[:4] for m in months_sorted})

year_tabs = st.tabs([str(y) for y in years_sorted])
for i, year in enumerate(years_sorted):
    with year_tabs[i]:
        year_months = [m for m in months_sorted if m.startswith(year)]
        month_tabs = st.tabs([_month_label(m) for m in year_months])
        for j, month in enumerate(year_months):
            with month_tabs[j]:
                subset = (
                    existing.filter(pl.col("month") == month)
                    .sort(["Transaction Date", "Transaction Counter"])
                )
                st.dataframe(subset, width="stretch", height=400)
