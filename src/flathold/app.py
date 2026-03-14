"""Flathold Streamlit app — multi-page bank statement and data management."""

import streamlit as st

from flathold.bank_delta import read_existing_table
from flathold.ledger_delta import update_ledger_from_bank

st.set_page_config(page_title="Flathold", page_icon="🏦", layout="wide")

with st.sidebar:
    st.caption("Ledger")
    if st.button(
        "Update ledger",
        help="Rebuild the ledger table from bank data (adds ids to each transaction)",
        key="main_update_ledger",
    ):
        with st.spinner("Updating ledger…"):
            result = update_ledger_from_bank()
        if result.success:
            st.success(result.message)
        else:
            st.error(result.message)

st.title("🏦 Flathold")
st.caption("Bank statements and data")

st.markdown(
    "Use the sidebar to open **Upload statements** (CSV → Delta with deduplication) "
    "or **View data**."
)

existing = read_existing_table()
if existing is not None and len(existing) > 0:
    st.info(f"Delta table currently has **{len(existing)}** transactions.")
else:
    st.info("No data in the Delta table yet. Go to **Upload statements** to add a CSV.")
