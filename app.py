"""Flathold Streamlit app — multi-page bank statement and data management."""

import streamlit as st

from flathold.bank_delta import read_existing_table

st.set_page_config(page_title="Flathold", page_icon="🏦", layout="wide")

st.title("🏦 Flathold")
st.caption("Bank statements and data")

st.markdown(
    "Use the sidebar to open **Upload statements** (CSV → Delta with deduplication) or **View data**."
)

existing = read_existing_table()
if existing is not None and len(existing) > 0:
    st.info(f"Delta table currently has **{len(existing)}** transactions.")
else:
    st.info("No data in the Delta table yet. Go to **Upload statements** to add a CSV.")
