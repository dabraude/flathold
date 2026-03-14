"""View bank statement data from the Delta table."""

import streamlit as st

from flathold.bank_delta import read_existing_table

st.title("📋 View statements")
existing = read_existing_table()

if existing is None or len(existing) == 0:
    st.info("No data in the Delta table yet. Upload a CSV on the **Upload statements** page.")
    st.stop()

st.dataframe(existing, use_container_width=True, height=400)
