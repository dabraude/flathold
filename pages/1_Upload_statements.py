"""Upload bank statements (CSV) to Delta table with deduplication."""

import streamlit as st

from flathold.bank_delta import (
    load_csv_bytes_to_dataframe,
    read_existing_table,
    save_to_delta,
)

st.title("📤 Upload bank statements")
st.markdown("Upload a CSV bank statement. It is stored in a Delta table.")

uploaded = st.file_uploader(
    "Choose a CSV file",
    type=["csv"],
    help="CSV with columns: Transaction Date, Transaction Type, Sort Code, Account Number, "
    "Transaction Description, Debit Amount, Credit Amount, Balance",
)

if uploaded is not None:
    try:
        df = load_csv_bytes_to_dataframe(uploaded.read())
        with st.spinner("Saving…"):
            stored = save_to_delta(df)

        st.success(f"Done. **{stored}** rows saved.")
    except Exception as e:
        st.error(f"Upload failed: {e}")
        raise

# Show current table size
existing = read_existing_table()
if existing is not None:
    st.info(f"Delta table currently has **{len(existing)}** transactions.")
else:
    st.info("No data in the Delta table yet. Upload a CSV to create it.")
