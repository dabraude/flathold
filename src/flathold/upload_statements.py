"""Upload bank statements (CSV) to Delta table with deduplication."""

import streamlit as st

from flathold.bank_delta import (
    load_csv_bytes_to_dataframe,
    read_existing_table,
    save_to_delta,
)
from flathold.ledger_delta import recreate_ledger_from_bank, update_ledger_from_bank

st.set_page_config(page_title="Upload statements", page_icon="📤", layout="wide")

with st.sidebar:
    st.caption("Ledger")
    if st.button(
        "Update ledger",
        help="Rebuild the ledger table from bank data (adds ids to each transaction)",
        key="upload_update_ledger",
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
        key="upload_recreate_ledger",
    ):
        with st.spinner("Recreating ledger…"):
            result = recreate_ledger_from_bank()
        if result.success:
            st.success(result.message)
        else:
            st.error(result.message)

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
            result = save_to_delta(df)

        msg = (
            f"Done. **{len(df)}** rows in file → **{result.new_rows}** added, "
            f"**{result.duplicated}** duplicated (skipped). **{result.total}** total in table."
        )
        st.success(msg)
    except Exception as e:
        st.error(f"Upload failed: {e}")
        raise

# Show current table size
existing = read_existing_table()
if existing is not None:
    st.info(f"Delta table currently has **{len(existing)}** transactions.")
else:
    st.info("No data in the Delta table yet. Upload a CSV to create it.")
