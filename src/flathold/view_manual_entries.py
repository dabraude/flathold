"""Add non-bank transactions stored in ``db/manual_ledger`` (shown on the ledger with bank rows)."""

import streamlit as st

from flathold.agata_weekly_manual import (
    AGATA_WEEKLY_MANUAL_DESCRIPTION,
    sync_agata_weekly_manual_entries,
)
from flathold.ledger_delta import refresh_ledger_and_tags
from flathold.manual_ledger import (
    ManualLedgerAppendInput,
    append_manual_ledger_row,
    read_manual_ledger_table,
)

st.set_page_config(page_title="Manual entries", page_icon="✏️", layout="wide")

with st.sidebar:
    if st.button(
        "Update",
        help=(
            "Prune tags to current bank + manual ledger, remove legacy ledger files, "
            "then reapply tag rules"
        ),
        key="manual_refresh_ledger_tags",
        width="stretch",
    ):
        with st.spinner("Updating…"):
            result = refresh_ledger_and_tags()
        if result.success:
            st.success(result.message)
        else:
            st.error(result.message)

st.title("✏️ Manual entries")
st.caption(
    "Not from uploaded CSVs. Ids are `manual-…`; on **View ledger** they show as **ledger_source** "
    "`manual`. Tag rules include them."
)

with st.form("add_manual_entry", clear_on_submit=True):
    c1, c2 = st.columns(2)
    with c1:
        tx_date = st.text_input(
            "Transaction date",
            placeholder="DD/MM/YYYY",
            help="UK-style date, e.g. 15/03/2025",
        )
    with c2:
        tx_type = st.text_input("Transaction type", value="MANUAL")
    desc = st.text_input("Description", placeholder="What this entry is for")
    d1, d2 = st.columns(2)
    with d1:
        debit = st.number_input("Debit amount", min_value=0.0, value=0.0, step=0.01)
    with d2:
        credit = st.number_input("Credit amount", min_value=0.0, value=0.0, step=0.01)
    s1, s2 = st.columns(2)
    with s1:
        sort_code = st.text_input("Sort code (optional)", value="")
    with s2:
        account_number = st.text_input("Account number (optional)", value="")
    retag = st.checkbox(
        "Reapply tags after save",
        value=True,
        help="Runs the same **Update** as the sidebar (prune + tag rules).",
    )
    submitted = st.form_submit_button("Save manual transaction")
    if submitted:
        if not tx_date.strip() or not desc.strip():
            st.error("Transaction date and description are required.")
        else:
            out = append_manual_ledger_row(
                ManualLedgerAppendInput(
                    transaction_date=tx_date.strip(),
                    transaction_description=desc.strip(),
                    debit_amount=float(debit),
                    credit_amount=float(credit),
                    transaction_type=tx_type.strip() or "MANUAL",
                    sort_code=sort_code.strip(),
                    account_number=account_number.strip(),
                )
            )
            if out.success:
                st.success(f"{out.message} id=`{out.id}`")
                if retag:
                    with st.spinner("Reapplying tags…"):
                        r2 = refresh_ledger_and_tags()
                    if r2.success:
                        st.success(r2.message)
                    else:
                        st.error(r2.message)
            else:
                st.error(out.message)

manual = read_manual_ledger_table()
if manual is None or len(manual) == 0:
    st.info("No manual entries yet. Use the form above to add one.")
else:
    st.subheader("Stored manual rows")
    show = manual.sort(["Transaction Date", "Transaction Counter"])
    st.dataframe(show, width="stretch", hide_index=True)

st.divider()

st.subheader("Weekly Agata (£45)")
st.markdown(
    f"Creates one **MANUAL** debit of **£45** every **Monday** from the earliest date in your "
    f"bank/manual data through today, with description `{AGATA_WEEKLY_MANUAL_DESCRIPTION}`. "
    "Tag rules apply **cleaning** and **cash-spend**. Re-running replaces rows with ids "
    "`manual-agata-weekly-*` (and legacy `manual-aga-weekly-*`)."
)
c_ag1, c_ag2 = st.columns(2)
with c_ag1:
    if st.button("Sync Agata weekly rows", key="sync_agata_weekly", width="stretch"):
        with st.spinner("Writing manual ledger…"):
            r = sync_agata_weekly_manual_entries()
        if r.success:
            st.success(r.message)
            with st.spinner("Reapplying tags…"):
                r2 = refresh_ledger_and_tags()
            if r2.success:
                st.success(r2.message)
            else:
                st.error(r2.message)
        else:
            st.error(r.message)
with c_ag2:
    st.caption("Or call `sync_agata_weekly_manual_entries()` from code.")
