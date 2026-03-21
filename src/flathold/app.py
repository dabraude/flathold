"""Streamlit entrypoint — `st.navigation` sets sidebar labels (e.g. Dashboard)."""

from pathlib import Path

import streamlit as st

_ROOT = Path(__file__).resolve().parent

dashboard_page = st.Page(
    _ROOT / "dashboard.py",
    title="Dashboard",
    icon="🏦",
    default=True,
)
upload_page = st.Page(
    _ROOT / "upload_statements.py",
    title="Upload statements",
    icon="📤",
)
ledger_page = st.Page(
    _ROOT / "view_ledger.py",
    title="View ledger",
    icon="📋",
)

pg = st.navigation([dashboard_page, upload_page, ledger_page])
st.set_page_config(page_title="Flathold", page_icon="🏦", layout="wide")
pg.run()
