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
manual_page = st.Page(
    _ROOT / "view_manual_entries.py",
    title="Manual entries",
    icon="✏️",
)
ledger_page = st.Page(
    _ROOT / "view_ledger.py",
    title="View ledger",
    icon="📋",
)
tags_page = st.Page(
    _ROOT / "view_tags.py",
    title="Tags",
    icon="🏷️",
)
household_contribution_page = st.Page(
    _ROOT / "household_contribution.py",
    title="Household contribution",
    icon="💷",
)

pg = st.navigation(
    [
        dashboard_page,
        upload_page,
        manual_page,
        ledger_page,
        tags_page,
        household_contribution_page,
    ]
)
st.set_page_config(page_title="Flathold", page_icon="🏦", layout="wide")
pg.run()
