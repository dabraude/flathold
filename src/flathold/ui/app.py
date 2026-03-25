"""Streamlit entrypoint — `st.navigation` sets sidebar labels (e.g. Dashboard)."""

from pathlib import Path

import streamlit as st

_PAGES = Path(__file__).resolve().parent / "pages"

dashboard_page = st.Page(
    _PAGES / "dashboard.py",
    title="Dashboard",
    icon="🏦",
    default=True,
)
upload_page = st.Page(
    _PAGES / "upload_statements.py",
    title="Upload statements",
    icon="📤",
)
manual_page = st.Page(
    _PAGES / "view_manual_entries.py",
    title="Manual entries",
    icon="✏️",
)
ledger_page = st.Page(
    _PAGES / "view_ledger.py",
    title="View ledger",
    icon="📋",
)
tags_page = st.Page(
    _PAGES / "view_tags.py",
    title="Tags",
    icon="🏷️",
)
household_contribution_page = st.Page(
    _PAGES / "household_contribution.py",
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
