"""Dashboard — tagged allocation over time (loaded by `app.py` via `st.navigation`)."""

from datetime import date, datetime

import polars as pl
import streamlit as st

from flathold.bank_delta import read_existing_table
from flathold.ledger_delta import (
    clear_transaction_tags,
    read_ledger_table,
    read_transaction_tags_table,
    recreate_ledger_from_bank,
    update_ledger_from_bank,
    update_transaction_tags,
)
from flathold.tag_rules import tag_show_on_dashboard_default

st.set_page_config(page_title="Dashboard", page_icon="🏦", layout="wide")


def _to_date(d: object) -> date:
    """Coerce Polars/Streamlit date-like values to ``datetime.date``."""
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    msg = f"Expected date-like value, got {type(d).__name__}"
    raise TypeError(msg)


def _first_of_month(d: date) -> date:
    return date(d.year, d.month, 1)


def _shift_months_first(d: date, delta: int) -> date:
    """First-of-month date ``d`` shifted by ``delta`` months."""
    m = d.month - 1 + delta
    y = d.year + m // 12
    m = m % 12 + 1
    return date(y, m, 1)


def _clamp_range(lo: date, hi: date, bound_lo: date, bound_hi: date) -> tuple[date, date]:
    """Clamp ``[lo, hi]`` to ``[bound_lo, bound_hi]``; if empty, return full bounds."""
    lo2 = max(lo, bound_lo)
    hi2 = min(hi, bound_hi)
    if lo2 > hi2:
        return bound_lo, bound_hi
    return lo2, hi2


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
    if st.button(
        "Recreate ledger",
        help="Delete the ledger table and rebuild it from scratch from bank data",
        key="main_recreate_ledger",
    ):
        with st.spinner("Recreating ledger…"):
            result = recreate_ledger_from_bank()
        if result.success:
            st.success(result.message)
        else:
            st.error(result.message)
    st.caption("Tags")
    if st.button(
        "Update tags",
        help="Apply hard-coded rules in tag_rules/rules.py and replace all transaction tags",
        key="main_update_tags",
    ):
        with st.spinner("Updating tags…"):
            result = update_transaction_tags()
        if result.success:
            st.success(result.message)
        else:
            st.error(result.message)
    if st.button(
        "Clear tags",
        help="Delete all transaction tags (rules are not applied)",
        key="main_clear_tags",
    ):
        with st.spinner("Clearing tags…"):
            result = clear_transaction_tags()
        if result.success:
            st.success(result.message)
        else:
            st.error(result.message)

st.title("Dashboard")
st.caption(
    "Use the shortcuts or date picker for the metric and chart; then choose tags for the chart."
)

ledger = read_ledger_table()
tags_df = read_transaction_tags_table()

if ledger is None or len(ledger) == 0:
    st.info(
        "No ledger data yet. Upload a CSV on **Upload statements**, then use "
        "**Update ledger** in the sidebar."
    )
    st.stop()

ledger_periods = ledger.with_columns(
    pl.date(pl.col("year"), pl.col("month"), 1).alias("period"),
)
period_min = _to_date(ledger_periods.select(pl.col("period").min()).item())
period_max = _to_date(ledger_periods.select(pl.col("period").max()).item())
if "dashboard_date_range" not in st.session_state:
    st.session_state.dashboard_date_range = (period_min, period_max)

today = date.today()
first_this_month = _first_of_month(today)
last_3_lo, last_3_hi = _clamp_range(
    _shift_months_first(first_this_month, -2),
    first_this_month,
    period_min,
    period_max,
)
last_6_lo, last_6_hi = _clamp_range(
    _shift_months_first(first_this_month, -5),
    first_this_month,
    period_min,
    period_max,
)
prev_year = today.year - 1
last_year_lo, last_year_hi = _clamp_range(
    date(prev_year, 1, 1),
    date(prev_year, 12, 1),
    period_min,
    period_max,
)
ytd_lo, ytd_hi = _clamp_range(
    date(today.year, 1, 1),
    first_this_month,
    period_min,
    period_max,
)

shortcut_cols = st.columns(5)
with shortcut_cols[0]:
    if st.button("Last 3 months", key="shortcut_last_3m", use_container_width=True):
        st.session_state.dashboard_date_range = (last_3_lo, last_3_hi)
with shortcut_cols[1]:
    if st.button("Last 6 months", key="shortcut_last_6m", use_container_width=True):
        st.session_state.dashboard_date_range = (last_6_lo, last_6_hi)
with shortcut_cols[2]:
    if st.button("Last year", key="shortcut_last_year", use_container_width=True):
        st.session_state.dashboard_date_range = (last_year_lo, last_year_hi)
with shortcut_cols[3]:
    if st.button("Year to date", key="shortcut_ytd", use_container_width=True):
        st.session_state.dashboard_date_range = (ytd_lo, ytd_hi)
with shortcut_cols[4]:
    if st.button("All time", key="shortcut_all_time", use_container_width=True):
        st.session_state.dashboard_date_range = (period_min, period_max)

date_range_raw = st.date_input(
    "Date range",
    min_value=period_min,
    max_value=period_max,
    help="Applies to average expenditure and to the tag chart. Months are first-of-month points.",
    key="dashboard_date_range",
)
match date_range_raw:
    case (a, b):
        range_start = _to_date(a)
        range_end = _to_date(b)
    case (single,):
        range_start = range_end = _to_date(single)
    case _:
        range_start = range_end = _to_date(date_range_raw)

lo = _first_of_month(range_start)
hi = _first_of_month(range_end)
if lo > hi:
    lo, hi = hi, lo

monthly_debit_totals = (
    ledger.group_by(["year", "month"])
    .agg(
        pl.col("Debit Amount").sum().alias("monthly_debit"),
    )
    .with_columns(pl.date(pl.col("year"), pl.col("month"), 1).alias("period"))
)
monthly_in_range = monthly_debit_totals.filter((pl.col("period") >= lo) & (pl.col("period") <= hi))
monthly_debits = monthly_in_range.get_column("monthly_debit").to_list()
avg_display = float(sum(monthly_debits) / len(monthly_debits)) if monthly_debits else 0.0
st.metric(
    "Total average monthly expenditure",
    f"£{avg_display:,.2f}",
    help="Mean of each calendar month's total debit amounts in the ledger, within the date range.",
)

if tags_df is None or len(tags_df) == 0:
    st.info(
        "No transaction tags yet. Use **Update tags** in the sidebar (after the ledger exists)."
    )
    st.stop()

period_cols = ledger.select(["id", "year", "month"]).unique()
joined = tags_df.join(period_cols, on="id", how="inner").with_columns(
    pl.date(pl.col("year"), pl.col("month"), 1).alias("period")
)
agg = joined.group_by(["period", "tag"]).agg(pl.col("allocation").sum().alias("allocation"))
all_tags = sorted(agg["tag"].unique().to_list())

if not all_tags:
    st.info("No tags to chart after joining with the ledger.")
    st.stop()

agg_chart = agg.filter((pl.col("period") >= lo) & (pl.col("period") <= hi))
all_tags_in_range = sorted(agg_chart["tag"].unique().to_list())
if not all_tags_in_range:
    st.warning("No tagged allocations in this date range.")
    st.stop()

default_tags = [t for t in all_tags_in_range if tag_show_on_dashboard_default(t)]
if not default_tags:
    default_tags = list(all_tags_in_range)

if "dashboard_tags" not in st.session_state:
    st.session_state.dashboard_tags = list(default_tags)

# Apply "Defaults" before the multiselect is created — cannot assign `dashboard_tags` after
# the widget with that key is instantiated (StreamlitAPIException).
if st.session_state.pop("_apply_dashboard_tag_defaults", False):
    st.session_state.dashboard_tags = list(default_tags)

# Keep the user's tag selection when the date range changes; drop tags not in this range.
# If nothing remains (cleared selection or no overlap with the new range), keep [] — do not
# substitute rule defaults, or clearing the multiselect would immediately repopulate.
_tags_valid = [t for t in st.session_state.dashboard_tags if t in all_tags_in_range]
st.session_state.dashboard_tags = _tags_valid

selected = st.multiselect(
    "Tags to show",
    options=all_tags_in_range,
    key="dashboard_tags",
    help=(
        "Monthly sum of allocation per tag. Initial selection uses each rule's "
        "`show_on_dashboard_by_default` in tag_rules/rules.py. "
        "Selection is kept when you change the date range (tags not in range are removed). "
        "If none remain, the chart stays empty until you pick tags or use Defaults."
    ),
)

if not selected:
    st.warning("Select at least one tag to see the chart.")
    st.stop()

chart_df = (
    agg_chart.filter(pl.col("tag").is_in(selected))
    .pivot(on="tag", index="period", values="allocation")
    .sort("period")
)
for tag in selected:
    if tag not in chart_df.columns:
        chart_df = chart_df.with_columns(pl.lit(0.0).alias(tag))
    else:
        chart_df = chart_df.with_columns(pl.col(tag).fill_null(0.0))
chart_df = chart_df.select(["period", *selected])
if chart_df.is_empty():
    st.info("No data points for the selected tags in this range.")
    st.stop()

chart_col, reset_col = st.columns([6, 1], vertical_alignment="center")
with chart_col:
    st.line_chart(chart_df, x="period", y=selected, width="stretch")
with reset_col:

    def _request_dashboard_tag_defaults() -> None:
        st.session_state["_apply_dashboard_tag_defaults"] = True

    st.button(
        "Defaults",
        key="dashboard_tags_reset_defaults",
        help="Reset tag selection to rule defaults for this date range.",
        use_container_width=True,
        on_click=_request_dashboard_tag_defaults,
    )

bank = read_existing_table()
if bank is not None and len(bank) > 0:
    n = len(bank)
    st.caption(
        f"Bank Delta table: **{n}** statement row(s). Use **Upload statements** to add data."
    )
