"""Dashboard — tagged allocation over time (loaded by `app.py` via `st.navigation`)."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta

import altair as alt
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


def _inclusive_day_spine(range_start: date, range_end: date) -> pl.DataFrame:
    """One row per calendar day from ``range_start`` through ``range_end`` (inclusive)."""
    rs, re = (range_start, range_end) if range_start <= range_end else (range_end, range_start)
    return pl.DataFrame({"period": pl.date_range(rs, re, interval="1d", eager=True)})


@dataclass(frozen=True, slots=True)
class _TaggedUniqueMonthlyAvgInput:
    ledger: pl.DataFrame
    tags_df: pl.DataFrame
    selected_tags: tuple[str, ...]
    period_cols: pl.DataFrame
    lo: date
    hi: date
    monthly_in_range: pl.DataFrame


def _avg_monthly_tagged_unique_debit(inp: _TaggedUniqueMonthlyAvgInput) -> float:
    """Mean monthly debit for transactions with a selected tag; each transaction counted once."""
    if not inp.selected_tags:
        return 0.0
    tags_sel = inp.tags_df.filter(pl.col("tag").is_in(list(inp.selected_tags)))
    if tags_sel.is_empty():
        return 0.0
    ledger_debit = inp.ledger.select(["id", "Debit Amount"]).unique(subset=["id"], keep="first")
    enriched = (
        tags_sel.join(ledger_debit, on="id", how="inner")
        .join(inp.period_cols, on="id", how="inner")
        .with_columns(pl.date(pl.col("year"), pl.col("month"), 1).alias("period"))
        .filter((pl.col("period") >= inp.lo) & (pl.col("period") <= inp.hi))
    )
    if enriched.is_empty():
        return 0.0
    unique_per = enriched.group_by(["id", "period"]).agg(
        pl.col("Debit Amount").first().alias("debit")
    )
    monthly_sums = unique_per.group_by("period").agg(pl.col("debit").sum().alias("tagged"))
    monthly_periods = inp.monthly_in_range.select("period").unique()
    aligned = monthly_periods.join(monthly_sums, on="period", how="left").with_columns(
        pl.col("tagged").fill_null(0.0),
    )
    if aligned.is_empty():
        return 0.0
    tagged = aligned.get_column("tagged")
    n = len(tagged)
    return float(tagged.sum()) / float(n) if n else 0.0


def _request_dashboard_tag_defaults() -> None:
    st.session_state["_apply_dashboard_tag_defaults"] = True


# Defaults match Streamlit's `theme.chartCategoricalColors` docs (config.py).
_STREAMLIT_CHART_CATEGORICAL_LIGHT: tuple[str, ...] = (
    "#0068c9",
    "#83c9ff",
    "#ff2b2b",
    "#ffabab",
    "#29b09d",
    "#7defa1",
    "#ff8700",
    "#ffd16a",
    "#6d3fc0",
    "#d5dae5",
)
_STREAMLIT_CHART_CATEGORICAL_DARK: tuple[str, ...] = (
    "#83c9ff",
    "#0068c9",
    "#ffabab",
    "#ff2b2b",
    "#7defa1",
    "#29b09d",
    "#ffd16a",
    "#ff8700",
    "#6d3fc0",
    "#d5dae5",
)


def _streamlit_chart_categorical_palette() -> tuple[str, ...]:
    """Palette used by ``st.line_chart`` / built-in charts (theme + light/dark defaults)."""
    raw = st.get_option("theme.chartCategoricalColors")
    if isinstance(raw, (list, tuple)) and len(raw) > 0:
        return tuple(str(c) for c in raw)
    theme_type = getattr(st.context.theme, "type", None)
    if theme_type == "dark":
        return _STREAMLIT_CHART_CATEGORICAL_DARK
    return _STREAMLIT_CHART_CATEGORICAL_LIGHT


def _tag_color_scale(selected_tags: tuple[str, ...]) -> alt.Scale:
    """Same colour order as ``st.line_chart(..., y=list(selected))``."""
    palette = _streamlit_chart_categorical_palette()
    n = len(palette)
    colors = [palette[i % n] for i in range(len(selected_tags))]
    return alt.Scale(domain=list(selected_tags), range=colors)


def _tag_line_chart(
    chart_df: pl.DataFrame,
    *,
    selected_tags: tuple[str, ...],
    value_tooltip_title: str = "Amount",
) -> alt.Chart:
    """Line chart with a bottom, multi-column legend so long tag lists stay on-screen."""
    long_df = chart_df.unpivot(
        on=list(selected_tags),
        index="period",
        variable_name="tag",
        value_name="value",
    )
    n = len(selected_tags)
    legend_cols = min(4, max(1, n))
    legend_rows = (n + legend_cols - 1) // legend_cols
    plot_height = 300
    chart_height = min(plot_height + legend_rows * 32 + 24, 560)
    return (
        alt.Chart(long_df)
        .mark_line()
        .encode(
            x=alt.X("period:T", title=None),
            y=alt.Y("value:Q", title=None),
            color=alt.Color(
                "tag:N",
                scale=_tag_color_scale(selected_tags),
                legend=alt.Legend(
                    orient="bottom",
                    direction="horizontal",
                    columns=legend_cols,
                    labelLimit=0,
                ),
            ),
            tooltip=[
                alt.Tooltip("period:T", title="Period"),
                alt.Tooltip("tag:N", title="Tag"),
                alt.Tooltip("value:Q", format=",.2f", title=value_tooltip_title),
            ],
        )
        .properties(width="container", height=chart_height)
        .configure_view(strokeWidth=0)
        .interactive()
    )


def _tag_bar_chart_pounds(
    bar_df: pl.DataFrame,
    *,
    ratio_denominator: float,
    selected_tags: tuple[str, ...],
) -> alt.Chart | alt.LayerChart:
    """Vertical bars: £ on the left; right axis is value / ``ratio_denominator``."""
    n = len(bar_df)
    height = max(280, min(420, 180 + 28 * n))
    bars = (
        alt.Chart(bar_df)
        .mark_bar()
        .encode(
            x=alt.X("tag:N", sort="-y", axis=alt.Axis(title=None, labelLimit=0)),
            y=alt.Y(
                "allocated:Q",
                axis=alt.Axis(
                    orient="left",
                    title="£",
                    format=",.0f",
                ),
            ),
            color=alt.Color(
                "tag:N",
                scale=_tag_color_scale(selected_tags),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("tag:N", title="Tag"),
                alt.Tooltip("allocated:Q", format=",.2f", title="Amount"),
            ],
        )
    )
    if ratio_denominator <= 0:
        return bars.properties(width="container", height=height).configure_view(strokeWidth=0)
    rd = float(ratio_denominator)
    ghost = (
        alt.Chart(bar_df)
        .mark_point(opacity=0, size=0)
        .encode(
            x=alt.X("tag:N", sort="-y", axis=None),
            y=alt.Y(
                "allocated:Q",
                axis=alt.Axis(
                    orient="right",
                    title="",
                    labelExpr=f"format(datum.value / {rd}, '.2f')",
                    grid=False,
                ),
            ),
        )
    )
    return (
        alt.layer(bars, ghost)
        .resolve_scale(x="shared", y="shared")
        .properties(width="container", height=height)
        .configure_view(strokeWidth=0)
    )


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
this_month_lo, this_month_hi = _clamp_range(
    first_this_month,
    today,
    period_min,
    period_max,
)
last_30_lo, last_30_hi = _clamp_range(
    today - timedelta(days=29),
    today,
    period_min,
    period_max,
)

shortcut_cols = st.columns(7)
with shortcut_cols[0]:
    if st.button("This month", key="shortcut_this_month", use_container_width=True):
        st.session_state.dashboard_date_range = (this_month_lo, this_month_hi)
with shortcut_cols[1]:
    if st.button("Last 30 days", key="shortcut_last_30d", use_container_width=True):
        st.session_state.dashboard_date_range = (last_30_lo, last_30_hi)
with shortcut_cols[2]:
    if st.button("Last 3 months", key="shortcut_last_3m", use_container_width=True):
        st.session_state.dashboard_date_range = (last_3_lo, last_3_hi)
with shortcut_cols[3]:
    if st.button("Last 6 months", key="shortcut_last_6m", use_container_width=True):
        st.session_state.dashboard_date_range = (last_6_lo, last_6_hi)
with shortcut_cols[4]:
    if st.button("Last year", key="shortcut_last_year", use_container_width=True):
        st.session_state.dashboard_date_range = (last_year_lo, last_year_hi)
with shortcut_cols[5]:
    if st.button("Year to date", key="shortcut_ytd", use_container_width=True):
        st.session_state.dashboard_date_range = (ytd_lo, ytd_hi)
with shortcut_cols[6]:
    if st.button("All time", key="shortcut_all_time", use_container_width=True):
        st.session_state.dashboard_date_range = (period_min, period_max)

date_range_raw = st.date_input(
    "Date range",
    min_value=period_min,
    max_value=period_max,
    help=(
        "Applies to average expenditure and to the tag chart. "
        "The tag line chart includes every calendar day in range."
    ),
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

if range_start > range_end:
    range_start, range_end = range_end, range_start

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

period_cols = ledger.select(["id", "year", "month", "day"]).unique()
joined = tags_df.join(period_cols, on="id", how="inner").with_columns(
    pl.date(pl.col("year"), pl.col("month"), pl.col("day")).alias("period"),
)
agg = joined.group_by(["period", "tag"]).agg(pl.col("allocation").sum().alias("allocation"))
all_tags = sorted(agg["tag"].unique().to_list())

if not all_tags:
    st.info("No tags to chart after joining with the ledger.")
    st.stop()

agg_chart = agg.filter(
    (pl.col("period") >= pl.lit(range_start)) & (pl.col("period") <= pl.lit(range_end)),
)
all_tags_in_range = sorted(agg_chart["tag"].unique().to_list())
default_tags = [t for t in all_tags_in_range if tag_show_on_dashboard_default(t)]
if not default_tags:
    default_tags = list(all_tags_in_range)

if "dashboard_tags" not in st.session_state:
    st.session_state.dashboard_tags = list(default_tags)

# Apply "Defaults" before the multiselect is created — cannot assign `dashboard_tags` after
# the widget with that key is instantiated (StreamlitAPIException).
if st.session_state.pop("_apply_dashboard_tag_defaults", False):
    st.session_state.dashboard_tags = list(default_tags)

# Keep the user's tag selection when the date range changes; drop unknown tags only.
_tags_valid = [t for t in st.session_state.dashboard_tags if t in all_tags]
st.session_state.dashboard_tags = _tags_valid

selected = st.multiselect(
    "Tags to show",
    options=all_tags,
    key="dashboard_tags",
    help=(
        "Daily allocation per tag; missing days are zero. Every tag in the ledger is listed. "
        "Initial selection and Defaults use `show_on_dashboard_by_default` among tags with "
        "allocation in the date range, or all such tags if none match. "
        "Selection is kept when you change the date range. "
        "If none remain, the chart stays empty until you pick tags or use Defaults."
    ),
)

st.button(
    "Defaults",
    key="dashboard_tags_reset_defaults",
    help="Reset tag selection to rule defaults for this date range.",
    on_click=_request_dashboard_tag_defaults,
)

if not selected:
    st.warning("Select at least one tag to see the chart.")
    st.stop()

day_spine = _inclusive_day_spine(range_start, range_end)
if day_spine.is_empty():
    st.info("No days in the selected date range.")
    st.stop()

pivoted = (
    agg_chart.filter(pl.col("tag").is_in(selected))
    .pivot(on="tag", index="period", values="allocation")
    .sort("period")
)
if pivoted.is_empty():
    chart_df = day_spine.with_columns(*[pl.lit(0.0).alias(t) for t in selected])
else:
    for tag in selected:
        if tag not in pivoted.columns:
            pivoted = pivoted.with_columns(pl.lit(0.0).alias(tag))
        else:
            pivoted = pivoted.with_columns(pl.col(tag).fill_null(0.0))
    pivoted = pivoted.select(["period", *selected])
    chart_df = day_spine.join(pivoted, on="period", how="left")
    for tag in selected:
        chart_df = chart_df.with_columns(pl.col(tag).fill_null(0.0))

ledger_in_range = ledger.with_columns(
    pl.date(pl.col("year"), pl.col("month"), pl.col("day")).alias("txn_date")
).filter((pl.col("txn_date") >= pl.lit(range_start)) & (pl.col("txn_date") <= pl.lit(range_end)))
total_debit = float(ledger_in_range.select(pl.col("Debit Amount").sum()).item() or 0.0)

tag_totals_df = (
    agg_chart.filter(pl.col("tag").is_in(selected))
    .group_by("tag")
    .agg(pl.col("allocation").sum().alias("amount"))
)
tag_amounts = {str(row[0]): float(row[1]) for row in tag_totals_df.iter_rows()}

n_days = chart_df.height
if n_days > 0:
    bar_allocated = [tag_amounts.get(t, 0.0) / n_days for t in selected]
    ratio_denominator = total_debit / n_days
else:
    bar_allocated = [0.0 for t in selected]
    ratio_denominator = 0.0

bar_df = pl.DataFrame(
    {
        "tag": selected,
        "allocated": bar_allocated,
    }
)

avg_tagged = _avg_monthly_tagged_unique_debit(
    _TaggedUniqueMonthlyAvgInput(
        ledger=ledger,
        tags_df=tags_df,
        selected_tags=tuple(selected),
        period_cols=period_cols,
        lo=lo,
        hi=hi,
        monthly_in_range=monthly_in_range,
    ),
)
n_months_metric = monthly_in_range.height
st.metric(
    "Average tagged monthly expenditure",
    f"£{avg_tagged:,.2f}",
    help=(
        "Mean per calendar month of debit from transactions that carry at least one selected "
        "tag; each transaction is counted once per month (same months as total average above). "
        f"{n_months_metric} month(s) in range."
    ),
)

bar_col, line_col = st.columns([3, 5], vertical_alignment="center")
with bar_col:
    st.altair_chart(
        _tag_bar_chart_pounds(
            bar_df,
            ratio_denominator=ratio_denominator,
            selected_tags=tuple(selected),
        ),
        use_container_width=True,
    )
with line_col:
    line_mode = st.radio(
        "Line chart",
        ["Per day", "Cumulative"],
        index=1,
        horizontal=True,
        key="dashboard_line_chart_mode",
        help=(
            "Per day: each tag's allocated amount for that day. "
            "Cumulative: running total of allocation over the selected range."
        ),
    )
    if line_mode == "Cumulative":
        line_chart_df = chart_df.with_columns(
            *[pl.col(t).cum_sum().alias(t) for t in selected],
        )
        line_tooltip_title = "Cumulative"
    else:
        line_chart_df = chart_df
        line_tooltip_title = "Daily"
    st.altair_chart(
        _tag_line_chart(
            line_chart_df,
            selected_tags=tuple(selected),
            value_tooltip_title=line_tooltip_title,
        ),
        use_container_width=True,
    )

bank = read_existing_table()
if bank is not None and len(bank) > 0:
    n = len(bank)
    st.caption(
        f"Bank Delta table: **{n}** statement row(s). Use **Upload statements** to add data."
    )
