"""Dashboard — tagged allocation over time (loaded by `ui/app.py` via `st.navigation`)."""

from datetime import date, timedelta

import altair as alt
import polars as pl
import streamlit as st

from flathold.analytics.dashboard_metrics import (
    clamp_range,
    first_of_month,
    shift_months_first,
    to_date,
)
from flathold.analytics.dashboard_views import (
    RuleTagSpendMetricsInput,
    all_tags_for_dashboard,
    avg_monthly_ledger_expenditure,
    build_dashboard_allocation_long,
    chart_and_bar_for_selected_tags,
    monthly_debits_in_range,
    spend_metrics_for_rule_tags,
)
from flathold.core.tag_group import TagGroup
from flathold.services.dashboard_service import bank_statement_row_count, get_dashboard_inputs
from flathold.services.tagging_service import refresh_ledger_and_tags

st.set_page_config(page_title="Dashboard", page_icon="🏦", layout="wide")


def _request_dashboard_group(group_value: str) -> None:
    st.session_state["_dashboard_apply_group"] = group_value


def _tag_group_button_label(group: TagGroup) -> str:
    return group.value.replace("-", " ").title()


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
    raw = st.get_option("theme.chartCategoricalColors")
    if isinstance(raw, (list, tuple)) and len(raw) > 0:
        return tuple(str(c) for c in raw)
    theme_type = getattr(st.context.theme, "type", None)
    if theme_type == "dark":
        return _STREAMLIT_CHART_CATEGORICAL_DARK
    return _STREAMLIT_CHART_CATEGORICAL_LIGHT


def _tag_color_scale(selected_tags: tuple[str, ...]) -> alt.Scale:
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
    if st.button(
        "Update",
        help=(
            "Sync stored tags with current bank data (prune orphans, remove legacy ledger files), "
            "then reapply tag rules from tag_rules"
        ),
        key="main_refresh_ledger_tags",
        width="stretch",
    ):
        with st.spinner("Updating…"):
            result = refresh_ledger_and_tags()
        if result.success:
            st.success(result.message)
        else:
            st.error(result.message)

st.title("Dashboard")
st.caption(
    "Use the shortcuts or date picker for the metric and chart; then choose tags for the chart."
)

inputs = get_dashboard_inputs()
if inputs is None:
    st.info(
        "No ledger data yet. Upload a CSV on **Upload statements** and/or add rows on "
        "**Manual entries**."
    )
    st.stop()

ledger = inputs.ledger
tags_df = inputs.tags_df
tag_meta = inputs.tag_meta

ledger_periods = ledger.with_columns(
    pl.date(pl.col("year"), pl.col("month"), 1).alias("period"),
)
period_min = to_date(ledger_periods.select(pl.col("period").min()).item())
period_max = to_date(ledger_periods.select(pl.col("period").max()).item())
if "dashboard_date_range" not in st.session_state:
    st.session_state.dashboard_date_range = (period_min, period_max)

today = date.today()
first_this_month = first_of_month(today)
last_3_lo, last_3_hi = clamp_range(
    shift_months_first(first_this_month, -2),
    first_this_month,
    period_min,
    period_max,
)
last_6_lo, last_6_hi = clamp_range(
    shift_months_first(first_this_month, -5),
    first_this_month,
    period_min,
    period_max,
)
prev_year = today.year - 1
last_year_lo, last_year_hi = clamp_range(
    date(prev_year, 1, 1),
    date(prev_year, 12, 1),
    period_min,
    period_max,
)
ytd_lo, ytd_hi = clamp_range(
    date(today.year, 1, 1),
    first_this_month,
    period_min,
    period_max,
)
this_month_lo, this_month_hi = clamp_range(
    first_this_month,
    today,
    period_min,
    period_max,
)
last_30_lo, last_30_hi = clamp_range(
    today - timedelta(days=29),
    today,
    period_min,
    period_max,
)

shortcut_cols = st.columns(7)
with shortcut_cols[0]:
    if st.button("This month", key="shortcut_this_month", width="stretch"):
        st.session_state.dashboard_date_range = (this_month_lo, this_month_hi)
with shortcut_cols[1]:
    if st.button("Last 30 days", key="shortcut_last_30d", width="stretch"):
        st.session_state.dashboard_date_range = (last_30_lo, last_30_hi)
with shortcut_cols[2]:
    if st.button("Last 3 months", key="shortcut_last_3m", width="stretch"):
        st.session_state.dashboard_date_range = (last_3_lo, last_3_hi)
with shortcut_cols[3]:
    if st.button("Last 6 months", key="shortcut_last_6m", width="stretch"):
        st.session_state.dashboard_date_range = (last_6_lo, last_6_hi)
with shortcut_cols[4]:
    if st.button("Last year", key="shortcut_last_year", width="stretch"):
        st.session_state.dashboard_date_range = (last_year_lo, last_year_hi)
with shortcut_cols[5]:
    if st.button("Year to date", key="shortcut_ytd", width="stretch"):
        st.session_state.dashboard_date_range = (ytd_lo, ytd_hi)
with shortcut_cols[6]:
    if st.button("All time", key="shortcut_all_time", width="stretch"):
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
        range_start = to_date(a)
        range_end = to_date(b)
    case (single,):
        range_start = range_end = to_date(single)
    case _:
        range_start = range_end = to_date(date_range_raw)

if range_start > range_end:
    range_start, range_end = range_end, range_start

lo = first_of_month(range_start)
hi = first_of_month(range_end)
if lo > hi:
    lo, hi = hi, lo

monthly_in_range = monthly_debits_in_range(ledger, lo, hi)
avg_display = avg_monthly_ledger_expenditure(ledger, lo, hi)
st.metric(
    "Total average monthly expenditure",
    f"£{avg_display:,.2f}",
    help="Mean of each calendar month's total debit amounts in the ledger, within the date range.",
)

if tags_df is None or len(tags_df) == 0:
    st.info("No transaction tags yet. Use **Update** in the sidebar (after bank data is uploaded).")
    st.stop()

agg_long = build_dashboard_allocation_long(
    ledger,
    tags_df,
    tag_meta,
    range_start,
    range_end,
)
all_tags = all_tags_for_dashboard(agg_long, tag_meta)

if not all_tags:
    st.info("No tags to chart after joining with the ledger.")
    st.stop()

agg_chart = agg_long.filter(
    (pl.col("period") >= pl.lit(range_start)) & (pl.col("period") <= pl.lit(range_end)),
)
all_tags_in_range = sorted(agg_chart["tag"].unique().to_list())
sector_default_tags = [t for t in all_tags_in_range if TagGroup.SECTOR_CODES in tag_meta[t].groups]
if "dashboard_tags" not in st.session_state:
    st.session_state.dashboard_tags = list(sector_default_tags)

pending_group = st.session_state.pop("_dashboard_apply_group", None)
if pending_group is not None:
    try:
        g = TagGroup(pending_group)
    except ValueError:
        g = None
    if g is not None:
        picked = [t for t in all_tags_in_range if g in tag_meta[t].groups]
        st.session_state.dashboard_tags = list(picked)

_tags_valid = [t for t in st.session_state.dashboard_tags if t in all_tags]
st.session_state.dashboard_tags = _tags_valid

selected = st.multiselect(
    "Tags to show",
    options=all_tags,
    key="dashboard_tags",
    help=(
        "Daily allocation per tag; missing days are zero. Includes rule tags and calculated "
        "tags (e.g. unknown-cash, untagged-spend, uncategorised-sector). "
        "Initial selection uses the `sector-codes` group among tags with allocation in the "
        "date range (same as clicking the Sector-codes button below). Use a group button "
        "below to show only tags in that group (with allocation in range). Selection is kept "
        "when you change the date range. If none remain, the chart stays empty until you "
        "pick tags or a group."
    ),
)

group_btn_cols = st.columns(len(TagGroup), gap="small")
for col, grp in zip(group_btn_cols, TagGroup, strict=True):
    with col:
        st.button(
            _tag_group_button_label(grp),
            key=f"dashboard_tags_group_{grp.value}",
            help=f"Show only tags in the `{grp.value}` group (with allocation in this range).",
            on_click=_request_dashboard_group,
            args=(grp.value,),
            width="stretch",
        )

if not selected:
    st.warning("Select at least one tag to see the chart.")
    st.stop()

bundle = chart_and_bar_for_selected_tags(
    agg_long,
    ledger,
    range_start,
    range_end,
    tuple(selected),
)
chart_df = bundle.chart_df
bar_df = bundle.bar_df
ratio_denominator = bundle.ratio_denominator

spend = spend_metrics_for_rule_tags(
    RuleTagSpendMetricsInput(
        ledger=ledger,
        tags_df=tags_df,
        tag_meta=tag_meta,
        selected_tags=tuple(selected),
        lo=lo,
        hi=hi,
        monthly_in_range=monthly_in_range,
    ),
)
total_spend = spend.total_spend
avg_spend = spend.avg_spend_per_month
n_months_metric = spend.n_months_in_range

metric_cols = st.columns(2)
with metric_cols[0]:
    st.metric(
        "Total spend (selected tags)",
        "—" if total_spend is None else f"£{total_spend:,.2f}",
        help="Total of unique tagged transaction debit (each txn counted once per month).",
    )
with metric_cols[1]:
    st.metric(
        "Average spend per calendar month",
        "—" if avg_spend is None else f"£{avg_spend:,.2f}",
        help=(
            "Total spend divided by the number of calendar months in the selected date range. "
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
        width="stretch",
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
        width="stretch",
    )

n_bank = bank_statement_row_count()
if n_bank is not None:
    st.caption(
        f"Bank Delta table: **{n_bank}** statement row(s). Use **Upload statements** to add data."
    )
