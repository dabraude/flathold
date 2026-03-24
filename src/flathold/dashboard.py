"""Dashboard — tagged allocation over time (loaded by `app.py` via `st.navigation`)."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta

import altair as alt
import polars as pl
import streamlit as st

from flathold.bank_delta import read_existing_table
from flathold.ledger_delta import (
    read_ledger_table,
    read_transaction_tags_table,
    refresh_ledger_and_tags,
)
from flathold.tag_definitions_store import read_tag_rule_metadata_map
from flathold.tag_group import TagGroup
from flathold.tag_rules import tag_show_on_dashboard_default
from flathold.uncategorised_sector import (
    UNCATEGORISED_SECTOR_TAG,
    average_monthly_uncategorised_sector,
    uncategorised_sector_daily_allocations,
)
from flathold.unknown_cash import (
    UNKNOWN_CASH_TAG,
    average_monthly_unknown_cash,
    unknown_cash_daily_allocations,
)
from flathold.untagged_spend import (
    UNTAGGED_SPEND_TAG,
    average_monthly_untagged_spend,
    untagged_spend_daily_allocations,
)

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


def _request_dashboard_group(group_value: str) -> None:
    st.session_state["_dashboard_apply_group"] = group_value


def _tag_group_button_label(group: TagGroup) -> str:
    return group.value.replace("-", " ").title()


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

ledger = read_ledger_table()
tags_df = read_transaction_tags_table()

if ledger is None or len(ledger) == 0:
    st.info(
        "No ledger data yet. Upload a CSV on **Upload statements** and/or add rows on "
        "**Manual entries**."
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
    st.info("No transaction tags yet. Use **Update** in the sidebar (after bank data is uploaded).")
    st.stop()

tag_meta = read_tag_rule_metadata_map()
period_cols = ledger.select(["id", "year", "month", "day"]).unique()
joined = tags_df.join(period_cols, on="id", how="inner").with_columns(
    pl.date(pl.col("year"), pl.col("month"), pl.col("day")).alias("period"),
)
agg_from_rules = joined.group_by(["period", "tag"]).agg(
    pl.col("allocation").sum().alias("allocation")
)
unknown_daily = unknown_cash_daily_allocations(tags_df, period_cols, range_start, range_end)
untagged_daily = untagged_spend_daily_allocations(ledger, tags_df, range_start, range_end)
uncategorised_daily = uncategorised_sector_daily_allocations(
    ledger, tags_df, range_start, range_end, tag_meta
)
agg = pl.concat(
    [agg_from_rules, unknown_daily, untagged_daily, uncategorised_daily], how="vertical"
)
calculated_tag_names = {t for t, m in tag_meta.items() if m.calculated}
all_tags = sorted(set(agg["tag"].unique().to_list()) | calculated_tag_names)

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

# Apply group filter before the multiselect — cannot assign `dashboard_tags` after the widget
# with that key is instantiated (StreamlitAPIException).
pending_group = st.session_state.pop("_dashboard_apply_group", None)
if pending_group is not None:
    try:
        g = TagGroup(pending_group)
    except ValueError:
        g = None
    if g is not None:
        picked = [t for t in all_tags_in_range if g in tag_meta[t].groups]
        st.session_state.dashboard_tags = list(picked)

# Keep the user's tag selection when the date range changes; drop unknown tags only.
_tags_valid = [t for t in st.session_state.dashboard_tags if t in all_tags]
st.session_state.dashboard_tags = _tags_valid

selected = st.multiselect(
    "Tags to show",
    options=all_tags,
    key="dashboard_tags",
    help=(
        "Daily allocation per tag; missing days are zero. Includes rule tags and calculated "
        "tags (e.g. unknown-cash, untagged-spend, uncategorised-sector). "
        "Initial selection uses `show_on_dashboard_by_default` among tags with allocation in "
        "the date range, or all such tags if none match. Use a group button below to show only "
        "tags in that group (with allocation in range). Selection is kept when you change the "
        "date range. If none remain, the chart stays empty until you pick tags or a group."
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

rule_selected = tuple(t for t in selected if tag_meta.get(t) is None or not tag_meta[t].calculated)
avg_tagged = _avg_monthly_tagged_unique_debit(
    _TaggedUniqueMonthlyAvgInput(
        ledger=ledger,
        tags_df=tags_df,
        selected_tags=rule_selected,
        period_cols=period_cols,
        lo=lo,
        hi=hi,
        monthly_in_range=monthly_in_range,
    ),
)
n_months_metric = monthly_in_range.height
metric_cols = st.columns(4)
with metric_cols[0]:
    if rule_selected:
        st.metric(
            "Average tagged monthly expenditure",
            f"£{avg_tagged:,.2f}",
            help=(
                "Mean per calendar month of debit from transactions that carry at least one "
                "selected rule tag; each transaction is counted once per month (same months as "
                f"total average above). {n_months_metric} month(s) in range. "
                "Calculated tags are excluded."
            ),
        )
    else:
        st.metric(
            "Average tagged monthly expenditure",
            "—",
            help="No rule tags selected; pick tags other than calculated-only to compute this.",
        )
with metric_cols[1]:
    if UNKNOWN_CASH_TAG in selected:
        avg_unknown = average_monthly_unknown_cash(tags_df, period_cols, lo, hi)
        st.metric(
            "Average monthly unknown cash",
            f"£{avg_unknown:,.2f}",
            help=(
                "Mean per calendar month of (cash-spend allocations minus cash-withdrawal "
                f"allocations). {n_months_metric} month(s) in range."
            ),
        )
    else:
        st.metric(
            "Average monthly unknown cash",
            "—",
            help="Select **unknown-cash** to see spend minus withdrawal by month (spread per day).",
        )
with metric_cols[2]:
    if UNTAGGED_SPEND_TAG in selected:
        avg_untagged = average_monthly_untagged_spend(ledger, tags_df, lo, hi)
        st.metric(
            "Average monthly untagged spend",
            f"£{avg_untagged:,.2f}",
            help=(
                "Mean per calendar month of |line| minus sum of |tag allocations| per "
                f"transaction (then summed). {n_months_metric} month(s) in range."
            ),
        )
    else:
        st.metric(
            "Average monthly untagged spend",
            "—",
            help=(
                "Select **untagged-spend** for debit not covered by tag allocations "
                "(spread per day)."
            ),
        )
with metric_cols[3]:
    if UNCATEGORISED_SECTOR_TAG in selected:
        avg_uncat = average_monthly_uncategorised_sector(ledger, tags_df, tag_meta, lo, hi)
        st.metric(
            "Average monthly uncategorised sector",
            f"£{avg_uncat:,.2f}",
            help=(
                "Mean per calendar month of |line| minus sum of |allocations| on tags in the "
                f"sector-codes group only (then summed). {n_months_metric} month(s) in range."
            ),
        )
    else:
        st.metric(
            "Average monthly uncategorised sector",
            "—",
            help=(
                "Select **uncategorised-sector** for debit not covered by sector-code tag "
                "allocations (spread per day)."
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

bank = read_existing_table()
if bank is not None and len(bank) > 0:
    n = len(bank)
    st.caption(
        f"Bank Delta table: **{n}** statement row(s). Use **Upload statements** to add data."
    )
