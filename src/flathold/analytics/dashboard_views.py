"""Analytic views for the dashboard: chart-ready frames and spend metrics."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date

import polars as pl

from flathold.analytics.dashboard_metrics import (
    TaggedUniqueMonthlyAvgInput,
    avg_monthly_tagged_unique_debit,
    inclusive_day_spine,
)
from flathold.analytics.enhanced_ledger import DailyTagAllocationsInput, daily_tag_allocations_long
from flathold.core.tag_rule_metadata import TagRuleMetadata


@dataclass(frozen=True, slots=True)
class DashboardChartBundle:
    chart_df: pl.DataFrame
    bar_df: pl.DataFrame
    ratio_denominator: float


@dataclass(frozen=True, slots=True)
class DashboardSpendMetrics:
    total_spend: float | None
    avg_spend_per_month: float | None
    n_months_in_range: int


@dataclass(frozen=True, slots=True)
class RuleTagSpendMetricsInput:
    ledger: pl.DataFrame
    tags_df: pl.DataFrame
    tag_meta: Mapping[str, TagRuleMetadata]
    selected_tags: tuple[str, ...]
    lo: date
    hi: date
    monthly_in_range: pl.DataFrame


def avg_monthly_ledger_expenditure(ledger: pl.DataFrame, lo: date, hi: date) -> float:
    """Mean of each calendar month's total debits in ``[lo, hi]`` (first-of-month bounds)."""
    monthly_debit_totals = (
        ledger.group_by(["year", "month"])
        .agg(pl.col("Debit Amount").sum().alias("monthly_debit"))
        .with_columns(pl.date(pl.col("year"), pl.col("month"), 1).alias("period"))
    )
    monthly_in_range = monthly_debit_totals.filter(
        (pl.col("period") >= lo) & (pl.col("period") <= hi)
    )
    monthly_debits = monthly_in_range.get_column("monthly_debit").to_list()
    return float(sum(monthly_debits) / len(monthly_debits)) if monthly_debits else 0.0


def monthly_debits_in_range(ledger: pl.DataFrame, lo: date, hi: date) -> pl.DataFrame:
    monthly_debit_totals = (
        ledger.group_by(["year", "month"])
        .agg(pl.col("Debit Amount").sum().alias("monthly_debit"))
        .with_columns(pl.date(pl.col("year"), pl.col("month"), 1).alias("period"))
    )
    return monthly_debit_totals.filter((pl.col("period") >= lo) & (pl.col("period") <= hi))


def all_tags_for_dashboard(
    agg_long: pl.DataFrame,
    tag_meta: Mapping[str, TagRuleMetadata],
) -> list[str]:
    calculated_tag_names = {t for t, m in tag_meta.items() if m.calculated}
    return sorted(set(agg_long["tag"].unique().to_list()) | calculated_tag_names)


def chart_and_bar_for_selected_tags(
    agg_long: pl.DataFrame,
    ledger: pl.DataFrame,
    range_start: date,
    range_end: date,
    selected_tags: tuple[str, ...],
) -> DashboardChartBundle:
    """Wide daily ``chart_df`` and bar chart frame; same semantics as the legacy dashboard page."""
    day_spine = inclusive_day_spine(range_start, range_end)
    agg_chart = agg_long.filter(
        (pl.col("period") >= pl.lit(range_start)) & (pl.col("period") <= pl.lit(range_end)),
    )
    pivoted = (
        agg_chart.filter(pl.col("tag").is_in(list(selected_tags)))
        .pivot(on="tag", index="period", values="allocation")
        .sort("period")
    )
    if pivoted.is_empty():
        chart_df = day_spine.with_columns(*[pl.lit(0.0).alias(t) for t in selected_tags])
    else:
        for tag in selected_tags:
            if tag not in pivoted.columns:
                pivoted = pivoted.with_columns(pl.lit(0.0).alias(tag))
            else:
                pivoted = pivoted.with_columns(pl.col(tag).fill_null(0.0))
        pivoted = pivoted.select(["period", *selected_tags])
        chart_df = day_spine.join(pivoted, on="period", how="left")
        for tag in selected_tags:
            chart_df = chart_df.with_columns(pl.col(tag).fill_null(0.0))

    ledger_in_range = ledger.with_columns(
        pl.date(pl.col("year"), pl.col("month"), pl.col("day")).alias("txn_date")
    ).filter(
        (pl.col("txn_date") >= pl.lit(range_start)) & (pl.col("txn_date") <= pl.lit(range_end))
    )
    total_debit = float(ledger_in_range.select(pl.col("Debit Amount").sum()).item() or 0.0)

    tag_totals_df = (
        agg_chart.filter(pl.col("tag").is_in(list(selected_tags)))
        .group_by("tag")
        .agg(pl.col("allocation").sum().alias("amount"))
    )
    tag_amounts = {str(row[0]): float(row[1]) for row in tag_totals_df.iter_rows()}

    n_days = chart_df.height
    if n_days > 0:
        bar_allocated = [tag_amounts.get(t, 0.0) / n_days for t in selected_tags]
        ratio_denominator = total_debit / n_days
    else:
        bar_allocated = [0.0 for t in selected_tags]
        ratio_denominator = 0.0

    bar_df = pl.DataFrame(
        {
            "tag": list(selected_tags),
            "allocated": bar_allocated,
        }
    )
    return DashboardChartBundle(
        chart_df=chart_df,
        bar_df=bar_df,
        ratio_denominator=ratio_denominator,
    )


def spend_metrics_for_rule_tags(inp: RuleTagSpendMetricsInput) -> DashboardSpendMetrics:
    """Unique tagged debit per month; only when selection is non-empty rule tags."""
    period_cols = inp.ledger.select(["id", "year", "month", "day"]).unique()
    n_months_metric = inp.monthly_in_range.height
    rule_selected = tuple(
        t
        for t in inp.selected_tags
        if inp.tag_meta.get(t) is None or not inp.tag_meta[t].calculated
    )
    if not rule_selected:
        return DashboardSpendMetrics(
            total_spend=None,
            avg_spend_per_month=None,
            n_months_in_range=n_months_metric,
        )
    avg_tagged = avg_monthly_tagged_unique_debit(
        TaggedUniqueMonthlyAvgInput(
            ledger=inp.ledger,
            tags_df=inp.tags_df,
            selected_tags=rule_selected,
            period_cols=period_cols,
            lo=inp.lo,
            hi=inp.hi,
            monthly_in_range=inp.monthly_in_range,
        ),
    )
    return DashboardSpendMetrics(
        total_spend=avg_tagged * float(n_months_metric),
        avg_spend_per_month=avg_tagged,
        n_months_in_range=n_months_metric,
    )


def build_dashboard_allocation_long(
    ledger: pl.DataFrame,
    tags_df: pl.DataFrame,
    tag_meta: Mapping[str, TagRuleMetadata],
    range_start: date,
    range_end: date,
) -> pl.DataFrame:
    """Delegate to enhanced ledger daily series (rule + calculated tags)."""
    return daily_tag_allocations_long(
        DailyTagAllocationsInput(
            ledger=ledger,
            tags_df=tags_df,
            tag_meta=tag_meta,
            range_start=range_start,
            range_end=range_end,
        ),
    )
