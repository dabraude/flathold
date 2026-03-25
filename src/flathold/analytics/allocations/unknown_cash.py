"""Calculated ``unknown-cash`` tag: cash-spend minus cash-withdrawal per month, spread per day."""

from __future__ import annotations

from calendar import monthrange
from datetime import date

import polars as pl

UNKNOWN_CASH_TAG = "unknown-cash"

_CASH_WITHDRAWAL = "cash-withdrawal"
_CASH_SPEND = "cash-spend"


def _inclusive_day_spine(range_start: date, range_end: date) -> pl.DataFrame:
    """One row per calendar day from ``range_start`` through ``range_end`` (inclusive)."""
    rs, re = (range_start, range_end) if range_start <= range_end else (range_end, range_start)
    return pl.DataFrame({"period": pl.date_range(rs, re, interval="1d", eager=True)})


def monthly_unknown_cash_diff(tags_df: pl.DataFrame, period_cols: pl.DataFrame) -> pl.DataFrame:
    """One row per (year, month): ``unknown_month`` is spend alloc minus withdrawal alloc."""
    j = tags_df.join(period_cols, on="id", how="inner").filter(
        pl.col("tag").is_in([_CASH_WITHDRAWAL, _CASH_SPEND])
    )
    if j.is_empty():
        return pl.DataFrame(
            schema={
                "year": pl.Int64,
                "month": pl.Int64,
                "unknown_month": pl.Float64,
            }
        )
    g = j.group_by(["year", "month", "tag"]).agg(pl.col("allocation").sum().alias("allocation"))
    pivot = g.pivot(on="tag", index=["year", "month"], values="allocation")
    if _CASH_WITHDRAWAL not in pivot.columns:
        pivot = pivot.with_columns(pl.lit(0.0).alias(_CASH_WITHDRAWAL))
    if _CASH_SPEND not in pivot.columns:
        pivot = pivot.with_columns(pl.lit(0.0).alias(_CASH_SPEND))
    return pivot.with_columns(
        (pl.col(_CASH_SPEND).fill_null(0.0) - pl.col(_CASH_WITHDRAWAL).fill_null(0.0)).alias(
            "unknown_month"
        )
    ).select(["year", "month", "unknown_month"])


def average_monthly_unknown_cash(
    tags_df: pl.DataFrame,
    period_cols: pl.DataFrame,
    lo: date,
    hi: date,
) -> float:
    """Mean of ``unknown_month`` over calendar months in ``[lo, hi]`` (first-of-month bounds)."""
    m = monthly_unknown_cash_diff(tags_df, period_cols)
    if m.is_empty():
        return 0.0
    m = m.with_columns(pl.date(pl.col("year"), pl.col("month"), 1).alias("period"))
    m = m.filter((pl.col("period") >= lo) & (pl.col("period") <= hi))
    if m.is_empty():
        return 0.0
    item = m.select(pl.col("unknown_month").mean()).item()
    return float(item) if item is not None else 0.0


def unknown_cash_daily_allocations(
    tags_df: pl.DataFrame,
    period_cols: pl.DataFrame,
    range_start: date,
    range_end: date,
) -> pl.DataFrame:
    """Daily ``unknown-cash`` rows: monthly residual split evenly across days in that month."""
    monthly = monthly_unknown_cash_diff(tags_df, period_cols)
    if monthly.is_empty():
        spine = _inclusive_day_spine(range_start, range_end)
        return spine.with_columns(
            pl.lit(UNKNOWN_CASH_TAG).alias("tag"),
            pl.lit(0.0).alias("allocation"),
        ).select(["period", "tag", "allocation"])
    monthly = monthly.with_columns(
        pl.struct(["year", "month"])
        .map_elements(
            lambda s: monthrange(int(s["year"]), int(s["month"]))[1],
            return_dtype=pl.Int64,
        )
        .alias("days_in_month")
    ).with_columns((pl.col("unknown_month") / pl.col("days_in_month")).alias("daily_alloc"))

    spine = _inclusive_day_spine(range_start, range_end).with_columns(
        pl.col("period").dt.year().alias("year"),
        pl.col("period").dt.month().alias("month"),
    )
    out = spine.join(
        monthly.select(["year", "month", "daily_alloc"]),
        on=["year", "month"],
        how="left",
    ).with_columns(
        pl.lit(UNKNOWN_CASH_TAG).alias("tag"),
        pl.col("daily_alloc").fill_null(0.0).alias("allocation"),
    )
    return out.select(["period", "tag", "allocation"])
