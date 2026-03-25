"""Pure Polars helpers for the dashboard (dates, tagged monthly averages)."""

from dataclasses import dataclass
from datetime import date, datetime

import polars as pl


def to_date(d: object) -> date:
    """Coerce Polars/Streamlit date-like values to ``datetime.date``."""
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    msg = f"Expected date-like value, got {type(d).__name__}"
    raise TypeError(msg)


def first_of_month(d: date) -> date:
    return date(d.year, d.month, 1)


def shift_months_first(d: date, delta: int) -> date:
    """First-of-month date ``d`` shifted by ``delta`` months."""
    m = d.month - 1 + delta
    y = d.year + m // 12
    m = m % 12 + 1
    return date(y, m, 1)


def clamp_range(lo: date, hi: date, bound_lo: date, bound_hi: date) -> tuple[date, date]:
    """Clamp ``[lo, hi]`` to ``[bound_lo, bound_hi]``; if empty, return full bounds."""
    lo2 = max(lo, bound_lo)
    hi2 = min(hi, bound_hi)
    if lo2 > hi2:
        return bound_lo, bound_hi
    return lo2, hi2


def inclusive_day_spine(range_start: date, range_end: date) -> pl.DataFrame:
    """One row per calendar day from ``range_start`` through ``range_end`` (inclusive)."""
    rs, re = (range_start, range_end) if range_start <= range_end else (range_end, range_start)
    return pl.DataFrame({"period": pl.date_range(rs, re, interval="1d", eager=True)})


@dataclass(frozen=True, slots=True)
class TaggedUniqueMonthlyAvgInput:
    ledger: pl.DataFrame
    tags_df: pl.DataFrame
    selected_tags: tuple[str, ...]
    period_cols: pl.DataFrame
    lo: date
    hi: date
    monthly_in_range: pl.DataFrame


def avg_monthly_tagged_unique_debit(inp: TaggedUniqueMonthlyAvgInput) -> float:
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
