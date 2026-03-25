"""Calculated ``untagged-spend``: per-txn |line| minus sum |alloc|, by month, spread per day."""

from __future__ import annotations

from calendar import monthrange
from datetime import date

import polars as pl

UNTAGGED_SPEND_TAG = "untagged-spend"


def _inclusive_day_spine(range_start: date, range_end: date) -> pl.DataFrame:
    """One row per calendar day from ``range_start`` through ``range_end`` (inclusive)."""
    rs, re = (range_start, range_end) if range_start <= range_end else (range_end, range_start)
    return pl.DataFrame({"period": pl.date_range(rs, re, interval="1d", eager=True)})


def _per_transaction_untagged(ledger: pl.DataFrame, tags_df: pl.DataFrame) -> pl.DataFrame:
    """Per ``id``: ``abs_line`` minus sum of ``|allocation|`` (clamped at 0)."""
    line = ledger.unique(subset=["id"], keep="first").select(
        [
            "id",
            "year",
            "month",
            "day",
            (pl.col("Debit Amount") + pl.col("Credit Amount")).abs().alias("abs_line"),
        ]
    )
    if len(tags_df) == 0:
        tagged = pl.DataFrame(
            schema={"id": pl.Utf8, "tagged_abs": pl.Float64},
        )
    else:
        tagged = tags_df.group_by("id").agg(pl.col("allocation").abs().sum().alias("tagged_abs"))
    merged = line.join(tagged, on="id", how="left").with_columns(
        pl.col("tagged_abs").fill_null(0.0)
    )
    return merged.with_columns(
        (pl.col("abs_line") - pl.col("tagged_abs")).clip(lower_bound=0.0).alias("untagged")
    )


def monthly_untagged_spend_totals(
    ledger: pl.DataFrame,
    tags_df: pl.DataFrame,
) -> pl.DataFrame:
    """One row per (year, month): sum of per-transaction untagged remainder."""
    per = _per_transaction_untagged(ledger, tags_df)
    if len(per) == 0:
        return pl.DataFrame(
            schema={
                "year": pl.Int64,
                "month": pl.Int64,
                "untagged_month": pl.Float64,
            }
        )
    return per.group_by(["year", "month"]).agg(pl.col("untagged").sum().alias("untagged_month"))


def average_monthly_untagged_spend(
    ledger: pl.DataFrame,
    tags_df: pl.DataFrame,
    lo: date,
    hi: date,
) -> float:
    """Mean of ``untagged_month`` over calendar months in ``[lo, hi]``."""
    m = monthly_untagged_spend_totals(ledger, tags_df)
    if len(m) == 0:
        return 0.0
    m = m.with_columns(pl.date(pl.col("year"), pl.col("month"), 1).alias("period"))
    m = m.filter((pl.col("period") >= lo) & (pl.col("period") <= hi))
    if len(m) == 0:
        return 0.0
    item = m.select(pl.col("untagged_month").mean()).item()
    return float(item) if item is not None else 0.0


def untagged_spend_daily_allocations(
    ledger: pl.DataFrame,
    tags_df: pl.DataFrame,
    range_start: date,
    range_end: date,
) -> pl.DataFrame:
    """Daily rows for ``untagged-spend``: monthly total split evenly across days in that month."""
    monthly = monthly_untagged_spend_totals(ledger, tags_df)
    if len(monthly) == 0:
        spine = _inclusive_day_spine(range_start, range_end)
        return spine.with_columns(
            pl.lit(UNTAGGED_SPEND_TAG).alias("tag"),
            pl.lit(0.0).alias("allocation"),
        ).select(["period", "tag", "allocation"])
    monthly = monthly.with_columns(
        pl.struct(["year", "month"])
        .map_elements(
            lambda s: monthrange(int(s["year"]), int(s["month"]))[1],
            return_dtype=pl.Int64,
        )
        .alias("days_in_month")
    ).with_columns((pl.col("untagged_month") / pl.col("days_in_month")).alias("daily_alloc"))

    spine = _inclusive_day_spine(range_start, range_end).with_columns(
        pl.col("period").dt.year().alias("year"),
        pl.col("period").dt.month().alias("month"),
    )
    out = spine.join(
        monthly.select(["year", "month", "daily_alloc"]),
        on=["year", "month"],
        how="left",
    ).with_columns(
        pl.lit(UNTAGGED_SPEND_TAG).alias("tag"),
        pl.col("daily_alloc").fill_null(0.0).alias("allocation"),
    )
    return out.select(["period", "tag", "allocation"])
