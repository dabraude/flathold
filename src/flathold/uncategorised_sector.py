"""Calculated ``uncategorised-sector``: |line| minus sector-tag allocations, by month/day."""

from __future__ import annotations

from calendar import monthrange
from collections.abc import Mapping
from datetime import date

import polars as pl

from flathold.tag_group import TagGroup
from flathold.tag_rule_metadata import TagRuleMetadata

UNCATEGORISED_SECTOR_TAG = "uncategorised-sector"


def _sector_tag_names(meta: Mapping[str, TagRuleMetadata]) -> frozenset[str]:
    """Non-calculated tags whose definitions include the ``sector-codes`` group."""
    return frozenset(
        {t for t, m in meta.items() if TagGroup.SECTOR_CODES in m.groups and not m.calculated}
    )


def _inclusive_day_spine(range_start: date, range_end: date) -> pl.DataFrame:
    rs, re = (range_start, range_end) if range_start <= range_end else (range_end, range_start)
    return pl.DataFrame({"period": pl.date_range(rs, re, interval="1d", eager=True)})


def _per_transaction_uncategorised_sector(
    ledger: pl.DataFrame,
    tags_df: pl.DataFrame,
    meta: Mapping[str, TagRuleMetadata],
) -> pl.DataFrame:
    """Per ``id``: ``abs_line`` minus sum of ``|allocation|`` on sector-code tags only (>= 0)."""
    sector_tags = _sector_tag_names(meta)
    line = ledger.unique(subset=["id"], keep="first").select(
        [
            "id",
            "year",
            "month",
            "day",
            (pl.col("Debit Amount") + pl.col("Credit Amount")).abs().alias("abs_line"),
        ]
    )
    if len(tags_df) == 0 or len(sector_tags) == 0:
        tagged = pl.DataFrame(schema={"id": pl.Utf8, "sector_tagged_abs": pl.Float64})
    else:
        sector_df = tags_df.filter(pl.col("tag").is_in(list(sector_tags)))
        if len(sector_df) == 0:
            tagged = pl.DataFrame(schema={"id": pl.Utf8, "sector_tagged_abs": pl.Float64})
        else:
            tagged = sector_df.group_by("id").agg(
                pl.col("allocation").abs().sum().alias("sector_tagged_abs")
            )
    merged = line.join(tagged, on="id", how="left").with_columns(
        pl.col("sector_tagged_abs").fill_null(0.0)
    )
    return merged.with_columns(
        (pl.col("abs_line") - pl.col("sector_tagged_abs"))
        .clip(lower_bound=0.0)
        .alias("uncategorised")
    )


def monthly_uncategorised_sector_totals(
    ledger: pl.DataFrame,
    tags_df: pl.DataFrame,
    meta: Mapping[str, TagRuleMetadata],
) -> pl.DataFrame:
    per = _per_transaction_uncategorised_sector(ledger, tags_df, meta)
    if len(per) == 0:
        return pl.DataFrame(
            schema={
                "year": pl.Int64,
                "month": pl.Int64,
                "uncat_month": pl.Float64,
            }
        )
    return per.group_by(["year", "month"]).agg(pl.col("uncategorised").sum().alias("uncat_month"))


def average_monthly_uncategorised_sector(
    ledger: pl.DataFrame,
    tags_df: pl.DataFrame,
    meta: Mapping[str, TagRuleMetadata],
    lo: date,
    hi: date,
) -> float:
    m = monthly_uncategorised_sector_totals(ledger, tags_df, meta)
    if len(m) == 0:
        return 0.0
    m = m.with_columns(pl.date(pl.col("year"), pl.col("month"), 1).alias("period"))
    m = m.filter((pl.col("period") >= lo) & (pl.col("period") <= hi))
    if len(m) == 0:
        return 0.0
    item = m.select(pl.col("uncat_month").mean()).item()
    return float(item) if item is not None else 0.0


def uncategorised_sector_daily_allocations(
    ledger: pl.DataFrame,
    tags_df: pl.DataFrame,
    range_start: date,
    range_end: date,
    meta: Mapping[str, TagRuleMetadata],
) -> pl.DataFrame:
    monthly = monthly_uncategorised_sector_totals(ledger, tags_df, meta)
    if len(monthly) == 0:
        spine = _inclusive_day_spine(range_start, range_end)
        return spine.with_columns(
            pl.lit(UNCATEGORISED_SECTOR_TAG).alias("tag"),
            pl.lit(0.0).alias("allocation"),
        ).select(["period", "tag", "allocation"])
    monthly = monthly.with_columns(
        pl.struct(["year", "month"])
        .map_elements(
            lambda s: monthrange(int(s["year"]), int(s["month"]))[1],
            return_dtype=pl.Int64,
        )
        .alias("days_in_month")
    ).with_columns((pl.col("uncat_month") / pl.col("days_in_month")).alias("daily_alloc"))

    spine = _inclusive_day_spine(range_start, range_end).with_columns(
        pl.col("period").dt.year().alias("year"),
        pl.col("period").dt.month().alias("month"),
    )
    out = spine.join(
        monthly.select(["year", "month", "daily_alloc"]),
        on=["year", "month"],
        how="left",
    ).with_columns(
        pl.lit(UNCATEGORISED_SECTOR_TAG).alias("tag"),
        pl.col("daily_alloc").fill_null(0.0).alias("allocation"),
    )
    return out.select(["period", "tag", "allocation"])
