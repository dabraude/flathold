"""Enhanced ledger: rule-tag daily allocations + calculated-tag series (in-memory; no IO)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date

import polars as pl

from flathold.analytics.allocations.uncategorised_sector import (
    UNCATEGORISED_SECTOR_TAG,
    per_transaction_uncategorised_sector_remainder,
    uncategorised_sector_daily_allocations,
)
from flathold.analytics.allocations.unknown_cash import unknown_cash_daily_allocations
from flathold.analytics.allocations.untagged_spend import (
    UNTAGGED_SPEND_TAG,
    per_transaction_untagged_remainder,
    untagged_spend_daily_allocations,
)
from flathold.core.ledger_columns import CALCULATED_TAGS_COLUMN
from flathold.core.tag_rule_metadata import TagRuleMetadata


@dataclass(frozen=True, slots=True)
class DailyTagAllocationsInput:
    ledger: pl.DataFrame
    tags_df: pl.DataFrame
    tag_meta: Mapping[str, TagRuleMetadata]
    range_start: date
    range_end: date


def build_enhanced_ledger(
    ledger: pl.DataFrame,
    tags_df: pl.DataFrame | None,
    tag_meta: Mapping[str, TagRuleMetadata],
) -> pl.DataFrame:
    """Base ledger plus ``calculated_tags`` per row (hints tied to per-transaction remainders).

    Month/day-only series (e.g. ``unknown-cash``) are omitted at row level.
    """

    if ledger.is_empty():
        return ledger
    tags = (
        tags_df
        if tags_df is not None and len(tags_df) > 0
        else pl.DataFrame(
            schema={
                "id": pl.Utf8,
                "tag": pl.Utf8,
                "allocation": pl.Float64,
                "counter_party": pl.Boolean,
            }
        )
    )
    if "allocation" not in tags.columns:
        tags = tags.with_columns(pl.lit(0.0).alias("allocation"))
    u = per_transaction_untagged_remainder(ledger, tags).select(["id", "untagged"])
    c = per_transaction_uncategorised_sector_remainder(ledger, tags, tag_meta).select(
        ["id", "uncategorised"]
    )
    out = (
        ledger.join(u, on="id", how="left")
        .join(c, on="id", how="left")
        .with_columns(
            pl.col("untagged").fill_null(0.0),
            pl.col("uncategorised").fill_null(0.0),
        )
        .with_columns(
            pl.concat_list(
                [
                    pl.when(pl.col("untagged") > 0).then(pl.lit(UNTAGGED_SPEND_TAG)),
                    pl.when((pl.col("untagged") <= 0) & (pl.col("uncategorised") > 0)).then(
                        pl.lit(UNCATEGORISED_SECTOR_TAG)
                    ),
                ]
            )
            .list.drop_nulls()
            .alias(CALCULATED_TAGS_COLUMN)
        )
        .drop(["untagged", "uncategorised"])
    )
    return out


def daily_tag_allocations_long(
    inp: DailyTagAllocationsInput,
    virtual_txn_rows: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """
    Long-form ``period``, ``tag``, ``allocation`` for rule-applied tags plus calculated tags.

    ``virtual_txn_rows`` is reserved for future synthetic transactions; when non-empty, it will
    be merged into this series (currently ignored).
    """
    period_cols = inp.ledger.select(["id", "year", "month", "day"]).unique()
    joined = inp.tags_df.join(period_cols, on="id", how="inner").with_columns(
        pl.date(pl.col("year"), pl.col("month"), pl.col("day")).alias("period"),
    )
    agg_from_rules = joined.group_by(["period", "tag"]).agg(
        pl.col("allocation").sum().alias("allocation")
    )
    unknown_daily = unknown_cash_daily_allocations(
        inp.tags_df, period_cols, inp.range_start, inp.range_end
    )
    untagged_daily = untagged_spend_daily_allocations(
        inp.ledger, inp.tags_df, inp.range_start, inp.range_end
    )
    uncategorised_daily = uncategorised_sector_daily_allocations(
        inp.ledger,
        inp.tags_df,
        inp.range_start,
        inp.range_end,
        inp.tag_meta,
    )
    parts: list[pl.DataFrame] = [
        agg_from_rules,
        unknown_daily,
        untagged_daily,
        uncategorised_daily,
    ]
    if virtual_txn_rows is not None and len(virtual_txn_rows) > 0:
        parts.append(virtual_txn_rows)
    return pl.concat(parts, how="vertical")
