"""Enhanced ledger: rule-tag daily allocations + calculated-tag series (in-memory; no IO)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date

import polars as pl

from flathold.analytics.allocations.uncategorised_sector import (
    uncategorised_sector_daily_allocations,
)
from flathold.analytics.allocations.unknown_cash import unknown_cash_daily_allocations
from flathold.analytics.allocations.untagged_spend import untagged_spend_daily_allocations
from flathold.core.tag_rule_metadata import TagRuleMetadata


@dataclass(frozen=True, slots=True)
class DailyTagAllocationsInput:
    ledger: pl.DataFrame
    tags_df: pl.DataFrame
    tag_meta: Mapping[str, TagRuleMetadata]
    range_start: date
    range_end: date


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
