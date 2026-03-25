"""Tag rule types, validation, and applying rules to a ledger."""

from collections.abc import Mapping
from dataclasses import dataclass

import polars as pl

from flathold.core.tag_group import TagGroup
from flathold.core.tag_pattern import validate_kebab_tag
from flathold.core.tag_rule_metadata import TagRuleMetadata

# Float tolerance when comparing summed allocations to |Debit + Credit|.
_GROUP_ALLOC_EPS = 1e-6


@dataclass(frozen=True, slots=True)
class TagRule:
    """Rows matching `predicate` get `tag`; multiple rules can apply to one transaction.

    Allocation is ``amount_absolute + amount_proportion * line_amount`` where ``line_amount`` is
    ``Debit Amount + Credit Amount`` on the matched ledger row.

    Display and grouping (dashboard defaults, counterparty, allocation groups) live in the
    ``tag_definitions`` Delta table, not on ``TagRule``.
    """

    tag: str
    predicate: pl.Expr
    amount_absolute: float = 0.0
    amount_proportion: float = 0.0


def _rule_allocation_expr(rule: TagRule) -> pl.Expr:
    line_amount = pl.col("Debit Amount") + pl.col("Credit Amount")
    return pl.lit(rule.amount_absolute) + pl.lit(rule.amount_proportion) * line_amount


def validate_tag_group_allocations(
    ledger: pl.DataFrame,
    tags_df: pl.DataFrame,
    tag_meta: Mapping[str, TagRuleMetadata],
) -> None:
    """Raise ValueError if any transaction has group allocation sums over 100% of |line amount|.

    Each tag's ``groups`` entry is a separate bucket: for each ``(id, group)`` we require
    ``sum(|allocation|) <= |Debit + Credit|`` for that transaction, where each tag row counts
    its ``|allocation|`` once toward every group it belongs to.
    """
    if len(tags_df) == 0:
        return
    tag_to_groups = {t: m.groups for t, m in tag_meta.items()}
    amounts = ledger.select(
        pl.col("id"),
        (pl.col("Debit Amount") + pl.col("Credit Amount")).abs().alias("abs_line"),
    )
    records: list[tuple[str, str, float]] = []
    for row in tags_df.iter_rows(named=True):
        tid = row["id"]
        tag = row["tag"]
        alloc_abs = abs(float(row["allocation"]))
        for g in tag_to_groups.get(tag, ()):
            records.append((tid, str(g), alloc_abs))
    if not records:
        return
    exploded = pl.DataFrame(
        {
            "id": [r[0] for r in records],
            "group": [r[1] for r in records],
            "allocation_abs": [r[2] for r in records],
        }
    )
    by_group = exploded.group_by(["id", "group"]).agg(
        pl.col("allocation_abs").sum().alias("group_sum_abs")
    )
    check = by_group.join(amounts, on="id", how="inner")
    bad = check.filter(pl.col("group_sum_abs") > pl.col("abs_line") + _GROUP_ALLOC_EPS)
    if len(bad) > 0:
        sample = bad.head(10).select(["id", "group", "group_sum_abs", "abs_line"])
        lines = [
            f"id={r['id']!r} group={r['group']!r} sum(|alloc|)={r['group_sum_abs']:.6g} "
            f"cap=|line|={r['abs_line']:.6g}"
            for r in sample.iter_rows(named=True)
        ]
        msg = (
            "Tag group allocations exceed 100% of |Debit + Credit| for "
            f"{len(bad)} (transaction, group) pair(s). Examples:\n" + "\n".join(lines)
        )
        raise ValueError(msg)


def validate_at_most_one_counter_party_tag_per_transaction(
    tags_df: pl.DataFrame,
    tag_meta: Mapping[str, TagRuleMetadata],
) -> None:
    """Raise ValueError if any transaction has more than one tag in the ``counter-party`` group."""
    if len(tags_df) == 0:
        return
    tag_to_groups = {t: m.groups for t, m in tag_meta.items()}
    cp_rows: list[tuple[str, str]] = []
    for row in tags_df.iter_rows(named=True):
        tag = row["tag"]
        if TagGroup.COUNTER_PARTY in tag_to_groups.get(tag, ()):
            cp_rows.append((row["id"], tag))
    if not cp_rows:
        return
    exploded = pl.DataFrame(
        {
            "id": [r[0] for r in cp_rows],
            "tag": [r[1] for r in cp_rows],
        }
    )
    by_id = exploded.group_by("id").agg(pl.col("tag").implode().alias("counter_party_tags"))
    bad = by_id.filter(pl.col("counter_party_tags").list.len() > 1)
    if len(bad) > 0:
        sample = bad.head(10)
        lines = [
            f"id={r['id']!r} tags={r['counter_party_tags']!r}" for r in sample.iter_rows(named=True)
        ]
        msg = (
            "Each transaction may have at most one tag in the counter-party group; "
            f"{len(bad)} transaction(s) have more. Examples:\n" + "\n".join(lines)
        )
        raise ValueError(msg)


def validate_transaction_tags_no_calculated_tags(
    tags_df: pl.DataFrame,
    tag_meta: Mapping[str, TagRuleMetadata],
) -> None:
    """Raise ``ValueError`` if any row uses a tag marked calculated in definitions."""
    if len(tags_df) == 0:
        return
    bad: list[tuple[str, str]] = []
    for row in tags_df.iter_rows(named=True):
        tag = str(row["tag"])
        m = tag_meta.get(tag)
        if m is not None and m.calculated:
            bad.append((str(row["id"]), tag))
    if not bad:
        return
    sample = bad[:10]
    lines = [f"id={tid!r} tag={t!r}" for tid, t in sample]
    msg = (
        "Transaction tags must not use calculated tags: "
        f"{len(bad)} row(s). Examples:\n" + "\n".join(lines)
    )
    raise ValueError(msg)


def apply_tag_rules(
    ledger: pl.DataFrame,
    rules: tuple[TagRule, ...],
    tag_meta: Mapping[str, TagRuleMetadata],
) -> pl.DataFrame:
    """Return one row per (id, tag) with allocation from each rule's absolute and proportion."""
    empty = pl.DataFrame(
        {
            "id": pl.Series([], dtype=pl.Utf8),
            "tag": pl.Series([], dtype=pl.Utf8),
            "allocation": pl.Series([], dtype=pl.Float64),
            "counter_party": pl.Series([], dtype=pl.Boolean),
        }
    )
    if not rules:
        return empty
    parts: list[pl.DataFrame] = []
    for rule in rules:
        meta = tag_meta.get(rule.tag)
        if meta is None:
            msg = f"Missing tag_definitions metadata for rule tag {rule.tag!r}"
            raise ValueError(msg)
        matched = ledger.filter(rule.predicate).select(
            pl.col("id"),
            pl.lit(rule.tag).alias("tag"),
            _rule_allocation_expr(rule).alias("allocation"),
            pl.lit(meta.counter_party).alias("counter_party"),
        )
        parts.append(matched)
    if not parts:
        return empty
    out = pl.concat(parts)
    out = out.unique(subset=["id", "tag"])
    for t in out["tag"].to_list():
        validate_kebab_tag(t)
    validate_tag_group_allocations(ledger, out, tag_meta)
    validate_at_most_one_counter_party_tag_per_transaction(out, tag_meta)
    validate_transaction_tags_no_calculated_tags(out, tag_meta)
    return out
