"""Tag rule types, validation, and applying rules to a ledger."""

import re
from dataclasses import dataclass, field

import polars as pl

# Lowercase letters, digits, and single hyphens between segments (kebab-case).
KEBAB_TAG_PATTERN = r"^[a-z0-9]+(-[a-z0-9]+)*$"
_TAG_RE = re.compile(KEBAB_TAG_PATTERN)
# Float tolerance when comparing summed allocations to |Debit + Credit|.
_GROUP_ALLOC_EPS = 1e-6


def validate_kebab_tag(tag: str) -> None:
    """Raise ValueError if `tag` is not kebab-case alphanumeric."""
    if not _TAG_RE.fullmatch(tag):
        msg = f"Invalid tag {tag!r}: expected kebab-case alphanumeric (pattern {KEBAB_TAG_PATTERN})"
        raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class TagRule:
    """Rows matching `predicate` get `tag`; multiple rules can apply to one transaction.

    Allocation is ``amount_absolute + amount_proportion * line_amount`` where ``line_amount`` is
    ``Debit Amount + Credit Amount`` on the matched ledger row.

    ``show_on_dashboard_by_default`` controls the dashboard tag multiselect initial selection for
    this tag (tags not listed in rules default to False).

    ``counter_party`` marks tags that identify a specific counterparty (person or institution).

    ``groups`` classifies the tag (e.g. ``"counter_party"``, ``"utilities"``). If ``counter_party``
    is true, ``"counter_party"`` is appended in ``__post_init__`` when missing.
    """

    tag: str
    predicate: pl.Expr
    amount_absolute: float = 0.0
    amount_proportion: float = 0.0
    show_on_dashboard_by_default: bool = False
    counter_party: bool = False
    groups: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not self.counter_party or "counter_party" in self.groups:
            return
        object.__setattr__(self, "groups", (*self.groups, "counter_party"))


def _rule_allocation_expr(rule: TagRule) -> pl.Expr:
    line_amount = pl.col("Debit Amount") + pl.col("Credit Amount")
    return pl.lit(rule.amount_absolute) + pl.lit(rule.amount_proportion) * line_amount


def validate_tag_group_allocations(
    ledger: pl.DataFrame,
    tags_df: pl.DataFrame,
    rules: tuple[TagRule, ...],
) -> None:
    """Raise ValueError if any transaction has group allocation sums over 100% of |line amount|.

    Each ``TagRule.groups`` entry is a separate bucket: for each ``(id, group)`` we require
    ``sum(|allocation|) <= |Debit + Credit|`` for that transaction, where each tag row counts
    its ``|allocation|`` once toward every group it belongs to.
    """
    if len(tags_df) == 0:
        return
    tag_to_groups = {r.tag: r.groups for r in rules}
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
            records.append((tid, g, alloc_abs))
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


def apply_tag_rules(ledger: pl.DataFrame, rules: tuple[TagRule, ...]) -> pl.DataFrame:
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
        matched = ledger.filter(rule.predicate).select(
            pl.col("id"),
            pl.lit(rule.tag).alias("tag"),
            _rule_allocation_expr(rule).alias("allocation"),
            pl.lit(rule.counter_party).alias("counter_party"),
        )
        parts.append(matched)
    if not parts:
        return empty
    out = pl.concat(parts)
    out = out.unique(subset=["id", "tag"])
    for t in out["tag"].to_list():
        validate_kebab_tag(t)
    validate_tag_group_allocations(ledger, out, rules)
    return out
