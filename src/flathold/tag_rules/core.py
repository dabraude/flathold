"""Tag rule types, validation, and applying rules to a ledger."""

import re
from dataclasses import dataclass

import polars as pl

# Lowercase letters, digits, and single hyphens between segments (kebab-case).
KEBAB_TAG_PATTERN = r"^[a-z0-9]+(-[a-z0-9]+)*$"
_TAG_RE = re.compile(KEBAB_TAG_PATTERN)


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
    """

    tag: str
    predicate: pl.Expr
    amount_absolute: float = 0.0
    amount_proportion: float = 0.0


def _rule_allocation_expr(rule: TagRule) -> pl.Expr:
    line_amount = pl.col("Debit Amount") + pl.col("Credit Amount")
    return pl.lit(rule.amount_absolute) + pl.lit(rule.amount_proportion) * line_amount


def apply_tag_rules(ledger: pl.DataFrame, rules: tuple[TagRule, ...]) -> pl.DataFrame:
    """Return one row per (id, tag) with allocation from each rule's absolute and proportion."""
    empty = pl.DataFrame(
        {
            "id": pl.Series([], dtype=pl.Utf8),
            "tag": pl.Series([], dtype=pl.Utf8),
            "allocation": pl.Series([], dtype=pl.Float64),
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
        )
        parts.append(matched)
    if not parts:
        return empty
    out = pl.concat(parts)
    out = out.unique(subset=["id", "tag"])
    for t in out["tag"].to_list():
        validate_kebab_tag(t)
    return out
