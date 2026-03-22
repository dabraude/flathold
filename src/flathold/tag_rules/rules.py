"""Concrete `TAG_RULES` list."""

import polars as pl

from flathold.tag_rules.core import TagRule, validate_kebab_tag

TAG_RULES: tuple[TagRule, ...] = (
    TagRule(
        tag="dog-food",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)^SP YEARS\.COM$"),
        amount_proportion=1,
    ),
    TagRule(
        tag="pets",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)^SP YEARS\.COM$"),
        amount_proportion=1,
    ),
    TagRule(
        tag="printer",
        predicate=pl.col("Transaction Description").str.contains("(?i)HPI\\s+INSTANT\\s+INK\\s+UK"),
        amount_proportion=1,
    ),
    TagRule(
        tag="gas-and-electricity",
        predicate=pl.col("Transaction Description").str.contains("(?i)octopus\\s+energy"),
        amount_proportion=1,
    ),
    TagRule(
        tag="hyperoptic",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)HYPEROPTIC\s+DD"),
        amount_proportion=1,
    ),
    TagRule(
        tag="utilities",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)(HYPEROPTIC\s+DD|octopus\s+energy)"),
        amount_proportion=1,
        show_on_dashboard_by_default=True,
    ),
    TagRule(
        tag="natwest",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)NATWEST\s+BANK"),
        amount_proportion=1,
    ),
    TagRule(
        tag="mortgage",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)NATWEST\s+BANK"),
        amount_proportion=1,
    ),
    TagRule(
        tag="bank",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)(NATWEST\s+BANK|CREATION\.CO\.UK|TOYOTA\s+FIN\s+SERV)"),
        amount_proportion=1,
        show_on_dashboard_by_default=True,
    ),
    TagRule(
        tag="car",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)TOYOTA\s+FIN\s+SERV"),
        amount_proportion=1,
    ),
    TagRule(
        tag="kitchen",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)CREATION\.CO\.UK"),
        amount_proportion=1,
    ),
    TagRule(
        tag="disney",
        predicate=pl.col("Transaction Description").str.contains(r"(?i)DISNEY\s+PLUS"),
        amount_proportion=1,
    ),
    TagRule(
        tag="netflix",
        predicate=pl.col("Transaction Description").str.contains(r"(?i)netflix\.com"),
        amount_proportion=1,
    ),
    TagRule(
        tag="youtube-premium",
        predicate=pl.col("Transaction Description").str.contains(r"(?i)Google\s+YouTubePrem"),
        amount_proportion=1,
    ),
    TagRule(
        tag="entertainment",
        predicate=pl.col("Transaction Description").str.contains(
            r"(?i)(DISNEY\s+PLUS|netflix\.com|Google\s+YouTubePrem)"
        ),
        amount_proportion=1,
        show_on_dashboard_by_default=True,
    ),
    TagRule(
        tag="dish-washer-tablets",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)SMOL\s+LIMITED"),
        amount_proportion=1,
    ),
    TagRule(
        tag="groceries",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)SMOL\s+LIMITED"),
        amount_proportion=1,
    ),
)

_rule_tags = [r.tag for r in TAG_RULES]
if len(_rule_tags) != len(set(_rule_tags)):
    dup = [t for t in set(_rule_tags) if _rule_tags.count(t) > 1]
    msg = f"TAG_RULES must use unique tag names; duplicates: {sorted(dup)!r}"
    raise ValueError(msg)
for r in TAG_RULES:
    validate_kebab_tag(r.tag)


def tag_show_on_dashboard_default(tag: str) -> bool:
    """Return whether ``tag`` is selected by default on the dashboard (from ``TAG_RULES``)."""
    for rule in TAG_RULES:
        if rule.tag == tag:
            return rule.show_on_dashboard_by_default
    return False
