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
        tag="entertainment",
        predicate=pl.col("Transaction Description").str.contains(
            r"(?i)(DISNEY\s+PLUS|netflix\.com)"
        ),
        amount_proportion=1,
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
