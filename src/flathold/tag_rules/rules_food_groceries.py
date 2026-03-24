"""Tag rules: dining out and groceries."""

import polars as pl

from flathold.tag_rules.constants import TESCO_STORES_OR_GROCERY
from flathold.tag_rules.core import TagRule

TAG_RULES_FOOD_GROCERIES: tuple[TagRule, ...] = (
    TagRule(
        tag="eating-out",
        predicate=(
            pl.col("Transaction Description")
            .str.strip_chars()
            .str.contains(r"(?i)VICTOR\s+HUGO\s+SHORE")
            | pl.col("Transaction Description")
            .str.strip_chars()
            .str.contains(r"(?i)SQ\s*\*WILLIAMS\s*&\s*JOH")
        ),
        amount_proportion=1,
    ),
    TagRule(
        tag="williams-and-johnson",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)SQ\s*\*WILLIAMS\s*&\s*JOH"),
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
        predicate=(
            pl.col("Transaction Description").str.strip_chars().str.contains(r"(?i)SMOL\s+LIMITED")
            | pl.col("Transaction Description")
            .str.strip_chars()
            .str.contains(TESCO_STORES_OR_GROCERY)
            | pl.col("Transaction Description")
            .str.strip_chars()
            .str.contains(r"(?i)SP\s+FIELD\s+DOCTOR")
            | pl.col("Transaction Description")
            .str.strip_chars()
            .str.contains(r"(?i)Zettle.*Phantassie")
            | pl.col("Transaction Description")
            .str.strip_chars()
            .str.contains(r"(?i)Zettle.*At\s+the\s+Mar")
            | pl.col("Transaction Description")
            .str.strip_chars()
            .str.contains(r"(?i)SUMUP.*ABBIS\s+PANT")
            | pl.col("Transaction Description")
            .str.strip_chars()
            .str.contains(r"(?i)WAITROSE\s+750")
            | pl.col("Transaction Description")
            .str.strip_chars()
            .str.contains(r"(?i)THE\s+GREAT\s+GROG")
            | pl.col("Transaction Description")
            .str.strip_chars()
            .str.contains(r"(?i)SP\s+CHOCOLATE\s+SHOP")
        ),
        amount_proportion=1,
    ),
    TagRule(
        tag="farmers-market",
        predicate=(
            pl.col("Transaction Description")
            .str.strip_chars()
            .str.contains(r"(?i)Zettle.*Phantassie")
            | pl.col("Transaction Description")
            .str.strip_chars()
            .str.contains(r"(?i)Zettle.*At\s+the\s+Mar")
        ),
        amount_proportion=1,
    ),
    TagRule(
        tag="abbis-pantry",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)SUMUP.*ABBIS\s+PANT"),
        amount_proportion=1,
    ),
    TagRule(
        tag="waitrose",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)WAITROSE\s+750"),
        amount_proportion=1,
    ),
    TagRule(
        tag="great-grog",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)THE\s+GREAT\s+GROG"),
        amount_proportion=1,
    ),
    TagRule(
        tag="field-doctor",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)SP\s+FIELD\s+DOCTOR"),
        amount_proportion=1,
    ),
    TagRule(
        tag="tescos",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(TESCO_STORES_OR_GROCERY),
        amount_proportion=1,
    ),
)
