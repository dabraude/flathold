"""Tag rules: people, pets, and insurance."""

import polars as pl

from flathold.tag_rules.constants import DAVID_LIFE_INSURANCE_AVIVA_AMOUNT
from flathold.tag_rules.core import TagRule

TAG_RULES_PERSONAL: tuple[TagRule, ...] = (
    TagRule(
        tag="dog-food",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)^SP YEARS\.COM$"),
        amount_proportion=1,
    ),
    TagRule(
        tag="dave",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)D\s+BRAUDE"),
        amount_proportion=1,
        counter_party=True,
    ),
    TagRule(
        tag="us",
        predicate=(
            pl.col("Transaction Description").str.strip_chars().str.contains(r"(?i)D\s+BRAUDE")
            | pl.col("Transaction Description").str.strip_chars().str.contains(r"(?i)CLAIRE\s+GIRY")
        ),
        amount_proportion=1,
    ),
    TagRule(
        tag="claire",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)CLAIRE\s+GIRY"),
        amount_proportion=1,
        counter_party=True,
    ),
    TagRule(
        tag="pets",
        predicate=(
            pl.col("Transaction Description").str.strip_chars().str.contains(r"(?i)^SP YEARS\.COM$")
            | pl.col("Transaction Description")
            .str.strip_chars()
            .str.contains(r"(?i)PET\s+PLAN\s+LTD")
            | pl.col("Transaction Description")
            .str.strip_chars()
            .str.contains(r"(?i)MONIKA\s+WOJTASZCZYK")
            | pl.col("Transaction Description")
            .str.strip_chars()
            .str.contains(r"(?i)SUMUP.*DOG\s+DAZE\s+D")
        ),
        amount_proportion=1,
    ),
    TagRule(
        tag="groomer",
        predicate=(
            pl.col("Transaction Description")
            .str.strip_chars()
            .str.contains(r"(?i)MONIKA\s+WOJTASZCZYK")
            | pl.col("Transaction Description")
            .str.strip_chars()
            .str.contains(r"(?i)SUMUP.*DOG\s+DAZE\s+D")
        ),
        amount_proportion=1,
    ),
    TagRule(
        tag="insurance",
        predicate=(
            pl.col("Transaction Description")
            .str.strip_chars()
            .str.contains(r"(?i)PET\s+PLAN\s+LTD")
            | pl.col("Transaction Description")
            .str.strip_chars()
            .str.contains(r"(?i)VITALITY\s+HEALTH")
            | pl.col("Transaction Description")
            .str.strip_chars()
            .str.contains(r"(?i)CLOSE-BORLAND\s+CLIE")
            | pl.col("Transaction Description").str.strip_chars().str.contains(r"(?i)AVIVA")
        ),
        amount_proportion=1,
    ),
    TagRule(
        tag="aviva",
        predicate=pl.col("Transaction Description").str.strip_chars().str.contains(r"(?i)AVIVA"),
        amount_proportion=1,
        counter_party=True,
    ),
    TagRule(
        tag="david-life-insurance",
        predicate=(
            pl.col("Transaction Description").str.strip_chars().str.contains(r"(?i)AVIVA")
            & (
                (pl.col("Debit Amount") + pl.col("Credit Amount")).abs().round(2)
                == pl.lit(DAVID_LIFE_INSURANCE_AVIVA_AMOUNT)
            )
        ),
        amount_proportion=1,
    ),
    TagRule(
        tag="health-insurance",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)VITALITY\s+HEALTH"),
        amount_proportion=1,
    ),
    TagRule(
        tag="vitality",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)VITALITY\s+HEALTH"),
        amount_proportion=1,
    ),
    TagRule(
        tag="house-insurance",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)CLOSE-BORLAND\s+CLIE"),
        amount_proportion=1,
    ),
    TagRule(
        tag="borlands",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)CLOSE-BORLAND\s+CLIE"),
        amount_proportion=1,
        counter_party=True,
    ),
    TagRule(
        tag="pet-plan",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)PET\s+PLAN\s+LTD"),
        amount_proportion=1,
        counter_party=True,
    ),
)
