"""Tag rules: home services, utilities, banking, and transport."""

import polars as pl

from flathold.agata_weekly_manual import AGATA_WEEKLY_MANUAL_TAG_PREDICATE
from flathold.tag_rules.core import TagRule

TAG_RULES_HOME_FINANCE: tuple[TagRule, ...] = (
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
        .str.contains(
            r"(?i)(HYPEROPTIC\s+DD|octopus\s+energy|ROSS\s*&\s*LIDDELL\s+LTD|EDINBURGH\s+COUNCIL|RING\s+BASIC\s+PLAN)"
        ),
        amount_proportion=1,
    ),
    TagRule(
        tag="ring",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)RING\s+BASIC\s+PLAN"),
        amount_proportion=1,
    ),
    TagRule(
        tag="edinburgh-council",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)EDINBURGH\s+COUNCIL"),
        amount_proportion=1,
    ),
    TagRule(
        tag="council-tax",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)EDINBURGH\s+COUNCIL"),
        amount_proportion=1,
    ),
    TagRule(
        tag="tax",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)EDINBURGH\s+COUNCIL"),
        amount_proportion=1,
    ),
    TagRule(
        tag="factors",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)ROSS\s*&\s*LIDDELL\s+LTD"),
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
        tag="loans",
        predicate=(
            pl.col("Transaction Description")
            .str.strip_chars()
            .str.contains(r"(?i)(NATWEST\s+BANK|CREATION\.CO\.UK|TOYOTA\s+FIN\s+SERV)")
            | (
                pl.col("Transaction Description")
                .str.strip_chars()
                .str.contains(r"(?i)ACCOUNT\s+FEE")
                & (pl.col("Transaction Type") == "FEE")
            )
        ),
        amount_proportion=1,
    ),
    TagRule(
        tag="car",
        predicate=(
            pl.col("Transaction Description")
            .str.strip_chars()
            .str.contains(r"(?i)TOYOTA\s+FIN\s+SERV")
            | pl.col("Transaction Description").str.strip_chars().str.contains(r"(?i)FRESH\s+CAR")
        ),
        amount_proportion=1,
    ),
    TagRule(
        tag="cleaning",
        predicate=(
            pl.col("Transaction Description").str.strip_chars().str.contains(r"(?i)FRESH\s+CAR")
            | AGATA_WEEKLY_MANUAL_TAG_PREDICATE
        ),
        amount_proportion=1,
    ),
    TagRule(
        tag="cash-spend",
        predicate=AGATA_WEEKLY_MANUAL_TAG_PREDICATE,
        amount_proportion=1,
    ),
    TagRule(
        tag="fresh-car",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)FRESH\s+CAR"),
        amount_proportion=1,
    ),
    TagRule(
        tag="kitchen",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)CREATION\.CO\.UK"),
        amount_proportion=1,
    ),
)
