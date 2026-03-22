"""Tag rules: counterparty-only (person or institution)."""

import polars as pl

from flathold.tag_rules.core import TagRule

TAG_RULES_COUNTER_PARTY: tuple[TagRule, ...] = (
    TagRule(
        tag="dave",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)D\s+BRAUDE"),
        amount_proportion=1,
        counter_party=True,
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
        tag="chocolaterium",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)SP\s+CHOCOLATE\s+SHOP"),
        amount_proportion=1,
        counter_party=True,
    ),
    TagRule(
        tag="aviva",
        predicate=pl.col("Transaction Description").str.strip_chars().str.contains(r"(?i)AVIVA"),
        amount_proportion=1,
        counter_party=True,
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
    TagRule(
        tag="hp",
        predicate=pl.col("Transaction Description").str.contains("(?i)HPI\\s+INSTANT\\s+INK\\s+UK"),
        amount_proportion=1,
        counter_party=True,
    ),
    TagRule(
        tag="ross-and-liddell",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)ROSS\s*&\s*LIDDELL\s+LTD"),
        amount_proportion=1,
        counter_party=True,
    ),
    TagRule(
        tag="natwest",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)NATWEST\s+BANK"),
        amount_proportion=1,
        counter_party=True,
    ),
    TagRule(
        tag="bos",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)ACCOUNT\s+FEE")
        & (pl.col("Transaction Type") == "FEE"),
        amount_proportion=1,
        counter_party=True,
    ),
    TagRule(
        tag="toyota-finance",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)TOYOTA\s+FIN\s+SERV"),
        amount_proportion=1,
        counter_party=True,
    ),
    TagRule(
        tag="wren",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)CREATION\.CO\.UK"),
        amount_proportion=1,
        counter_party=True,
    ),
    TagRule(
        tag="cash",
        predicate=pl.col("Transaction Type") == "CPT",
        amount_proportion=1,
        counter_party=True,
    ),
)
