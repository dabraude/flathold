"""Tag rules: counterparty-only (person or institution)."""

import polars as pl

from flathold.tag_rules.core import TagRule

_AMAZON_DESCRIPTION_PREDICATE = (
    pl.col("Transaction Description")
    .str.strip_chars()
    .str.contains(r"(?i)(www\.amazon|amazon\.co\.uk|\bamazon\b)")
)

TAG_RULES_COUNTER_PARTY: tuple[TagRule, ...] = (
    TagRule(
        tag="dave",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)D\s+BRAUDE"),
        amount_proportion=1,
    ),
    TagRule(
        tag="google",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)Google\s+YouTubePrem"),
        amount_proportion=1,
    ),
    TagRule(
        tag="claire",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)CLAIRE\s+GIRY"),
        amount_proportion=1,
    ),
    TagRule(
        tag="chocolaterium",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)SP\s+CHOCOLATE\s+SHOP"),
        amount_proportion=1,
    ),
    TagRule(
        tag="aviva",
        predicate=pl.col("Transaction Description").str.strip_chars().str.contains(r"(?i)AVIVA"),
        amount_proportion=1,
    ),
    TagRule(
        tag="amazon",
        predicate=_AMAZON_DESCRIPTION_PREDICATE,
        amount_proportion=1,
    ),
    TagRule(
        tag="amazon-spend",
        predicate=_AMAZON_DESCRIPTION_PREDICATE,
        amount_proportion=1,
    ),
    TagRule(
        tag="borlands",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)CLOSE-BORLAND\s+CLIE"),
        amount_proportion=1,
    ),
    TagRule(
        tag="pet-plan",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)PET\s+PLAN\s+LTD"),
        amount_proportion=1,
    ),
    TagRule(
        tag="hp",
        predicate=pl.col("Transaction Description").str.contains("(?i)HPI\\s+INSTANT\\s+INK\\s+UK"),
        amount_proportion=1,
    ),
    TagRule(
        tag="ross-and-liddell",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)ROSS\s*&\s*LIDDELL\s+LTD"),
        amount_proportion=1,
    ),
    TagRule(
        tag="smol",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)SMOL\s+LIMITED"),
        amount_proportion=1,
    ),
    TagRule(
        tag="natwest",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)NATWEST\s+BANK"),
        amount_proportion=1,
    ),
    TagRule(
        tag="bos",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)ACCOUNT\s+FEE")
        & (pl.col("Transaction Type") == "FEE"),
        amount_proportion=1,
    ),
    TagRule(
        tag="toyota-finance",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)TOYOTA\s+FIN\s+SERV"),
        amount_proportion=1,
    ),
    TagRule(
        tag="wren",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)CREATION\.CO\.UK"),
        amount_proportion=1,
    ),
    TagRule(
        tag="years",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)SP\s+YEARS\.COM"),
        amount_proportion=1,
    ),
    TagRule(
        tag="octopus",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)OCTOPUS\s+ENERGY"),
        amount_proportion=1,
    ),
    TagRule(
        tag="cash",
        predicate=pl.col("Transaction Type") == "CPT",
        amount_proportion=1,
    ),
    TagRule(
        tag="cash-withdrawal",
        predicate=pl.col("Transaction Type") == "CPT",
        amount_proportion=1,
    ),
)
