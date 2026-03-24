"""Tag rules: streaming, TV licence, and bundled entertainment."""

import polars as pl

from flathold.tag_rules.core import TagRule

TAG_RULES_ENTERTAINMENT: tuple[TagRule, ...] = (
    TagRule(
        tag="disney",
        predicate=pl.col("Transaction Description").str.contains(r"(?i)DISNEY\s+PLUS"),
        amount_proportion=1,
    ),
    TagRule(
        tag="netflix",
        predicate=pl.col("Transaction Description").str.contains(r"(?i)netflix(\.com)?"),
        amount_proportion=1,
    ),
    TagRule(
        tag="youtube-premium",
        predicate=pl.col("Transaction Description").str.contains(r"(?i)Google\s+YouTubePrem"),
        amount_proportion=1,
    ),
    TagRule(
        tag="entertainment",
        predicate=(
            pl.col("Transaction Description").str.contains(
                r"(?i)(DISNEY\s+PLUS|netflix(\.com)?|Google\s+YouTubePrem)"
            )
            | pl.col("Transaction Description")
            .str.strip_chars()
            .str.contains(r"(?i)TV\s+LICENCE\s+DDA")
        ),
        amount_proportion=1,
    ),
    TagRule(
        tag="bbc",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)TV\s+LICENCE\s+DDA"),
        amount_proportion=1,
    ),
    TagRule(
        tag="tv-license",
        predicate=pl.col("Transaction Description")
        .str.strip_chars()
        .str.contains(r"(?i)TV\s+LICENCE\s+DDA"),
        amount_proportion=1,
    ),
)
