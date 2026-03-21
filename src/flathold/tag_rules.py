"""Hard-coded rules mapping ledger rows to transaction tags."""

import re

import polars as pl

# Lowercase letters, digits, and single hyphens between segments (kebab-case).
KEBAB_TAG_PATTERN = r"^[a-z0-9]+(-[a-z0-9]+)*$"
_TAG_RE = re.compile(KEBAB_TAG_PATTERN)


def validate_kebab_tag(tag: str) -> None:
    """Raise ValueError if `tag` is not kebab-case alphanumeric."""
    if not _TAG_RE.fullmatch(tag):
        msg = f"Invalid tag {tag!r}: expected kebab-case alphanumeric (pattern {KEBAB_TAG_PATTERN})"
        raise ValueError(msg)


# (tag, predicate). Rows matching the predicate get that tag; multiple rules can apply.
# Tags must satisfy validate_kebab_tag (checked at import).
TAG_RULES: tuple[tuple[str, pl.Expr], ...] = (
    (
        "dog-food",
        pl.col("Transaction Description").str.strip_chars().str.contains(r"(?i)^SP YEARS\.COM$"),
    ),
    (
        "printer",
        pl.col("Transaction Description").str.contains("(?i)HPI\\s+INSTANT\\s+INK\\s+UK"),
    ),
)

_rule_tags = [t for t, _ in TAG_RULES]
if len(_rule_tags) != len(set(_rule_tags)):
    dup = [t for t in set(_rule_tags) if _rule_tags.count(t) > 1]
    msg = f"TAG_RULES must use unique tag names; duplicates: {sorted(dup)!r}"
    raise ValueError(msg)
for _tag, _ in TAG_RULES:
    validate_kebab_tag(_tag)


def apply_tag_rules(ledger: pl.DataFrame) -> pl.DataFrame:
    """Return a long-form id/tag table (unique id, tag pairs)."""
    empty = pl.DataFrame(
        {
            "id": pl.Series([], dtype=pl.Utf8),
            "tag": pl.Series([], dtype=pl.Utf8),
        }
    )
    if not TAG_RULES:
        return empty
    parts: list[pl.DataFrame] = []
    for tag, predicate in TAG_RULES:
        matched = ledger.filter(predicate).select(
            pl.col("id"),
            pl.lit(tag).alias("tag"),
        )
        parts.append(matched)
    if not parts:
        return empty
    out = pl.concat(parts)
    out = out.unique(subset=["id", "tag"])
    for t in out["tag"].to_list():
        validate_kebab_tag(t)
    return out
