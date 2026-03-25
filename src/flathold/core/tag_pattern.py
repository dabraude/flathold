"""Shared kebab-case tag pattern for validation (schemas and tag rules)."""

import re

# Lowercase letters, digits, and single hyphens between segments (kebab-case).
KEBAB_TAG_PATTERN = r"^[a-z0-9]+(-[a-z0-9]+)*$"
_TAG_RE = re.compile(KEBAB_TAG_PATTERN)


def validate_kebab_tag(tag: str) -> None:
    """Raise ValueError if `tag` is not kebab-case alphanumeric."""
    if not _TAG_RE.fullmatch(tag):
        msg = f"Invalid tag {tag!r}: expected kebab-case alphanumeric (pattern {KEBAB_TAG_PATTERN})"
        raise ValueError(msg)
