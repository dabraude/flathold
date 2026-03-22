"""Enumerated tag rule allocation groups (not kebab-case transaction tags)."""

from enum import StrEnum


class TagGroup(StrEnum):
    """Buckets for ``TagRule.groups``: each group sums to at most 100% of |line| per transaction."""

    COUNTER_PARTY = "counter_party"
    UTILITIES = "utilities"
