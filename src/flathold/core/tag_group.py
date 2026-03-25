"""Enumerated tag allocation groups (not kebab-case transaction tags)."""

from enum import StrEnum


class TagGroup(StrEnum):
    """Buckets for tag ``groups``: each group sums to at most 100% of |line| per transaction."""

    SECTOR_CODES = "sector-codes"
    COUNTER_PARTY = "counter-party"
