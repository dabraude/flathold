"""Enumerated tag groups (not kebab-case transaction tags)."""

from __future__ import annotations

from enum import StrEnum


class TagGroup(StrEnum):
    """Metadata groups on tag definitions: dashboard filters and allocation buckets.

    For each group a tag belongs to, ``validate_tag_group_allocations`` requires that
    per-transaction ``sum(|allocation|)`` for tags in that group does not exceed ``|line|``.
    """

    SECTOR_CODES = "sector-codes"
    COUNTER_PARTY = "counter-party"
    CASH_TRANSACTIONS = "cash-transactions"
