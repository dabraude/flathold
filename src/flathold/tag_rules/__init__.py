"""Tag rules: validation, `TagRule`, and applying rules to ledger rows."""

import polars as pl

from flathold.tag_rules.core import (
    KEBAB_TAG_PATTERN,
    TagRule,
    validate_at_most_one_counter_party_tag_per_transaction,
    validate_kebab_tag,
    validate_tag_group_allocations,
)
from flathold.tag_rules.core import (
    apply_tag_rules as apply_tag_rules_impl,
)
from flathold.tag_rules.rules import (
    TAG_RULES,
    tag_counter_party,
    tag_groups,
    tag_show_on_dashboard_default,
)


def apply_tag_rules(ledger: pl.DataFrame) -> pl.DataFrame:
    """Apply `TAG_RULES` from `flathold.tag_rules.rules`."""
    return apply_tag_rules_impl(ledger, TAG_RULES)


__all__ = [
    "KEBAB_TAG_PATTERN",
    "TAG_RULES",
    "TagRule",
    "apply_tag_rules",
    "tag_counter_party",
    "tag_groups",
    "tag_show_on_dashboard_default",
    "validate_at_most_one_counter_party_tag_per_transaction",
    "validate_kebab_tag",
    "validate_tag_group_allocations",
]
