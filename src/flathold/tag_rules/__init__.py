"""Tag rules: validation, `TagRule`, and applying rules to ledger rows."""

import polars as pl

from flathold.core.tag_pattern import KEBAB_TAG_PATTERN
from flathold.core.tag_rule_metadata import TagRuleMetadata
from flathold.data.tables.tag_definitions_table import (
    metadata_map_covers_rules,
    read_tag_rule_metadata_map,
    tag_counter_party,
    tag_groups,
)
from flathold.tag_rules.core import (
    TagRule,
    validate_at_most_one_counter_party_tag_per_transaction,
    validate_kebab_tag,
    validate_tag_group_allocations,
    validate_transaction_tags_no_calculated_tags,
)
from flathold.tag_rules.core import (
    apply_tag_rules as apply_tag_rules_impl,
)
from flathold.tag_rules.rules import TAG_RULES
from flathold.tag_rules.tag_group import TagGroup


def apply_tag_rules(ledger: pl.DataFrame) -> pl.DataFrame:
    """Apply `TAG_RULES` using metadata from ``db/tag_definitions``."""
    meta = read_tag_rule_metadata_map()
    metadata_map_covers_rules(TAG_RULES, meta)
    return apply_tag_rules_impl(ledger, TAG_RULES, meta)


__all__ = [
    "KEBAB_TAG_PATTERN",
    "TAG_RULES",
    "TagGroup",
    "TagRule",
    "TagRuleMetadata",
    "apply_tag_rules",
    "tag_counter_party",
    "tag_groups",
    "validate_at_most_one_counter_party_tag_per_transaction",
    "validate_kebab_tag",
    "validate_tag_group_allocations",
    "validate_transaction_tags_no_calculated_tags",
]
