"""Tag definitions table and rule-tag set (orchestration boundary for UI)."""

from __future__ import annotations

import polars as pl

from flathold.core.tag_rule_metadata import TagRuleMetadata
from flathold.data.tables.tag_definitions_table import (
    ensure_tag_definitions_table,
    read_tag_definitions_table,
    read_tag_rule_metadata_map,
    reset_tag_definitions_to_seed,
)
from flathold.tag_rules.rules import TAG_RULES


def read_definitions_dataframe() -> pl.DataFrame:
    return read_tag_definitions_table()


def get_tag_rule_metadata_map() -> dict[str, TagRuleMetadata]:
    return read_tag_rule_metadata_map()


def merge_seed_tags() -> None:
    ensure_tag_definitions_table()


def reset_to_seed() -> None:
    reset_tag_definitions_to_seed()


def rule_tag_names() -> set[str]:
    return {r.tag for r in TAG_RULES}
