"""Tag definitions table and rule-tag set (orchestration boundary for UI)."""

from __future__ import annotations

import importlib

import polars as pl

from flathold.core.tag_rule_metadata import TagRuleMetadata
from flathold.data import tag_definitions_seed
from flathold.data.tables import tag_definitions_table
from flathold.tag_rules.rules import TAG_RULES


def read_definitions_dataframe() -> pl.DataFrame:
    return tag_definitions_table.read_tag_definitions_table()


def get_tag_rule_metadata_map() -> dict[str, TagRuleMetadata]:
    return tag_definitions_table.read_tag_rule_metadata_map()


def merge_seed_tags() -> None:
    # Pick up code changes to TAG_DEFINITIONS_SEED_ROWS without requiring app restart.
    importlib.reload(tag_definitions_seed)
    importlib.reload(tag_definitions_table)
    tag_definitions_table.ensure_tag_definitions_table()


def reset_to_seed() -> None:
    tag_definitions_table.reset_tag_definitions_to_seed()


def rule_tag_names() -> set[str]:
    return {r.tag for r in TAG_RULES}
