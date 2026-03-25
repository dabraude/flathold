"""Tag definitions: persisted columns and migration for legacy ``show_on_dashboard_by_default``."""

import polars as pl

from flathold.data.tables.tag_definitions_table import (
    _TAG_DEFINITIONS_COLUMNS,
    _normalize_tag_definitions_columns,
)


def test_persisted_columns_exclude_legacy_show_flag() -> None:
    assert "show_on_dashboard_by_default" not in _TAG_DEFINITIONS_COLUMNS
    assert _TAG_DEFINITIONS_COLUMNS == (
        "tag",
        "counter_party",
        "calculated",
        "groups",
    )


def test_normalize_accepts_four_column_delta_without_show_column() -> None:
    """Matches on-disk ``tag_definitions`` that never had the legacy boolean column."""
    raw = pl.DataFrame(
        {
            "tag": ["utilities"],
            "counter_party": [False],
            "calculated": [False],
            "groups": ["sector-codes"],
        }
    )
    out = _normalize_tag_definitions_columns(raw)
    assert out.columns == list(_TAG_DEFINITIONS_COLUMNS)
    assert "sector-codes" in out["groups"][0]


def test_normalize_strips_legacy_dashboard_default_from_groups() -> None:
    raw = pl.DataFrame(
        {
            "tag": ["groceries"],
            "counter_party": [False],
            "calculated": [False],
            "groups": ["dashboard-default|sector-codes"],
        }
    )
    out = _normalize_tag_definitions_columns(raw)
    g = out["groups"][0]
    assert "dashboard-default" not in g
    assert "sector-codes" in g
