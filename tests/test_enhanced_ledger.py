"""Tests for ``build_enhanced_ledger`` (wide per-row enhanced ledger frame)."""

from __future__ import annotations

import polars as pl
import pytest

from flathold.analytics.enhanced_ledger import build_enhanced_ledger
from flathold.core.ledger_columns import CALCULATED_TAGS_COLUMN
from flathold.core.tag_group import TagGroup
from flathold.core.tag_rule_metadata import TagRuleMetadata


def _ledger_row(
    *,
    id_: str,
    debit: float = 0.0,
    credit: float = 0.0,
) -> dict[str, object]:
    return {
        "Transaction Counter": 1,
        "Transaction Date": "01/01/2024",
        "Transaction Type": "DEBIT",
        "Sort Code": "",
        "Account Number": "",
        "Transaction Description": "x",
        "Debit Amount": debit,
        "Credit Amount": credit,
        "id": id_,
        "year": 2024,
        "month": 1,
        "day": 1,
        "ledger_source": "bank",
        "tags": [],
    }


@pytest.fixture
def sector_meta() -> dict[str, TagRuleMetadata]:
    return {
        "groceries": TagRuleMetadata(
            counter_party=False,
            calculated=False,
            groups=(TagGroup.SECTOR_CODES,),
        ),
    }


def test_enhanced_adds_empty_calculated_when_fully_tagged(
    sector_meta: dict[str, TagRuleMetadata],
) -> None:
    ledger = pl.DataFrame(
        [
            {
                **_ledger_row(id_="a", debit=50.0),
                "tags": ["groceries"],
            }
        ]
    )
    tags = pl.DataFrame(
        {
            "id": ["a"],
            "tag": ["groceries"],
            "allocation": [50.0],
            "counter_party": [False],
        }
    )
    out = build_enhanced_ledger(ledger, tags, sector_meta)
    assert out[CALCULATED_TAGS_COLUMN].to_list() == [[]]


def test_enhanced_prefers_untagged_over_uncategorised_hint(
    sector_meta: dict[str, TagRuleMetadata],
) -> None:
    ledger = pl.DataFrame([_ledger_row(id_="b", debit=50.0)])
    tags = pl.DataFrame(
        {
            "id": ["b"],
            "tag": ["groceries"],
            "allocation": [10.0],
            "counter_party": [False],
        }
    )
    out = build_enhanced_ledger(ledger, tags, sector_meta)
    assert out[CALCULATED_TAGS_COLUMN].to_list() == [["untagged-spend"]]


def test_enhanced_without_transaction_tags(sector_meta: dict[str, TagRuleMetadata]) -> None:
    ledger = pl.DataFrame([_ledger_row(id_="c", debit=30.0)])
    out = build_enhanced_ledger(ledger, None, sector_meta)
    assert out[CALCULATED_TAGS_COLUMN].to_list() == [["untagged-spend"]]


def test_enhanced_uncategorised_when_fully_allocated_but_non_sector(
    sector_meta: dict[str, TagRuleMetadata],
) -> None:
    ledger = pl.DataFrame([_ledger_row(id_="f", debit=50.0)])
    tags = pl.DataFrame(
        {
            "id": ["f"],
            "tag": ["other"],
            "allocation": [50.0],
            "counter_party": [False],
        }
    )
    out = build_enhanced_ledger(ledger, tags, sector_meta)
    assert out[CALCULATED_TAGS_COLUMN].to_list() == [["uncategorised-sector"]]


def test_enhanced_us_tag_excludes_uncategorised_sector(
    sector_meta: dict[str, TagRuleMetadata],
) -> None:
    ledger = pl.DataFrame([_ledger_row(id_="d", debit=30.0)])
    tags = pl.DataFrame(
        {
            "id": ["d"],
            "tag": ["us"],
            "allocation": [0.0],
            "counter_party": [False],
        }
    )
    out = build_enhanced_ledger(ledger, tags, sector_meta)
    assert out[CALCULATED_TAGS_COLUMN].to_list() == [["untagged-spend"]]


def test_enhanced_cash_withdrawal_excludes_uncategorised_sector(
    sector_meta: dict[str, TagRuleMetadata],
) -> None:
    ledger = pl.DataFrame([_ledger_row(id_="e", debit=30.0)])
    tags = pl.DataFrame(
        {
            "id": ["e"],
            "tag": ["cash-withdrawal"],
            "allocation": [0.0],
            "counter_party": [False],
        }
    )
    out = build_enhanced_ledger(ledger, tags, sector_meta)
    assert out[CALCULATED_TAGS_COLUMN].to_list() == [["untagged-spend"]]


def test_enhanced_empty_ledger() -> None:
    empty = pl.DataFrame(
        schema={
            "Transaction Counter": pl.Int64,
            "Transaction Date": pl.Utf8,
            "Transaction Type": pl.Utf8,
            "Sort Code": pl.Utf8,
            "Account Number": pl.Utf8,
            "Transaction Description": pl.Utf8,
            "Debit Amount": pl.Float64,
            "Credit Amount": pl.Float64,
            "id": pl.Utf8,
            "year": pl.Int64,
            "month": pl.Int64,
            "day": pl.Int64,
            "ledger_source": pl.Utf8,
            "tags": pl.List(pl.Utf8),
        }
    )
    out = build_enhanced_ledger(empty, None, {})
    assert out.is_empty()
