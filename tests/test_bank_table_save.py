"""Tests for bank statement ingest: first write, partial overlap, full dedupe."""

from __future__ import annotations

from pathlib import Path

import pytest

from flathold.data.tables import bank_table
from flathold.data.tables.bank_table import (
    compute_bank_transaction_ids,
    load_csv_bytes_to_dataframe,
    load_csv_to_dataframe,
    read_existing_table,
    save_to_delta,
)

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SAMPLE_BANK_CSV = _REPO_ROOT / "data" / "bank_statement.csv"

# Representative of ``data/bank_statement.csv`` (sort/account/types; see that file for full shape).
_SAMPLE_SORT = "88-33-01"
_SAMPLE_ACCOUNT = "84729103"

_CSV_COLS = [
    "Transaction Date",
    "Transaction Type",
    "Sort Code",
    "Account Number",
    "Transaction Description",
    "Debit Amount",
    "Credit Amount",
    "Balance",
]


def _csv_bytes(rows: list[dict[str, str]]) -> bytes:
    lines = [",".join(_CSV_COLS)]
    for r in rows:
        lines.append(",".join(str(r[c]) for c in _CSV_COLS))
    return ("\n".join(lines) + "\n").encode("utf-8")


def _row(date_dd_mm_yyyy: str, description: str, **overrides: str) -> dict[str, str]:
    """Synthetic row shaped like ``data/bank_statement.csv`` (defaults match that sample)."""
    row: dict[str, str] = {
        "Transaction Date": date_dd_mm_yyyy,
        "Transaction Type": "DD",
        "Sort Code": _SAMPLE_SORT,
        "Account Number": _SAMPLE_ACCOUNT,
        "Transaction Description": description,
        "Debit Amount": "10.00",
        "Credit Amount": "0.00",
        "Balance": "1000.00",
    }
    row.update(overrides)
    return row


@pytest.fixture(autouse=True)
def _isolated_bank_table(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(bank_table, "BANK_TABLE", tmp_path / "bank")


def test_first_save_adds_all_rows() -> None:
    raw = _csv_bytes(
        [
            _row("01/01/2025", "Coffee shop"),
            _row("02/01/2025", "Groceries"),
            _row("03/01/2025", "Rent"),
        ]
    )
    df = load_csv_bytes_to_dataframe(raw)
    result = save_to_delta(df)

    assert result.total == 3
    assert result.new_rows == 3
    assert result.duplicated == 0

    stored = read_existing_table()
    assert stored is not None
    assert len(stored) == 3
    assert stored["id"].n_unique() == 3


@pytest.mark.skipif(
    not _SAMPLE_BANK_CSV.is_file(),
    reason="Optional local file data/bank_statement.csv (omit from version control)",
)
def test_data_bank_statement_csv_round_trip() -> None:
    """Split the real CSV into overlapping chunks (same ``Transaction Counter`` as full file).

    Upload order: first chunk, then overlapping second chunk (partial dedupe), then full file
    again (all duplicates). Slices are taken *after* full-file normalize so counters match ids.
    """
    df = load_csv_to_dataframe(_SAMPLE_BANK_CSV)
    n = len(df)
    assert n > 0

    third = n // 3
    part1_len = n - third
    start_b = third
    part1 = df.head(part1_len)
    part2 = df.slice(start_b, n - start_b)
    overlap_ids = set(compute_bank_transaction_ids(part1)["id"].to_list()) & set(
        compute_bank_transaction_ids(part2)["id"].to_list()
    )
    overlap = len(overlap_ids)
    assert overlap == len(part1) + len(part2) - n

    r1 = save_to_delta(part1)
    assert r1.total == part1_len
    assert r1.new_rows == part1_len
    assert r1.duplicated == 0

    r2 = save_to_delta(part2)
    assert r2.total == n
    assert r2.new_rows == n - part1_len
    assert r2.duplicated == len(part2) - r2.new_rows
    assert r2.duplicated == overlap

    r3 = save_to_delta(df)
    assert r3.total == n
    assert r3.new_rows == 0
    assert r3.duplicated == n

    stored = read_existing_table()
    assert stored is not None
    assert len(stored) == n
    assert stored["id"].n_unique() == n


def test_second_upload_full_overlap_all_duplicated() -> None:
    raw = _csv_bytes(
        [
            _row("10/01/2025", "A"),
            _row("11/01/2025", "B"),
        ]
    )
    df = load_csv_bytes_to_dataframe(raw)
    first = save_to_delta(df)
    assert first.new_rows == 2
    assert first.duplicated == 0

    df_again = load_csv_bytes_to_dataframe(raw)
    second = save_to_delta(df_again)

    assert second.total == 2
    assert second.new_rows == 0
    assert second.duplicated == 2

    stored = read_existing_table()
    assert stored is not None
    assert len(stored) == 2


def test_second_upload_partial_overlap() -> None:
    first_raw = _csv_bytes(
        [
            _row("05/02/2025", "Keep me"),
            _row("06/02/2025", "Also keep"),
        ]
    )
    df1 = load_csv_bytes_to_dataframe(first_raw)
    r1 = save_to_delta(df1)
    assert r1.total == 2
    assert r1.new_rows == 2

    second_raw = _csv_bytes(
        [
            _row("05/02/2025", "Keep me"),
            _row("07/02/2025", "New row"),
        ]
    )
    df2 = load_csv_bytes_to_dataframe(second_raw)
    r2 = save_to_delta(df2)

    assert r2.total == 3
    assert r2.new_rows == 1
    assert r2.duplicated == 1

    stored = read_existing_table()
    assert stored is not None
    assert len(stored) == 3
    assert stored["id"].n_unique() == 3
