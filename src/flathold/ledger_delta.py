"""Build and maintain a ledger Delta table from the bank table, with stable IDs per transaction."""

import hashlib
import shutil
from dataclasses import dataclass

import polars as pl
from deltalake import write_deltalake

from flathold.bank_delta import read_existing_table
from flathold.constants import LEDGER_TABLE, TRANSACTION_TAGS_TABLE
from flathold.schemas import TransactionTagsSchema
from flathold.tag_rules import apply_tag_rules


@dataclass(frozen=True, slots=True)
class UpdateLedgerResult:
    success: bool
    message: str


@dataclass(frozen=True, slots=True)
class UpdateTagsResult:
    success: bool
    message: str


def _read_ledger_delta_only() -> pl.DataFrame | None:
    """Read the ledger Delta table if it exists. Returns None if missing."""
    if not LEDGER_TABLE.exists():
        return None
    try:
        return pl.read_delta(str(LEDGER_TABLE))
    except Exception:
        return None


def _normalize_transaction_tags_df(df: pl.DataFrame) -> pl.DataFrame:
    """Fill in columns expected by `TransactionTagsSchema` (legacy tables may omit allocation)."""
    if "allocation" not in df.columns:
        return df.with_columns(pl.lit(0.0).alias("allocation"))
    return df


def _read_transaction_tags_raw() -> pl.DataFrame | None:
    """Read the transaction tags Delta table if it exists. Returns None if missing."""
    if not TRANSACTION_TAGS_TABLE.exists():
        return None
    try:
        return _normalize_transaction_tags_df(pl.read_delta(str(TRANSACTION_TAGS_TABLE)))
    except Exception:
        return None


def _ledger_with_tags_left_join(ledger: pl.DataFrame) -> pl.DataFrame:
    """Attach `tags` (list[str]) via left join; allocation stays on transaction_tags only."""
    if "tags" in ledger.columns:
        ledger = ledger.drop("tags")
    tags_df = _read_transaction_tags_raw()
    if tags_df is None or len(tags_df) == 0:
        return ledger.with_columns(
            pl.Series("tags", [[] for _ in range(ledger.height)]).cast(pl.List(pl.Utf8))
        )
    tags_agg = tags_df.group_by("id").agg(pl.col("tag").implode().alias("tags"))
    joined = ledger.join(tags_agg, on="id", how="left")
    return joined.with_columns(
        pl.when(pl.col("tags").is_null())
        .then(pl.lit([]).cast(pl.List(pl.Utf8)))
        .otherwise(pl.col("tags"))
        .alias("tags")
    )


def read_ledger_table() -> pl.DataFrame | None:
    """Read ledger with `tags` from transaction_tags (left join, default [])."""
    ledger = _read_ledger_delta_only()
    if ledger is None:
        return None
    return _ledger_with_tags_left_join(ledger)


def ensure_ledger_dir() -> None:
    """Create the ledger directory if it does not exist."""
    LEDGER_TABLE.parent.mkdir(parents=True, exist_ok=True)


def _row_sha256(rows: pl.Series) -> pl.Series:
    """Hash each 'col1=val1|col2=val2|...' string with SHA-256 (for map_batches)."""
    return pl.Series([hashlib.sha256(s.encode("utf-8")).hexdigest() for s in rows])


def _build_ledger_from_bank_df(bank: pl.DataFrame) -> pl.DataFrame:
    """Build ledger from bank table; year, month, day from Transaction Date."""
    ordered = bank.sort(["Transaction Date", "Transaction Counter"])
    if "month" in ordered.columns:
        ordered = ordered.drop("month")
    date_parsed = pl.col("Transaction Date").str.to_date("%d/%m/%Y")
    ordered = ordered.with_columns(
        date_parsed.dt.year().alias("year"),
        date_parsed.dt.month().alias("month"),
        date_parsed.dt.day().alias("day"),
    )
    sorted_cols = sorted(ordered.columns)
    row_str = pl.concat_str(
        [
            pl.concat_str([pl.lit(f"{c}="), pl.col(c).cast(pl.Utf8).fill_null("")])
            for c in sorted_cols
        ],
        separator="|",
    )
    ids = row_str.map_batches(_row_sha256)
    ledger = ordered.with_columns(ids.alias("id")).with_columns(
        pl.lit("").alias("Counter Party"),
        pl.lit("").alias("Item"),
    )
    return ledger


def _assert_unique_id_tag_pairs(df: pl.DataFrame) -> None:
    """Raise if any (id, tag) pair appears more than once."""
    if len(df) != len(df.unique(subset=["id", "tag"])):
        msg = "transaction_tags rows must be unique on (id, tag)"
        raise ValueError(msg)


def _write_transaction_tags_table(tags_df: pl.DataFrame) -> None:
    """Persist one row per (id, tag, allocation); empty input removes the table."""
    if len(tags_df) == 0:
        if TRANSACTION_TAGS_TABLE.exists():
            shutil.rmtree(TRANSACTION_TAGS_TABLE)
        return
    _assert_unique_id_tag_pairs(tags_df)
    TransactionTagsSchema.validate(tags_df)
    TRANSACTION_TAGS_TABLE.parent.mkdir(parents=True, exist_ok=True)
    write_deltalake(
        str(TRANSACTION_TAGS_TABLE),
        tags_df.to_arrow(),
        mode="overwrite",
    )


def update_transaction_tags() -> UpdateTagsResult:
    """Replace transaction tags with tags derived from `tag_rules.rules.TAG_RULES`."""
    ledger = _read_ledger_delta_only()
    if ledger is None or len(ledger) == 0:
        return UpdateTagsResult(
            success=False,
            message="No ledger data. Update the ledger from bank statements first.",
        )
    tags_df = apply_tag_rules(ledger)
    _write_transaction_tags_table(tags_df)
    n = len(tags_df)
    return UpdateTagsResult(
        success=True,
        message=f"Tags updated: {n} tag row(s) from rules.",
    )


def clear_transaction_tags() -> UpdateTagsResult:
    """Remove all rows from the transaction tags table."""
    if TRANSACTION_TAGS_TABLE.exists():
        shutil.rmtree(TRANSACTION_TAGS_TABLE)
    return UpdateTagsResult(success=True, message="All transaction tags cleared.")


def _prune_transaction_tags_to_ledger_ids(ledger_ids: pl.Series) -> None:
    """Drop tag rows whose `id` is not in the current ledger."""
    if not TRANSACTION_TAGS_TABLE.exists():
        return
    tags = _normalize_transaction_tags_df(pl.read_delta(str(TRANSACTION_TAGS_TABLE)))
    valid = set(ledger_ids.to_list())
    pruned = tags.filter(pl.col("id").is_in(list(valid)))
    if len(pruned) == 0:
        shutil.rmtree(TRANSACTION_TAGS_TABLE)
        return
    _write_transaction_tags_table(pruned)


def update_ledger_from_bank() -> UpdateLedgerResult:
    """
    Rebuild the ledger table from the current bank table, assigning a stable id per
    transaction: SHA-256 of the entire row with columns in sorted order.
    """
    ensure_ledger_dir()
    bank = read_existing_table()
    if bank is None or len(bank) == 0:
        return UpdateLedgerResult(
            success=False,
            message="No bank data to build ledger from. Upload statements first.",
        )
    ledger = _build_ledger_from_bank_df(bank)
    write_deltalake(
        str(LEDGER_TABLE),
        ledger.to_arrow(),
        mode="overwrite",
        partition_by=["year", "month"],
    )
    _prune_transaction_tags_to_ledger_ids(ledger["id"])
    return UpdateLedgerResult(
        success=True,
        message=f"Ledger updated: {len(ledger)} transactions with stable SHA-256 ids.",
    )


def recreate_ledger_from_bank() -> UpdateLedgerResult:
    """
    Delete the ledger table completely and recreate it from the current bank table.
    Use when you want a fresh ledger (e.g. after schema or id logic changes).
    """
    if LEDGER_TABLE.exists():
        shutil.rmtree(LEDGER_TABLE)
    if TRANSACTION_TAGS_TABLE.exists():
        shutil.rmtree(TRANSACTION_TAGS_TABLE)
    return update_ledger_from_bank()
