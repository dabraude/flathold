"""Ledger rows from the bank table; tags and per-tag metadata live in Delta tables."""

import hashlib
import shutil
from dataclasses import dataclass

import polars as pl
from deltalake import write_deltalake

from flathold.bank_delta import read_existing_table
from flathold.constants import LEDGER_TABLE, TRANSACTION_TAGS_TABLE
from flathold.manual_ledger import read_manual_ledger_table
from flathold.schemas import LEDGER_COLUMN_NAMES, TransactionTagsSchema
from flathold.tag_definitions_store import ensure_tag_definitions_table, read_tag_rule_metadata_map
from flathold.tag_rules import apply_tag_rules
from flathold.tag_rules.core import validate_transaction_tags_no_calculated_tags


@dataclass(frozen=True, slots=True)
class UpdateLedgerResult:
    success: bool
    message: str


@dataclass(frozen=True, slots=True)
class UpdateTagsResult:
    success: bool
    message: str


def _remove_legacy_ledger_table() -> None:
    """Remove persisted ``db/ledger`` if present (older versions wrote a Delta ledger)."""
    if LEDGER_TABLE.exists():
        shutil.rmtree(LEDGER_TABLE)


def _normalize_transaction_tags_df(df: pl.DataFrame) -> pl.DataFrame:
    """Fill in columns expected by `TransactionTagsSchema` (legacy tables may omit columns)."""
    out = df
    if "allocation" not in out.columns:
        out = out.with_columns(pl.lit(0.0).alias("allocation"))
    if "counter_party" not in out.columns:
        out = out.with_columns(pl.lit(False).alias("counter_party"))
    return out


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


def _compute_ledger_from_bank() -> pl.DataFrame | None:
    """Build the ledger frame from the bank Delta table, or None if there is no bank data."""
    bank = read_existing_table()
    if bank is None or len(bank) == 0:
        return None
    return _align_ledger_columns(_build_ledger_from_bank_df(bank))


def _align_ledger_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Enforce ``LEDGER_COLUMN_NAMES`` order and dtypes so bank + manual ``vstack`` matches."""
    aligned = df.select(LEDGER_COLUMN_NAMES)
    return aligned.with_columns(
        pl.col("Transaction Counter").cast(pl.Int64, strict=False),
        pl.col("Transaction Date").cast(pl.Utf8, strict=False),
        pl.col("Transaction Type").cast(pl.Utf8, strict=False),
        pl.col("Sort Code").cast(pl.Utf8, strict=False),
        pl.col("Account Number").cast(pl.Utf8, strict=False),
        pl.col("Transaction Description").cast(pl.Utf8, strict=False),
        pl.col("Debit Amount").cast(pl.Float64, strict=False),
        pl.col("Credit Amount").cast(pl.Float64, strict=False),
        pl.col("id").cast(pl.Utf8, strict=False),
        pl.col("year").cast(pl.Int64, strict=False),
        pl.col("month").cast(pl.Int64, strict=False),
        pl.col("day").cast(pl.Int64, strict=False),
    )


def _combine_bank_and_manual_ledger(
    bank_ledger: pl.DataFrame | None,
    manual_ledger: pl.DataFrame | None,
) -> pl.DataFrame | None:
    """Union bank-derived and manual rows (no ``ledger_source`` column)."""
    parts: list[pl.DataFrame] = []
    if bank_ledger is not None and len(bank_ledger) > 0:
        parts.append(_align_ledger_columns(bank_ledger))
    if manual_ledger is not None and len(manual_ledger) > 0:
        parts.append(_align_ledger_columns(manual_ledger))
    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]
    return pl.concat(parts, how="vertical")


def _ledger_with_source(
    bank_ledger: pl.DataFrame | None,
    manual_ledger: pl.DataFrame | None,
) -> pl.DataFrame | None:
    """Like ``_combine_bank_and_manual_ledger`` but adds ``ledger_source`` ``bank`` / ``manual``."""
    parts: list[pl.DataFrame] = []
    if bank_ledger is not None and len(bank_ledger) > 0:
        parts.append(
            _align_ledger_columns(bank_ledger).with_columns(pl.lit("bank").alias("ledger_source"))
        )
    if manual_ledger is not None and len(manual_ledger) > 0:
        parts.append(
            _align_ledger_columns(manual_ledger).with_columns(
                pl.lit("manual").alias("ledger_source")
            )
        )
    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]
    return pl.concat(parts, how="vertical")


def read_ledger_table() -> pl.DataFrame | None:
    """Return bank + manual ledger with ``ledger_source``, plus ``tags`` from transaction_tags."""
    ensure_tag_definitions_table()
    bank_ledger = _compute_ledger_from_bank()
    manual = read_manual_ledger_table()
    manual_df = manual if manual is not None and len(manual) > 0 else None
    ledger = _ledger_with_source(bank_ledger, manual_df)
    if ledger is None:
        return None
    return _ledger_with_tags_left_join(ledger)


def read_transaction_tags_table() -> pl.DataFrame | None:
    """Read the transaction_tags Delta table if it exists; otherwise None."""
    return _read_transaction_tags_raw()


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
    return ordered.with_columns(ids.alias("id")).drop("Balance")


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
    validate_transaction_tags_no_calculated_tags(tags_df, read_tag_rule_metadata_map())
    TRANSACTION_TAGS_TABLE.parent.mkdir(parents=True, exist_ok=True)
    write_deltalake(
        str(TRANSACTION_TAGS_TABLE),
        tags_df.to_arrow(),
        mode="overwrite",
        schema_mode="overwrite",
    )


def update_transaction_tags() -> UpdateTagsResult:
    """Replace transaction tags with tags derived from `tag_rules.rules.TAG_RULES`."""
    bank_ledger = _compute_ledger_from_bank()
    manual = read_manual_ledger_table()
    manual_df = manual if manual is not None and len(manual) > 0 else None
    ledger = _combine_bank_and_manual_ledger(bank_ledger, manual_df)
    if ledger is None or len(ledger) == 0:
        return UpdateTagsResult(
            success=False,
            message="No ledger data. Upload bank CSV and/or add manual entries.",
        )
    try:
        tags_df = apply_tag_rules(ledger)
    except ValueError as e:
        return UpdateTagsResult(success=False, message=str(e))
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
    Prune stored tags to ids present in the current bank + manual ledger,
    and remove any legacy persisted ledger Delta directory.
    """
    bank = read_existing_table()
    manual = read_manual_ledger_table()
    bank_ledger = _build_ledger_from_bank_df(bank) if bank is not None and len(bank) > 0 else None
    manual_df = manual if manual is not None and len(manual) > 0 else None
    ledger = _combine_bank_and_manual_ledger(bank_ledger, manual_df)
    if ledger is None or len(ledger) == 0:
        return UpdateLedgerResult(
            success=False,
            message="No bank or manual ledger data.",
        )
    n_bank = len(bank_ledger) if bank_ledger is not None else 0
    n_manual = len(manual_df) if manual_df is not None else 0
    _remove_legacy_ledger_table()
    _prune_transaction_tags_to_ledger_ids(ledger["id"])
    msg = (
        f"Tags pruned to current ledger ({len(ledger)} rows: {n_bank} bank, {n_manual} manual). "
        "Ledger is computed on read."
    )
    return UpdateLedgerResult(success=True, message=msg)


def recreate_ledger_from_bank() -> UpdateLedgerResult:
    """
    Clear all transaction tags and remove any legacy persisted ledger table.
    The ledger view is derived from bank + manual tables when the app reads data.
    """
    _remove_legacy_ledger_table()
    if TRANSACTION_TAGS_TABLE.exists():
        shutil.rmtree(TRANSACTION_TAGS_TABLE)
    msg = (
        "All transaction tags cleared. "
        "Ledger rows come from bank and manual data when you view the app."
    )
    return UpdateLedgerResult(success=True, message=msg)


def refresh_ledger_and_tags() -> UpdateTagsResult:
    """Prune tags to bank + manual ledger, drop legacy ledger files, reapply tag rules."""
    prune = update_ledger_from_bank()
    if not prune.success:
        return UpdateTagsResult(success=False, message=prune.message)
    rules = update_transaction_tags()
    if not rules.success:
        return UpdateTagsResult(success=False, message=rules.message)
    return UpdateTagsResult(success=True, message=f"{prune.message}\n\n{rules.message}")
