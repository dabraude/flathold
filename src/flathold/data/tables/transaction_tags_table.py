"""Persisted ``transaction_tags`` Delta table: one row per (id, tag) with allocation."""

import shutil

import polars as pl
from deltalake import write_deltalake

from flathold.data.paths import TRANSACTION_TAGS_TABLE
from flathold.data.schemas import TransactionTagsSchema
from flathold.data.tables.tag_definitions_table import read_tag_rule_metadata_map
from flathold.tag_rules.core import validate_transaction_tags_no_calculated_tags


def _normalize_transaction_tags_df(df: pl.DataFrame) -> pl.DataFrame:
    out = df
    if "allocation" not in out.columns:
        out = out.with_columns(pl.lit(0.0).alias("allocation"))
    if "counter_party" not in out.columns:
        out = out.with_columns(pl.lit(False).alias("counter_party"))
    return out


def read_transaction_tags_table() -> pl.DataFrame | None:
    if not TRANSACTION_TAGS_TABLE.exists():
        return None
    try:
        return _normalize_transaction_tags_df(pl.read_delta(str(TRANSACTION_TAGS_TABLE)))
    except Exception:
        return None


def _assert_unique_id_tag_pairs(df: pl.DataFrame) -> None:
    if len(df) != len(df.unique(subset=["id", "tag"])):
        msg = "transaction_tags rows must be unique on (id, tag)"
        raise ValueError(msg)


def write_transaction_tags_table(tags_df: pl.DataFrame) -> None:
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


def clear_transaction_tags_table() -> None:
    if TRANSACTION_TAGS_TABLE.exists():
        shutil.rmtree(TRANSACTION_TAGS_TABLE)


def prune_transaction_tags_to_ledger_ids(ledger_ids: pl.Series) -> None:
    if not TRANSACTION_TAGS_TABLE.exists():
        return
    tags = _normalize_transaction_tags_df(pl.read_delta(str(TRANSACTION_TAGS_TABLE)))
    valid = set(ledger_ids.to_list())
    pruned = tags.filter(pl.col("id").is_in(list(valid)))
    if len(pruned) == 0:
        shutil.rmtree(TRANSACTION_TAGS_TABLE)
        return
    write_transaction_tags_table(pruned)
