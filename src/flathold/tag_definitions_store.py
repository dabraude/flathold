"""Persisted tag list and metadata (``db/tag_definitions``); seed from ``tag_definitions_seed``."""

from __future__ import annotations

import shutil
from collections.abc import Mapping
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from flathold.tag_rules.core import TagRule
from deltalake import write_deltalake

from flathold.constants import TAG_DEFINITIONS_TABLE
from flathold.schemas import TagDefinitionsSchema
from flathold.tag_definitions_seed import CALCULATED_TAG_NAMES, TAG_DEFINITIONS_SEED_ROWS
from flathold.tag_group import TagGroup
from flathold.tag_pattern import validate_kebab_tag
from flathold.tag_rule_metadata import TagRuleMetadata

_GROUPS_SEP = "|"

# Persisted order for Delta and ``pl.concat`` (must match ``TagDefinitionsSchema`` field order).
_TAG_DEFINITIONS_COLUMNS: tuple[str, ...] = (
    "tag",
    "counter_party",
    "calculated",
    "groups",
)


def _groups_to_storage(groups: tuple[str, ...]) -> str:
    return _GROUPS_SEP.join(sorted(groups))


def _groups_from_storage(s: str) -> tuple[str, ...]:
    if not s.strip():
        return ()
    parts = [p.strip() for p in s.split(_GROUPS_SEP) if p.strip()]
    return tuple(sorted(set(parts)))


def _seed_dataframe() -> pl.DataFrame:
    rows = [
        {
            "tag": tag,
            "counter_party": cp,
            "calculated": tag in CALCULATED_TAG_NAMES,
            "groups": _groups_to_storage(groups),
        }
        for tag, cp, groups in TAG_DEFINITIONS_SEED_ROWS
    ]
    return pl.DataFrame(rows).select(_TAG_DEFINITIONS_COLUMNS)


def _normalize_tag_definitions_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Backfill ``calculated`` if missing and enforce column order for concat / schema."""
    out = df
    if "calculated" not in out.columns:
        out = out.with_columns(pl.lit(False).alias("calculated"))
    return out.select(_TAG_DEFINITIONS_COLUMNS)


def _normalize_metadata(
    *,
    counter_party: bool,
    groups: tuple[TagGroup, ...],
) -> tuple[TagGroup, ...]:
    if not counter_party or TagGroup.COUNTER_PARTY in groups:
        return groups
    return (*groups, TagGroup.COUNTER_PARTY)


def row_to_tag_rule_metadata(row: dict[str, object]) -> TagRuleMetadata:
    """Build metadata from one row dict (e.g. ``to_dicts()[0]``)."""
    raw = _groups_from_storage(str(row["groups"]))
    groups_tuple = tuple(TagGroup(g) for g in raw)
    cp = bool(row["counter_party"])
    groups_tuple = _normalize_metadata(counter_party=cp, groups=groups_tuple)
    return TagRuleMetadata(
        counter_party=cp,
        calculated=bool(row["calculated"]),
        groups=groups_tuple,
    )


def _write_tag_definitions_table(df: pl.DataFrame) -> None:
    if len(df) == 0:
        if TAG_DEFINITIONS_TABLE.exists():
            shutil.rmtree(TAG_DEFINITIONS_TABLE)
        return
    if len(df) != len(df["tag"].unique()):
        msg = "tag_definitions must have unique tag per row"
        raise ValueError(msg)
    TagDefinitionsSchema.validate(df)
    TAG_DEFINITIONS_TABLE.parent.mkdir(parents=True, exist_ok=True)
    write_deltalake(
        str(TAG_DEFINITIONS_TABLE),
        df.to_arrow(),
        mode="overwrite",
        schema_mode="overwrite",
    )


def _read_tag_definitions_raw() -> pl.DataFrame | None:
    if not TAG_DEFINITIONS_TABLE.exists():
        return None
    try:
        return pl.read_delta(str(TAG_DEFINITIONS_TABLE))
    except Exception:
        return None


def reset_tag_definitions_to_seed() -> None:
    """Replace ``tag_definitions`` entirely with the code seed rows."""
    _write_tag_definitions_table(_seed_dataframe())


def ensure_tag_definitions_table() -> None:
    """Create ``tag_definitions`` from seed if missing; merge new seed tags not yet stored."""
    seed = _seed_dataframe()
    raw = _read_tag_definitions_raw()
    if raw is None or len(raw) == 0:
        _write_tag_definitions_table(seed)
        return
    had_old_show_column = "show_on_dashboard_by_default" in raw.columns
    had_calculated = "calculated" in raw.columns
    existing = _normalize_tag_definitions_columns(raw)
    have = set(existing["tag"].to_list())
    missing = seed.filter(~pl.col("tag").is_in(list(have)))
    if len(missing) == 0:
        if not had_calculated or had_old_show_column:
            _write_tag_definitions_table(existing)
        return
    merged = pl.concat([existing, missing], how="vertical")
    _write_tag_definitions_table(merged)


def read_tag_definitions_table() -> pl.DataFrame:
    """Read tag definitions after ensuring the table exists and schema validates."""
    ensure_tag_definitions_table()
    raw = _read_tag_definitions_raw()
    if raw is None or len(raw) == 0:
        msg = "tag_definitions table missing after ensure"
        raise RuntimeError(msg)
    normalized = _normalize_tag_definitions_columns(raw)
    TagDefinitionsSchema.validate(normalized)
    for t in normalized["tag"].to_list():
        validate_kebab_tag(str(t))
    return normalized


def read_tag_rule_metadata_map() -> dict[str, TagRuleMetadata]:
    """Map tag name to metadata for rule application and validation."""
    df = read_tag_definitions_table()
    out: dict[str, TagRuleMetadata] = {}
    for row in df.iter_rows(named=True):
        tag = str(row["tag"])
        out[tag] = row_to_tag_rule_metadata(row)
    return out


def tag_metadata_for_tag(tag: str) -> TagRuleMetadata | None:
    """Return metadata for ``tag``, or ``None`` if the tag is not in the definitions table."""
    df = read_tag_definitions_table()
    hit = df.filter(pl.col("tag") == tag)
    if len(hit) == 0:
        return None
    row = hit.to_dicts()[0]
    return row_to_tag_rule_metadata(row)


def metadata_map_covers_rules(
    rules: tuple[TagRule, ...],
    meta: Mapping[str, TagRuleMetadata],
) -> None:
    """Raise ``ValueError`` if any rule's tag is missing from ``meta`` or marked calculated."""
    missing = [r.tag for r in rules if r.tag not in meta]
    if missing:
        msg = (
            "Every TAG_RULES tag must exist in tag_definitions (add rows to seed or table): "
            f"{sorted(set(missing))!r}"
        )
        raise ValueError(msg)
    calculated = [r.tag for r in rules if meta[r.tag].calculated]
    if calculated:
        msg = (
            "TAG_RULES must not use tags marked calculated in tag_definitions: "
            f"{sorted(set(calculated))!r}"
        )
        raise ValueError(msg)


def tag_groups(tag: str) -> tuple[TagGroup, ...]:
    """Return group labels for ``tag`` from persisted definitions."""
    m = tag_metadata_for_tag(tag)
    return () if m is None else m.groups


def tag_counter_party(tag: str) -> bool:
    """Whether ``tag`` is a counterparty tag (person or institution)."""
    m = tag_metadata_for_tag(tag)
    return False if m is None else m.counter_party
