"""Tag definitions: names, dashboard defaults, counterparty, calculated flag, and groups."""

import polars as pl
import streamlit as st

from flathold.services.tag_definitions_service import (
    merge_seed_tags,
    read_definitions_dataframe,
    rule_tag_names,
)

_INFO_TAG_PREVIEW = 20

st.set_page_config(page_title="Tags", page_icon="🏷️", layout="wide")


with st.sidebar:
    st.caption("Stored in `db/tag_definitions` (Delta).")
    if st.button(
        "Refresh seeds",
        help=(
            "Bring `db/tag_definitions` up to date with the code seed by adding any missing "
            "seed tags."
        ),
        key="tags_merge_seed",
        width="stretch",
    ):
        merge_seed_tags()
        st.session_state["_tags_just_refreshed"] = True
        st.rerun()

st.title("🏷️ Tags")

if st.session_state.pop("_tags_just_refreshed", False):
    st.success("Seed tags refreshed from code.")

df = read_definitions_dataframe()
rule_tags = rule_tag_names()
def_tags = set(df["tag"].to_list())
only_in_rules = rule_tags - def_tags
calculated_def_tags = set(df.filter(pl.col("calculated"))["tag"].to_list())
only_in_def = def_tags - rule_tags - calculated_def_tags

if only_in_rules:
    st.error(
        "These tags are used in **TAG_RULES** but missing from definitions (add them to the seed "
        f"or table): `{sorted(only_in_rules)}`"
    )
if only_in_def:
    st.info(
        f"{len(only_in_def)} tag(s) exist in definitions only (no matching rule yet): "
        f"`{sorted(only_in_def)[:_INFO_TAG_PREVIEW]}`"
        f"{'…' if len(only_in_def) > _INFO_TAG_PREVIEW else ''}"
    )

display = (
    df.sort("tag")
    .with_columns(
        pl.when(pl.col("groups").str.len_chars() > 0)
        .then(pl.col("groups").str.split("|").list.join(", "))
        .otherwise(pl.lit("—"))
        .alias("Allocation groups"),
    )
    .select(
        pl.col("tag").alias("Tag"),
        pl.col("counter_party").alias("Counterparty"),
        pl.col("calculated").alias("Calculated"),
        pl.col("Allocation groups"),
    )
)

st.dataframe(
    display,
    column_config={
        "Tag": st.column_config.TextColumn("Tag", width="medium"),
        "Counterparty": st.column_config.CheckboxColumn("Counterparty"),
        "Calculated": st.column_config.CheckboxColumn("Calculated"),
        "Allocation groups": st.column_config.TextColumn("Allocation groups", width="large"),
    },
    hide_index=True,
    width="stretch",
)

st.caption(
    f"{len(display)} tag definition(s). Rules live in code; metadata is editable in "
    "`db/tag_definitions`. Seed refresh adds any missing rows from `TAG_DEFINITIONS_SEED_ROWS`."
)
