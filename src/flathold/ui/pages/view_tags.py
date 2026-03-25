"""Tag definitions: names, dashboard defaults, counterparty, calculated flag, and groups."""

import polars as pl
import streamlit as st

from flathold.services.tag_definitions_service import (
    merge_seed_tags,
    read_definitions_dataframe,
    reset_to_seed,
    rule_tag_names,
)

_INFO_TAG_PREVIEW = 20

st.set_page_config(page_title="Tags", page_icon="🏷️", layout="wide")


@st.dialog("Reset tag definitions")
def _reset_tags_dialog() -> None:
    st.warning(
        "This **deletes** every row in `db/tag_definitions` and rebuilds the table from "
        "`TAG_DEFINITIONS_SEED_ROWS` in code. Custom tags or edits are lost."
    )
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Confirm reset", type="primary", width="stretch", key="reset_confirm"):
            reset_to_seed()
            st.session_state["_tags_just_reset"] = True
            st.rerun()
    with c2:
        if st.button("Cancel", width="stretch", key="reset_cancel"):
            st.rerun()


with st.sidebar:
    st.caption("Stored in `db/tag_definitions` (Delta). Seed rows merge when missing.")
    if st.button(
        "Merge seed tags",
        help="Append tags from the code seed that are not yet in the table.",
        key="tags_merge_seed",
        width="stretch",
    ):
        merge_seed_tags()
        st.rerun()
    if st.button(
        "Reset tags to seed",
        help="Remove all rows and rebuild from the code seed (destructive).",
        key="tags_reset_seed",
        width="stretch",
    ):
        _reset_tags_dialog()

st.title("🏷️ Tags")

if st.session_state.pop("_tags_just_reset", False):
    st.success("Tag definitions were reset from the code seed.")

df = read_definitions_dataframe()
rule_tags = rule_tag_names()
def_tags = set(df["tag"].to_list())
only_in_rules = rule_tags - def_tags
only_in_def = def_tags - rule_tags

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
    "`db/tag_definitions` (or extend `TAG_DEFINITIONS_SEED_ROWS` and merge)."
)
