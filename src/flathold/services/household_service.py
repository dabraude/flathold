"""Household split settings (orchestration boundary for UI)."""

from __future__ import annotations

import polars as pl

from flathold.data.tables.bank_table import read_existing_table
from flathold.data.tables.household_split_settings_table import (
    HouseholdSplitSettings,
    read_household_split_settings,
    write_household_split_settings,
)

__all__ = [
    "HouseholdSplitSettings",
    "read_bank_for_household",
    "read_household_settings",
    "save_household_settings",
]


def read_bank_for_household() -> pl.DataFrame | None:
    return read_existing_table()


def read_household_settings() -> HouseholdSplitSettings | None:
    return read_household_split_settings()


def save_household_settings(settings: HouseholdSplitSettings) -> None:
    write_household_split_settings(settings)
