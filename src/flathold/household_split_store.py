"""Persist household contribution settings in ``db/household_split_settings`` (Delta)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import polars as pl
from deltalake import write_deltalake

from flathold.constants import HOUSEHOLD_SPLIT_SETTINGS_TABLE
from flathold.schemas import HouseholdSplitSettingsSchema


@dataclass(frozen=True, slots=True)
class HouseholdSplitSettings:
    salary_dave_annual_gbp: float
    salary_claire_annual_gbp: float
    sundries_monthly_gbp: float
    projection_range_start: date
    projection_range_end: date


def _iso(d: date) -> str:
    return d.isoformat()


def _parse_iso(s: str) -> date:
    return date.fromisoformat(s.strip())


def read_household_split_settings() -> HouseholdSplitSettings | None:
    if not HOUSEHOLD_SPLIT_SETTINGS_TABLE.exists():
        return None
    try:
        df = pl.read_delta(str(HOUSEHOLD_SPLIT_SETTINGS_TABLE))
    except Exception:
        return None
    if len(df) == 0:
        return None
    row = df.row(0, named=True)
    return HouseholdSplitSettings(
        salary_dave_annual_gbp=float(row["salary_dave_annual_gbp"]),
        salary_claire_annual_gbp=float(row["salary_claire_annual_gbp"]),
        sundries_monthly_gbp=float(row["sundries_monthly_gbp"]),
        projection_range_start=_parse_iso(str(row["projection_range_start"])),
        projection_range_end=_parse_iso(str(row["projection_range_end"])),
    )


def write_household_split_settings(settings: HouseholdSplitSettings) -> None:
    df = pl.DataFrame(
        {
            "salary_dave_annual_gbp": [settings.salary_dave_annual_gbp],
            "salary_claire_annual_gbp": [settings.salary_claire_annual_gbp],
            "sundries_monthly_gbp": [settings.sundries_monthly_gbp],
            "projection_range_start": [_iso(settings.projection_range_start)],
            "projection_range_end": [_iso(settings.projection_range_end)],
        }
    )
    HouseholdSplitSettingsSchema.validate(df)
    HOUSEHOLD_SPLIT_SETTINGS_TABLE.parent.mkdir(parents=True, exist_ok=True)
    write_deltalake(
        str(HOUSEHOLD_SPLIT_SETTINGS_TABLE),
        df.to_arrow(),
        mode="overwrite",
        schema_mode="overwrite",
    )
