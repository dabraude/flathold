"""Pandera schemas for bank, ledger, tags, tag definitions, and household settings."""

import pandera.polars as pa

from flathold.core.tag_pattern import KEBAB_TAG_PATTERN


class BankSchema(pa.DataFrameModel):
    """Schema for the bank statement Delta table."""

    id: str = pa.Field()
    transaction_counter: int = pa.Field(alias="Transaction Counter")
    transaction_date: str = pa.Field(alias="Transaction Date")
    transaction_type: str = pa.Field(alias="Transaction Type")
    sort_code: str = pa.Field(alias="Sort Code")
    account_number: str = pa.Field(alias="Account Number")
    transaction_description: str = pa.Field(alias="Transaction Description")
    debit_amount: float = pa.Field(alias="Debit Amount")
    credit_amount: float = pa.Field(alias="Credit Amount")
    balance: str = pa.Field(alias="Balance")
    month: str = pa.Field()


class LedgerSchema(pa.DataFrameModel):
    """Ledger frame: bank columns except Balance, plus id and year/month/day."""

    transaction_counter: int = pa.Field(alias="Transaction Counter")
    transaction_date: str = pa.Field(alias="Transaction Date")
    transaction_type: str = pa.Field(alias="Transaction Type")
    sort_code: str = pa.Field(alias="Sort Code")
    account_number: str = pa.Field(alias="Account Number")
    transaction_description: str = pa.Field(alias="Transaction Description")
    debit_amount: float = pa.Field(alias="Debit Amount")
    credit_amount: float = pa.Field(alias="Credit Amount")
    id: str = pa.Field()
    year: int = pa.Field()
    month: int = pa.Field()
    day: int = pa.Field()


LEDGER_COLUMN_NAMES: tuple[str, ...] = (
    "Transaction Counter",
    "Transaction Date",
    "Transaction Type",
    "Sort Code",
    "Account Number",
    "Transaction Description",
    "Debit Amount",
    "Credit Amount",
    "id",
    "year",
    "month",
    "day",
)


class TransactionTagsSchema(pa.DataFrameModel):
    """One row per tag on a ledger transaction; (id, tag) is unique."""

    id: str = pa.Field()
    tag: str = pa.Field(str_matches=KEBAB_TAG_PATTERN)
    allocation: float = pa.Field()
    counter_party: bool = pa.Field()


class HouseholdSplitSettingsSchema(pa.DataFrameModel):
    """Single-row Delta table for household contribution page."""

    salary_dave_annual_gbp: float = pa.Field()
    salary_claire_annual_gbp: float = pa.Field()
    sundries_monthly_gbp: float = pa.Field()
    projection_range_start: str = pa.Field()
    projection_range_end: str = pa.Field()


class TagDefinitionsSchema(pa.DataFrameModel):
    """One row per tag: display and grouping metadata."""

    tag: str = pa.Field(str_matches=KEBAB_TAG_PATTERN)
    show_on_dashboard_by_default: bool = pa.Field()
    counter_party: bool = pa.Field()
    calculated: bool = pa.Field()
    groups: str = pa.Field()
