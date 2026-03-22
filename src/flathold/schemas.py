"""Pandera schemas for bank and stored tags; ledger omits Balance (see LedgerSchema)."""

import pandera.polars as pa

from flathold.tag_rules import KEBAB_TAG_PATTERN


class BankSchema(pa.DataFrameModel):
    """Schema for the bank statement Delta table."""

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
    """Schema for a ledger frame: bank columns except Balance, plus id and year/month/day."""

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


class TransactionTagsSchema(pa.DataFrameModel):
    """One row per tag on a ledger transaction; (id, tag) is unique (no duplicate tags per txn)."""

    id: str = pa.Field()
    tag: str = pa.Field(str_matches=KEBAB_TAG_PATTERN)
    allocation: float = pa.Field()
    counter_party: bool = pa.Field()
