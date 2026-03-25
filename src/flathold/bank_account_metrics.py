"""Per-bank-account monthly outflow means and optional balance stats from the bank Delta table."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import polars as pl


@dataclass(frozen=True, slots=True)
class BankAccountOutflowRow:
    sort_code: str
    account_number: str
    mean_monthly_outflow_gbp: float
    months_in_sample: int
    min_balance_in_range: float | None
    last_balance_in_range: float | None


@dataclass(frozen=True, slots=True)
class BankAccountMetricsResult:
    accounts: tuple[BankAccountOutflowRow, ...]
    household_total_mean_outflow_gbp: float


def _first_of_month(d: date) -> date:
    return date(d.year, d.month, 1)


def _parse_balance(raw: object) -> float | None:
    if raw is None:
        return None
    s = str(raw).strip().strip("'").replace(",", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _bank_with_dates(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("Transaction Date").str.to_date("%d/%m/%Y").alias("txn_date"),
        pl.col("Sort Code").cast(pl.Utf8).fill_null("").alias("_sort"),
        pl.col("Account Number").cast(pl.Utf8).fill_null("").alias("_acct"),
    )


def compute_bank_account_metrics(
    bank_df: pl.DataFrame,
    range_start: date,
    range_end: date,
) -> BankAccountMetricsResult:
    """Per-account mean monthly debits in range; household line = sum of those means."""
    if len(bank_df) == 0:
        return BankAccountMetricsResult(accounts=(), household_total_mean_outflow_gbp=0.0)

    rs, re = (range_start, range_end) if range_start <= range_end else (range_end, range_start)
    lo_m = _first_of_month(rs)
    hi_m = _first_of_month(re)

    b = _bank_with_dates(bank_df)
    b = b.filter((pl.col("txn_date") >= pl.lit(rs)) & (pl.col("txn_date") <= pl.lit(re)))

    if b.is_empty():
        return BankAccountMetricsResult(accounts=(), household_total_mean_outflow_gbp=0.0)

    monthly = (
        b.with_columns(
            pl.col("txn_date").dt.year().alias("year"),
            pl.col("txn_date").dt.month().alias("month"),
        )
        .group_by(["_sort", "_acct", "year", "month"])
        .agg(pl.col("Debit Amount").sum().alias("monthly_debit"))
        .with_columns(pl.date(pl.col("year"), pl.col("month"), 1).alias("period"))
        .filter((pl.col("period") >= pl.lit(lo_m)) & (pl.col("period") <= pl.lit(hi_m)))
    )

    rows: list[BankAccountOutflowRow] = []
    household_sum = 0.0

    by_acct = monthly.partition_by(["_sort", "_acct"], maintain_order=True, as_dict=True)
    for key, sub in by_acct.items():
        sort_s, acct_s = str(key[0]), str(key[1])
        means = sub.get_column("monthly_debit")
        vals = [float(x) for x in means.to_list()]
        n = len(vals)
        mean_out = sum(vals) / float(n) if n > 0 else 0.0
        household_sum += mean_out

        acct_rows = b.filter((pl.col("_sort") == sort_s) & (pl.col("_acct") == acct_s)).sort(
            ["txn_date", "Transaction Counter"]
        )
        bal_vals: list[float] = []
        for cell in acct_rows["Balance"].to_list():
            p = _parse_balance(cell)
            if p is not None:
                bal_vals.append(p)
        min_b = min(bal_vals) if bal_vals else None
        last_b = bal_vals[-1] if bal_vals else None

        rows.append(
            BankAccountOutflowRow(
                sort_code=sort_s,
                account_number=acct_s,
                mean_monthly_outflow_gbp=mean_out,
                months_in_sample=n,
                min_balance_in_range=min_b,
                last_balance_in_range=last_b,
            )
        )

    rows.sort(key=lambda r: (r.sort_code, r.account_number))
    return BankAccountMetricsResult(
        accounts=tuple(rows),
        household_total_mean_outflow_gbp=household_sum,
    )
