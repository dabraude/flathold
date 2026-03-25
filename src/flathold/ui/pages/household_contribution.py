"""Household contribution split — per-account outflows and salary-based fair share."""

from dataclasses import dataclass
from datetime import date, datetime

import polars as pl
import streamlit as st

from flathold.bank_account_metrics import compute_bank_account_metrics
from flathold.data.tables.bank_table import read_existing_table
from flathold.data.tables.household_split_settings_table import (
    HouseholdSplitSettings,
    read_household_split_settings,
    write_household_split_settings,
)
from flathold.services.tagging_service import refresh_ledger_and_tags

st.set_page_config(page_title="Household contribution", page_icon="💷", layout="wide")


def _to_date(d: object) -> date:
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    msg = f"Expected date-like value, got {type(d).__name__}"
    raise TypeError(msg)


def _first_of_month(d: date) -> date:
    return date(d.year, d.month, 1)


def _clamp_range(lo: date, hi: date, bound_lo: date, bound_hi: date) -> tuple[date, date]:
    lo2 = max(lo, bound_lo)
    hi2 = min(hi, bound_hi)
    if lo2 > hi2:
        return bound_lo, bound_hi
    return lo2, hi2


@dataclass(frozen=True, slots=True)
class _BankBounds:
    period_min: date
    period_max: date


def _bank_date_bounds(bank: pl.DataFrame) -> _BankBounds:
    dcol = pl.col("Transaction Date").str.to_date("%d/%m/%Y")
    bounds = bank.select(dcol.min().alias("lo"), dcol.max().alias("hi")).row(0)
    return _BankBounds(period_min=_to_date(bounds[0]), period_max=_to_date(bounds[1]))


with st.sidebar:
    if st.button(
        "Update",
        help="Sync tags with bank + manual ledger, then reapply tag rules",
        key="hc_refresh_ledger_tags",
        width="stretch",
    ):
        with st.spinner("Updating…"):
            result = refresh_ledger_and_tags()
        if result.success:
            st.success(result.message)
        else:
            st.error(result.message)

st.title("💷 Household contribution")
st.caption(
    "Per bank account: mean monthly **debits** (outflows) in your date range — use as a guide for "
    "how much to fund each account. Household total is the **sum** of those per-account averages. "
    "Sundries are added, then the total is split by annual salary (Dave / Claire)."
)

bank = read_existing_table()
if bank is None or len(bank) == 0:
    st.info("No bank data yet. Upload a CSV on **Upload statements**.")
    st.stop()

bounds = _bank_date_bounds(bank)
period_min, period_max = bounds.period_min, bounds.period_max

saved = read_household_split_settings()

if "hc_date_range" not in st.session_state:
    if saved is not None:
        lo0, hi0 = _clamp_range(
            saved.projection_range_start,
            saved.projection_range_end,
            period_min,
            period_max,
        )
        st.session_state.hc_date_range = (lo0, hi0)
    else:
        st.session_state.hc_date_range = (period_min, period_max)

for key, val in (
    ("hc_salary_dave", float(saved.salary_dave_annual_gbp) if saved else 0.0),
    ("hc_salary_claire", float(saved.salary_claire_annual_gbp) if saved else 0.0),
    ("hc_sundries", float(saved.sundries_monthly_gbp) if saved else 0.0),
):
    if key not in st.session_state:
        st.session_state[key] = val

st.subheader("Settings")
c1, c2, c3 = st.columns(3)
with c1:
    st.number_input(
        "Dave annual salary (£)",
        min_value=0.0,
        step=1000.0,
        format="%.0f",
        key="hc_salary_dave",
    )
with c2:
    st.number_input(
        "Claire annual salary (£)",
        min_value=0.0,
        step=1000.0,
        format="%.0f",
        key="hc_salary_claire",
    )
with c3:
    st.number_input(
        "Sundries (£ / month)",
        min_value=0.0,
        step=50.0,
        format="%.2f",
        key="hc_sundries",
    )

date_range_raw = st.date_input(
    "Projection date range (bank transactions)",
    min_value=period_min,
    max_value=period_max,
    help="Debits are included when the transaction date falls in this range; monthly means use "
    "calendar months between the first-of-month of each end.",
    key="hc_date_range",
)

match date_range_raw:
    case (a, b):
        range_start = _to_date(a)
        range_end = _to_date(b)
    case (single,):
        range_start = range_end = _to_date(single)
    case _:
        range_start = range_end = _to_date(date_range_raw)

if range_start > range_end:
    range_start, range_end = range_end, range_start

range_start, range_end = _clamp_range(range_start, range_end, period_min, period_max)

if st.button("Save settings", type="primary", key="hc_save_settings"):
    write_household_split_settings(
        HouseholdSplitSettings(
            salary_dave_annual_gbp=float(st.session_state.hc_salary_dave),
            salary_claire_annual_gbp=float(st.session_state.hc_salary_claire),
            sundries_monthly_gbp=float(st.session_state.hc_sundries),
            projection_range_start=range_start,
            projection_range_end=range_end,
        )
    )
    st.success("Saved to `db/household_split_settings`.")

metrics = compute_bank_account_metrics(bank, range_start, range_end)

st.subheader("Per-account funding (from bank debits)")
if not metrics.accounts:
    st.warning("No transactions in the selected date range.")
else:
    table_rows = [
        {
            "Sort code": r.sort_code,
            "Account": r.account_number,
            "Mean monthly outflow (£)": round(r.mean_monthly_outflow_gbp, 2),
            "Months with debits": r.months_in_sample,
            "Min balance in range (£)": None
            if r.min_balance_in_range is None
            else round(r.min_balance_in_range, 2),
            "Latest balance in range (£)": None
            if r.last_balance_in_range is None
            else round(r.last_balance_in_range, 2),
        }
        for r in metrics.accounts
    ]
    st.dataframe(
        pl.DataFrame(table_rows),
        width="stretch",
        hide_index=True,
    )
    st.caption(
        "Mean monthly outflow = average of each calendar month's **Debit Amount** sum for that "
        "account (months that appear in the range). Balances come from statement **Balance** on "
        "bank rows only."
    )

sundries = float(st.session_state.hc_sundries)
household_need = metrics.household_total_mean_outflow_gbp + sundries
salary_dave = float(st.session_state.hc_salary_dave)
salary_claire = float(st.session_state.hc_salary_claire)
salary_sum = salary_dave + salary_claire

st.subheader("Household total and fair share")
mcols = st.columns(4)
with mcols[0]:
    st.metric(
        "Household mean outflows (£/mo)",
        f"£{metrics.household_total_mean_outflow_gbp:,.2f}",
        help="Sum of each account's mean monthly debit total.",
    )
with mcols[1]:
    st.metric("Sundries (£/mo)", f"£{sundries:,.2f}")
with mcols[2]:
    st.metric("Household need (£/mo)", f"£{household_need:,.2f}", help="Outflows + sundries.")
with mcols[3]:
    if salary_sum <= 0:
        st.metric("Dave / Claire share", "—", help="Enter annual salaries to split.")
    else:
        st.metric(
            "Salary ratio Dave : Claire",
            f"{100.0 * salary_dave / salary_sum:.1f}% : {100.0 * salary_claire / salary_sum:.1f}%",
        )

if salary_sum <= 0:
    st.warning(
        "Enter positive annual salaries for Dave and Claire to see each person's monthly share."
    )
else:
    dave_mo = household_need * (salary_dave / salary_sum)
    claire_mo = household_need * (salary_claire / salary_sum)
    pcols = st.columns(2)
    with pcols[0]:
        st.metric("Dave (£/month)", f"£{dave_mo:,.2f}")
    with pcols[1]:
        st.metric("Claire (£/month)", f"£{claire_mo:,.2f}")
