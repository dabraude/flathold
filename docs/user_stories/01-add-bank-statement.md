# User story: Add a bank statement (CSV upload)

## Story

As a user, I want to upload a bank statement CSV so that new transactions are stored in the bank Delta table while duplicates (relative to what is already stored) are skipped, and each row can be given a stable identity for downstream use.

## Acceptance criteria

1. The upload page accepts a CSV file and passes its bytes into the bank flow (no direct Delta writes from the UI).
2. After parsing the CSV, each row gets a **Transaction Counter** that is **scoped to the transaction date**: within a given `Transaction Date`, counters reflect row order in the file (1, 2, 3, …); a new date starts again from 1.
3. Incoming rows are merged with the existing bank table; **duplicates are defined by the persisted `id`**. **Existing rows win** when the same `id` appears again (the new upload row is dropped).
4. The user sees how many rows were in the file, how many were added, how many were treated as duplicates, and the total row count in the table after the save.

## Partitioning (physical layout)

The bank Delta table lives at **`db/bank`** (project-relative). Rows are **partitioned by calendar month**:

- A **`month`** column is stored on each row: `YYYY-MM`, derived from **`Transaction Date`** (parsed as `DD/MM/YYYY` and formatted as year-month).
- On write, Delta Lake uses **`partition_by=["month"]`**, so data is laid out under **`db/bank/month=YYYY-MM/`** (one subdirectory per month present in the table).
- **`id` is computed before `month` is attached** for persistence. The partition column is **not** part of the canonical string hashed for `id` (the hash uses the same inputs as before `month` existed on the frame).

## Current implementation flow

| Step | What happens | Where |
|------|----------------|-------|
| 1 | User selects a CSV; the page reads bytes and calls `load_csv_bytes` then `save_bank_to_delta`. | `flathold/ui/pages/upload_statements.py` |
| 2 | CSV is parsed with Polars; columns are normalized (names stripped, sort code cleaned, debit/credit as floats). | `flathold/services/bank_service.py` → `flathold/data/tables/bank_table.py` (`load_csv_bytes_to_dataframe` → `_normalize`) |
| 3 | **Transaction Counter** is added: ordinal rank **within each `Transaction Date`**, preserving CSV row order within that day. | `bank_table._add_transaction_counter` |
| 4 | A deterministic **`id`** is computed and attached (see **Stable identity**). | `bank_table.compute_bank_transaction_ids` |
| 5 | A **`month`** partition column (`YYYY-MM`) is derived from `Transaction Date` and stored on the row for partitioning. | `bank_table._add_month_partition` |
| 6 | Existing bank Delta data is read (if present), concatenated **with existing rows first**, then **`unique(subset=["id"], keep="first")`**. | `bank_table.save_to_delta` |
| 7 | The combined result **overwrites** the bank Delta table with **`partition_by=["month"]`** (partitioned physical layout under `db/bank`). | `bank_table.save_to_delta` → `write_deltalake` |

## Stable identity and “idempotent id”

- Each row gets a deterministic **`id`** (SHA-256 over a canonical string of bank columns plus derived calendar parts used in the hash) **before** merge. That column is **written to the bank Delta table** with the rest of the row.
- **Deduplication on upload** uses **`id`**: `existing` rows are concatenated before new rows, then `unique(subset=["id"], keep="first")`, so rows already in the table win.
- The **ledger** view builds bank-derived rows from the bank table using the **stored** `id` (with a fallback that recomputes `id` only if legacy data lacks the column). See `flathold/data/tables/bank_table.py` (`compute_bank_transaction_ids`) and `flathold/data/views/ledger_view.py` (`build_ledger_from_bank_df`).

## Out of scope for this story

- Tag rules, `transaction_tags`, and the sidebar “Update” / refresh flow (separate use case).
- Manual ledger entries (different table and flow).
