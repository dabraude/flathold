# flathold

Bank statement and transaction annotation tools.

## Setup

```bash
uv sync
```

## Run

- **Streamlit app** (after `uv sync`): `uv run flathold` — opens the multi-page app (upload bank statements, view ledger). Or run directly: `uv run streamlit run src/flathold/ui/app.py`. Bank statements are stored in a Delta table under `db/bank` (deduplicated on upload). The ledger (transactions with stable ids and calendar fields) is computed from bank data when the app loads; rule-based tags are stored under `db/transaction_tags`.

Data and DB paths are relative to the project root: `data/`, `db/`.
