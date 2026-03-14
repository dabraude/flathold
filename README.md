# flathold

Bank statement and transaction annotation tools.

## Setup

```bash
uv sync
```

## Run

- **Streamlit app** (after `uv sync`): `uv run flathold` — opens the multi-page app (upload bank statements, view ledger). Or run directly: `uv run streamlit run src/flathold/app.py`. Data is stored in Delta tables under `db/bank` and `db/ledger` with automatic deduplication on upload.

Data and DB paths are relative to the project root: `data/`, `db/`.
