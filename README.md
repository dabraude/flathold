# flathold

Bank statement and transaction annotation tools.

## Setup

```bash
uv sync
```

## Run

- **Streamlit app** (after `uv sync`): `uv run streamlit run app.py` — opens the multi-page app (upload bank statements, view data). Data is stored in a Delta table under `db/bank` with automatic deduplication on upload.

Data and DB paths are relative to the project root: `data/`, `db/`.
