# Contributing / development rules

## Conventions
- **No backward compatibility required**: prefer clear naming and structure.
- **Tables vs views**:
  - Persisted Delta datasets: `*_table` (source of truth).
  - Derived computed-on-read datasets: `*_view` (not source of truth).
- **Layering**: keep Streamlit in `ui/` only; keep Delta IO in `data/tables/` only.

## Required quality gate commands (run in this exact order)

1. `uv run ruff format`
2. `uv run ruff check --fix`
3. `uv run pyright`
4. `uv run pre-commit run --all-files`
