# Workflows

This doc describes common flows and where the code should live after the refactor.

## Upload bank statements
- **UI**: Streamlit page under `src/flathold/ui/pages/upload_statements.py`
- **Persisted writes**: `src/flathold/data/tables/bank_table.py`

## Add a manual ledger row
- **UI**: Streamlit page under `src/flathold/ui/pages/view_manual_entries.py`
- **Persisted writes**: `src/flathold/data/tables/manual_ledger_table.py`

## Refresh tags (Update button)
This is a use-case: it mutates the `transaction_tags` table based on rules and current ledger ids.

- **UI**: pages call `services.tagging_service.refresh_ledger_and_tags()`
- **Orchestration**: `src/flathold/services/tagging_service.py`
- **Rule application**: `src/flathold/tag_rules/*`
- **Persisted writes**: `src/flathold/data/tables/transaction_tags_table.py`

## View ledger
- **Derived read model**: `src/flathold/data/views/ledger_view.py` (`read_ledger_view()`), called from **`services`** (UI does not import `flathold/data`).
- **Presentation transform**: `src/flathold/ui/presenters/ledger_presenter.py`
- **UI**: `src/flathold/ui/pages/view_ledger.py`

## Dashboard
- **Inputs**: base ledger, `transaction_tags`, and tag metadata are read via **`services`** (and passed into `analytics` as needed); UI imports only `services`, `analytics`, and `core`.
- **Enhanced ledger and analytic views**: `src/flathold/analytics/` — combine base ledger with allocations and tag definitions, derive calculated tags, expose narrow aggregates (daily/monthly series, rollups) for charts and metrics.
- **UI**: `src/flathold/ui/pages/dashboard.py` — must consume those analytic outputs and apply only trivial operations (filter, date range, sum/mean, chart wiring). Until refactored, some logic may still live in the page as technical debt; see `docs/architecture.md`.
