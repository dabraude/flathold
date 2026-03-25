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
- **Derived read model**: `src/flathold/data/views/ledger_view.py` (`read_ledger_view()`)
- **Presentation transform**: `src/flathold/ui/presenters/ledger_presenter.py`
- **UI**: `src/flathold/ui/pages/view_ledger.py`

## Dashboard
- **Derived read models**: `data/views/ledger_view.py`, `data/tables/transaction_tags_table.py`
- **Pure transforms**: `src/flathold/analytics/` (daily allocations, rollups, chart-ready datasets)
- **UI**: `src/flathold/ui/pages/dashboard.py`
