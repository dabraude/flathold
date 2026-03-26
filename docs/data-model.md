# Data model (tables vs views)

This app separates **source-of-truth persisted tables** from **derived views** that are computed on read.

## Source of truth (persisted Delta tables)

All persisted tables live under `db/` (Delta Lake directories). The code that reads/writes them must live in `src/flathold/data/tables/`.

Expected tables:
- **`bank`**: imported bank statement rows (includes deterministic `id` per row, same value used when joining tags and building the ledger).
- **`manual_ledger`**: manually entered ledger rows.
- **`transaction_tags`**: one row per `(id, tag)` with `allocation` and `counter_party`.
- **`tag_definitions`**: one row per tag with metadata (groups, calculated, etc.).
- **`household_split_settings`**: settings for household contribution view.

## Derived views (computed on read)

Derived views must live in `src/flathold/data/views/`. They are **not a source of truth** and should not be written as Delta tables.

### `ledger_view` (derived “one big table”)

`ledger_view` is computed on read from the persisted tables:

1. Build a base ledger from `bank ∪ manual_ledger` (union), with:
   - stable `id`
   - `year`, `month`, `day`
   - `ledger_source` = `"bank"` or `"manual"`

2. Attach tags as a **simple left join by `id`**:
   - aggregate `transaction_tags` to `tags: List[str]` per `id`
   - left join onto the base ledger on `id`
   - missing tags become `[]`

#### `id` semantics
- **Bank-derived ledger rows**: `id` is stored on the **`bank` Delta table** and is deterministic from the bank row content (same formula as before, now persisted at ingest). The ledger view reads that `id` when building bank-derived rows.
- **Manual ledger rows**: `id` is generated once and stored (e.g. prefixed `manual-...`); it must be stable across runs.

## Three ledgers (base, enhanced, viewable)

These names describe **how far derived** the data is: persisted facts plus stored tags → analytics (including calculated tags) → UI-ready layout. Only the **base** ledger is the single wide “ledger table” in `data/views/`; the others are in-memory or screen-oriented.

### 1. Base ledger

- **Definition**: One row per bank or manual transaction: banking columns, stable `id`, `ledger_source` (`"bank"` | `"manual"`), calendar parts, and `tags: List[str]` from **stored** `transaction_tags` only (aggregated per `id`, left-joined; missing → `[]`).
- **Excludes**: Calculated tags. Those must not appear in persisted `transaction_tags` (see `tag_rules` validation).
- **Code**: `read_ledger_view()` in `src/flathold/data/views/ledger_view.py`. Services/UI entrypoint: `get_ledger_view()` in `src/flathold/services/ledger_service.py`.

### 2. Enhanced ledger

- **Definition**: The **analytic** layer that starts from the base ledger and adds **allocation-level** `transaction_tags` (long form: `id`, `tag`, `allocation`, …) plus **tag metadata** from `tag_definitions` (e.g. `calculated`, groups). **Calculated tags** are derived here — not as extra Delta columns, but as computed series (and, in future designs possibly other shapes) on read.
- **Implementation today**:
  - Long-form daily series `period`, `tag`, `allocation` from `daily_tag_allocations_long()` in `src/flathold/analytics/enhanced_ledger.py` (dashboard via `build_dashboard_allocation_long()` in `dashboard_views.py`).
  - Per-row frame: `build_enhanced_ledger()` adds `calculated_tags: List[str]` per row from per-transaction remainders (e.g. `untagged-spend`, `uncategorised-sector`). Row-level `unknown-cash` is not included (monthly series only). **Stored** `tags` and **derived** `calculated_tags` stay separate.
- **Not yet**: Merging calculated tag names into the `tags` list; synthetic rows (`virtual_txn_rows` in `enhanced_ledger.py`).

### 3. Viewable ledger

- **Definition**: What the **View ledger** (and similar screens) **display** after **clarity-only** transforms: no new persisted or analytic facts, only presentation — e.g. splitting `tags` into **Counter Party** vs **Tags** using tag group metadata, column order, and styling.
- **Code**: `get_enhanced_ledger()` in `src/flathold/services/ledger_service.py` (orchestration), then `src/flathold/ui/presenters/ledger_presenter.py` (`ledger_to_ledger_view`, etc.).
- **Current vs target**: The **View ledger** page loads **enhanced** data via `get_enhanced_ledger()` then the viewable presenter. Further columns or presenter tweaks remain optional.

### Analytic views (built on enhanced)

**Analytic views** are named transforms on top of the enhanced ledger (or frames derived from it) that return chart-ready or metric-ready Polars outputs — e.g. pivoted daily series, monthly rollups. UI pages should consume these and apply only trivial operations (filter, date range, sum, mean); see `docs/architecture.md`.
