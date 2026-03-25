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

## Analytics layer (not persisted)

The **base ledger** above is the only “ledger” under `data/views/`: a join of persisted tables plus stored tags as `tags: List[str]`. It does **not** include **calculated** tags.

**Enhanced ledger** (built in `src/flathold/analytics/`, not under `data/views/`): an in-memory model that combines:

- the base ledger (from `read_ledger_view()`),
- **allocation-level** `transaction_tags` (long form: `id`, `tag`, `allocation`, …),
- **tag metadata** from `tag_definitions` (e.g. `calculated`, groups).

That is where **calculated tags** are derived — tags that must **not** appear in persisted `transaction_tags` (see `tag_rules` validation). Calculated tags are **not** extra columns on a Delta table; they are computed in analytics and may appear as synthetic daily allocation rows or other long-form series. Current homes include modules under `src/flathold/analytics/allocations/`.

**Analytic views**: named transforms on top of the enhanced ledger (or explicit frames derived from it) that return chart-ready or metric-ready Polars frames — e.g. daily allocations per tag, monthly rollups. Pages should consume these and apply only trivial operations (filter, date range, sum, mean); see `docs/architecture.md`.

## UI-specific presentation (not a view)

Some transformations exist only for display. For example, the View ledger page splits `tags` into:
- “Counter Party” tags (based on tag group membership)
- the rest under “Tags”

These transformations should live under `src/flathold/ui/presenters/` (presentation-only), not in `data/views/`.
