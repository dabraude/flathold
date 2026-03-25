# Data model (tables vs views)

This app separates **source-of-truth persisted tables** from **derived views** that are computed on read.

## Source of truth (persisted Delta tables)

All persisted tables live under `db/` (Delta Lake directories). The code that reads/writes them must live in `src/flathold/data/tables/`.

Expected tables:
- **`bank`**: imported bank statement rows.
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
- **Bank-derived ledger rows**: `id` must be deterministic from the bank row content so it is stable across runs.
- **Manual ledger rows**: `id` is generated once and stored (e.g. prefixed `manual-...`); it must be stable across runs.

## UI-specific presentation (not a view)

Some transformations exist only for display. For example, the View ledger page splits `tags` into:
- “Counter Party” tags (based on tag group membership)
- the rest under “Tags”

These transformations should live under `src/flathold/ui/presenters/` (presentation-only), not in `data/views/`.
