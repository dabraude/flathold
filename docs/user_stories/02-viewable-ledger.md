# User story: Viewable ledger

## Story

As a user, I want the **View ledger** screen to show a clear, readable table of my transactions so that I can scan amounts, sources, and tagging at a glance — including, over time, **calculated** tag context — without mixing **presentation** changes with **stored** data or **analytics** rules.

## Ledger chain (what happens when)

Data flows through **three** ledger concepts in order. Each step runs **when** something asks for that layer; nothing “skips” a layer in the conceptual model, though today some screens only use an early link in the chain.

| Order | Ledger | When it is produced | What happens |
|------|--------|---------------------|--------------|
| 1 | **Base** | Whenever code calls `read_ledger_view()` (or `get_ledger_view()`). | **Read** bank and manual Delta tables, **union** rows (`ledger_source` = `bank` \| `manual`), **left-join** aggregated **stored** tags from `transaction_tags` into `tags: List[str]` per `id`. **Calculated tags are not** in this frame. (The implementation also **ensures** a `tag_definitions` table exists via `ensure_tag_definitions_table()` — see `docs/data-model.md`.) |
| 2 | **Enhanced** | When analytics needs rule + calculated behaviour (e.g. dashboard allocations, future ledger enrichment). | Start from base ledger **plus** long-form `transaction_tags` (allocations) **plus** tag metadata from `tag_definitions`. **Derive calculated tags** and combined allocation series in memory (e.g. `daily_tag_allocations_long` in `analytics/enhanced_ledger.py`). **No** Delta write; validation still forbids persisting calculated tags on `transaction_tags`. |
| 3 | **Viewable** | When the UI (or a presenter) prepares a frame for display. | Apply **clarity-only** transforms: split `tags` into **Counter Party** vs **Tags** using tag groups, reorder columns, styling — **no** new source of truth. Current page input is the **enhanced** ledger (or a projection of it). |

**Mental model:** **base** = persisted facts + stored tags; **enhanced** = base + analytic derivations (calculated tags, allocation logic); **viewable** = enhanced + human-friendly layout.

## Acceptance criteria

1. **Chain is documented and respected in code layout**: base in `data/views/`, enhanced in `analytics/`, viewable transforms in `ui/presenters/` (see `docs/data-model.md`).
2. **View ledger page** applies **only** viewable-layer presentation (presenter) on top of whatever ledger frame it receives; it does not reimplement analytics or write Delta tables.
3. The frame passed into the viewable presenter is the **enhanced** ledger (or a projection of it), so calculated-tag context appears on screen **without** persisting those tags on `transaction_tags`.
4. The page passes the **enhanced** ledger into the presenter; **split counter party / tags** applies to stored `tags` only, and **Calculated tags** is a separate column.

## Current implementation

| Stage | Status | Where |
|-------|--------|--------|
| Base → read | Implemented | `flathold/data/views/ledger_view.py` (`read_ledger_view`), `flathold/services/ledger_service.py` (`get_ledger_view`) |
| Enhanced → per-row frame | Implemented | `build_enhanced_ledger()` in `flathold/analytics/enhanced_ledger.py`; `get_enhanced_ledger()` in `flathold/services/ledger_service.py` |
| Enhanced → daily series | Implemented (dashboard) | `daily_tag_allocations_long`, `build_dashboard_allocation_long` |
| Enhanced → viewable (table) | Implemented | `view_ledger.py` uses `get_enhanced_ledger()` then `ledger_to_ledger_view` (`calculated_tags` → **Calculated tags** column) |

## Out of scope for this story

- Defining every calculated tag rule (covered by analytics / tag rule modules).
- Persisting calculated tags (explicitly forbidden on `transaction_tags`).
- Replacing the dashboard’s long-form allocation pipeline (`build_dashboard_allocation_long`, chart/metric series). That pipeline stays the right shape for dashboard analytics; the **View ledger** screen uses the **per-row** enhanced ledger (`build_enhanced_ledger`) plus the presenter — the two are complementary, not a single table replacing the other.

## References

- `docs/data-model.md` — **Three ledgers (base, enhanced, viewable)**
- `docs/architecture.md` — pipeline and viewable layer note
