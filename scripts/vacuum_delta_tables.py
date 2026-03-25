#!/usr/bin/env python3
"""Remove unreferenced old files from Delta tables under ``db/`` (VACUUM).

By default this is a **dry run** (lists files that would be removed). Pass ``--apply``
to delete. Retention defaults to 7 days (168 hours), matching Delta's usual minimum.

Usage::

    uv run python scripts/vacuum_delta_tables.py
    uv run python scripts/vacuum_delta_tables.py --apply
    uv run python scripts/vacuum_delta_tables.py --apply --retention-hours 168
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from deltalake import DeltaTable

from flathold.data.paths import DB_DIR


def _delta_table_roots(db: Path) -> list[Path]:
    if not db.is_dir():
        return []
    return sorted(p for p in db.iterdir() if p.is_dir() and (p / "_delta_log").exists())


def _vacuum_one(
    table_path: Path,
    *,
    retention_hours: int | None,
    dry_run: bool,
    enforce_retention_duration: bool,
) -> list[str]:
    dt = DeltaTable(str(table_path))
    return dt.vacuum(
        retention_hours=retention_hours,
        dry_run=dry_run,
        enforce_retention_duration=enforce_retention_duration,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="VACUUM Delta tables under db/.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete orphaned files (default: dry run only)",
    )
    parser.add_argument(
        "--retention-hours",
        type=int,
        default=168,
        help="Minimum age of files before they can be removed (default: 168 = 7 days)",
    )
    parser.add_argument(
        "--no-enforce-retention",
        action="store_true",
        help="Allow vacuum below table retention (use with care)",
    )
    args = parser.parse_args()
    dry_run = not args.apply
    roots = _delta_table_roots(DB_DIR)
    if not roots:
        print(f"No Delta tables found under {DB_DIR}", file=sys.stderr)
        return
    if dry_run:
        print("Dry run (no files deleted). Use --apply to vacuum.\n", file=sys.stderr)
    for root in roots:
        label = "would remove" if dry_run else "removed"
        try:
            files = _vacuum_one(
                root,
                retention_hours=args.retention_hours,
                dry_run=dry_run,
                enforce_retention_duration=not args.no_enforce_retention,
            )
        except Exception as e:
            print(f"{root}: error: {e}", file=sys.stderr)
            raise
        print(f"{root} ({label}, n={len(files)}):")
        for f in files:
            print(f"  {f}")


if __name__ == "__main__":
    main()
