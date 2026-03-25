"""Paths and column names used across the app."""

from pathlib import Path

# Project root (repo root); data/ and db/ live there
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_DIR = _PROJECT_ROOT / "db"
BANK_TABLE = DB_DIR / "bank"
MANUAL_LEDGER_TABLE = DB_DIR / "manual_ledger"
# Legacy: older builds persisted a Delta ledger here; removed on sync (see ledger_delta).
LEDGER_TABLE = DB_DIR / "ledger"
TRANSACTION_TAGS_TABLE = DB_DIR / "transaction_tags"
TAG_DEFINITIONS_TABLE = DB_DIR / "tag_definitions"
HOUSEHOLD_SPLIT_SETTINGS_TABLE = DB_DIR / "household_split_settings"
