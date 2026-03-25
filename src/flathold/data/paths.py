"""Paths under ``db/`` for Delta Lake tables."""

from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DB_DIR = _PROJECT_ROOT / "db"
BANK_TABLE = DB_DIR / "bank"
MANUAL_LEDGER_TABLE = DB_DIR / "manual_ledger"
LEDGER_TABLE = DB_DIR / "ledger"
TRANSACTION_TAGS_TABLE = DB_DIR / "transaction_tags"
TAG_DEFINITIONS_TABLE = DB_DIR / "tag_definitions"
HOUSEHOLD_SPLIT_SETTINGS_TABLE = DB_DIR / "household_split_settings"
