"""Paths and column names used across the app."""

from pathlib import Path

# Project root (repo root); data/ and db/ live there
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_DIR = _PROJECT_ROOT / "db"
BANK_TABLE = DB_DIR / "bank"
LEDGER_TABLE = DB_DIR / "ledger"
