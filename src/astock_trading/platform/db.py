"""platform/db.py — MySQL runtime connection management."""

from __future__ import annotations

from datetime import datetime, timezone

from astock_trading.platform.database import Database, DatabaseSettings

_SCHEMA_VERSION = 8
_RUNTIME_DB: Database | None = None
_RUNTIME_DB_URL: str | None = None


def _runtime_database() -> Database:
    global _RUNTIME_DB, _RUNTIME_DB_URL
    settings = DatabaseSettings.from_env()
    if _RUNTIME_DB is None or _RUNTIME_DB_URL != settings.url:
        if _RUNTIME_DB is not None:
            try:
                _RUNTIME_DB.engine.dispose()
            except Exception:
                pass
        _RUNTIME_DB = Database(settings)
        _RUNTIME_DB_URL = settings.url
    return _RUNTIME_DB


def connect():
    """Open the configured MySQL runtime database connection."""
    return _runtime_database().connect()


def init_db():
    """Create all MySQL runtime tables and record the current schema version."""
    db = _runtime_database()
    db.create_schema()
    conn = db.connect()
    try:
        _set_schema_version(conn, _SCHEMA_VERSION)
        return db.settings.url
    finally:
        conn.close()


def get_schema_version(conn) -> int:
    """Return the latest recorded runtime schema version."""
    try:
        row = conn.execute(
            "SELECT version FROM _schema_version ORDER BY version DESC LIMIT 1"
        ).fetchone()
        return int(row[0]) if row else 0
    except Exception:
        return 0


def _set_schema_version(conn, version: int) -> None:
    conn.execute(
        "REPLACE INTO _schema_version (version, applied_at) VALUES (?, ?)",
        (version, datetime.now(timezone.utc).isoformat()),
    )
