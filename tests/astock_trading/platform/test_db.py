"""Tests for platform/db.py migration flow."""

from __future__ import annotations

import sqlite3

import pytest

from astock_trading.platform.db import connect, get_schema_version, init_db
from astock_trading.platform.database import MissingDatabaseUrl


def test_init_db_sets_latest_schema_version(tmp_path):
    db_path = tmp_path / "test.db"

    init_db(db_path)

    conn = connect(db_path)
    try:
        version = get_schema_version(conn)
        assert version == 6

        columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(projection_positions)")
        }
        assert "currency" in columns
        assert "cost_basis_cents" in columns
        stream_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(event_streams)")
        }
        assert {"stream", "stream_type", "next_version", "updated_at"} <= stream_columns
        history_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(signal_history_snapshots)")
        }
        assert {
            "snapshot_id",
            "snapshot_date",
            "history_group_id",
            "run_id",
            "phase",
            "snapshot_type",
            "payload_json",
            "created_at",
        } <= history_columns
    finally:
        conn.close()


def test_runtime_db_requires_database_url(monkeypatch):
    import astock_trading.platform.db as db_module

    db_module._RUNTIME_DB = None
    monkeypatch.delenv("ASTOCK_DATABASE_URL", raising=False)

    with pytest.raises(MissingDatabaseUrl, match="ASTOCK_DATABASE_URL is required"):
        init_db()


def test_runtime_db_uses_sqlalchemy_url(monkeypatch, tmp_path):
    import astock_trading.platform.db as db_module

    db_module._RUNTIME_DB = None
    db_path = tmp_path / "runtime.db"
    monkeypatch.setenv("ASTOCK_DATABASE_URL", f"sqlite:///{db_path}")

    init_db()
    conn = connect()
    try:
        assert get_schema_version(conn) == 6
        conn.execute(
            "INSERT INTO event_log "
            "(event_id, stream, stream_type, stream_version, event_type, payload_json, metadata_json, occurred_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("evt_1", "s:1", "t", 1, "e", "{}", "{}", "2026-01-01T00:00:00+00:00"),
        )
        assert conn.execute("SELECT COUNT(*) FROM event_log").fetchone()[0] == 1
    finally:
        conn.close()


def test_runtime_db_reloads_when_database_url_changes(monkeypatch, tmp_path):
    import astock_trading.platform.db as db_module

    db_module._RUNTIME_DB = None
    db_module._RUNTIME_DB_URL = None
    first_path = tmp_path / "runtime-1.db"
    second_path = tmp_path / "runtime-2.db"

    monkeypatch.setenv("ASTOCK_DATABASE_URL", f"sqlite:///{first_path}")
    init_db()
    conn = connect()
    try:
        conn.execute(
            "INSERT INTO event_log "
            "(event_id, stream, stream_type, stream_version, event_type, payload_json, metadata_json, occurred_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("evt_first", "s:first", "t", 1, "e", "{}", "{}", "2026-01-01T00:00:00+00:00"),
        )
    finally:
        conn.close()

    monkeypatch.setenv("ASTOCK_DATABASE_URL", f"sqlite:///{second_path}")
    init_db()
    conn = connect()
    try:
        assert conn.execute("SELECT COUNT(*) FROM event_log").fetchone()[0] == 0
    finally:
        conn.close()


def test_init_db_migrates_v1_projection_positions(tmp_path):
    db_path = tmp_path / "legacy.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """CREATE TABLE projection_positions (
                   code TEXT PRIMARY KEY,
                   name TEXT NOT NULL,
                   style TEXT NOT NULL,
                   shares INTEGER NOT NULL,
                   avg_cost_cents INTEGER NOT NULL,
                   entry_date TEXT NOT NULL,
                   entry_day_low_cents INTEGER,
                   stop_loss_cents INTEGER,
                   take_profit_cents INTEGER,
                   highest_since_entry_cents INTEGER,
                   current_price_cents INTEGER,
                   unrealized_pnl_cents INTEGER,
                   updated_at TEXT NOT NULL
               )"""
        )
        conn.execute(
            "CREATE TABLE _schema_version (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT INTO _schema_version (version, applied_at) VALUES (1, '2026-01-01T00:00:00+00:00')"
        )
        conn.commit()
    finally:
        conn.close()

    init_db(db_path)

    conn = connect(db_path)
    try:
        version = get_schema_version(conn)
        assert version == 6

        columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(projection_positions)")
        }
        assert "currency" in columns
        assert "cost_basis_cents" in columns
        stream_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(event_streams)")
        }
        assert {"stream", "stream_type", "next_version", "updated_at"} <= stream_columns
        history_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(signal_history_snapshots)")
        }
        assert "history_group_id" in history_columns
    finally:
        conn.close()
