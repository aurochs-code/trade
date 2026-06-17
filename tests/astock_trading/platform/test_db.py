"""Tests for MySQL-only runtime database configuration."""

from __future__ import annotations

import pytest

from astock_trading.platform.database import DatabaseSettings, MissingDatabaseUrl


def test_runtime_db_requires_database_url(monkeypatch):
    monkeypatch.delenv("ASTOCK_DATABASE_URL", raising=False)

    with pytest.raises(MissingDatabaseUrl, match="ASTOCK_DATABASE_URL is required"):
        DatabaseSettings.from_env()


def test_runtime_db_rejects_non_mysql_url(monkeypatch):
    monkeypatch.setenv("ASTOCK_DATABASE_URL", "postgresql://user:pass@localhost/astock")

    with pytest.raises(MissingDatabaseUrl, match="mysql\\+pymysql"):
        DatabaseSettings.from_env()


def test_runtime_db_accepts_mysql_pymysql_url(monkeypatch):
    monkeypatch.setenv(
        "ASTOCK_DATABASE_URL",
        "mysql+pymysql://user:password@127.0.0.1:3306/a_stock_trading",
    )

    settings = DatabaseSettings.from_env()

    assert settings.url.startswith("mysql+pymysql://")


def test_backtest_persistence_tables_registered_in_mysql_metadata():
    from astock_trading.platform.schema import metadata

    assert {
        "backtest_runs",
        "backtest_trades",
        "backtest_equity_curve",
        "signal_history_discoveries",
    } <= set(metadata.tables)

    run_columns = set(metadata.tables["backtest_runs"].columns.keys())
    assert {
        "run_id",
        "preset",
        "codes_json",
        "start_date",
        "end_date",
        "metrics_json",
        "created_at",
    } <= run_columns

    trade_columns = set(metadata.tables["backtest_trades"].columns.keys())
    assert {
        "run_id",
        "trade_index",
        "trade_date",
        "code",
        "side",
        "price",
        "shares",
        "payload_json",
    } <= trade_columns

    equity_columns = set(metadata.tables["backtest_equity_curve"].columns.keys())
    assert {
        "run_id",
        "curve_index",
        "trade_date",
        "equity",
        "cash",
        "positions",
        "payload_json",
    } <= equity_columns

    discovery_columns = set(metadata.tables["signal_history_discoveries"].columns.keys())
    assert {
        "snapshot_date",
        "history_group_id",
        "code",
        "source",
        "run_id",
        "phase",
        "created_at",
    } <= discovery_columns
