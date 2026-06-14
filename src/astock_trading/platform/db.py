"""
platform/db.py — SQLite 连接管理 + schema migration

所有表按 PG 风格设计，金额字段用 _cents 整数。
WAL 模式启用，支持读写并发。
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from typing import Optional

from astock_trading.platform.database import Database, DatabaseSettings

_BASE_SCHEMA_VERSION = 1
_SCHEMA_VERSION = 7

# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """\
-- ═══════════════════════════════════════════════════════════════
-- 业务事实 (append-only)
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS event_log (
    event_id        TEXT PRIMARY KEY,
    stream          TEXT NOT NULL,
    stream_type     TEXT NOT NULL,
    stream_version  INTEGER NOT NULL,
    event_type      TEXT NOT NULL,
    payload_json    TEXT NOT NULL,
    metadata_json   TEXT NOT NULL DEFAULT '{}',
    occurred_at     TEXT NOT NULL,
    UNIQUE(stream, stream_version)
);

CREATE INDEX IF NOT EXISTS idx_event_log_type
    ON event_log(event_type);
CREATE INDEX IF NOT EXISTS idx_event_log_stream
    ON event_log(stream);
CREATE INDEX IF NOT EXISTS idx_event_log_occurred
    ON event_log(occurred_at);

CREATE TABLE IF NOT EXISTS event_streams (
    stream          TEXT PRIMARY KEY,
    stream_type     TEXT NOT NULL,
    next_version    INTEGER NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_event_streams_type
    ON event_streams(stream_type);
"""

_SCHEMA_SQL_2 = """\
-- ═══════════════════════════════════════════════════════════════
-- 规则版本
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS config_versions (
    config_version  TEXT PRIMARY KEY,
    config_hash     TEXT NOT NULL UNIQUE,
    config_json     TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    activated_at    TEXT
);

-- ═══════════════════════════════════════════════════════════════
-- 运行记录
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS run_log (
    run_id          TEXT PRIMARY KEY,
    run_type        TEXT NOT NULL,
    scope           TEXT NOT NULL DEFAULT 'cn_a',
    config_version  TEXT NOT NULL,
    data_cutoff     TEXT,
    status          TEXT NOT NULL DEFAULT 'running',
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    error_message   TEXT,
    artifacts_json  TEXT DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_run_log_type_date
    ON run_log(run_type, started_at);

-- ═══════════════════════════════════════════════════════════════
-- 市场观察
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS market_observations (
    observation_id  TEXT PRIMARY KEY,
    source          TEXT NOT NULL,
    kind            TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    observed_at     TEXT NOT NULL,
    run_id          TEXT,
    payload_json    TEXT NOT NULL,
    UNIQUE(source, kind, symbol, observed_at)
);

CREATE INDEX IF NOT EXISTS idx_market_obs_symbol
    ON market_observations(symbol, kind, observed_at);
CREATE INDEX IF NOT EXISTS idx_market_obs_kind_observed
    ON market_observations(kind, observed_at);

CREATE TABLE IF NOT EXISTS market_bars (
    symbol          TEXT NOT NULL,
    bar_date        TEXT NOT NULL,
    period          TEXT NOT NULL DEFAULT 'daily',
    open_cents      INTEGER NOT NULL,
    high_cents      INTEGER NOT NULL,
    low_cents       INTEGER NOT NULL,
    close_cents     INTEGER NOT NULL,
    volume          INTEGER NOT NULL,
    amount_cents    INTEGER NOT NULL,
    source          TEXT NOT NULL,
    fetched_at      TEXT NOT NULL,
    PRIMARY KEY (symbol, bar_date, period)
);
"""

_SCHEMA_SQL_3 = """\
-- ═══════════════════════════════════════════════════════════════
-- 投影表 (全部可删可重建)
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS projection_positions (
    code            TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    style           TEXT NOT NULL,
    shares          INTEGER NOT NULL,
    avg_cost_cents  INTEGER NOT NULL,
    cost_basis_cents INTEGER,
    entry_date      TEXT NOT NULL,
    entry_day_low_cents INTEGER,
    stop_loss_cents INTEGER,
    take_profit_cents INTEGER,
    highest_since_entry_cents INTEGER,
    current_price_cents INTEGER,
    unrealized_pnl_cents INTEGER,
    currency        TEXT NOT NULL DEFAULT 'CNY',
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projection_orders (
    order_id        TEXT PRIMARY KEY,
    code            TEXT NOT NULL,
    side            TEXT NOT NULL,
    shares          INTEGER NOT NULL,
    price_cents     INTEGER NOT NULL,
    status          TEXT NOT NULL,
    broker          TEXT,
    created_at      TEXT NOT NULL,
    filled_at       TEXT,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projection_balances (
    scope           TEXT PRIMARY KEY,
    cash_cents      INTEGER NOT NULL,
    total_asset_cents INTEGER,  -- NULL: 待 rebuild_all() 从 cash+市值 重算
    weekly_buy_count INTEGER NOT NULL DEFAULT 0,
    daily_pnl_cents INTEGER NOT NULL DEFAULT 0,
    consecutive_loss_days INTEGER NOT NULL DEFAULT 0,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projection_candidate_pool (
    code            TEXT NOT NULL,
    pool_tier       TEXT NOT NULL,
    name            TEXT,
    score           REAL,
    added_at        TEXT NOT NULL,
    last_scored_at  TEXT,
    streak_days     INTEGER DEFAULT 0,
    note            TEXT,
    PRIMARY KEY (code, pool_tier)
);

CREATE TABLE IF NOT EXISTS projection_market_state (
    index_symbol    TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    signal          TEXT,
    price_cents     INTEGER,
    change_pct      REAL,
    ma20_pct        REAL,
    ma60_pct        REAL,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS report_artifacts (
    artifact_id     TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL,
    report_type     TEXT NOT NULL,
    format          TEXT NOT NULL,
    content         TEXT NOT NULL,
    delivered_to    TEXT,
    created_at      TEXT NOT NULL
);

-- ═══════════════════════════════════════════════════════════════
-- Schema 版本追踪
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS _schema_version (
    version     INTEGER PRIMARY KEY,
    applied_at  TEXT NOT NULL
);
"""

_SCHEMA_SQL_4 = """\
-- ═══════════════════════════════════════════════════════════════
-- 历史信号镜像
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS signal_history_snapshots (
    snapshot_id      TEXT PRIMARY KEY,
    snapshot_date    TEXT NOT NULL,
    history_group_id TEXT NOT NULL,
    run_id           TEXT NOT NULL,
    phase            TEXT NOT NULL,
    snapshot_type    TEXT NOT NULL,
    payload_json     TEXT NOT NULL,
    created_at       TEXT NOT NULL,
    UNIQUE(history_group_id, snapshot_type)
);

CREATE INDEX IF NOT EXISTS idx_signal_history_date
    ON signal_history_snapshots(snapshot_date, created_at);
CREATE INDEX IF NOT EXISTS idx_signal_history_group
    ON signal_history_snapshots(history_group_id);
"""

_SCHEMA_SQL_7 = """\
-- ═══════════════════════════════════════════════════════════════
-- 回测市场数据底座
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS market_price_bars (
    symbol          TEXT NOT NULL,
    bar_date        TEXT NOT NULL,
    period          TEXT NOT NULL DEFAULT 'daily',
    adjustflag      TEXT NOT NULL DEFAULT '2',
    source          TEXT NOT NULL,
    open_cents      INTEGER NOT NULL,
    high_cents      INTEGER NOT NULL,
    low_cents       INTEGER NOT NULL,
    close_cents     INTEGER NOT NULL,
    volume          INTEGER NOT NULL,
    amount_cents    INTEGER NOT NULL,
    change_pct      REAL,
    fetched_at      TEXT NOT NULL,
    raw_json        TEXT,
    PRIMARY KEY (symbol, bar_date, period, adjustflag, source)
);

CREATE INDEX IF NOT EXISTS idx_market_price_symbol_date
    ON market_price_bars(symbol, bar_date);
CREATE INDEX IF NOT EXISTS idx_market_price_date
    ON market_price_bars(bar_date);

CREATE TABLE IF NOT EXISTS market_financials (
    symbol              TEXT NOT NULL,
    report_year         INTEGER NOT NULL,
    report_quarter      INTEGER NOT NULL,
    source              TEXT NOT NULL,
    report_date         TEXT NOT NULL,
    available_date      TEXT NOT NULL,
    roe                 REAL,
    roe_3y_ago          REAL,
    revenue_growth      REAL,
    net_profit_growth   REAL,
    operating_cash_flow REAL,
    pe_ttm              REAL,
    pb                  REAL,
    debt_ratio          REAL,
    fetched_at          TEXT NOT NULL,
    raw_json            TEXT,
    PRIMARY KEY (symbol, report_year, report_quarter, source)
);

CREATE INDEX IF NOT EXISTS idx_market_financials_available
    ON market_financials(symbol, available_date);
CREATE INDEX IF NOT EXISTS idx_market_financials_report
    ON market_financials(symbol, report_year, report_quarter);

CREATE TABLE IF NOT EXISTS market_fund_flows (
    symbol                   TEXT NOT NULL,
    trade_date               TEXT NOT NULL,
    source                   TEXT NOT NULL,
    net_inflow_1d            REAL,
    net_inflow_5d            REAL,
    main_force_ratio         REAL,
    northbound_net           REAL,
    consecutive_outflow_days INTEGER,
    fetched_at               TEXT NOT NULL,
    raw_json                 TEXT,
    PRIMARY KEY (symbol, trade_date, source)
);

CREATE INDEX IF NOT EXISTS idx_market_fund_flows_symbol_date
    ON market_fund_flows(symbol, trade_date);

CREATE TABLE IF NOT EXISTS market_data_coverage (
    coverage_key TEXT PRIMARY KEY,
    domain       TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    start_date   TEXT,
    end_date     TEXT,
    period       TEXT,
    adjustflag   TEXT,
    source       TEXT NOT NULL,
    row_count    INTEGER NOT NULL DEFAULT 0,
    status       TEXT NOT NULL,
    fetched_at   TEXT NOT NULL,
    error_json   TEXT
);

CREATE INDEX IF NOT EXISTS idx_market_data_coverage_lookup
    ON market_data_coverage(domain, symbol, source);
"""


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

_DEFAULT_DB_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data"
_RUNTIME_DB: Database | None = None
_RUNTIME_DB_URL: str | None = None


def _default_db_path() -> Path:
    return _DEFAULT_DB_DIR / "astock_trading.db"


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


def _ensure_runtime_schema_version(conn) -> None:
    _ensure_schema_version_table(conn)
    current_version = get_schema_version(conn)
    if current_version == 0:
        _set_schema_version(conn, _BASE_SCHEMA_VERSION)
        current_version = _BASE_SCHEMA_VERSION
    current_version = _apply_migrations(conn, current_version)
    if current_version >= 5 and not _column_exists(conn, "projection_positions", "cost_basis_cents"):
        _migrate_to_v5(conn)
    if current_version >= 6 and not _index_exists(
        conn, "market_observations", "idx_market_obs_kind_observed"
    ):
        _migrate_to_v6(conn)
    if current_version < _SCHEMA_VERSION:
        _set_schema_version(conn, _SCHEMA_VERSION)


def connect(db_path: Optional[Path] = None):
    """Open a DB connection.

    Passing db_path opens SQLite for tests and one-time migration sources.
    Runtime code must omit db_path and configure ASTOCK_DATABASE_URL.
    """
    if db_path is None:
        conn = _runtime_database().connect()
        _ensure_runtime_schema_version(conn)
        return conn

    path = db_path
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(path), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_schema_version_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS _schema_version (
               version     INTEGER PRIMARY KEY,
               applied_at  TEXT NOT NULL
           )"""
    )


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    dialect = getattr(conn, "dialect", "")
    if str(dialect).startswith("mysql"):
        row = conn.execute(
            """SELECT COUNT(*)
               FROM information_schema.columns
               WHERE table_schema = DATABASE()
                 AND table_name = ?
                 AND column_name = ?""",
            (table, column),
        ).fetchone()
        return bool(row and row[0])
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row["name"] == column for row in rows)


def _index_exists(conn: sqlite3.Connection, table: str, index: str) -> bool:
    dialect = getattr(conn, "dialect", "")
    if str(dialect).startswith("mysql"):
        row = conn.execute(
            """SELECT COUNT(*)
               FROM information_schema.statistics
               WHERE table_schema = DATABASE()
                 AND table_name = ?
                 AND index_name = ?""",
            (table, index),
        ).fetchone()
        return bool(row and row[0])
    rows = conn.execute(f"PRAGMA index_list({table})").fetchall()
    return any(row["name"] == index for row in rows)


def _set_schema_version(conn: sqlite3.Connection, version: int) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO _schema_version (version, applied_at) VALUES (?, ?)",
        (version, _now_iso()),
    )


def _bootstrap_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(_SCHEMA_SQL)
    conn.executescript(_SCHEMA_SQL_2)
    conn.executescript(_SCHEMA_SQL_3)
    conn.executescript(_SCHEMA_SQL_4)
    conn.executescript(_SCHEMA_SQL_7)
    _ensure_schema_version_table(conn)


def _migrate_to_v2(conn: sqlite3.Connection) -> None:
    if not _column_exists(conn, "projection_positions", "currency"):
        conn.execute(
            "ALTER TABLE projection_positions "
            "ADD COLUMN currency TEXT NOT NULL DEFAULT 'CNY'"
        )


def _migrate_to_v3(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS event_streams (
               stream          TEXT PRIMARY KEY,
               stream_type     TEXT NOT NULL,
               next_version    INTEGER NOT NULL,
               updated_at      TEXT NOT NULL
           )"""
    )


def _migrate_to_v4(conn: sqlite3.Connection) -> None:
    conn.executescript(_SCHEMA_SQL_4)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_event_streams_type "
        "ON event_streams(stream_type)"
    )


def _migrate_to_v5(conn: sqlite3.Connection) -> None:
    if not _column_exists(conn, "projection_positions", "cost_basis_cents"):
        conn.execute(
            "ALTER TABLE projection_positions "
            "ADD COLUMN cost_basis_cents INTEGER"
        )
    conn.execute(
        """UPDATE projection_positions
           SET cost_basis_cents = avg_cost_cents * shares
           WHERE cost_basis_cents IS NULL OR cost_basis_cents = 0"""
    )


def _migrate_to_v6(conn: sqlite3.Connection) -> None:
    if not _index_exists(conn, "market_observations", "idx_market_obs_kind_observed"):
        conn.execute(
            "CREATE INDEX idx_market_obs_kind_observed "
            "ON market_observations(kind, observed_at)"
        )


def _migrate_to_v7(conn: sqlite3.Connection) -> None:
    if str(getattr(conn, "dialect", "")).startswith("mysql"):
        return
    conn.executescript(_SCHEMA_SQL_7)


_MIGRATIONS: dict[int, Callable[[sqlite3.Connection], None]] = {
    2: _migrate_to_v2,
    3: _migrate_to_v3,
    4: _migrate_to_v4,
    5: _migrate_to_v5,
    6: _migrate_to_v6,
    7: _migrate_to_v7,
}


def _apply_migrations(conn: sqlite3.Connection, current_version: int) -> int:
    """Apply incremental migrations and return the final schema version."""
    version = current_version
    for target_version in sorted(_MIGRATIONS):
        if target_version <= version:
            continue
        _MIGRATIONS[target_version](conn)
        _set_schema_version(conn, target_version)
        version = target_version
    return version


def init_db(db_path: Optional[Path] = None):
    """Create all tables if they don't exist. Returns the db path or runtime URL."""
    if db_path is None:
        db = _runtime_database()
        db.create_schema()
        conn = db.connect()
        try:
            _ensure_runtime_schema_version(conn)
            return db.settings.url
        finally:
            conn.close()

    path = db_path
    conn = connect(path)
    try:
        _bootstrap_schema(conn)

        current_version = get_schema_version(conn)
        if current_version == 0:
            _set_schema_version(conn, _BASE_SCHEMA_VERSION)
            current_version = _BASE_SCHEMA_VERSION

        current_version = _apply_migrations(conn, current_version)
        if current_version >= 5 and not _column_exists(conn, "projection_positions", "cost_basis_cents"):
            _migrate_to_v5(conn)
        if current_version >= 6 and not _index_exists(
            conn, "market_observations", "idx_market_obs_kind_observed"
        ):
            _migrate_to_v6(conn)
        if current_version < _SCHEMA_VERSION:
            _set_schema_version(conn, _SCHEMA_VERSION)
        return path
    finally:
        conn.close()


def get_schema_version(conn) -> int:
    """Return current schema version, or 0 if not initialized."""
    try:
        row = conn.execute(
            "SELECT version FROM _schema_version ORDER BY version DESC LIMIT 1"
        ).fetchone()
        return row[0] if row else 0
    except (sqlite3.OperationalError, Exception):
        return 0
