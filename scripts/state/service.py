#!/usr/bin/env python3
"""
Structured ledger-backed state service.

This module provides the single structured truth used by automation while
Obsidian remains the projection and review surface.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

try:
    import yaml
except ImportError:  # pragma: no cover - same behavior as config_loader
    yaml = None

from scripts.utils.config_loader import clear_config_cache, get_stocks, get_strategy
from scripts.utils.cache import load_json_cache, save_json_cache
from scripts.utils.logger import get_logger
from scripts.utils.obsidian import ObsidianVault
from scripts.utils.parser import parse_md_table, parse_user_reply
from scripts.mx.mx_moni import MXMoni
from scripts.state.reason_codes import normalize_reason_code

LOGGER = get_logger("state.service")
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LEDGER_DIR = PROJECT_ROOT / "data" / "ledger"
LEDGER_DB_PATH = LEDGER_DIR / "trade_state.sqlite3"
DB_ENV = "TRADE_STATE_DB_PATH"

AUTOMATED_RULES = [
    {"name": "dynamic_stop_loss", "mode": "automated"},
    {"name": "absolute_stop_loss", "mode": "automated"},
    {"name": "take_profit_batch_1", "mode": "automated"},
    {"name": "weekly_buy_limit", "mode": "automated"},
    {"name": "time_stop", "mode": "advisory"},
    {"name": "drawdown_take_profit", "mode": "advisory"},
]

PRIMARY_SCOPE = "cn_a_system"
SECONDARY_SCOPE = "hk_legacy"
PAPER_SCOPE = "paper_mx"
ALL_SCOPES = (PRIMARY_SCOPE, SECONDARY_SCOPE, PAPER_SCOPE)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS state_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS portfolio_positions (
  scope TEXT NOT NULL,
  code TEXT NOT NULL,
  name TEXT NOT NULL,
  market TEXT DEFAULT '',
  shares INTEGER NOT NULL DEFAULT 0,
  avg_cost REAL NOT NULL DEFAULT 0,
  current_price REAL NOT NULL DEFAULT 0,
  market_value REAL NOT NULL DEFAULT 0,
  status TEXT DEFAULT '',
  note TEXT DEFAULT '',
  source TEXT DEFAULT '',
  as_of_date TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  metadata_json TEXT DEFAULT '{}',
  PRIMARY KEY(scope, code)
);

CREATE TABLE IF NOT EXISTS portfolio_balances (
  scope TEXT PRIMARY KEY,
  cash_value REAL NOT NULL DEFAULT 0,
  total_capital REAL NOT NULL DEFAULT 0,
  total_market_value REAL NOT NULL DEFAULT 0,
  exposure REAL NOT NULL DEFAULT 0,
  source TEXT DEFAULT '',
  as_of_date TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  metadata_json TEXT DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS trade_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  external_id TEXT UNIQUE,
  scope TEXT NOT NULL,
  market TEXT DEFAULT '',
  code TEXT DEFAULT '',
  name TEXT DEFAULT '',
  side TEXT DEFAULT '',
  event_type TEXT DEFAULT '',
  shares INTEGER NOT NULL DEFAULT 0,
  price REAL NOT NULL DEFAULT 0,
  amount REAL NOT NULL DEFAULT 0,
  realized_pnl REAL NOT NULL DEFAULT 0,
  event_date TEXT NOT NULL,
  reason_code TEXT DEFAULT '',
  reason_text TEXT DEFAULT '',
  source TEXT DEFAULT '',
  metadata_json TEXT DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  external_id TEXT UNIQUE,
  scope TEXT NOT NULL,
  broker TEXT DEFAULT '',
  broker_order_id TEXT DEFAULT '',
  code TEXT DEFAULT '',
  name TEXT DEFAULT '',
  side TEXT DEFAULT '',
  order_class TEXT DEFAULT '',
  order_type TEXT DEFAULT '',
  condition_type TEXT DEFAULT '',
  requested_shares INTEGER NOT NULL DEFAULT 0,
  filled_shares INTEGER NOT NULL DEFAULT 0,
  trigger_price REAL NOT NULL DEFAULT 0,
  limit_price REAL NOT NULL DEFAULT 0,
  avg_fill_price REAL NOT NULL DEFAULT 0,
  status TEXT DEFAULT '',
  confirm_status TEXT DEFAULT '',
  reason_code TEXT DEFAULT '',
  reason_text TEXT DEFAULT '',
  source TEXT DEFAULT '',
  placed_at TEXT DEFAULT '',
  triggered_at TEXT DEFAULT '',
  filled_at TEXT DEFAULT '',
  cancelled_at TEXT DEFAULT '',
  confirmed_at TEXT DEFAULT '',
  updated_at TEXT NOT NULL,
  metadata_json TEXT DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS pool_entries (
  code TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  bucket TEXT NOT NULL,
  total_score REAL NOT NULL DEFAULT 0,
  technical_score REAL NOT NULL DEFAULT 0,
  fundamental_score REAL NOT NULL DEFAULT 0,
  flow_score REAL NOT NULL DEFAULT 0,
  sentiment_score REAL NOT NULL DEFAULT 0,
  veto_triggered INTEGER NOT NULL DEFAULT 0,
  veto_signals_json TEXT DEFAULT '[]',
  passed_text TEXT DEFAULT '',
  note TEXT DEFAULT '',
  source TEXT DEFAULT '',
  added_date TEXT DEFAULT '',
  snapshot_date TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  metadata_json TEXT DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS pool_actions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  snapshot_date TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  code TEXT NOT NULL,
  name TEXT DEFAULT '',
  action TEXT NOT NULL,
  previous_bucket TEXT DEFAULT '',
  current_bucket TEXT DEFAULT '',
  source TEXT DEFAULT '',
  reason_text TEXT DEFAULT '',
  metadata_json TEXT DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS market_snapshots (
  id INTEGER PRIMARY KEY CHECK(id = 1),
  signal TEXT NOT NULL,
  source TEXT DEFAULT '',
  source_chain_json TEXT DEFAULT '[]',
  as_of_date TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  detail_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS market_snapshot_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  history_group_id TEXT DEFAULT '',
  pipeline TEXT DEFAULT '',
  snapshot_date TEXT NOT NULL,
  as_of_date TEXT NOT NULL,
  signal TEXT NOT NULL,
  source TEXT DEFAULT '',
  source_chain_json TEXT DEFAULT '[]',
  updated_at TEXT NOT NULL,
  detail_json TEXT NOT NULL,
  metadata_json TEXT DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS alert_snapshots (
  id INTEGER PRIMARY KEY CHECK(id = 1),
  snapshot_date TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  status TEXT NOT NULL,
  alert_count INTEGER NOT NULL DEFAULT 0,
  level_counts_json TEXT NOT NULL DEFAULT '{}',
  code_counts_json TEXT NOT NULL DEFAULT '{}',
  ack_counts_json TEXT NOT NULL DEFAULT '{}',
  detail_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS decision_snapshot_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  history_group_id TEXT DEFAULT '',
  pipeline TEXT DEFAULT '',
  snapshot_date TEXT NOT NULL,
  decision_action TEXT DEFAULT '',
  market_signal TEXT DEFAULT '',
  updated_at TEXT NOT NULL,
  detail_json TEXT NOT NULL,
  metadata_json TEXT DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS candidate_snapshot_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  history_group_id TEXT DEFAULT '',
  pipeline TEXT DEFAULT '',
  snapshot_date TEXT NOT NULL,
  pool TEXT DEFAULT '',
  universe TEXT DEFAULT '',
  source TEXT DEFAULT '',
  candidate_count INTEGER NOT NULL DEFAULT 0,
  actionable_count INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL,
  metadata_json TEXT DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS candidate_snapshot_entries (
  snapshot_id INTEGER NOT NULL,
  rank INTEGER NOT NULL DEFAULT 0,
  code TEXT NOT NULL,
  name TEXT DEFAULT '',
  total_score REAL NOT NULL DEFAULT 0,
  technical_score REAL NOT NULL DEFAULT 0,
  fundamental_score REAL NOT NULL DEFAULT 0,
  flow_score REAL NOT NULL DEFAULT 0,
  sentiment_score REAL NOT NULL DEFAULT 0,
  veto_triggered INTEGER NOT NULL DEFAULT 0,
  veto_signals_json TEXT DEFAULT '[]',
  passed_text TEXT DEFAULT '',
  recommendation TEXT DEFAULT '',
  bucket TEXT DEFAULT '',
  data_quality TEXT DEFAULT '',
  note TEXT DEFAULT '',
  source TEXT DEFAULT '',
  detail_json TEXT NOT NULL DEFAULT '{}',
  PRIMARY KEY(snapshot_id, code)
);

CREATE TABLE IF NOT EXISTS pool_snapshot_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  history_group_id TEXT DEFAULT '',
  pipeline TEXT DEFAULT '',
  snapshot_date TEXT NOT NULL,
  source TEXT DEFAULT '',
  updated_at TEXT NOT NULL,
  summary_json TEXT NOT NULL DEFAULT '{}',
  metadata_json TEXT DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS pool_snapshot_history_entries (
  snapshot_id INTEGER NOT NULL,
  code TEXT NOT NULL,
  name TEXT NOT NULL,
  bucket TEXT NOT NULL,
  total_score REAL NOT NULL DEFAULT 0,
  technical_score REAL NOT NULL DEFAULT 0,
  fundamental_score REAL NOT NULL DEFAULT 0,
  flow_score REAL NOT NULL DEFAULT 0,
  sentiment_score REAL NOT NULL DEFAULT 0,
  veto_triggered INTEGER NOT NULL DEFAULT 0,
  veto_signals_json TEXT DEFAULT '[]',
  passed_text TEXT DEFAULT '',
  note TEXT DEFAULT '',
  source TEXT DEFAULT '',
  added_date TEXT DEFAULT '',
  entry_snapshot_date TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  data_quality TEXT DEFAULT '',
  detail_json TEXT NOT NULL DEFAULT '{}',
  PRIMARY KEY(snapshot_id, code)
);

CREATE INDEX IF NOT EXISTS idx_market_snapshot_history_lookup
  ON market_snapshot_history(snapshot_date, history_group_id, updated_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_decision_snapshot_history_lookup
  ON decision_snapshot_history(snapshot_date, history_group_id, updated_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_candidate_snapshot_history_lookup
  ON candidate_snapshot_history(snapshot_date, history_group_id, updated_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_candidate_snapshot_entries_lookup
  ON candidate_snapshot_entries(snapshot_id, rank, total_score DESC, code);

CREATE INDEX IF NOT EXISTS idx_pool_snapshot_history_lookup
  ON pool_snapshot_history(snapshot_date, history_group_id, updated_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_pool_snapshot_history_entries_lookup
  ON pool_snapshot_history_entries(snapshot_id, bucket, total_score DESC, code);
"""


def _now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _today_str() -> str:
    return date.today().isoformat()


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _json_loads(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except Exception:
        return default


def _db_path() -> Path:
    configured = os.environ.get(DB_ENV, "").strip()
    return Path(configured) if configured else LEDGER_DB_PATH


@contextmanager
def _connect():
    db_path = _db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _meta_get(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value FROM state_meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def _meta_set(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO state_meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = str(value).strip()
    if not cleaned:
        return default
    negative = "亏" in cleaned and "-" not in cleaned
    cleaned = cleaned.replace(",", "")
    cleaned = re.sub(r"[^\d.\-]", "", cleaned)
    if cleaned in ("", "-", ".", "-."):
        return default
    try:
        number = float(cleaned)
    except ValueError:
        return default
    return -abs(number) if negative else number


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _dedupe(values: list[str]) -> list[str]:
    seen = set()
    result = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _extract_realized_pnl(value: Any, default: float = 0.0) -> float:
    text = str(value or "").strip()
    if not text:
        return default

    loss_match = re.search(r"亏[^0-9\-]*([0-9][0-9,]*(?:\.\d+)?)", text)
    if loss_match:
        return -abs(_safe_float(loss_match.group(1), default))

    gain_match = re.search(r"(?:盈|赚)[^0-9\-]*([0-9][0-9,]*(?:\.\d+)?)", text)
    if gain_match:
        return abs(_safe_float(gain_match.group(1), default))

    pnl_match = re.search(r"(?:pnl|profit)[^0-9\-]*([-+]?[0-9][0-9,]*(?:\.\d+)?)", text, re.IGNORECASE)
    if pnl_match:
        return _safe_float(pnl_match.group(1), default)

    return default


def _normalize_code(value: Any) -> str:
    if value is None:
        return ""
    code = str(value).strip()
    if not code:
        return ""
    if code.isdigit():
        return code.zfill(6)
    numeric = re.sub(r"\D", "", code)
    if numeric and len(numeric) <= 6 and numeric == code.replace(".", "").replace("HK", ""):
        return numeric.zfill(6)
    return code


def _parse_mmdd_date(value: str, fallback_year: int) -> str:
    raw = str(value).strip()
    if not raw:
        return _today_str()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        return raw
    if re.match(r"^\d{2}-\d{2}$", raw):
        return f"{fallback_year}-{raw}"
    return raw


def _parse_stock_cell(cell: str) -> tuple[str, str]:
    raw = str(cell).strip()
    if not raw:
        return "", ""
    match = re.match(r"^(.*?)\(([\dA-Za-z.]+)\)$", raw)
    if match:
        return match.group(1).strip(), _normalize_code(match.group(2))
    return raw, ""


def _scope_from_record(market: str, record_type: str = "", note: str = "") -> str:
    combined = " ".join(str(item) for item in (market, record_type, note))
    if "模拟盘" in combined or "paper" in combined.lower():
        return PAPER_SCOPE
    if "港股" in combined or "遗留" in combined:
        return SECONDARY_SCOPE
    return PRIMARY_SCOPE


def _reason_code_from_text(side: str, reason: str, scope: str) -> str:
    text = str(reason or "")
    if "绝对止损" in text:
        return "absolute_stop_loss"
    if "动态止损" in text or ("止损" in text and "绝对" not in text):
        return "dynamic_stop_loss"
    if "止盈第一批" in text or "止盈1" in text:
        return "take_profit_batch_1"
    if "时间止损" in text:
        return "time_stop_advisory"
    if scope == PAPER_SCOPE and side == "buy":
        return "paper_buy"
    if scope == PAPER_SCOPE and side == "sell":
        return "paper_sell"
    if side == "buy":
        return "manual_buy"
    if side == "sell":
        return "manual_sell"
    return "manual_event"


def _recommendation_for_entry(entry: dict) -> str:
    if entry.get("veto_triggered"):
        return "❌"
    score = float(entry.get("total_score", 0) or 0)
    if score >= 7:
        return "✅"
    if score >= 5:
        return "🟡"
    return "❌"


def _load_md_rows(relative_path: str) -> list[dict]:
    vault = ObsidianVault()
    full_path = Path(vault.vault_path) / relative_path
    if not full_path.exists():
        return []
    content = full_path.read_text(encoding="utf-8")
    tables = parse_md_table(content)
    if not tables:
        return []
    return tables[0].get("rows", [])


def _ensure_bootstrapped(conn: sqlite3.Connection) -> None:
    if _meta_get(conn, "bootstrap_completed") == "1":
        return
    bootstrap_state(force=False)


def _write_balances(conn: sqlite3.Connection, balances: Iterable[dict]) -> None:
    conn.execute("DELETE FROM portfolio_balances")
    for balance in balances:
        conn.execute(
            """
            INSERT INTO portfolio_balances(
              scope, cash_value, total_capital, total_market_value, exposure,
              source, as_of_date, updated_at, metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                balance["scope"],
                balance.get("cash_value", 0.0),
                balance.get("total_capital", 0.0),
                balance.get("total_market_value", 0.0),
                balance.get("exposure", 0.0),
                balance.get("source", "bootstrap"),
                balance.get("as_of_date", _today_str()),
                balance.get("updated_at", _now_ts()),
                _json_dumps(balance.get("metadata", {})),
            ),
        )


def _write_positions(conn: sqlite3.Connection, positions: Iterable[dict]) -> None:
    conn.execute("DELETE FROM portfolio_positions")
    for position in positions:
        conn.execute(
            """
            INSERT INTO portfolio_positions(
              scope, code, name, market, shares, avg_cost, current_price, market_value,
              status, note, source, as_of_date, updated_at, metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                position["scope"],
                position["code"],
                position["name"],
                position.get("market", ""),
                position.get("shares", 0),
                position.get("avg_cost", 0.0),
                position.get("current_price", 0.0),
                position.get("market_value", 0.0),
                position.get("status", ""),
                position.get("note", ""),
                position.get("source", "bootstrap"),
                position.get("as_of_date", _today_str()),
                position.get("updated_at", _now_ts()),
                _json_dumps(position.get("metadata", {})),
            ),
        )


def _replace_portfolio_scope_snapshot(
    conn: sqlite3.Connection,
    scope: str,
    positions: Iterable[dict],
    balances: Iterable[dict],
) -> None:
    conn.execute("DELETE FROM portfolio_positions WHERE scope = ?", (scope,))
    conn.execute("DELETE FROM portfolio_balances WHERE scope = ?", (scope,))
    for position in positions:
        conn.execute(
            """
            INSERT INTO portfolio_positions(
              scope, code, name, market, shares, avg_cost, current_price, market_value,
              status, note, source, as_of_date, updated_at, metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                position["scope"],
                position["code"],
                position["name"],
                position.get("market", ""),
                position.get("shares", 0),
                position.get("avg_cost", 0.0),
                position.get("current_price", 0.0),
                position.get("market_value", 0.0),
                position.get("status", ""),
                position.get("note", ""),
                position.get("source", "bootstrap"),
                position.get("as_of_date", _today_str()),
                position.get("updated_at", _now_ts()),
                _json_dumps(position.get("metadata", {})),
            ),
        )
    for balance in balances:
        conn.execute(
            """
            INSERT INTO portfolio_balances(
              scope, cash_value, total_capital, total_market_value, exposure,
              source, as_of_date, updated_at, metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                balance["scope"],
                balance.get("cash_value", 0.0),
                balance.get("total_capital", 0.0),
                balance.get("total_market_value", 0.0),
                balance.get("exposure", 0.0),
                balance.get("source", "bootstrap"),
                balance.get("as_of_date", _today_str()),
                balance.get("updated_at", _now_ts()),
                _json_dumps(balance.get("metadata", {})),
            ),
        )


def _replace_trade_events(conn: sqlite3.Connection, events: Iterable[dict]) -> None:
    conn.execute("DELETE FROM trade_events")
    for event in events:
        _upsert_trade_event(conn, event)


def _upsert_trade_event(conn: sqlite3.Connection, event: dict) -> None:
    conn.execute(
        """
        INSERT INTO trade_events(
          external_id, scope, market, code, name, side, event_type, shares, price,
          amount, realized_pnl, event_date, reason_code, reason_text, source,
          metadata_json, created_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(external_id) DO UPDATE SET
          scope = excluded.scope,
          market = excluded.market,
          code = excluded.code,
          name = excluded.name,
          side = excluded.side,
          event_type = excluded.event_type,
          shares = excluded.shares,
          price = excluded.price,
          amount = excluded.amount,
          realized_pnl = excluded.realized_pnl,
          event_date = excluded.event_date,
          reason_code = excluded.reason_code,
          reason_text = excluded.reason_text,
          source = excluded.source,
          metadata_json = excluded.metadata_json,
          created_at = excluded.created_at
        """,
        (
            event.get("external_id"),
            event.get("scope", PRIMARY_SCOPE),
            event.get("market", ""),
            event.get("code", ""),
            event.get("name", ""),
            event.get("side", ""),
            event.get("event_type", event.get("side", "")),
            event.get("shares", 0),
            event.get("price", 0.0),
            event.get("amount", 0.0),
            event.get("realized_pnl", 0.0),
            event.get("event_date", _today_str()),
            event.get("reason_code", ""),
            event.get("reason_text", ""),
            event.get("source", "bootstrap"),
            _json_dumps(event.get("metadata", {})),
            event.get("created_at", _now_ts()),
        ),
    )


_ORDER_TERMINAL_STATUSES = {"filled", "cancelled", "reviewed", "exception"}
_ORDER_PENDING_STATUSES = {"candidate", "pending", "confirm_pending"}
_ORDER_OPEN_STATUSES = {"placed", "partially_filled", "cancel_requested", "triggered", "cancel_replace_pending"}
_ORDER_EXCEPTION_STATUSES = {"exception", "rejected", "failed", "cancel_failed"}
_ORDER_REVIEW_QUEUE_STATUSES = {"review_required", "review_pending"}


def _normalize_order_payload(order: dict, existing: dict | None = None) -> dict:
    existing = existing or {}
    metadata = existing.get("metadata", {})
    payload_metadata = order.get("metadata", {})
    if isinstance(metadata, dict) and isinstance(payload_metadata, dict):
        metadata = {**metadata, **payload_metadata}
    elif isinstance(payload_metadata, dict):
        metadata = payload_metadata

    external_id = str(
        order.get("external_id")
        or order.get("broker_order_id")
        or order.get("client_order_id")
        or existing.get("external_id", "")
    ).strip()
    if not external_id:
        raise ValueError("order requires external_id or broker_order_id")

    scope = str(order.get("scope", existing.get("scope", PRIMARY_SCOPE))).strip() or PRIMARY_SCOPE
    updated_at = str(order.get("updated_at", existing.get("updated_at", _now_ts()))).strip() or _now_ts()

    def pick(key: str, default: Any = "") -> Any:
        value = order.get(key, existing.get(key, default))
        if value in (None, "") and key in existing:
            return existing.get(key, default)
        return value if value is not None else default

    return {
        "external_id": external_id,
        "scope": scope,
        "broker": str(pick("broker", "")).strip(),
        "broker_order_id": str(pick("broker_order_id", order.get("external_id", ""))).strip(),
        "code": _normalize_code(pick("code", "")),
        "name": str(pick("name", "")).strip(),
        "side": str(pick("side", "")).strip(),
        "order_class": str(pick("order_class", "")).strip(),
        "order_type": str(pick("order_type", "")).strip(),
        "condition_type": str(pick("condition_type", "")).strip(),
        "requested_shares": _safe_int(pick("requested_shares", existing.get("requested_shares", 0))),
        "filled_shares": _safe_int(pick("filled_shares", existing.get("filled_shares", 0))),
        "trigger_price": _safe_float(pick("trigger_price", existing.get("trigger_price", 0.0))),
        "limit_price": _safe_float(pick("limit_price", existing.get("limit_price", 0.0))),
        "avg_fill_price": _safe_float(pick("avg_fill_price", existing.get("avg_fill_price", 0.0))),
        "status": str(pick("status", existing.get("status", "candidate"))).strip() or "candidate",
        "confirm_status": str(pick("confirm_status", existing.get("confirm_status", "not_required"))).strip() or "not_required",
        "reason_code": str(pick("reason_code", "")).strip(),
        "reason_text": str(pick("reason_text", "")).strip(),
        "source": str(pick("source", "runtime")).strip() or "runtime",
        "placed_at": str(pick("placed_at", existing.get("placed_at", updated_at))).strip(),
        "triggered_at": str(pick("triggered_at", existing.get("triggered_at", ""))).strip(),
        "filled_at": str(pick("filled_at", existing.get("filled_at", ""))).strip(),
        "cancelled_at": str(pick("cancelled_at", existing.get("cancelled_at", ""))).strip(),
        "confirmed_at": str(pick("confirmed_at", existing.get("confirmed_at", ""))).strip(),
        "updated_at": updated_at,
        "metadata": metadata,
    }


def _load_order_row(conn: sqlite3.Connection, external_id: str) -> dict | None:
    row = conn.execute("SELECT * FROM orders WHERE external_id = ?", (external_id,)).fetchone()
    if not row:
        return None
    order = dict(row)
    order["metadata"] = _json_loads(order.pop("metadata_json", "{}"), {})
    return order


def _order_rows(scope: str | None = None, status: str | None = None, conn: sqlite3.Connection | None = None) -> list[dict]:
    close_after = conn is None
    if close_after:
        context = _connect()
        conn = context.__enter__()
    try:
        query = "SELECT * FROM orders"
        params: list[Any] = []
        clauses = []
        if scope:
            clauses.append("scope = ?")
            params.append(scope)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY updated_at DESC, id DESC"
        rows = conn.execute(query, params).fetchall()
        orders = []
        for row in rows:
            order = dict(row)
            order["metadata"] = _json_loads(order.pop("metadata_json", "{}"), {})
            orders.append(order)
        return orders
    finally:
        if close_after:
            context.__exit__(None, None, None)


def upsert_order_state(order: dict, conn: sqlite3.Connection | None = None) -> dict:
    """Insert or update a structured order row without touching trade events."""
    own_conn = conn is None
    if own_conn:
        context = _connect()
        conn = context.__enter__()
    try:
        external_id = str(
            order.get("external_id")
            or order.get("broker_order_id")
            or order.get("client_order_id")
            or ""
        ).strip()
        existing = _load_order_row(conn, external_id) if external_id else None
        payload = _normalize_order_payload(order, existing)
        conn.execute(
            """
            INSERT INTO orders(
              external_id, scope, broker, broker_order_id, code, name, side, order_class,
              order_type, condition_type, requested_shares, filled_shares, trigger_price,
              limit_price, avg_fill_price, status, confirm_status, reason_code, reason_text,
              source, placed_at, triggered_at, filled_at, cancelled_at, confirmed_at,
              updated_at, metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(external_id) DO UPDATE SET
              scope = excluded.scope,
              broker = excluded.broker,
              broker_order_id = excluded.broker_order_id,
              code = excluded.code,
              name = excluded.name,
              side = excluded.side,
              order_class = excluded.order_class,
              order_type = excluded.order_type,
              condition_type = excluded.condition_type,
              requested_shares = excluded.requested_shares,
              filled_shares = excluded.filled_shares,
              trigger_price = excluded.trigger_price,
              limit_price = excluded.limit_price,
              avg_fill_price = excluded.avg_fill_price,
              status = excluded.status,
              confirm_status = excluded.confirm_status,
              reason_code = excluded.reason_code,
              reason_text = excluded.reason_text,
              source = excluded.source,
              placed_at = excluded.placed_at,
              triggered_at = excluded.triggered_at,
              filled_at = excluded.filled_at,
              cancelled_at = excluded.cancelled_at,
              confirmed_at = excluded.confirmed_at,
              updated_at = excluded.updated_at,
              metadata_json = excluded.metadata_json
            """,
            (
                payload["external_id"],
                payload["scope"],
                payload["broker"],
                payload["broker_order_id"],
                payload["code"],
                payload["name"],
                payload["side"],
                payload["order_class"],
                payload["order_type"],
                payload["condition_type"],
                payload["requested_shares"],
                payload["filled_shares"],
                payload["trigger_price"],
                payload["limit_price"],
                payload["avg_fill_price"],
                payload["status"],
                payload["confirm_status"],
                payload["reason_code"],
                payload["reason_text"],
                payload["source"],
                payload["placed_at"],
                payload["triggered_at"],
                payload["filled_at"],
                payload["cancelled_at"],
                payload["confirmed_at"],
                payload["updated_at"],
                _json_dumps(payload["metadata"]),
            ),
        )
        return {**payload, "db_path": str(_db_path())}
    finally:
        if own_conn:
            context.__exit__(None, None, None)


def _project_stocks_yaml(snapshot: dict) -> str:
    if yaml is None:
        raise ImportError("PyYAML not installed")

    target = {
        "core_pool": [],
        "watch_pool": [],
        "blacklist": get_stocks().get("blacklist", {"permanent": [], "temporary": []}),
    }
    snapshot_date = snapshot.get("snapshot_date", _today_str())
    for bucket_key, bucket_name in (("core_pool", "core_pool"), ("watch_pool", "watch_pool")):
        entries = snapshot.get(bucket_name, [])
        for entry in entries:
            target[bucket_key].append(
                {
                    "code": str(entry.get("code", "")),
                    "name": entry.get("name", ""),
                    "added": entry.get("added_date") or snapshot_date,
                    "score": round(float(entry.get("total_score", 0) or 0), 1),
                }
            )

    config_path = PROJECT_ROOT / "config" / "stocks.yaml"
    with open(config_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(target, f, allow_unicode=True, sort_keys=False)
    clear_config_cache("stocks")
    return str(config_path)


def _render_pool_markdown(title: str, tags: list[str], entries: list[dict], snapshot_date: str) -> str:
    lines = [
        "---",
        f"date: {snapshot_date}",
        f"type: {'watchlist_core' if '核心池' in title else 'watchlist_observe'}",
        f"tags: [{', '.join(tags)}]",
        f"updated_at: {snapshot_date}",
        "---",
        "",
        f"# {title}",
        "",
        "> 自动投影自结构化账本（trade_state.sqlite3）",
        "",
        "| # | 股票 | 代码 | 四维总分 | 技术 | 基本面 | 资金 | 舆情 | 通过 | 备注 |",
        "|---|------|------|---------|------|--------|------|------|------|------|",
    ]
    if entries:
        for idx, entry in enumerate(entries, 1):
            lines.append(
                f"| {idx} | {entry.get('name', '')} | {entry.get('code', '')} | "
                f"{float(entry.get('total_score', 0) or 0):.1f} | "
                f"{float(entry.get('technical_score', 0) or 0):.1f} | "
                f"{float(entry.get('fundamental_score', 0) or 0):.1f} | "
                f"{float(entry.get('flow_score', 0) or 0):.1f} | "
                f"{float(entry.get('sentiment_score', 0) or 0):.1f} | "
                f"{entry.get('passed_text') or _recommendation_for_entry(entry)} | "
                f"{entry.get('note', '')} |"
            )
    else:
        lines.append("| — | — | — | — | — | — | — | — | — | 暂无 |")
    return "\n".join(lines) + "\n"


def _project_pool_markdown(snapshot: dict) -> dict:
    vault = ObsidianVault()
    snapshot_date = snapshot.get("snapshot_date", _today_str())

    core_content = _render_pool_markdown("核心池", ["核心池", "选股"], snapshot.get("core_pool", []), snapshot_date)
    watch_content = _render_pool_markdown("观察池", ["观察池", "选股"], snapshot.get("watch_pool", []), snapshot_date)

    core_path = Path(vault.vault_path) / "04-选股" / "核心池.md"
    watch_path = Path(vault.vault_path) / "04-选股" / "观察池.md"
    core_path.parent.mkdir(parents=True, exist_ok=True)
    watch_path.parent.mkdir(parents=True, exist_ok=True)
    core_path.write_text(core_content, encoding="utf-8")
    watch_path.write_text(watch_content, encoding="utf-8")
    return {"core_pool_path": str(core_path), "watch_pool_path": str(watch_path)}


def _bootstrap_pool_entries() -> list[dict]:
    stocks_cfg = get_stocks()
    core_rows = {str(row.get("代码", "")).strip(): row for row in _load_md_rows("04-选股/核心池.md")}
    watch_rows = {str(row.get("代码", "")).strip(): row for row in _load_md_rows("04-选股/观察池.md")}
    snapshot_date = _today_str()
    entries = []

    for bucket, items, md_rows in (
        ("core", stocks_cfg.get("core_pool", []), core_rows),
        ("watch", stocks_cfg.get("watch_pool", []), watch_rows),
    ):
        for item in items:
            code = _normalize_code(item.get("code", ""))
            if not code:
                continue
            md_row = md_rows.get(code, {})
            total_score = _safe_float(md_row.get("四维总分", item.get("score", 0)))
            entry = {
                "bucket": bucket,
                "code": code,
                "name": item.get("name", md_row.get("股票", code)),
                "total_score": round(total_score, 1),
                "technical_score": _safe_float(md_row.get("技术", 0)),
                "fundamental_score": _safe_float(md_row.get("基本面", 0)),
                "flow_score": _safe_float(md_row.get("资金", md_row.get("主力", 0))),
                "sentiment_score": _safe_float(md_row.get("舆情", 0)),
                "veto_triggered": False,
                "veto_signals": [],
                "passed_text": str(md_row.get("通过", _recommendation_for_entry({"total_score": total_score}))),
                "note": str(md_row.get("备注", "")).strip(),
                "source": "bootstrap",
                "added_date": item.get("added", snapshot_date),
                "snapshot_date": snapshot_date,
                "updated_at": _now_ts(),
                "metadata": {"bootstrap_source": "config+obsidian"},
            }
            entries.append(entry)
    return entries


def _bootstrap_portfolio_snapshot() -> tuple[list[dict], list[dict], str]:
    vault = ObsidianVault()
    portfolio = vault.read_portfolio()
    meta = portfolio.get("meta", {})
    account_overview = portfolio.get("account_overview", [])
    overview_map = {str(row.get("项目", "")).strip(): row for row in account_overview}
    as_of_date = str(meta.get("updated_at") or meta.get("date") or _today_str())
    updated_at = _now_ts()

    def build_position(row: dict, scope: str, market: str) -> dict | None:
        name = str(row.get("股票", "")).strip()
        code = _normalize_code(row.get("代码", ""))
        shares = _safe_int(row.get("持有股数", 0))
        if name in ("", "—", "空仓") or not code or code == "—" or shares <= 0:
            return None
        avg_cost = _safe_float(row.get("平均成本", row.get("成本", 0)))
        current_price = _safe_float(row.get("现价", row.get("最新价", avg_cost)))
        market_value = _safe_float(row.get("持仓市值", row.get("市值", current_price * shares)))
        return {
            "scope": scope,
            "code": code,
            "name": name,
            "market": market,
            "shares": shares,
            "avg_cost": avg_cost,
            "current_price": current_price,
            "market_value": market_value,
            "status": str(row.get("状态", row.get("操作", ""))).strip(),
            "note": str(row.get("备注", row.get("操作计划", ""))).strip(),
            "source": "bootstrap:portfolio",
            "as_of_date": as_of_date,
            "updated_at": updated_at,
            "metadata": row,
        }

    positions = []
    for row in portfolio.get("cn_a_holdings", []):
        position = build_position(row, PRIMARY_SCOPE, "CN_A")
        if position:
            positions.append(position)
    for row in portfolio.get("legacy_cn_holdings", []):
        position = build_position(row, SECONDARY_SCOPE, "CN_A_LEGACY")
        if position:
            positions.append(position)
    for row in portfolio.get("hk_legacy_holdings", []):
        position = build_position(row, SECONDARY_SCOPE, "HK")
        if position:
            positions.append(position)

    cn_market_value = sum(item["market_value"] for item in positions if item["scope"] == PRIMARY_SCOPE)
    hk_market_value = sum(item["market_value"] for item in positions if item["scope"] == SECONDARY_SCOPE)
    cn_cash_value = _safe_float(overview_map.get("可交易现金", {}).get("金额", meta.get("cash_value", 0)))
    cn_total_capital = _safe_float(meta.get("total_capital", 0)) or cn_cash_value + cn_market_value

    balances = [
        {
            "scope": PRIMARY_SCOPE,
            "cash_value": cn_cash_value,
            "total_capital": cn_total_capital,
            "total_market_value": cn_market_value,
            "exposure": (cn_market_value / cn_total_capital) if cn_total_capital else 0.0,
            "source": "bootstrap:portfolio",
            "as_of_date": as_of_date,
            "updated_at": updated_at,
            "metadata": {"account_overview": account_overview},
        },
        {
            "scope": SECONDARY_SCOPE,
            "cash_value": 0.0,
            "total_capital": hk_market_value,
            "total_market_value": hk_market_value,
            "exposure": 1.0 if hk_market_value else 0.0,
            "source": "bootstrap:portfolio",
            "as_of_date": as_of_date,
            "updated_at": updated_at,
            "metadata": {
                "legacy_cn_holdings": portfolio.get("legacy_cn_holdings", []),
                "hk_legacy_holdings": portfolio.get("hk_legacy_holdings", []),
            },
        },
        {
            "scope": PAPER_SCOPE,
            "cash_value": 0.0,
            "total_capital": 0.0,
            "total_market_value": 0.0,
            "exposure": 0.0,
            "source": "bootstrap:shadow",
            "as_of_date": as_of_date,
            "updated_at": updated_at,
            "metadata": {},
        },
    ]
    return positions, balances, as_of_date


def _mx_payload(result: Any) -> dict:
    if not isinstance(result, dict):
        return {}
    payload = result.get("data")
    return payload if isinstance(payload, dict) else result


def _build_paper_position(row: dict, as_of_date: str, updated_at: str) -> dict | None:
    name = str(row.get("stockName", row.get("secuName", row.get("name", "")))).strip()
    code = _normalize_code(row.get("stockCode", row.get("secuCode", row.get("code", ""))))
    shares = _safe_int(row.get("totalQty", row.get("currentQty", row.get("shares", 0))))
    if not code or shares <= 0:
        return None

    avg_cost = _safe_float(row.get("costPrice", row.get("avgCost", row.get("cost", 0))))
    current_price = _safe_float(row.get("lastPrice", row.get("currentPrice", row.get("price", 0))))
    market_value = _safe_float(row.get("marketValue", row.get("market_value", 0)))
    if market_value <= 0 and current_price > 0:
        market_value = round(current_price * shares, 2)
    if current_price <= 0 and market_value > 0 and shares > 0:
        current_price = round(market_value / shares, 4)

    return {
        "scope": PAPER_SCOPE,
        "code": code,
        "name": name or code,
        "market": str(row.get("market", row.get("marketType", "MX_PAPER"))).strip() or "MX_PAPER",
        "shares": shares,
        "avg_cost": avg_cost,
        "current_price": current_price,
        "market_value": market_value,
        "status": str(row.get("status", row.get("positionStatus", "持仓"))).strip(),
        "note": str(row.get("note", row.get("remark", ""))).strip(),
        "source": "broker:mx_moni",
        "as_of_date": as_of_date,
        "updated_at": updated_at,
        "metadata": row,
    }


def _paper_portfolio_snapshot() -> tuple[list[dict], list[dict], str]:
    mx = MXMoni()
    positions_result = _mx_payload(mx.positions())
    balance_result = _mx_payload(mx.balance())
    as_of_date = _today_str()
    updated_at = _now_ts()

    raw_positions = positions_result.get("posList", [])
    positions = []
    for row in raw_positions if isinstance(raw_positions, list) else []:
        if not isinstance(row, dict):
            continue
        position = _build_paper_position(row, as_of_date, updated_at)
        if position:
            positions.append(position)

    total_assets = _safe_float(balance_result.get("totalAssets", 0))
    cash_value = _safe_float(balance_result.get("availBalance", 0))
    total_market_value = _safe_float(balance_result.get("totalPosValue", 0))
    if total_assets <= 0:
        total_assets = cash_value + total_market_value

    balances = [
        {
            "scope": PAPER_SCOPE,
            "cash_value": cash_value,
            "total_capital": total_assets,
            "total_market_value": total_market_value,
            "exposure": (total_market_value / total_assets) if total_assets else 0.0,
            "source": "broker:mx_moni",
            "as_of_date": as_of_date,
            "updated_at": updated_at,
            "metadata": {
                "balance": balance_result,
                "positions": positions_result,
            },
        }
    ]
    return positions, balances, as_of_date


def _portfolio_weekly_trade_events(source: str = "bootstrap:weekly_record") -> list[dict]:
    vault = ObsidianVault()
    portfolio = vault.read_portfolio()
    meta = portfolio.get("meta", {})
    default_year = int(str(meta.get("date", _today_str()))[:4])
    events = []

    for idx, row in enumerate(portfolio.get("weekly_records", []), start=1):
        market = str(row.get("市场", "")).strip()
        record_type = str(row.get("类型", "")).strip()
        note = str(row.get("备注", "")).strip()
        scope = _scope_from_record(market, record_type, note)
        side_text = str(row.get("操作", "")).strip()
        side = "buy" if "买" in side_text else "sell" if "卖" in side_text else "other"
        stock_name, stock_code = _parse_stock_cell(row.get("股票", ""))
        event_date = _parse_mmdd_date(str(row.get("日期", "")).strip(), default_year)
        events.append(
            {
                "external_id": f"weekly:{event_date}:{scope}:{side}:{stock_code or stock_name}:{idx}",
                "scope": scope,
                "market": market,
                "code": stock_code,
                "name": stock_name,
                "side": side,
                "event_type": side,
                "shares": _safe_int(row.get("数量", 0)),
                "price": _safe_float(row.get("价格", 0)),
                "amount": _safe_float(row.get("金额", 0)),
                "realized_pnl": _extract_realized_pnl(note, 0.0),
                "event_date": event_date,
                "reason_code": _reason_code_from_text(side, note or record_type, scope),
                "reason_text": note or record_type,
                "source": source,
                "metadata": row,
                "created_at": _now_ts(),
            }
        )
    return events


def _bootstrap_trade_events() -> list[dict]:
    vault = ObsidianVault()
    events = _portfolio_weekly_trade_events(source="bootstrap:weekly_record")

    shadow_path = Path(vault.vault_path) / "03-复盘" / "模拟盘" / "交易记录.md"
    if shadow_path.exists():
        content = shadow_path.read_text(encoding="utf-8")
        tables = parse_md_table(content)
        rows = tables[0].get("rows", []) if tables else []
        for idx, row in enumerate(rows, start=1):
            raw_time = str(row.get("时间", "")).strip()
            event_date = raw_time[:10] if raw_time else _today_str()
            side_text = str(row.get("操作", "")).strip()
            side = "buy" if "买" in side_text else "sell" if "卖" in side_text else "other"
            reason_text = str(row.get("原因", "")).strip()
            events.append(
                {
                    "external_id": f"bootstrap:paper:{idx}:{event_date}:{row.get('代码', '')}",
                    "scope": PAPER_SCOPE,
                    "market": "MX_PAPER",
                    "code": _normalize_code(row.get("代码", "")),
                    "name": str(row.get("股票", "")).strip(),
                    "side": side,
                    "event_type": side,
                    "shares": _safe_int(row.get("数量", 0)),
                    "price": _safe_float(row.get("价格", 0)),
                    "amount": _safe_float(row.get("金额", 0)),
                    "realized_pnl": _extract_realized_pnl(reason_text, 0.0),
                    "event_date": event_date,
                    "reason_code": _reason_code_from_text(side, reason_text, PAPER_SCOPE),
                    "reason_text": reason_text,
                    "source": "bootstrap:shadow_trade_log",
                    "metadata": row,
                    "created_at": _now_ts(),
                }
            )
    return events


def bootstrap_state(force: bool = False) -> dict:
    """Seed the structured ledger from current Markdown/config projections."""
    with _connect() as conn:
        if not force and _meta_get(conn, "bootstrap_completed") == "1":
            return {
                "status": "skipped",
                "db_path": str(_db_path()),
                "reason": "already_bootstrapped",
            }

        positions, balances, as_of_date = _bootstrap_portfolio_snapshot()
        trade_events = _bootstrap_trade_events()
        pool_entries = _bootstrap_pool_entries()

        _write_positions(conn, positions)
        _write_balances(conn, balances)
        _replace_trade_events(conn, trade_events)
        save_pool_snapshot(pool_entries, metadata={"source": "bootstrap"}, conn=conn)

        _meta_set(conn, "bootstrap_completed", "1")
        _meta_set(conn, "bootstrap_at", _now_ts())
        _meta_set(conn, "bootstrap_date", as_of_date)
        _meta_set(conn, "rule_automation_scope", _json_dumps(AUTOMATED_RULES))

        return {
            "status": "success",
            "db_path": str(_db_path()),
            "positions": len(positions),
            "trade_events": len(trade_events),
            "pool_entries": len(pool_entries),
            "as_of_date": as_of_date,
        }


def sync_portfolio_state() -> dict:
    """Refresh structured portfolio balances/positions from portfolio.md."""
    positions, balances, as_of_date = _bootstrap_portfolio_snapshot()
    paper_status = "skipped"
    paper_error = ""
    paper_positions: list[dict] = []
    paper_balances: list[dict] = []
    paper_as_of_date = ""
    try:
        paper_positions, paper_balances, paper_as_of_date = _paper_portfolio_snapshot()
        paper_status = "success"
    except Exception as exc:
        paper_error = str(exc)
        LOGGER.warning(f"[state] paper portfolio sync failed: {exc}")

    with _connect() as conn:
        _write_positions(conn, positions)
        _write_balances(conn, balances)
        if paper_status == "success":
            _replace_portfolio_scope_snapshot(conn, PAPER_SCOPE, paper_positions, paper_balances)
            _meta_set(conn, "paper_portfolio_sync_at", _now_ts())
            _meta_set(conn, "paper_portfolio_sync_date", paper_as_of_date)
        _meta_set(conn, "portfolio_sync_at", _now_ts())
        _meta_set(conn, "portfolio_sync_date", as_of_date)
        _meta_set(conn, "bootstrap_completed", "1")
        _meta_set(conn, "bootstrap_date", as_of_date)
    return {
        "status": "success",
        "db_path": str(_db_path()),
        "positions": len(positions),
        "scopes": [balance.get("scope", "") for balance in balances],
        "as_of_date": as_of_date,
        "paper_mx": {
            "status": paper_status,
            "positions": len(paper_positions),
            "balances": len(paper_balances),
            "as_of_date": paper_as_of_date,
            "error": paper_error,
        },
    }


def sync_activity_state() -> dict:
    """Refresh structured non-paper trade events from portfolio weekly records."""
    weekly_events = _portfolio_weekly_trade_events(source="sync:weekly_record")
    with _connect() as conn:
        _ensure_bootstrapped(conn)
        conn.execute(
            "DELETE FROM trade_events WHERE source IN (?, ?)",
            ("bootstrap:weekly_record", "sync:weekly_record"),
        )
        for event in weekly_events:
            _upsert_trade_event(conn, event)
        _meta_set(conn, "activity_sync_at", _now_ts())
        _meta_set(conn, "activity_sync_count", str(len(weekly_events)))

    counts = {
        PRIMARY_SCOPE: 0,
        SECONDARY_SCOPE: 0,
        PAPER_SCOPE: 0,
    }
    for event in weekly_events:
        scope = event.get("scope", PRIMARY_SCOPE)
        counts[scope] = counts.get(scope, 0) + 1
    return {
        "status": "success",
        "db_path": str(_db_path()),
        "imported_events": len(weekly_events),
        "counts_by_scope": counts,
    }


def _portfolio_rows(scope: str | None = None, conn: sqlite3.Connection | None = None) -> tuple[list[dict], list[dict]]:
    close_after = conn is None
    if close_after:
        context = _connect()
        conn = context.__enter__()
    try:
        _ensure_bootstrapped(conn)
        if scope:
            balance_rows = conn.execute(
                "SELECT * FROM portfolio_balances WHERE scope = ? ORDER BY scope",
                (scope,),
            ).fetchall()
            position_rows = conn.execute(
                "SELECT * FROM portfolio_positions WHERE scope = ? ORDER BY scope, code",
                (scope,),
            ).fetchall()
        else:
            balance_rows = conn.execute(
                "SELECT * FROM portfolio_balances ORDER BY scope"
            ).fetchall()
            position_rows = conn.execute(
                "SELECT * FROM portfolio_positions ORDER BY scope, code"
            ).fetchall()
        balances = [dict(row) for row in balance_rows]
        positions = [dict(row) for row in position_rows]
        for row in balances:
            row["metadata"] = _json_loads(row.pop("metadata_json", "{}"), {})
        for row in positions:
            row["metadata"] = _json_loads(row.pop("metadata_json", "{}"), {})
        return balances, positions
    finally:
        if close_after:
            context.__exit__(None, None, None)


def load_portfolio_snapshot(scope: str | None = None) -> dict:
    """Return the current structured portfolio snapshot."""
    if scope == PAPER_SCOPE:
        try:
            positions, balances, as_of_date = _paper_portfolio_snapshot()
            with _connect() as conn:
                _ensure_bootstrapped(conn)
                _replace_portfolio_scope_snapshot(conn, PAPER_SCOPE, positions, balances)
                _meta_set(conn, "paper_portfolio_sync_at", _now_ts())
                _meta_set(conn, "paper_portfolio_sync_date", as_of_date)
        except Exception as exc:
            LOGGER.warning(f"[state] paper portfolio snapshot refresh failed: {exc}")
    elif scope is None:
        try:
            positions, balances, as_of_date = _paper_portfolio_snapshot()
            with _connect() as conn:
                _ensure_bootstrapped(conn)
                _replace_portfolio_scope_snapshot(conn, PAPER_SCOPE, positions, balances)
                _meta_set(conn, "paper_portfolio_sync_at", _now_ts())
                _meta_set(conn, "paper_portfolio_sync_date", as_of_date)
        except Exception as exc:
            LOGGER.warning(f"[state] paper portfolio snapshot refresh failed: {exc}")
    balances, positions = _portfolio_rows(scope=scope)
    scopes = {}
    for balance in balances:
        scopes[balance["scope"]] = {
            "cash_value": balance["cash_value"],
            "total_capital": balance["total_capital"],
            "total_market_value": balance["total_market_value"],
            "exposure": balance["exposure"],
            "as_of_date": balance["as_of_date"],
            "source": balance.get("source", ""),
        }
    active_positions = [row for row in positions if _safe_int(row.get("shares", 0)) > 0]
    primary = scopes.get(PRIMARY_SCOPE, {})
    if scope == PAPER_SCOPE:
        paper = scopes.get(PAPER_SCOPE, {})
        return {
            "scope": PAPER_SCOPE,
            "as_of_date": paper.get("as_of_date", _today_str()),
            "positions": active_positions,
            "balances": balances,
            "summary": {
                "holding_count": len([row for row in active_positions if row.get("scope") == PAPER_SCOPE]),
                "current_exposure": round(paper.get("exposure", 0.0), 3),
                "cash_value": round(paper.get("cash_value", 0.0), 2),
                "total_capital": round(paper.get("total_capital", 0.0), 2),
                "scopes": scopes,
            },
        }
    return {
        "scope": scope or "all",
        "as_of_date": max((item.get("as_of_date", "") for item in balances), default=_today_str()),
        "positions": active_positions,
        "balances": balances,
        "summary": {
            "holding_count": len([row for row in active_positions if row.get("scope") == PRIMARY_SCOPE]),
            "current_exposure": round(primary.get("exposure", 0.0), 3),
            "cash_value": round(primary.get("cash_value", 0.0), 2),
            "total_capital": round(primary.get("total_capital", 0.0), 2),
            "scopes": scopes,
        },
    }


def load_order_snapshot(scope: str | None = None, status: str | None = None) -> dict:
    """Return the structured order snapshot without mutating any other table."""
    with _connect() as conn:
        orders = _order_rows(scope=scope, status=status, conn=conn)

    status_counts: dict[str, int] = {}
    scope_counts: dict[str, int] = {}
    class_counts: dict[str, int] = {}
    for order in orders:
        order_status = str(order.get("status", "")).strip() or "unknown"
        order_scope = str(order.get("scope", "")).strip() or "unknown"
        order_class = str(order.get("order_class", "")).strip() or "unknown"
        status_counts[order_status] = status_counts.get(order_status, 0) + 1
        scope_counts[order_scope] = scope_counts.get(order_scope, 0) + 1
        class_counts[order_class] = class_counts.get(order_class, 0) + 1

    terminal_count = sum(1 for order in orders if str(order.get("status", "")).strip() in _ORDER_TERMINAL_STATUSES)
    unresolved_count = len(orders) - terminal_count
    pending_count = sum(1 for order in orders if str(order.get("status", "")).strip() in _ORDER_PENDING_STATUSES)
    open_count = sum(1 for order in orders if str(order.get("status", "")).strip() in _ORDER_OPEN_STATUSES)
    exception_count = sum(1 for order in orders if str(order.get("status", "")).strip() in _ORDER_EXCEPTION_STATUSES)
    review_queue_count = sum(1 for order in orders if str(order.get("status", "")).strip() in _ORDER_REVIEW_QUEUE_STATUSES)
    partial_fill_count = sum(1 for order in orders if str(order.get("status", "")).strip() == "partially_filled")
    cancel_replace_count = sum(1 for order in orders if str(order.get("status", "")).strip() == "cancel_replace_pending")
    return {
        "scope": scope or "all",
        "status": status or "all",
        "orders": orders,
        "summary": {
            "order_count": len(orders),
            "open_count": open_count or unresolved_count,
            "pending_count": pending_count,
            "exception_count": exception_count,
            "terminal_count": terminal_count,
            "review_queue_count": review_queue_count,
            "partial_fill_count": partial_fill_count,
            "cancel_replace_count": cancel_replace_count,
            "status_counts": status_counts,
            "scope_counts": scope_counts,
            "class_counts": class_counts,
        },
        "db_path": str(_db_path()),
    }


def _condition_type_matches(order: dict, reply_type: str) -> bool:
    reply_text = str(reply_type or "").strip()
    if not reply_text:
        return True
    condition_type = str(order.get("condition_type", "")).strip().lower()
    if reply_text == "止损":
        return "stop" in condition_type
    if reply_text == "止盈":
        return "profit" in condition_type or condition_type.endswith("_tp")
    return True


def _order_matches_stock(order: dict, stock: str) -> bool:
    target = str(stock or "").strip()
    if not target:
        return True
    normalized_target = _normalize_code(target)
    order_code = _normalize_code(order.get("code", ""))
    order_name = str(order.get("name", "")).strip()
    return target == order_name or normalized_target == order_code


def _reply_stock_code(stock: str) -> str:
    normalized = _normalize_code(stock)
    return normalized if normalized.isdigit() else ""


def _pending_condition_orders(scope: str = PAPER_SCOPE, conn: sqlite3.Connection | None = None) -> list[dict]:
    orders = _order_rows(scope=scope, conn=conn)
    result = []
    for order in orders:
        status = str(order.get("status", "")).strip()
        confirm_status = str(order.get("confirm_status", "")).strip()
        if not (
            str(order.get("order_class", "")).strip() == "condition"
            or str(order.get("condition_type", "")).strip()
        ):
            continue
        if status in _ORDER_TERMINAL_STATUSES:
            continue
        if (
            confirm_status in {"pending", "timed_out", "review_pending"}
            or status in {"candidate", "review_required", "partially_filled", "cancel_replace_pending"}
        ):
            result.append(order)
    return result


def pending_condition_order_items(scope: str = PAPER_SCOPE) -> list[dict]:
    with _connect() as conn:
        _ensure_bootstrapped(conn)
        orders = _pending_condition_orders(scope=scope, conn=conn)

    items = []
    for order in orders:
        condition_type = str(order.get("condition_type", "")).strip().lower()
        order_type = "止盈" if ("profit" in condition_type or condition_type.endswith("_tp")) else "止损"
        price = _safe_float(order.get("trigger_price", order.get("limit_price", 0.0)), 0.0)
        items.append(
            {
                "external_id": order.get("external_id", ""),
                "name": order.get("name", ""),
                "code": order.get("code", ""),
                "type": order_type,
                "price": price,
                "currency": "¥",
                "status": str(order.get("confirm_status", "")).strip() or str(order.get("status", "")).strip(),
            }
        )
    return items


def apply_order_reply(reply_text: str, scope: str = PAPER_SCOPE) -> dict:
    parsed = parse_user_reply(reply_text)
    if not parsed.get("action") or not parsed.get("type") or not parsed.get("stock"):
        return {
            "status": "invalid_reply",
            "reply": parsed,
            "message": "reply_not_recognized",
            "db_path": str(_db_path()),
        }

    now_ts = _now_ts()
    trade_event_recorded = False
    created_order = False
    matched_order_count = 0

    with _connect() as conn:
        _ensure_bootstrapped(conn)
        candidates = [
            order
            for order in _order_rows(scope=scope, conn=conn)
            if _order_matches_stock(order, parsed.get("stock", ""))
            and _condition_type_matches(order, parsed.get("type", ""))
        ]
        candidates.sort(
            key=lambda item: (str(item.get("updated_at", "")), str(item.get("external_id", ""))),
            reverse=True,
        )
        matched_order_count = len(candidates)
        matched = candidates[0] if candidates else None

        if not matched and parsed["action"] == "挂单":
            manual_condition = "manual_tp" if parsed["type"] == "止盈" else "manual_stop"
            matched = upsert_order_state(
                {
                    "external_id": f"{scope}:reply:{datetime.now().strftime('%Y%m%d%H%M%S%f')}:{manual_condition}",
                    "scope": scope,
                    "broker": "manual_reply",
                    "code": _reply_stock_code(parsed.get("stock", "")),
                    "name": parsed.get("stock", ""),
                    "side": "sell",
                    "order_class": "condition",
                    "order_type": "conditional",
                    "condition_type": manual_condition,
                    "requested_shares": 0,
                    "filled_shares": 0,
                    "trigger_price": _safe_float(parsed.get("price", 0.0), 0.0),
                    "status": "placed",
                    "confirm_status": "confirmed",
                    "reason_code": "DISCORD_MANUAL_CONFIRM",
                    "reason_text": parsed.get("raw", ""),
                    "source": "discord_reply",
                    "placed_at": now_ts,
                    "updated_at": now_ts,
                    "metadata": {"reply_history": [parsed]},
                },
                conn=conn,
            )
            created_order = True
            matched_order_count = 1

        if not matched:
            return {
                "status": "not_found",
                "reply": parsed,
                "matched_order_count": 0,
                "message": "no_matching_order",
                "db_path": str(_db_path()),
            }

        metadata = matched.get("metadata", {}) if isinstance(matched.get("metadata", {}), dict) else {}
        reply_history = list(metadata.get("reply_history", []))
        if not reply_history or reply_history[-1] != parsed:
            reply_history.append(parsed)
        update = {
            "external_id": matched.get("external_id", ""),
            "updated_at": now_ts,
            "confirm_status": "confirmed",
            "confirmed_at": now_ts,
            "source": "discord_reply",
            "metadata": {**metadata, "reply_history": reply_history},
        }

        if parsed["action"] == "挂单":
            update["status"] = "placed"
            if parsed.get("price") is not None:
                update["trigger_price"] = _safe_float(parsed["price"], 0.0)
            if not matched.get("placed_at"):
                update["placed_at"] = now_ts
        elif parsed["action"] == "改挂":
            update["status"] = "cancel_replace_pending"
            update["confirm_status"] = "pending"
            if parsed.get("price") is not None:
                update["trigger_price"] = _safe_float(parsed["price"], 0.0)
            update["metadata"] = {
                **metadata,
                "reply_history": reply_history,
                "replace_requested": True,
                "replace_requested_at": now_ts,
            }
        elif parsed["action"] == "取消":
            update["status"] = "cancelled"
            update["cancelled_at"] = now_ts
        elif parsed["action"] == "复核":
            update["status"] = "review_required"
            update["confirm_status"] = "review_pending"
            update["metadata"] = {
                **metadata,
                "reply_history": reply_history,
                "review_required": True,
                "review_required_at": now_ts,
            }
        elif parsed["action"] == "部分成交":
            update["status"] = "partially_filled"
            update["filled_at"] = now_ts
            update["avg_fill_price"] = _safe_float(parsed.get("filled_price", 0.0), 0.0)
            parsed_filled = _safe_int(parsed.get("filled_shares", 0), 0)
            existing_filled = _safe_int(matched.get("filled_shares", 0), 0)
            update["filled_shares"] = max(existing_filled, parsed_filled)
        elif parsed["action"] == "触发":
            update["status"] = "filled"
            update["filled_at"] = now_ts
            update["avg_fill_price"] = _safe_float(parsed.get("filled_price", 0.0), 0.0)
            if not _safe_int(matched.get("filled_shares", 0), 0):
                update["filled_shares"] = _safe_int(matched.get("requested_shares", 0), 0)

        updated_order = upsert_order_state(update, conn=conn)

        trade_delta_shares = 0
        if parsed["action"] == "部分成交":
            trade_delta_shares = max(
                _safe_int(updated_order.get("filled_shares", 0), 0) - _safe_int(matched.get("filled_shares", 0), 0),
                0,
            )
        elif parsed["action"] == "触发":
            trade_delta_shares = _safe_int(updated_order.get("filled_shares", updated_order.get("requested_shares", 0)), 0)

        should_record_fill = parsed["action"] in {"部分成交", "触发"} and trade_delta_shares > 0
        if should_record_fill:
            shares = trade_delta_shares
            price = _safe_float(updated_order.get("avg_fill_price", 0.0), 0.0)
            side = str(updated_order.get("side", "sell")).strip() or "sell"
            if shares > 0 and price > 0:
                _upsert_trade_event(
                    conn,
                    {
                        "external_id": f"{updated_order.get('external_id', '')}:reply_fill",
                        "scope": scope,
                        "market": "MX_PAPER",
                        "code": updated_order.get("code", ""),
                        "name": updated_order.get("name", ""),
                        "side": side,
                        "event_type": side,
                        "shares": shares,
                        "price": price,
                        "amount": round(shares * price, 2),
                        "event_date": now_ts[:10],
                        "reason_code": updated_order.get("reason_code") or "DISCORD_REPLY_FILL",
                        "reason_text": parsed.get("raw", ""),
                        "source": "discord_reply",
                        "metadata": {
                            "order_external_id": updated_order.get("external_id", ""),
                            "reply": parsed,
                            "fill_action": parsed["action"],
                        },
                        "created_at": now_ts,
                    },
                )
                updated_order = upsert_order_state(
                    {
                        "external_id": updated_order.get("external_id", ""),
                        "updated_at": _now_ts(),
                        "metadata": {
                            **(
                                updated_order.get("metadata", {})
                                if isinstance(updated_order.get("metadata", {}), dict)
                                else {}
                            ),
                            "trade_event_logged": parsed["action"] == "触发",
                            "last_fill_action": parsed["action"],
                            "trade_event_source": "discord_reply",
                        },
                    },
                    conn=conn,
                )
                trade_event_recorded = True

    return {
        "status": "ok",
        "reply": parsed,
        "created_order": created_order,
        "matched_order_count": matched_order_count,
        "trade_event_recorded": trade_event_recorded,
        "order": updated_order,
        "db_path": str(_db_path()),
    }


def _normalize_market_detail(detail: dict) -> dict:
    if not isinstance(detail, dict):
        return {}
    indices = detail.get("indices") if isinstance(detail.get("indices"), dict) else detail
    normalized = {}
    for name, raw in indices.items():
        if not isinstance(raw, dict):
            continue
        price = _safe_float(raw.get("price", raw.get("close", 0)))
        ma20 = _safe_float(raw.get("ma20", raw.get("MA20", 0)))
        ma60 = _safe_float(raw.get("ma60", raw.get("MA60", 0)))
        chg_pct = _safe_float(raw.get("chg_pct", raw.get("change_pct", 0)))
        above_ma20 = bool(raw.get("above_ma20", price >= ma20 if ma20 else False))
        below_ma60_days = _safe_int(raw.get("below_ma60_days", raw.get("ma60_days", 0)))
        normalized[name] = {
            "price": price,
            "chg_pct": chg_pct,
            "ma20": ma20,
            "ma60": ma60,
            "ma20_pct": ((price / ma20) - 1) * 100 if ma20 else 0,
            "ma60_pct": ((price / ma60) - 1) * 100 if ma60 else 0,
            "above_ma20": above_ma20,
            "ma60_days": below_ma60_days,
            "signal": "GREEN" if above_ma20 else "RED",
            "source": raw.get("source", ""),
            "source_chain": raw.get("source_chain", []),
        }
    return normalized


def save_market_snapshot(snapshot: dict) -> dict:
    with _connect() as conn:
        _meta_set(conn, "bootstrap_completed", _meta_get(conn, "bootstrap_completed", "0") or "0")
        conn.execute(
            """
            INSERT INTO market_snapshots(id, signal, source, source_chain_json, as_of_date, updated_at, detail_json)
            VALUES(1, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              signal = excluded.signal,
              source = excluded.source,
              source_chain_json = excluded.source_chain_json,
              as_of_date = excluded.as_of_date,
              updated_at = excluded.updated_at,
              detail_json = excluded.detail_json
            """,
            (
                snapshot.get("signal", "CLEAR"),
                snapshot.get("source", ""),
                _json_dumps(snapshot.get("source_chain", [])),
                snapshot.get("as_of_date", _today_str()),
                snapshot.get("updated_at", _now_ts()),
                _json_dumps(snapshot.get("indices", {})),
            ),
        )
    return snapshot


def load_market_snapshot(refresh: bool = False) -> dict:
    """Return the latest unified market snapshot."""
    with _connect() as conn:
        if not refresh:
            row = conn.execute("SELECT * FROM market_snapshots WHERE id = 1").fetchone()
            if row and row["as_of_date"] == _today_str():
                return {
                    "as_of_date": row["as_of_date"],
                    "updated_at": row["updated_at"],
                    "signal": row["signal"],
                    "source": row["source"],
                    "source_chain": _json_loads(row["source_chain_json"], []),
                    "indices": _json_loads(row["detail_json"], {}),
                }
    from scripts.engine import market_timer as _mt_mod

    # 强制刷新时重置全局单例，确保重新拉取实时数据
    if refresh:
        _mt_mod._timer_instance = None

    snapshot = dict(_mt_mod.load_market_snapshot())
    snapshot.setdefault("updated_at", _now_ts())
    snapshot.setdefault("as_of_date", _today_str())
    snapshot.setdefault("signal", snapshot.get("market_signal", "CLEAR"))
    snapshot.setdefault("market_signal", snapshot.get("signal", "CLEAR"))
    snapshot.setdefault("source", "market_timer")
    snapshot.setdefault("source_chain", [])
    snapshot.setdefault("indices", {})
    return save_market_snapshot(snapshot)


def save_market_snapshot_history(
    snapshot: dict,
    *,
    pipeline: str = "",
    history_group_id: str = "",
    metadata: dict | None = None,
    conn: sqlite3.Connection | None = None,
) -> dict:
    """Archive a market snapshot for exact historical replay."""
    own_conn = conn is None
    if own_conn:
        context = _connect()
        conn = context.__enter__()
    try:
        snapshot = dict(snapshot or {})
        metadata = dict(metadata or {})
        as_of_date = str(snapshot.get("as_of_date", snapshot.get("snapshot_date", _today_str()))).strip() or _today_str()
        updated_at = str(snapshot.get("updated_at", _now_ts())).strip() or _now_ts()
        signal = str(snapshot.get("signal", snapshot.get("market_signal", "CLEAR"))).strip().upper() or "CLEAR"
        row = conn.execute(
            """
            INSERT INTO market_snapshot_history(
              history_group_id, pipeline, snapshot_date, as_of_date, signal,
              source, source_chain_json, updated_at, detail_json, metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(history_group_id or metadata.get("history_group_id", "")).strip(),
                str(pipeline or metadata.get("pipeline", "")).strip(),
                as_of_date,
                as_of_date,
                signal,
                str(snapshot.get("source", "")).strip(),
                _json_dumps(snapshot.get("source_chain", [])),
                updated_at,
                _json_dumps(snapshot.get("indices", {})),
                _json_dumps(metadata),
            ),
        )
        return {
            "history_id": int(row.lastrowid),
            "history_group_id": str(history_group_id or metadata.get("history_group_id", "")).strip(),
            "pipeline": str(pipeline or metadata.get("pipeline", "")).strip(),
            "snapshot_date": as_of_date,
            "as_of_date": as_of_date,
            "updated_at": updated_at,
            "signal": signal,
            "source": str(snapshot.get("source", "")).strip(),
            "source_chain": list(snapshot.get("source_chain", [])),
            "indices": dict(snapshot.get("indices", {})) if isinstance(snapshot.get("indices", {}), dict) else {},
            "metadata": metadata,
        }
    finally:
        if own_conn:
            context.__exit__(None, None, None)


def load_market_snapshot_history(
    snapshot_date: str | None = None,
    *,
    history_group_id: str | None = None,
    limit: int = 20,
) -> dict:
    """Load archived market snapshots."""
    with _connect() as conn:
        where = []
        params: list[Any] = []
        if snapshot_date:
            where.append("snapshot_date = ?")
            params.append(snapshot_date)
        if history_group_id:
            where.append("history_group_id = ?")
            params.append(history_group_id)
        query = "SELECT * FROM market_snapshot_history"
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY snapshot_date DESC, updated_at DESC, id DESC LIMIT ?"
        params.append(max(1, int(limit or 20)))
        rows = conn.execute(query, params).fetchall()

    items = [
        {
            "history_id": int(row["id"]),
            "history_group_id": str(row["history_group_id"] or ""),
            "pipeline": str(row["pipeline"] or ""),
            "snapshot_date": str(row["snapshot_date"] or ""),
            "as_of_date": str(row["as_of_date"] or ""),
            "updated_at": str(row["updated_at"] or ""),
            "signal": str(row["signal"] or ""),
            "source": str(row["source"] or ""),
            "source_chain": _json_loads(row["source_chain_json"], []),
            "indices": _json_loads(row["detail_json"], {}),
            "metadata": _json_loads(row["metadata_json"], {}),
        }
        for row in rows
    ]
    latest = items[0] if items else {}
    return {
        "snapshot_date": snapshot_date or str(latest.get("snapshot_date", "")),
        "history_group_id": history_group_id or str(latest.get("history_group_id", "")),
        "count": len(items),
        "items": items,
        "latest": latest,
    }


def _shadow_trade_snapshot() -> dict:
    empty_advisory = {
        "triggered_signal_count": 0,
        "triggered_position_count": 0,
        "triggered_rules": [],
        "positions": [],
    }
    try:
        from scripts.pipeline.shadow_trade import get_status, paper_trade_consistency_snapshot

        consistency = paper_trade_consistency_snapshot(window=180)
        shadow_status = get_status()
        actual_positions = [
            {
                "code": str(item.get("code", "")).strip(),
                "name": str(item.get("name", "")).strip(),
                "shares": int(float(item.get("shares", 0) or 0)),
            }
            for item in shadow_status.get("positions", [])
            if str(item.get("code", "")).strip() and int(float(item.get("shares", 0) or 0)) > 0
        ]
        return {
            "ok": True,
            "status": consistency["status"],
            "timestamp": shadow_status.get("timestamp", ""),
            "automation_scope": shadow_status.get("automation_scope", ""),
            "automated_rules": shadow_status.get("automated_rules", []),
            "advisory_rules": shadow_status.get("advisory_rules", []),
            "positions_count": len(actual_positions),
            "positions": actual_positions,
            "advisory_summary": shadow_status.get("advisory_summary", empty_advisory),
            "consistency": consistency,
        }
    except Exception as e:
        return {
            "ok": False,
            "status": "error",
            "error": str(e),
            "timestamp": "",
            "automation_scope": "",
            "automated_rules": [],
            "advisory_rules": [],
            "positions_count": 0,
            "positions": [],
            "advisory_summary": empty_advisory,
            "consistency": {
                "ok": False,
                "status": "error",
                "error": str(e),
                "inferred_open_codes": [],
                "actual_open_codes": [],
                "event_only_codes": [],
                "broker_only_codes": [],
                "event_trade_count": 0,
            },
        }


def _alert_level_order(level: str) -> int:
    level = str(level or "").strip().lower()
    return {"warning": 0, "error": 0, "block": 0, "info": 1, "ok": 2}.get(level, 3)


def _count_alerts(alerts: list[dict]) -> tuple[dict, dict, dict]:
    level_counts: dict[str, int] = {}
    code_counts: dict[str, int] = {}
    code_level_counts: dict[str, dict[str, int]] = {}
    for alert in alerts:
        level = str(alert.get("level", "")).strip().lower() or "info"
        code = str(alert.get("code", "")).strip() or "UNKNOWN"
        level_counts[level] = level_counts.get(level, 0) + 1
        code_counts[code] = code_counts.get(code, 0) + 1
        level_bucket = code_level_counts.setdefault(level, {})
        level_bucket[code] = level_bucket.get(code, 0) + 1
    return level_counts, code_counts, code_level_counts


def _prepare_alert_entry(level: str, code: str, summary: str, details: dict | None = None) -> dict:
    now = _now_ts()
    normalized_code = str(code or "").strip() or "UNKNOWN"
    normalized_summary = str(summary or "").strip()
    normalized_details = details or {}
    subject = str(
        normalized_details.get("code")
        or normalized_details.get("name")
        or normalized_details.get("snapshot_date")
        or normalized_details.get("market_signal")
        or ""
    ).strip()
    return {
        "level": str(level or "info").strip().lower() or "info",
        "code": normalized_code,
        "summary": normalized_summary,
        "details": normalized_details,
        "alert_key": f"{normalized_code}:{subject}:{normalized_summary}",
        "acknowledged": False,
        "acknowledged_at": "",
        "acknowledged_by": "",
        "handling_status": "pending",
        "throttled": False,
        "updated_at": now,
    }


def _dedupe_alerts(alerts: list[dict]) -> tuple[list[dict], int]:
    by_key: dict[str, dict] = {}
    suppressed_count = 0
    for alert in alerts:
        key = str(alert.get("alert_key", "")).strip() or f"{alert.get('code', '')}:{alert.get('summary', '')}"
        existing = by_key.get(key)
        if not existing:
            by_key[key] = alert
            continue
        suppressed_count += 1
        if _alert_level_order(alert.get("level", "")) < _alert_level_order(existing.get("level", "")):
            alert["throttled"] = False
            existing["throttled"] = True
            by_key[key] = alert
        else:
            existing["throttled"] = False
    return sorted(by_key.values(), key=lambda item: (_alert_level_order(item.get("level", "")), item.get("code", ""), item.get("summary", ""))), suppressed_count


def _apply_existing_alert_state(alerts: list[dict], previous_alerts: list[dict]) -> list[dict]:
    previous_by_key = {
        str(item.get("alert_key", "")).strip(): item
        for item in previous_alerts or []
        if str(item.get("alert_key", "")).strip()
    }
    for alert in alerts:
        previous = previous_by_key.get(str(alert.get("alert_key", "")).strip())
        if not previous:
            continue
        alert["acknowledged"] = bool(previous.get("acknowledged", False))
        alert["acknowledged_at"] = str(previous.get("acknowledged_at", ""))
        alert["acknowledged_by"] = str(previous.get("acknowledged_by", ""))
        alert["handling_status"] = str(previous.get("handling_status", "pending")) or "pending"
    return alerts


def _pool_snapshot_alerts(pool_snapshot: dict) -> list[dict]:
    alerts = []
    for entry in pool_snapshot.get("entries", []) if isinstance(pool_snapshot, dict) else []:
        if not isinstance(entry, dict):
            continue
        code = str(entry.get("code", "")).strip()
        name = str(entry.get("name", code)).strip() or code
        veto_signals = {str(item).strip() for item in entry.get("veto_signals", []) if str(item).strip()}
        metadata = entry.get("metadata", {}) if isinstance(entry.get("metadata", {}), dict) else {}
        score_delta = _safe_float(
            entry.get("score_delta", metadata.get("score_delta", metadata.get("score_change", 0.0))),
            0.0,
        )
        details = {
            "code": code,
            "name": name,
            "bucket": entry.get("bucket", ""),
            "total_score": entry.get("total_score", 0),
            "veto_signals": sorted(veto_signals),
        }
        if "earnings_bomb" in veto_signals:
            alerts.append(_prepare_alert_entry("warning", "FINANCIAL_EARNINGS_WARNING", f"{name} 财报风险", details))
        if "limit_up_today" in veto_signals or metadata.get("limit_up_pullback"):
            alerts.append(_prepare_alert_entry("info", "MARKET_LIMIT_UP_PULLBACK_WATCH", f"{name} 涨停后回落观察", details))
        if veto_signals.intersection({"volume_break", "high_volume_break", "breakdown_volume"}) or metadata.get("volume_break"):
            alerts.append(_prepare_alert_entry("warning", "MARKET_VOLUME_BREAK_WARNING", f"{name} 放量破位风险", details))
        if score_delta <= -1.0:
            alerts.append(_prepare_alert_entry(
                "warning",
                "POOL_SCORE_LOSS",
                f"{name} 池子评分失分",
                {**details, "score_delta": score_delta},
            ))
    return alerts


def build_alert_center_snapshot(
    today_decision: dict | None = None,
    pool_sync_state: dict | None = None,
    shadow_snapshot: dict | None = None,
    order_snapshot: dict | None = None,
    signal_bus: dict | None = None,
    pool_snapshot: dict | None = None,
    market_snapshot: dict | None = None,
) -> dict:
    """Build a structured alert center snapshot from the current state views."""
    today_decision = today_decision or {}
    pool_sync_state = pool_sync_state or {}
    shadow_snapshot = shadow_snapshot or {}
    order_snapshot = order_snapshot or {}
    signal_bus = signal_bus or {}
    pool_snapshot = pool_snapshot or {}
    market_snapshot = market_snapshot or {}

    alerts: list[dict] = []

    def add_alert(level: str, code: str, summary: str, details: dict | None = None) -> None:
        alerts.append(_prepare_alert_entry(level, code, summary, details))

    if pool_sync_state.get("status") not in {"", "ok"}:
        add_alert("warning", "POOL_SYNC_DRIFT", "池子投影存在漂移", {
            "status": pool_sync_state.get("status", ""),
            "snapshot_date": pool_sync_state.get("snapshot_date", ""),
        })

    consistency = shadow_snapshot.get("consistency", {}) or {}
    if consistency.get("status") not in {"", "ok"} or not consistency.get("ok", True):
        add_alert("warning", "TRADE_PAPER_RECONCILE_DRIFT", "模拟盘事件流与 broker 状态不一致", {
            "event_only_codes": consistency.get("event_only_codes", []),
            "broker_only_codes": consistency.get("broker_only_codes", []),
        })

    order_summary = order_snapshot.get("summary", {}) if isinstance(order_snapshot, dict) else {}
    if int(order_summary.get("pending_count", 0) or 0) > 0:
        add_alert("info", "ORDER_CONFIRM_PENDING", "存在待确认条件单", {
            "pending_count": order_summary.get("pending_count", 0),
            "condition_orders": order_snapshot.get("condition_orders", {}),
        })
    if int(order_summary.get("exception_count", 0) or 0) > 0:
        add_alert("warning", "ORDER_EXCEPTION", "存在异常订单", {
            "exception_count": order_summary.get("exception_count", 0),
        })

    portfolio_risk = today_decision.get("portfolio_risk", {}) if isinstance(today_decision, dict) else {}
    if portfolio_risk.get("state") == "block":
        add_alert("warning", "PORTFOLIO_RISK_BLOCK", "组合级风控阻断交易", {
            "reason_codes": portfolio_risk.get("reason_codes", []),
            "reasons": portfolio_risk.get("reasons", []),
        })
    elif portfolio_risk.get("state") == "warning":
        add_alert("info", "PORTFOLIO_RISK_WARNING", "组合级风控预警", {
            "reason_codes": portfolio_risk.get("reason_codes", []),
            "reasons": portfolio_risk.get("reasons", []),
        })

    market_signal = str(today_decision.get("market_signal", "")).strip().upper()
    if market_signal in {"RED", "CLEAR"}:
        add_alert("info", f"MARKET_{market_signal}", "当前市场状态不支持主动开仓", {
            "market_signal": market_signal,
        })

    advisory_summary = shadow_snapshot.get("advisory_summary", {}) or {}
    if int(advisory_summary.get("triggered_signal_count", 0) or 0) > 0:
        add_alert("info", "SHADOW_ADVISORY", "影子盘存在 advisory 风控提示", {
            "triggered_rules": advisory_summary.get("triggered_rules", []),
            "triggered_position_count": advisory_summary.get("triggered_position_count", 0),
        })

    alerts.extend(_pool_snapshot_alerts(pool_snapshot))
    alerts, suppressed_count = _dedupe_alerts(alerts)
    level_counts, code_counts, code_level_counts = _count_alerts(alerts)
    status = "ok"
    if any(level in {"warning", "error", "block"} for level in level_counts):
        status = "warning"
    elif level_counts.get("info", 0) > 0:
        status = "info"

    snapshot_date = (
        str(pool_snapshot.get("snapshot_date", "")).strip()
        or str(market_snapshot.get("as_of_date", "")).strip()
        or _today_str()
    )
    updated_at = _now_ts()
    ack_summary = {
        "acknowledged_count": sum(1 for alert in alerts if bool(alert.get("acknowledged"))),
        "pending_count": sum(1 for alert in alerts if not bool(alert.get("acknowledged"))),
        "all_acknowledged": bool(alerts) and all(bool(alert.get("acknowledged")) for alert in alerts),
        "suppressed_duplicate_count": suppressed_count,
    }

    summary = {
        "status": status,
        "alert_count": len(alerts),
        "level_counts": level_counts,
        "code_counts": code_counts,
        "ack_summary": ack_summary,
        "recent_updated_at": updated_at,
        "snapshot_date": snapshot_date,
    }

    classification = {
        "by_level": level_counts,
        "by_code": code_counts,
        "by_level_code": code_level_counts,
    }

    return {
        "status": status,
        "snapshot_date": snapshot_date,
        "updated_at": updated_at,
        "status_summary": summary,
        "classification": classification,
        "alert_count": len(alerts),
        "alerts": alerts,
        "signal_bus_state": str(signal_bus.get("state", "")).strip(),
        "pool_snapshot_date": str(pool_snapshot.get("snapshot_date", "")).strip(),
        "market_signal": str(market_snapshot.get("signal", market_snapshot.get("market_signal", ""))).strip(),
        "signal_bus": signal_bus,
        "pool_snapshot": pool_snapshot,
        "market_snapshot": market_snapshot,
        "ack_summary": ack_summary,
    }


def save_alert_snapshot(snapshot: dict, conn: sqlite3.Connection | None = None) -> dict:
    """Persist a structured alert snapshot to the ledger."""
    own_conn = conn is None
    if own_conn:
        context = _connect()
        conn = context.__enter__()
    try:
        snapshot = dict(snapshot or {})
        previous_row = conn.execute("SELECT detail_json FROM alert_snapshots WHERE id = 1").fetchone()
        if previous_row:
            previous_detail = _json_loads(previous_row["detail_json"], {})
            previous_alerts = previous_detail.get("alerts", []) if isinstance(previous_detail, dict) else []
            snapshot["alerts"] = _apply_existing_alert_state(snapshot.get("alerts", []), previous_alerts)
        detail = dict(snapshot)
        summary = dict(snapshot.get("status_summary", {}))
        classification = dict(snapshot.get("classification", {}))
        recalculated_ack = {
            "acknowledged_count": sum(1 for alert in snapshot.get("alerts", []) if bool(alert.get("acknowledged"))),
            "pending_count": sum(1 for alert in snapshot.get("alerts", []) if not bool(alert.get("acknowledged"))),
            "all_acknowledged": bool(snapshot.get("alerts", [])) and all(bool(alert.get("acknowledged")) for alert in snapshot.get("alerts", [])),
        }
        ack_summary = {**dict(snapshot.get("ack_summary", {})), **recalculated_ack}
        summary["ack_summary"] = ack_summary
        detail.setdefault("status_summary", summary)
        detail.setdefault("classification", classification)
        detail.setdefault("ack_summary", ack_summary)
        detail["status_summary"] = summary
        detail["ack_summary"] = ack_summary
        snapshot_date = str(snapshot.get("snapshot_date", _today_str())).strip() or _today_str()
        updated_at = str(snapshot.get("updated_at", _now_ts())).strip() or _now_ts()
        status = str(snapshot.get("status", summary.get("status", "ok"))).strip() or "ok"
        alert_count = int(snapshot.get("alert_count", summary.get("alert_count", 0)) or 0)
        level_counts = summary.get("level_counts", classification.get("by_level", {}))
        code_counts = summary.get("code_counts", classification.get("by_code", {}))

        conn.execute(
            """
            INSERT INTO alert_snapshots(
              id, snapshot_date, updated_at, status, alert_count,
              level_counts_json, code_counts_json, ack_counts_json, detail_json
            ) VALUES(1, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              snapshot_date = excluded.snapshot_date,
              updated_at = excluded.updated_at,
              status = excluded.status,
              alert_count = excluded.alert_count,
              level_counts_json = excluded.level_counts_json,
              code_counts_json = excluded.code_counts_json,
              ack_counts_json = excluded.ack_counts_json,
              detail_json = excluded.detail_json
            """,
            (
                snapshot_date,
                updated_at,
                status,
                alert_count,
                _json_dumps(level_counts),
                _json_dumps(code_counts),
                _json_dumps(ack_summary),
                _json_dumps(detail),
            ),
        )
        return {
            **detail,
            "snapshot_date": snapshot_date,
            "updated_at": updated_at,
            "status": status,
            "alert_count": alert_count,
        }
    finally:
        if own_conn:
            context.__exit__(None, None, None)


def load_alert_snapshot(context: dict | None = None, refresh: bool = False) -> dict:
    """Load or rebuild the structured alert center snapshot."""
    if context is None and not refresh:
        with _connect() as conn:
            row = conn.execute("SELECT * FROM alert_snapshots WHERE id = 1").fetchone()
            if row and row["snapshot_date"] == _today_str():
                detail = _json_loads(row["detail_json"], {})
                if isinstance(detail, dict) and detail:
                    detail.setdefault("snapshot_date", row["snapshot_date"])
                    detail.setdefault("updated_at", row["updated_at"])
                    detail.setdefault("status", row["status"])
                    detail.setdefault("alert_count", int(row["alert_count"]))
                    detail.setdefault("status_summary", {
                        "status": row["status"],
                        "alert_count": int(row["alert_count"]),
                        "level_counts": _json_loads(row["level_counts_json"], {}),
                        "code_counts": _json_loads(row["code_counts_json"], {}),
                        "ack_summary": _json_loads(row["ack_counts_json"], {}),
                        "recent_updated_at": row["updated_at"],
                        "snapshot_date": row["snapshot_date"],
                    })
                    detail.setdefault("classification", {
                        "by_level": _json_loads(row["level_counts_json"], {}),
                        "by_code": _json_loads(row["code_counts_json"], {}),
                        "by_level_code": {},
                    })
                    return detail

    if context is None:
        from scripts.engine.composite import build_today_decision
        from scripts.state.reason_codes import build_signal_bus_summary

        strategy = get_strategy()
        today_decision = build_today_decision(strategy=strategy)
        pool_snapshot = load_pool_snapshot()
        pool_sync_state = audit_state()
        market_snapshot = load_market_snapshot()
        shadow_snapshot = _shadow_trade_snapshot()
        order_snapshot = load_order_snapshot(scope="paper_mx")
        signal_bus = build_signal_bus_summary(
            market_snapshot=market_snapshot,
            pool_snapshot=pool_snapshot,
            pool_audit=pool_sync_state,
            today_decision=today_decision,
            shadow_snapshot=shadow_snapshot,
        )
        context = {
            "today_decision": today_decision,
            "pool_sync_state": pool_sync_state,
            "shadow_snapshot": shadow_snapshot,
            "order_snapshot": order_snapshot,
            "signal_bus": signal_bus,
            "pool_snapshot": pool_snapshot,
            "market_snapshot": market_snapshot,
        }

    snapshot = build_alert_center_snapshot(
        today_decision=context.get("today_decision", {}),
        pool_sync_state=context.get("pool_sync_state", {}),
        shadow_snapshot=context.get("shadow_snapshot", {}),
        order_snapshot=context.get("order_snapshot", {}),
        signal_bus=context.get("signal_bus", {}),
        pool_snapshot=context.get("pool_snapshot", {}),
        market_snapshot=context.get("market_snapshot", {}),
    )
    return save_alert_snapshot(snapshot)


def save_decision_snapshot_history(
    today_decision: dict,
    *,
    snapshot_date: str | None = None,
    pipeline: str = "",
    history_group_id: str = "",
    metadata: dict | None = None,
    conn: sqlite3.Connection | None = None,
) -> dict:
    """Archive a `today_decision` payload for later exact replay."""
    own_conn = conn is None
    if own_conn:
        context = _connect()
        conn = context.__enter__()
    try:
        today_decision = dict(today_decision or {})
        metadata = dict(metadata or {})
        snapshot_date = str(snapshot_date or metadata.get("snapshot_date") or _today_str()).strip() or _today_str()
        updated_at = str(today_decision.get("updated_at", metadata.get("updated_at", _now_ts()))).strip() or _now_ts()
        decision_action = str(
            today_decision.get("decision", today_decision.get("action", today_decision.get("state", "")))
        ).strip()
        market_signal = str(today_decision.get("market_signal", "")).strip().upper()
        row = conn.execute(
            """
            INSERT INTO decision_snapshot_history(
              history_group_id, pipeline, snapshot_date, decision_action,
              market_signal, updated_at, detail_json, metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(history_group_id or metadata.get("history_group_id", "")).strip(),
                str(pipeline or metadata.get("pipeline", "")).strip(),
                snapshot_date,
                decision_action,
                market_signal,
                updated_at,
                _json_dumps(today_decision),
                _json_dumps(metadata),
            ),
        )
        return {
            "history_id": int(row.lastrowid),
            "history_group_id": str(history_group_id or metadata.get("history_group_id", "")).strip(),
            "pipeline": str(pipeline or metadata.get("pipeline", "")).strip(),
            "snapshot_date": snapshot_date,
            "updated_at": updated_at,
            "decision_action": decision_action,
            "market_signal": market_signal,
            "today_decision": today_decision,
            "metadata": metadata,
        }
    finally:
        if own_conn:
            context.__exit__(None, None, None)


def load_decision_snapshot_history(
    snapshot_date: str | None = None,
    *,
    history_group_id: str | None = None,
    limit: int = 20,
) -> dict:
    """Load archived `today_decision` rows."""
    with _connect() as conn:
        where = []
        params: list[Any] = []
        if snapshot_date:
            where.append("snapshot_date = ?")
            params.append(snapshot_date)
        if history_group_id:
            where.append("history_group_id = ?")
            params.append(history_group_id)
        query = "SELECT * FROM decision_snapshot_history"
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY snapshot_date DESC, updated_at DESC, id DESC LIMIT ?"
        params.append(max(1, int(limit or 20)))
        rows = conn.execute(query, params).fetchall()

    items = [
        {
            "history_id": int(row["id"]),
            "history_group_id": str(row["history_group_id"] or ""),
            "pipeline": str(row["pipeline"] or ""),
            "snapshot_date": str(row["snapshot_date"] or ""),
            "updated_at": str(row["updated_at"] or ""),
            "decision_action": str(row["decision_action"] or ""),
            "market_signal": str(row["market_signal"] or ""),
            "today_decision": _json_loads(row["detail_json"], {}),
            "metadata": _json_loads(row["metadata_json"], {}),
        }
        for row in rows
    ]
    latest = items[0] if items else {}
    return {
        "snapshot_date": snapshot_date or str(latest.get("snapshot_date", "")),
        "history_group_id": history_group_id or str(latest.get("history_group_id", "")),
        "count": len(items),
        "items": items,
        "latest": latest,
    }


def save_candidate_snapshot_history(
    candidates: list[dict],
    *,
    snapshot_date: str | None = None,
    pipeline: str = "",
    history_group_id: str = "",
    pool: str = "",
    universe: str = "",
    source: str = "",
    actionable_count: int | None = None,
    metadata: dict | None = None,
    conn: sqlite3.Connection | None = None,
) -> dict:
    """Archive scored candidates so later replay can use the exact daily outputs."""
    own_conn = conn is None
    if own_conn:
        context = _connect()
        conn = context.__enter__()
    try:
        metadata = dict(metadata or {})
        snapshot_date = str(snapshot_date or metadata.get("snapshot_date") or _today_str()).strip() or _today_str()
        updated_at = str(metadata.get("updated_at", _now_ts())).strip() or _now_ts()
        rows = list(candidates or [])
        row = conn.execute(
            """
            INSERT INTO candidate_snapshot_history(
              history_group_id, pipeline, snapshot_date, pool, universe, source,
              candidate_count, actionable_count, updated_at, metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(history_group_id or metadata.get("history_group_id", "")).strip(),
                str(pipeline or metadata.get("pipeline", "")).strip(),
                snapshot_date,
                str(pool or metadata.get("pool", "")).strip(),
                str(universe or metadata.get("universe", "")).strip(),
                str(source or metadata.get("source", "")).strip(),
                len(rows),
                int(actionable_count if actionable_count is not None else metadata.get("actionable_count", len(rows))),
                updated_at,
                _json_dumps(metadata),
            ),
        )
        snapshot_id = int(row.lastrowid)
        for idx, entry in enumerate(rows, start=1):
            code = _normalize_code(entry.get("code", ""))
            if not code:
                continue
            conn.execute(
                """
                INSERT INTO candidate_snapshot_entries(
                  snapshot_id, rank, code, name, total_score, technical_score,
                  fundamental_score, flow_score, sentiment_score, veto_triggered,
                  veto_signals_json, passed_text, recommendation, bucket, data_quality,
                  note, source, detail_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot_id,
                    idx,
                    code,
                    str(entry.get("name", code)).strip(),
                    round(_safe_float(entry.get("total_score", 0)), 4),
                    round(_safe_float(entry.get("technical_score", 0)), 4),
                    round(_safe_float(entry.get("fundamental_score", 0)), 4),
                    round(_safe_float(entry.get("flow_score", 0)), 4),
                    round(_safe_float(entry.get("sentiment_score", 0)), 4),
                    1 if bool(entry.get("veto_triggered", False)) else 0,
                    _json_dumps(entry.get("veto_signals", [])),
                    str(entry.get("passed_text", "")).strip(),
                    str(entry.get("recommendation", entry.get("passed_text", ""))).strip(),
                    str(entry.get("bucket", "")).strip(),
                    str(entry.get("data_quality", "ok")).strip(),
                    str(entry.get("note", "")).strip(),
                    str(entry.get("source", source or metadata.get("source", ""))).strip(),
                    _json_dumps(entry),
                ),
            )
        return {
            "history_id": snapshot_id,
            "history_group_id": str(history_group_id or metadata.get("history_group_id", "")).strip(),
            "pipeline": str(pipeline or metadata.get("pipeline", "")).strip(),
            "snapshot_date": snapshot_date,
            "updated_at": updated_at,
            "candidate_count": len(rows),
            "actionable_count": int(actionable_count if actionable_count is not None else metadata.get("actionable_count", len(rows))),
            "pool": str(pool or metadata.get("pool", "")).strip(),
            "universe": str(universe or metadata.get("universe", "")).strip(),
            "source": str(source or metadata.get("source", "")).strip(),
            "metadata": metadata,
        }
    finally:
        if own_conn:
            context.__exit__(None, None, None)


def load_candidate_snapshot_history(
    snapshot_date: str | None = None,
    *,
    history_group_id: str | None = None,
    limit: int = 20,
) -> dict:
    """Load archived scored-candidate snapshots."""
    with _connect() as conn:
        where = []
        params: list[Any] = []
        if snapshot_date:
            where.append("snapshot_date = ?")
            params.append(snapshot_date)
        if history_group_id:
            where.append("history_group_id = ?")
            params.append(history_group_id)
        query = "SELECT * FROM candidate_snapshot_history"
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY snapshot_date DESC, updated_at DESC, id DESC LIMIT ?"
        params.append(max(1, int(limit or 20)))
        rows = conn.execute(query, params).fetchall()
        history_ids = [int(row["id"]) for row in rows]
        entries_by_snapshot: dict[int, list[dict[str, Any]]] = {history_id: [] for history_id in history_ids}
        if history_ids:
            placeholders = ",".join("?" for _ in history_ids)
            entry_rows = conn.execute(
                f"""
                SELECT * FROM candidate_snapshot_entries
                WHERE snapshot_id IN ({placeholders})
                ORDER BY snapshot_id, rank ASC, total_score DESC, code
                """,
                history_ids,
            ).fetchall()
            for entry_row in entry_rows:
                entries_by_snapshot[int(entry_row["snapshot_id"])].append(
                    _json_loads(entry_row["detail_json"], {})
                    or {
                        "code": str(entry_row["code"] or ""),
                        "name": str(entry_row["name"] or ""),
                        "total_score": _safe_float(entry_row["total_score"], 0.0),
                    }
                )

    items = []
    for row in rows:
        history_id = int(row["id"])
        candidates = entries_by_snapshot.get(history_id, [])
        items.append({
            "history_id": history_id,
            "history_group_id": str(row["history_group_id"] or ""),
            "pipeline": str(row["pipeline"] or ""),
            "snapshot_date": str(row["snapshot_date"] or ""),
            "updated_at": str(row["updated_at"] or ""),
            "pool": str(row["pool"] or ""),
            "universe": str(row["universe"] or ""),
            "source": str(row["source"] or ""),
            "candidate_count": int(row["candidate_count"] or 0),
            "actionable_count": int(row["actionable_count"] or 0),
            "candidates": candidates,
            "metadata": _json_loads(row["metadata_json"], {}),
        })
    latest = items[0] if items else {}
    return {
        "snapshot_date": snapshot_date or str(latest.get("snapshot_date", "")),
        "history_group_id": history_group_id or str(latest.get("history_group_id", "")),
        "count": len(items),
        "items": items,
        "latest": latest,
    }


def _resolve_signal_history_group_id(conn: sqlite3.Connection, snapshot_date: str) -> str:
    for table in (
        "candidate_snapshot_history",
        "decision_snapshot_history",
        "pool_snapshot_history",
        "market_snapshot_history",
    ):
        row = conn.execute(
            f"""
            SELECT history_group_id
            FROM {table}
            WHERE snapshot_date = ? AND TRIM(COALESCE(history_group_id, '')) <> ''
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
            (snapshot_date,),
        ).fetchone()
        if row and str(row["history_group_id"] or "").strip():
            return str(row["history_group_id"]).strip()
    return ""


def load_daily_signal_snapshot_bundle(snapshot_date: str, history_group_id: str | None = None) -> dict:
    """Load a best-effort historical signal bundle for one trading day."""
    resolved_group_id = ""
    with _connect() as conn:
        resolved_group_id = str(history_group_id or "").strip() or _resolve_signal_history_group_id(conn, snapshot_date)

    market = load_market_snapshot_history(snapshot_date, history_group_id=resolved_group_id or None, limit=1).get("latest", {})
    pool = load_pool_snapshot_history(snapshot_date, history_group_id=resolved_group_id or None, limit=1).get("latest", {})
    decision = load_decision_snapshot_history(snapshot_date, history_group_id=resolved_group_id or None, limit=1).get("latest", {})
    candidates = load_candidate_snapshot_history(snapshot_date, history_group_id=resolved_group_id or None, limit=1).get("latest", {})

    missing = [
        name
        for name, value in (
            ("market_snapshot", market),
            ("pool_snapshot", pool),
            ("today_decision", decision),
            ("scored_candidates", candidates),
        )
        if not value
    ]
    status = "ok" if not missing else ("partial" if len(missing) < 4 else "missing")
    return {
        "status": status,
        "snapshot_date": snapshot_date,
        "history_group_id": resolved_group_id,
        "missing_components": missing,
        "market_snapshot": market,
        "pool_snapshot": pool,
        "today_decision": decision.get("today_decision", {}) if decision else {},
        "decision_snapshot": decision,
        "scored_candidates": candidates.get("candidates", []) if candidates else [],
        "candidate_snapshot": candidates,
    }


def save_pool_snapshot_history(
    snapshot: dict,
    *,
    pipeline: str = "",
    history_group_id: str = "",
    metadata: dict | None = None,
    conn: sqlite3.Connection | None = None,
) -> dict:
    """Archive a full pool snapshot for later exact replay."""
    own_conn = conn is None
    if own_conn:
        context = _connect()
        conn = context.__enter__()
    try:
        snapshot = dict(snapshot or {})
        metadata = dict(metadata or snapshot.get("metadata", {}) or {})
        snapshot_date = str(snapshot.get("snapshot_date", metadata.get("snapshot_date", _today_str()))).strip() or _today_str()
        updated_at = str(snapshot.get("updated_at", metadata.get("updated_at", _now_ts()))).strip() or _now_ts()
        source = str(snapshot.get("source", metadata.get("source", ""))).strip()
        summary = dict(snapshot.get("summary", {}) if isinstance(snapshot.get("summary", {}), dict) else {})
        row = conn.execute(
            """
            INSERT INTO pool_snapshot_history(
              history_group_id, pipeline, snapshot_date, source, updated_at,
              summary_json, metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(history_group_id or metadata.get("history_group_id", "")).strip(),
                str(pipeline or metadata.get("pipeline", "")).strip(),
                snapshot_date,
                source,
                updated_at,
                _json_dumps(summary),
                _json_dumps(metadata),
            ),
        )
        history_id = int(row.lastrowid)
        for entry in snapshot.get("entries", []) if isinstance(snapshot.get("entries", []), list) else []:
            code = _normalize_code(entry.get("code", ""))
            if not code:
                continue
            entry_metadata = entry.get("metadata", {}) if isinstance(entry.get("metadata", {}), dict) else {}
            data_quality = str(entry.get("data_quality", entry_metadata.get("data_quality", "ok")) or "ok").strip()
            conn.execute(
                """
                INSERT INTO pool_snapshot_history_entries(
                  snapshot_id, code, name, bucket, total_score, technical_score,
                  fundamental_score, flow_score, sentiment_score, veto_triggered,
                  veto_signals_json, passed_text, note, source, added_date,
                  entry_snapshot_date, updated_at, data_quality, detail_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    history_id,
                    code,
                    str(entry.get("name", code)).strip(),
                    str(entry.get("bucket", "avoid")).strip() or "avoid",
                    round(_safe_float(entry.get("total_score", 0)), 4),
                    round(_safe_float(entry.get("technical_score", 0)), 4),
                    round(_safe_float(entry.get("fundamental_score", 0)), 4),
                    round(_safe_float(entry.get("flow_score", 0)), 4),
                    round(_safe_float(entry.get("sentiment_score", 0)), 4),
                    1 if bool(entry.get("veto_triggered", False)) else 0,
                    _json_dumps(entry.get("veto_signals", [])),
                    str(entry.get("passed_text", "")).strip(),
                    str(entry.get("note", "")).strip(),
                    str(entry.get("source", source)).strip(),
                    str(entry.get("added_date", snapshot_date)).strip(),
                    str(entry.get("snapshot_date", snapshot_date)).strip(),
                    str(entry.get("updated_at", updated_at)).strip(),
                    data_quality,
                    _json_dumps(entry),
                ),
            )
        return {
            "history_id": history_id,
            "history_group_id": str(history_group_id or metadata.get("history_group_id", "")).strip(),
            "pipeline": str(pipeline or metadata.get("pipeline", "")).strip(),
            "snapshot_date": snapshot_date,
            "updated_at": updated_at,
            "source": source,
            "summary": summary,
            "metadata": metadata,
        }
    finally:
        if own_conn:
            context.__exit__(None, None, None)


def load_pool_snapshot_history(
    snapshot_date: str | None = None,
    *,
    history_group_id: str | None = None,
    limit: int = 20,
) -> dict:
    """Load archived pool snapshots."""
    with _connect() as conn:
        where = []
        params: list[Any] = []
        if snapshot_date:
            where.append("snapshot_date = ?")
            params.append(snapshot_date)
        if history_group_id:
            where.append("history_group_id = ?")
            params.append(history_group_id)
        query = "SELECT * FROM pool_snapshot_history"
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY snapshot_date DESC, updated_at DESC, id DESC LIMIT ?"
        params.append(max(1, int(limit or 20)))
        rows = conn.execute(query, params).fetchall()
        history_ids = [int(row["id"]) for row in rows]
        entries_by_snapshot: dict[int, list[dict[str, Any]]] = {history_id: [] for history_id in history_ids}
        if history_ids:
            placeholders = ",".join("?" for _ in history_ids)
            entry_rows = conn.execute(
                f"""
                SELECT * FROM pool_snapshot_history_entries
                WHERE snapshot_id IN ({placeholders})
                ORDER BY snapshot_id, CASE bucket WHEN 'core' THEN 0 WHEN 'watch' THEN 1 ELSE 2 END, total_score DESC, code
                """,
                history_ids,
            ).fetchall()
            for entry_row in entry_rows:
                entries_by_snapshot[int(entry_row["snapshot_id"])].append(
                    _json_loads(entry_row["detail_json"], {})
                    or {
                        "code": str(entry_row["code"] or ""),
                        "name": str(entry_row["name"] or ""),
                        "bucket": str(entry_row["bucket"] or ""),
                        "total_score": _safe_float(entry_row["total_score"], 0.0),
                    }
                )

    items = []
    for row in rows:
        history_id = int(row["id"])
        entries = entries_by_snapshot.get(history_id, [])
        core_pool = [entry for entry in entries if str(entry.get("bucket", "")).strip() == "core"]
        watch_pool = [entry for entry in entries if str(entry.get("bucket", "")).strip() == "watch"]
        other_entries = [entry for entry in entries if str(entry.get("bucket", "")).strip() not in {"core", "watch"}]
        items.append({
            "history_id": history_id,
            "history_group_id": str(row["history_group_id"] or ""),
            "pipeline": str(row["pipeline"] or ""),
            "snapshot_date": str(row["snapshot_date"] or ""),
            "updated_at": str(row["updated_at"] or ""),
            "source": str(row["source"] or ""),
            "metadata": _json_loads(row["metadata_json"], {}),
            "entries": entries,
            "core_pool": core_pool,
            "watch_pool": watch_pool,
            "other_entries": other_entries,
            "summary": _json_loads(row["summary_json"], {}),
        })
    latest = items[0] if items else {}
    return {
        "snapshot_date": snapshot_date or str(latest.get("snapshot_date", "")),
        "history_group_id": history_group_id or str(latest.get("history_group_id", "")),
        "count": len(items),
        "items": items,
        "latest": latest,
    }


def save_pool_snapshot(entries: list[dict], metadata: dict | None = None, conn: sqlite3.Connection | None = None) -> dict:
    """Persist the latest pool snapshot and project it back to YAML/Obsidian."""
    own_conn = conn is None
    if own_conn:
        context = _connect()
        conn = context.__enter__()
    try:
        metadata = metadata or {}
        snapshot_date = metadata.get("snapshot_date", _today_str()) if metadata else _today_str()
        updated_at = _now_ts()
        previous_rows = conn.execute("SELECT code, name, bucket FROM pool_entries").fetchall()
        previous_entries = {
            str(row["code"]).strip(): {
                "code": str(row["code"]).strip(),
                "name": str(row["name"]).strip(),
                "bucket": str(row["bucket"]).strip(),
            }
            for row in previous_rows
        }
        conn.execute("DELETE FROM pool_entries")
        normalized_entries = []
        for entry in entries:
            bucket = str(entry.get("bucket", "avoid")).strip() or "avoid"
            code = _normalize_code(entry.get("code", ""))
            if not code:
                continue
            entry_metadata = entry.get("metadata", {}) if isinstance(entry.get("metadata", {}), dict) else {}
            data_quality = str(entry.get("data_quality", entry_metadata.get("data_quality", "ok")) or "ok").strip()
            data_missing_fields = entry.get("data_missing_fields", entry_metadata.get("data_missing_fields", []))
            if isinstance(data_missing_fields, str):
                data_missing_fields = [item.strip() for item in data_missing_fields.split(",") if item.strip()]
            elif isinstance(data_missing_fields, (list, tuple, set)):
                data_missing_fields = [str(item).strip() for item in data_missing_fields if str(item).strip()]
            else:
                data_missing_fields = []
            entry_metadata = {
                **entry_metadata,
                "data_quality": data_quality,
                "data_missing_fields": data_missing_fields,
            }
            normalized = {
                "bucket": bucket,
                "code": code,
                "name": entry.get("name", code),
                "total_score": round(_safe_float(entry.get("total_score", 0)), 1),
                "technical_score": round(_safe_float(entry.get("technical_score", 0)), 1),
                "fundamental_score": round(_safe_float(entry.get("fundamental_score", 0)), 1),
                "flow_score": round(_safe_float(entry.get("flow_score", 0)), 1),
                "sentiment_score": round(_safe_float(entry.get("sentiment_score", 0)), 1),
                "veto_triggered": bool(entry.get("veto_triggered", False)),
                "veto_signals": list(entry.get("veto_signals", [])),
                "passed_text": entry.get("passed_text") or _recommendation_for_entry(entry),
                "note": str(entry.get("note", "")).strip(),
                "source": entry.get("source", metadata.get("source", "unknown")),
                "added_date": entry.get("added_date", snapshot_date),
                "snapshot_date": entry.get("snapshot_date", snapshot_date),
                "updated_at": entry.get("updated_at", updated_at),
                "data_quality": data_quality,
                "data_missing_fields": data_missing_fields,
                "metadata": entry_metadata,
            }
            normalized_entries.append(normalized)
            conn.execute(
                """
                INSERT INTO pool_entries(
                  code, name, bucket, total_score, technical_score, fundamental_score,
                  flow_score, sentiment_score, veto_triggered, veto_signals_json,
                  passed_text, note, source, added_date, snapshot_date, updated_at, metadata_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized["code"],
                    normalized["name"],
                    normalized["bucket"],
                    normalized["total_score"],
                    normalized["technical_score"],
                    normalized["fundamental_score"],
                    normalized["flow_score"],
                    normalized["sentiment_score"],
                    1 if normalized["veto_triggered"] else 0,
                    _json_dumps(normalized["veto_signals"]),
                    normalized["passed_text"],
                    normalized["note"],
                    normalized["source"],
                    normalized["added_date"],
                    normalized["snapshot_date"],
                    normalized["updated_at"],
                    _json_dumps(normalized["metadata"]),
                ),
            )

        _record_pool_actions(
            conn,
            previous_entries=previous_entries,
            current_entries=normalized_entries,
            snapshot_date=snapshot_date,
            updated_at=updated_at,
            source=metadata.get("source", "unknown"),
        )
        snapshot = {
            "snapshot_date": snapshot_date,
            "updated_at": updated_at,
            "source": metadata.get("source", "unknown"),
            "metadata": metadata,
            "core_pool": [item for item in normalized_entries if item["bucket"] == "core"],
            "watch_pool": [item for item in normalized_entries if item["bucket"] == "watch"],
            "other_entries": [item for item in normalized_entries if item["bucket"] not in {"core", "watch"}],
        }
        snapshot["entries"] = normalized_entries
        snapshot["summary"] = {
            "core_count": len(snapshot["core_pool"]),
            "watch_count": len(snapshot["watch_pool"]),
            "other_count": len(snapshot["other_entries"]),
        }
        snapshot["action_history_summary"] = _latest_pool_action_summary(conn, snapshot_date=snapshot_date)
        projection_paths = {}
        if own_conn:
            projection_paths["stocks_yaml_path"] = _project_stocks_yaml(snapshot)
            try:
                vault = ObsidianVault()
                projection_paths.update(vault.sync_pool_projection(
                    normalized_entries,
                    {
                        **metadata,
                        "source": snapshot["source"],
                        "updated_at": snapshot_date,
                    },
                ))
            except Exception as exc:
                LOGGER.warning(f"[state] pool projection sync failed: {exc}")
            _meta_set(conn, "pool_projection_paths", _json_dumps(projection_paths))
            _meta_set(conn, "pool_snapshot_meta", _json_dumps({"snapshot_date": snapshot_date, **metadata}))
        snapshot["projection_paths"] = projection_paths
        snapshot["db_path"] = str(_db_path())
        history_record = save_pool_snapshot_history(
            snapshot,
            pipeline=str(metadata.get("pipeline", "")).strip(),
            history_group_id=str(metadata.get("history_group_id", "")).strip(),
            metadata=metadata,
            conn=conn,
        )
        snapshot["history_id"] = history_record.get("history_id")
        snapshot["history_group_id"] = history_record.get("history_group_id", "")
        return snapshot
    finally:
        if own_conn:
            context.__exit__(None, None, None)


def load_pool_snapshot() -> dict:
    """Return the latest structured pool snapshot."""
    with _connect() as conn:
        _ensure_bootstrapped(conn)
        rows = conn.execute(
            "SELECT * FROM pool_entries ORDER BY CASE bucket WHEN 'core' THEN 0 WHEN 'watch' THEN 1 ELSE 2 END, total_score DESC, code"
        ).fetchall()
        entries = []
        for row in rows:
            entry = dict(row)
            entry["veto_triggered"] = bool(entry["veto_triggered"])
            entry["veto_signals"] = _json_loads(entry.pop("veto_signals_json", "[]"), [])
            entry["metadata"] = _json_loads(entry.pop("metadata_json", "{}"), {})
            metadata = entry["metadata"] if isinstance(entry["metadata"], dict) else {}
            entry["data_quality"] = str(metadata.get("data_quality", "ok") or "ok").strip()
            missing_fields = metadata.get("data_missing_fields", [])
            if isinstance(missing_fields, str):
                missing_fields = [item.strip() for item in missing_fields.split(",") if item.strip()]
            elif isinstance(missing_fields, (list, tuple, set)):
                missing_fields = [str(item).strip() for item in missing_fields if str(item).strip()]
            else:
                missing_fields = []
            entry["data_missing_fields"] = missing_fields
            entries.append(entry)
        meta = _json_loads(_meta_get(conn, "pool_snapshot_meta", "{}"), {})
        action_history_summary = _latest_pool_action_summary(conn, snapshot_date=meta.get("snapshot_date", _today_str()))
    core_pool = [entry for entry in entries if entry["bucket"] == "core"]
    watch_pool = [entry for entry in entries if entry["bucket"] == "watch"]
    other_entries = [entry for entry in entries if entry["bucket"] not in {"core", "watch"}]
    return {
        "snapshot_date": meta.get("snapshot_date", _today_str()),
        "updated_at": max((entry.get("updated_at", "") for entry in entries), default=""),
        "source": meta.get("source", ""),
        "metadata": meta,
        "entries": entries,
        "core_pool": core_pool,
        "watch_pool": watch_pool,
        "other_entries": other_entries,
        "summary": {
            "core_count": len(core_pool),
            "watch_count": len(watch_pool),
            "other_count": len(other_entries),
        },
        "action_history_summary": action_history_summary,
    }


def _classify_pool_action(previous_bucket: str, current_bucket: str) -> str:
    previous_bucket = str(previous_bucket or "").strip()
    current_bucket = str(current_bucket or "").strip()
    if not previous_bucket and current_bucket == "core":
        return "promote"
    if not previous_bucket and current_bucket:
        return "keep"
    if previous_bucket == current_bucket:
        return "keep"
    if current_bucket == "core" and previous_bucket != "core":
        return "promote"
    if previous_bucket == "core" and current_bucket in {"watch", "avoid"}:
        return "demote"
    if current_bucket in {"avoid", ""}:
        return "remove"
    return "keep"


def _record_pool_actions(conn: sqlite3.Connection, previous_entries: dict[str, dict],
                         current_entries: list[dict], snapshot_date: str,
                         updated_at: str, source: str) -> None:
    current_map = {str(item.get("code", "")).strip(): item for item in current_entries if str(item.get("code", "")).strip()}
    all_codes = sorted(set(previous_entries.keys()) | set(current_map.keys()))
    conn.execute("DELETE FROM pool_actions WHERE snapshot_date = ?", (snapshot_date,))
    for code in all_codes:
        previous = previous_entries.get(code, {})
        current = current_map.get(code, {})
        previous_bucket = str(previous.get("bucket", "")).strip()
        current_bucket = str(current.get("bucket", "")).strip()
        action = _classify_pool_action(previous_bucket, current_bucket)
        name = current.get("name") or previous.get("name") or code
        reason_text = str(current.get("note", "") or previous.get("note", "")).strip()
        metadata = {
            "previous_bucket": previous_bucket,
            "current_bucket": current_bucket,
            "total_score": current.get("total_score", 0.0),
            "veto_triggered": bool(current.get("veto_triggered", False)),
        }
        conn.execute(
            """
            INSERT INTO pool_actions(
              snapshot_date, updated_at, code, name, action, previous_bucket,
              current_bucket, source, reason_text, metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot_date,
                updated_at,
                code,
                name,
                action,
                previous_bucket,
                current_bucket,
                source,
                reason_text,
                _json_dumps(metadata),
            ),
        )


def _latest_pool_action_summary(conn: sqlite3.Connection, snapshot_date: str) -> dict:
    rows = conn.execute(
        """
        SELECT action, COUNT(*) AS count
        FROM pool_actions
        WHERE snapshot_date = ?
        GROUP BY action
        ORDER BY action
        """,
        (snapshot_date,),
    ).fetchall()
    counts = {str(row["action"]).strip(): int(row["count"]) for row in rows}
    return {
        "snapshot_date": snapshot_date,
        "action_count": sum(counts.values()),
        "action_counts": counts,
    }


def load_pool_action_history(limit: int = 50, snapshot_date: str | None = None) -> dict:
    with _connect() as conn:
        _ensure_bootstrapped(conn)
        if snapshot_date:
            rows = conn.execute(
                """
                SELECT * FROM pool_actions
                WHERE snapshot_date = ?
                ORDER BY updated_at DESC, id DESC
                LIMIT ?
                """,
                (snapshot_date, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM pool_actions
                ORDER BY snapshot_date DESC, updated_at DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
    actions = []
    action_counts: dict[str, int] = {}
    for row in rows:
        item = dict(row)
        item["metadata"] = _json_loads(item.pop("metadata_json", "{}"), {})
        actions.append(item)
        action = str(item.get("action", "")).strip() or "unknown"
        action_counts[action] = action_counts.get(action, 0) + 1
    return {
        "snapshot_date": snapshot_date or (actions[0]["snapshot_date"] if actions else ""),
        "action_count": len(actions),
        "action_counts": action_counts,
        "actions": actions,
        "db_path": str(_db_path()),
    }


def record_trade_event(event: dict) -> dict:
    """Append one structured trade event."""
    payload = {
        "external_id": event.get("external_id"),
        "scope": event.get("scope", PRIMARY_SCOPE),
        "market": event.get("market", ""),
        "code": _normalize_code(event.get("code", "")),
        "name": event.get("name", ""),
        "side": event.get("side", ""),
        "event_type": event.get("event_type", event.get("side", "")),
        "shares": _safe_int(event.get("shares", 0)),
        "price": _safe_float(event.get("price", 0)),
        "amount": _safe_float(event.get("amount", 0)),
        "realized_pnl": _safe_float(event.get("realized_pnl", 0)),
        "event_date": event.get("event_date", _today_str()),
        "reason_code": event.get("reason_code") or _reason_code_from_text(
            event.get("side", ""), event.get("reason_text", ""), event.get("scope", PRIMARY_SCOPE)
        ),
        "reason_text": event.get("reason_text", ""),
        "source": event.get("source", "runtime"),
        "metadata": event.get("metadata", {}),
        "created_at": event.get("created_at", _now_ts()),
    }
    with _connect() as conn:
        _ensure_bootstrapped(conn)
        _upsert_trade_event(conn, payload)
    return payload


def load_activity_summary(window: str | int = "week", scope: str = PRIMARY_SCOPE) -> dict:
    """Return structured trade activity for the requested scope and time window."""
    end_date = date.today()
    if window == "week":
        start_date = end_date - timedelta(days=end_date.weekday())
    elif isinstance(window, int):
        start_date = end_date - timedelta(days=max(window - 1, 0))
    else:
        start_date = end_date - timedelta(days=6)

    with _connect() as conn:
        _ensure_bootstrapped(conn)
        rows = conn.execute(
            """
            SELECT * FROM trade_events
            WHERE scope = ? AND event_date >= ? AND event_date <= ?
            ORDER BY event_date ASC, id ASC
            """,
            (scope, start_date.isoformat(), end_date.isoformat()),
        ).fetchall()

    events = []
    for row in rows:
        event = dict(row)
        event["metadata"] = _json_loads(event.pop("metadata_json", "{}"), {})
        events.append(event)

    buy_events = [event for event in events if event.get("side") == "buy"]
    sell_events = [event for event in events if event.get("side") == "sell"]
    return {
        "scope": scope,
        "window": window,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "trade_events": events,
        "buy_count": len(buy_events),
        "sell_count": len(sell_events),
        "weekly_buy_count": len(buy_events),
        "weekly_sell_count": len(sell_events),
        "weekly_buys": len(buy_events),
        "trade_count": len(events),
        "total_trades": len(buy_events) + len(sell_events),
        "realized_pnl": round(sum(_safe_float(event.get("realized_pnl", 0)) for event in sell_events), 2),
        "source": "structured_ledger",
    }


def _trade_review_tags(reason_codes: list[str]) -> list[str]:
    tags = []
    for code in reason_codes:
        text = str(code or "").strip().upper()
        if not text:
            continue
        if text.startswith("RISK_") and "risk" not in tags:
            tags.append("risk")
        elif text.startswith("POOL_") and "pool" not in tags:
            tags.append("pool")
        elif text.startswith("PAPER_RECONCILE") and "reconcile" not in tags:
            tags.append("reconcile")
        elif text.startswith("BUY_") and "entry" not in tags:
            tags.append("entry")
        elif text.startswith("TRADE_") and "trade" not in tags:
            tags.append("trade")
    return tags


def _trade_holding_days_bucket(holding_days: int) -> str:
    if holding_days <= 3:
        return "0-3天"
    if holding_days <= 7:
        return "4-7天"
    if holding_days <= 14:
        return "8-14天"
    if holding_days <= 30:
        return "15-30天"
    return "31天+"


def _trade_exit_style(reason_codes: list[str]) -> tuple[str, str]:
    normalized = _dedupe(
        [
            normalize_reason_code(code, category="trade")
            for code in reason_codes
            if str(code or "").strip()
        ]
    )
    for code in normalized:
        if code.startswith("RISK_") or code in {
            "TRADE_PORTFOLIO_DAILY_LOSS_LIMIT",
            "TRADE_CONSECUTIVE_LOSS_COOLDOWN",
            "TRADE_WEEKLY_BUY_LIMIT",
            "TRADE_EXPOSURE_LIMIT",
            "TRADE_HOLDING_LIMIT",
        }:
            return "risk", code
    for code in normalized:
        if code.startswith("POOL_"):
            return "pool", code
    for code in normalized:
        if (
            "RECONCILE" in code
            or "MANUAL" in code
            or "REPLY" in code
            or code.startswith("DISCORD_")
        ):
            return "manual", code
    return "other", ""


def _trade_rule_compliance(reason_codes: list[str]) -> dict:
    normalized = _dedupe(
        [
            normalize_reason_code(code, category="trade")
            for code in reason_codes
            if str(code or "").strip()
        ]
    )

    has_drift = any(code == "TRADE_PAPER_RECONCILE_DRIFT" for code in normalized)
    has_reconcile = any(
        code.startswith("PAPER_RECONCILE_")
        or code in {
            "TRADE_PAPER_RECONCILE_OPEN",
            "TRADE_PAPER_RECONCILE_FLATTEN",
            "TRADE_PAPER_RECONCILE_ADD",
            "TRADE_PAPER_RECONCILE_REDUCE",
        }
        for code in normalized
    )
    has_manual_override = any(
        "MANUAL" in code or "REPLY" in code or code.startswith("DISCORD_")
        for code in normalized
    )
    rule_break_count = sum(int(flag) for flag in (has_drift, has_reconcile, has_manual_override))
    if has_drift:
        status = "drift"
    elif has_reconcile:
        status = "reconcile"
    elif has_manual_override:
        status = "manual_override"
    else:
        status = "compliant"
    return {
        "status": status,
        "has_drift": has_drift,
        "has_reconcile": has_reconcile,
        "has_manual_override": has_manual_override,
        "reason_codes": [
            code
            for code in normalized
            if code == "TRADE_PAPER_RECONCILE_DRIFT"
            or code.startswith("PAPER_RECONCILE_")
            or "MANUAL" in code
            or "REPLY" in code
            or code.startswith("DISCORD_")
        ],
        "rule_break_count": rule_break_count,
    }


def _trade_factor_summary(reason_codes: list[str], reason_texts: list[str] | None = None) -> list[dict]:
    normalized = _dedupe(
        [
            normalize_reason_code(code, category="trade")
            for code in reason_codes
            if str(code or "").strip()
        ]
    )
    texts = " ".join(str(item or "") for item in (reason_texts or [])).lower()
    factors = []

    def add_factor(code: str, label: str, source: str) -> None:
        if any(item["code"] == code for item in factors):
            return
        factors.append({"code": code, "label": label, "source": source})

    for code in normalized:
        if code.startswith("BUY_"):
            add_factor(code, "入场信号", "reason_code")
        elif code.startswith("RISK_") or code.startswith("TRADE_"):
            add_factor(code, "风控出场", "reason_code")
        elif code.startswith("POOL_"):
            add_factor(code, "池子变化", "reason_code")
        elif "RECONCILE" in code:
            add_factor(code, "账实对账", "reason_code")
        elif "MANUAL" in code or "REPLY" in code or code.startswith("DISCORD_"):
            add_factor(code, "人工干预", "reason_code")

    if "评分" in texts or "score" in texts:
        add_factor("FACTOR_SCORE", "评分驱动", "reason_text")
    if "核心池" in texts or "core" in texts:
        add_factor("FACTOR_CORE_POOL", "核心池驱动", "reason_text")
    if "止盈" in texts or "take profit" in texts:
        add_factor("FACTOR_TAKE_PROFIT", "止盈驱动", "reason_text")
    if "止损" in texts or "stop loss" in texts:
        add_factor("FACTOR_STOP_LOSS", "止损驱动", "reason_text")
    return factors


def _trade_pnl_attribution(trade: dict) -> dict:
    pnl = _safe_float(trade.get("realized_pnl", 0.0), 0.0)
    entry_price = _safe_float(trade.get("entry_price", 0.0), 0.0)
    exit_price = _safe_float(trade.get("exit_price", 0.0), 0.0)
    pnl_pct = round(((exit_price - entry_price) / entry_price) * 100, 2) if entry_price else 0.0
    mfe = trade.get("mfe_pct")
    mae = trade.get("mae_pct")
    capture_pct = None
    if mfe not in (None, "") and _safe_float(mfe, 0.0) > 0:
        capture_pct = round(max(pnl_pct, 0.0) / _safe_float(mfe, 0.0) * 100, 1)
    if pnl > 0:
        outcome = "win"
    elif pnl < 0:
        outcome = "loss"
    else:
        outcome = "flat"
    return {
        "outcome": outcome,
        "realized_pnl": round(pnl, 2),
        "pnl_pct": pnl_pct,
        "mfe_pct": mfe,
        "mae_pct": mae,
        "mfe_capture_pct": capture_pct,
        "holding_days_bucket": trade.get("holding_days_bucket", ""),
        "exit_style": trade.get("exit_style", ""),
    }


def _trade_rule_deviation(rule_compliance: dict) -> dict:
    status = str(rule_compliance.get("status", "compliant")).strip() or "compliant"
    return {
        "status": status,
        "deviation_count": int(rule_compliance.get("rule_break_count", 0) or 0),
        "reason_codes": list(rule_compliance.get("reason_codes", [])),
        "explanation": "规则内交易" if status == "compliant" else f"存在规则偏离: {status}",
    }


def _portfolio_attribution_summary(closed_trades: list[dict]) -> dict:
    entry_factor_counts: dict[str, int] = {}
    exit_factor_counts: dict[str, int] = {}
    pnl_by_exit_style: dict[str, float] = {}
    deviation_counts: dict[str, int] = {}
    for trade in closed_trades:
        for factor in trade.get("entry_factors", []):
            code = str(factor.get("code", "")).strip()
            if code:
                entry_factor_counts[code] = entry_factor_counts.get(code, 0) + 1
        for factor in trade.get("exit_factors", []):
            code = str(factor.get("code", "")).strip()
            if code:
                exit_factor_counts[code] = exit_factor_counts.get(code, 0) + 1
        exit_style = str(trade.get("exit_style", "other")).strip() or "other"
        pnl_by_exit_style[exit_style] = round(
            pnl_by_exit_style.get(exit_style, 0.0) + _safe_float(trade.get("realized_pnl", 0.0), 0.0),
            2,
        )
        deviation = trade.get("rule_deviation", {}) if isinstance(trade.get("rule_deviation", {}), dict) else {}
        status = str(deviation.get("status", "unknown")).strip() or "unknown"
        deviation_counts[status] = deviation_counts.get(status, 0) + 1
    return {
        "closed_trade_count": len(closed_trades),
        "entry_factor_counts": entry_factor_counts,
        "exit_factor_counts": exit_factor_counts,
        "pnl_by_exit_style": pnl_by_exit_style,
        "rule_deviation_counts": deviation_counts,
    }


def _estimate_trade_excursion(
    entry_price: float,
    exit_price: float,
    realized_pnl: float,
    exit_reason_codes: list[str],
) -> tuple[float | None, float | None, str]:
    entry = _safe_float(entry_price, 0.0)
    if entry <= 0:
        return None, None, "pending_market_history"

    exit_value = _safe_float(exit_price, entry)
    pnl = _safe_float(realized_pnl, 0.0)
    pnl_pct = ((exit_value - entry) / entry) if entry else 0.0
    codes = [str(code or "").strip().upper() for code in exit_reason_codes if str(code or "").strip()]

    high_price = max(entry, exit_value)
    low_price = min(entry, exit_value)
    if any("TAKE_PROFIT" in code for code in codes):
        high_price = max(high_price, entry * (1 + max(abs(pnl_pct) * 1.2, 0.04)))
        low_price = min(low_price, entry * (1 - min(max(abs(pnl_pct) * 0.35, 0.01), 0.03)))
    elif any("STOP_LOSS" in code for code in codes):
        high_price = max(high_price, entry * (1 + min(max(abs(pnl_pct) * 0.25, 0.005), 0.02)))
        low_price = min(low_price, entry * (1 - max(abs(pnl_pct) * 1.15, 0.03)))
    elif pnl >= 0:
        high_price = max(high_price, entry * (1 + max(abs(pnl_pct) * 1.05, 0.03)))
        low_price = min(low_price, entry * (1 - min(max(abs(pnl_pct) * 0.3, 0.01), 0.025)))
    else:
        high_price = max(high_price, entry * (1 + min(max(abs(pnl_pct) * 0.4, 0.01), 0.03)))
        low_price = min(low_price, entry * (1 - max(abs(pnl_pct) * 1.05, 0.025)))

    mfe_pct = round(((high_price - entry) / entry) * 100, 2)
    mae_pct = round(((low_price - entry) / entry) * 100, 2)
    return mfe_pct, mae_pct, "proxy_market_history"


def _load_trade_history_rows(code: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
    cache_key = f"{_normalize_code(code)}_{start_date}_{end_date}"
    cached = load_json_cache("trade_history", cache_key, max_age_seconds=86400 * 7)
    if cached and isinstance(cached.get("data"), list):
        return [row for row in cached["data"] if isinstance(row, dict)]

    try:
        from scripts.engine.technical import _get_hist_data
    except Exception:
        return []

    if not code or not start_date or not end_date:
        return []

    try:
        df = _get_hist_data(str(code), start_date, end_date)
    except TypeError:
        try:
            df = _get_hist_data(str(code), 120)
        except Exception:
            return []
    except Exception:
        return []

    if df is None:
        return []

    rows: list[dict[str, Any]] = []
    try:
        if hasattr(df, "to_dict"):
            records = df.to_dict(orient="records")
        else:
            records = list(df)
    except Exception:
        return []

    for row in records:
        if not isinstance(row, dict):
            continue
        row_date = str(row.get("日期", row.get("date", ""))).strip()[:10]
        if not row_date or row_date < start_date or row_date > end_date:
            continue
        rows.append(row)
    if rows:
        save_json_cache("trade_history", cache_key, rows, meta={"code": _normalize_code(code), "start": start_date, "end": end_date})
    return rows


def _compute_actual_trade_excursion(
    *,
    code: str,
    entry_date: str,
    exit_date: str,
    entry_price: float,
    exit_price: float,
) -> tuple[float | None, float | None, str]:
    entry = _safe_float(entry_price, 0.0)
    if entry <= 0 or not code or not entry_date or not exit_date:
        return None, None, "pending_market_history"

    rows = _load_trade_history_rows(code, entry_date, exit_date)
    if not rows:
        return None, None, "pending_market_history"

    highs = []
    lows = []
    for row in rows:
        high = row.get("最高", row.get("high", row.get("High")))
        low = row.get("最低", row.get("low", row.get("Low")))
        if high not in (None, ""):
            highs.append(_safe_float(high, 0.0))
        if low not in (None, ""):
            lows.append(_safe_float(low, 0.0))

    exit_value = _safe_float(exit_price, entry)
    if exit_value > 0:
        highs.append(exit_value)
        lows.append(exit_value)
    highs.append(entry)
    lows.append(entry)

    valid_highs = [value for value in highs if value > 0]
    valid_lows = [value for value in lows if value > 0]
    if not valid_highs or not valid_lows:
        return None, None, "pending_market_history"

    mfe_pct = round(((max(valid_highs) - entry) / entry) * 100, 2)
    mae_pct = round(((min(valid_lows) - entry) / entry) * 100, 2)
    return mfe_pct, mae_pct, "actual_market_history"


def load_trade_review(window: int = 90, scope: str = PRIMARY_SCOPE) -> dict:
    activity = load_activity_summary(window, scope=scope)
    trade_events = list(activity.get("trade_events", []))
    open_positions: dict[str, dict] = {}
    closed_trades: list[dict] = []

    def _event_date(value: dict) -> str:
        return str(value.get("event_date") or value.get("trade_date") or value.get("date") or "").strip()[:10]

    def _event_side(value: dict) -> str:
        return str(value.get("side") or value.get("action") or "").strip().lower()

    for event in sorted(trade_events, key=lambda item: (str(item.get("event_date", "")), str(item.get("created_at", "")))):
        code = _normalize_code(event.get("code", ""))
        if not code:
            continue
        side = _event_side(event)
        shares = _safe_int(event.get("shares", 0), 0)
        if shares <= 0:
            continue
        reason_code = str(event.get("reason_code", "")).strip()
        reason_text = str(event.get("reason_text", event.get("reason", ""))).strip()
        event_date = _event_date(event)
        price = _safe_float(event.get("price", 0.0), 0.0)

        if side == "buy":
            position = open_positions.setdefault(
                code,
                {
                    "code": code,
                    "name": event.get("name", code),
                    "entry_date": event_date,
                    "entry_price": price,
                    "entry_reason_code": reason_code,
                    "entry_reason_text": reason_text,
                    "entry_reason_codes": [],
                    "buy_count": 0,
                    "sell_count": 0,
                    "shares_open": 0,
                    "cost_amount": 0.0,
                    "realized_pnl": 0.0,
                    "exit_reason_codes": [],
                    "exit_reason_texts": [],
                    "exit_dates": [],
                    "metadata": {"mfe_pct": None, "mae_pct": None},
                },
            )
            if position["shares_open"] == 0:
                position["entry_date"] = event_date
                position["entry_price"] = price
                position["entry_reason_code"] = reason_code
                position["entry_reason_text"] = reason_text
            position["name"] = event.get("name", position["name"])
            position["buy_count"] += 1
            position["shares_open"] += shares
            position["cost_amount"] += round(price * shares, 2)
            if reason_code and reason_code not in position["entry_reason_codes"]:
                position["entry_reason_codes"].append(reason_code)
            continue

        if side != "sell":
            continue

        position = open_positions.get(code)
        if not position:
            continue
        close_qty = min(shares, position["shares_open"])
        if close_qty <= 0:
            continue
        position["sell_count"] += 1
        position["shares_open"] -= close_qty
        avg_cost = (position["cost_amount"] / (position["shares_open"] + close_qty)) if (position["shares_open"] + close_qty) > 0 else 0.0
        position["cost_amount"] = max(position["cost_amount"] - avg_cost * close_qty, 0.0)
        position["realized_pnl"] += _safe_float(event.get("realized_pnl", 0.0), 0.0)
        if reason_code and reason_code not in position["exit_reason_codes"]:
            position["exit_reason_codes"].append(reason_code)
        if reason_text and reason_text not in position["exit_reason_texts"]:
            position["exit_reason_texts"].append(reason_text)
        if event_date:
            position["exit_dates"].append(event_date)

            if position["shares_open"] == 0:
                hold_days = 0
                if position["entry_date"] and event_date:
                    try:
                        hold_days = (
                        datetime.strptime(event_date, "%Y-%m-%d").date()
                        - datetime.strptime(position["entry_date"], "%Y-%m-%d").date()
                    ).days
                    except Exception:
                        hold_days = 0
            holding_days_bucket = _trade_holding_days_bucket(hold_days)
            entry_tags = _trade_review_tags(position["entry_reason_codes"])
            exit_tags = _trade_review_tags(position["exit_reason_codes"])
            all_reason_codes = list(position["entry_reason_codes"]) + list(position["exit_reason_codes"])
            exit_style, exit_style_code = _trade_exit_style(position["exit_reason_codes"])
            rule_compliance = _trade_rule_compliance(all_reason_codes)
            entry_factors = _trade_factor_summary(
                position["entry_reason_codes"],
                [position.get("entry_reason_text", "")],
            )
            exit_factors = _trade_factor_summary(
                position["exit_reason_codes"],
                position["exit_reason_texts"],
            )
            mfe_pct, mae_pct, excursion_source = _compute_actual_trade_excursion(
                code=code,
                entry_date=position["entry_date"],
                exit_date=event_date,
                entry_price=position["entry_price"],
                exit_price=price,
            )
            if mfe_pct is None or mae_pct is None:
                mfe_pct, mae_pct, excursion_source = _estimate_trade_excursion(
                    entry_price=position["entry_price"],
                    exit_price=price,
                    realized_pnl=position["realized_pnl"],
                    exit_reason_codes=position["exit_reason_codes"],
                )
            trade_payload = {
                "code": code,
                "name": position["name"],
                "entry_date": position["entry_date"],
                "exit_date": event_date,
                "holding_days": hold_days,
                "holding_days_bucket": holding_days_bucket,
                "entry_price": round(position["entry_price"], 3),
                "exit_price": round(price, 3),
                "buy_count": position["buy_count"],
                "sell_count": position["sell_count"],
                "entry_reason_code": position["entry_reason_code"],
                "entry_reason_codes": position["entry_reason_codes"],
                "entry_reason_text": position["entry_reason_text"],
                "entry_factors": entry_factors,
                "exit_reason_codes": position["exit_reason_codes"],
                "exit_reason_texts": position["exit_reason_texts"],
                "exit_factors": exit_factors,
                "realized_pnl": round(position["realized_pnl"], 2),
                "rule_tags": sorted(set(entry_tags + exit_tags)),
                "exit_style": exit_style,
                "exit_style_reason_code": exit_style_code,
                "rule_compliance": rule_compliance,
                "rule_deviation": _trade_rule_deviation(rule_compliance),
                "rule_break_count": rule_compliance["rule_break_count"],
                "mfe_pct": mfe_pct,
                "mae_pct": mae_pct,
                "excursion_source": excursion_source,
            }
            trade_payload["pnl_attribution"] = _trade_pnl_attribution(trade_payload)
            closed_trades.append(
                trade_payload
            )
            open_positions.pop(code, None)

    total_realized_pnl = round(sum(_safe_float(item.get("realized_pnl", 0.0), 0.0) for item in closed_trades), 2)
    winners = [item for item in closed_trades if _safe_float(item.get("realized_pnl", 0.0), 0.0) > 0]
    losers = [item for item in closed_trades if _safe_float(item.get("realized_pnl", 0.0), 0.0) < 0]
    holding_days_values = [int(item.get("holding_days", 0) or 0) for item in closed_trades]
    avg_holding_days = round(sum(holding_days_values) / len(holding_days_values), 1) if holding_days_values else 0.0
    avg_win = round(
        sum(_safe_float(item.get("realized_pnl", 0.0), 0.0) for item in winners) / len(winners),
        2,
    ) if winners else 0.0
    avg_loss = round(
        sum(_safe_float(item.get("realized_pnl", 0.0), 0.0) for item in losers) / len(losers),
        2,
    ) if losers else 0.0
    mfe_values = [_safe_float(item.get("mfe_pct"), 0.0) for item in closed_trades if item.get("mfe_pct") is not None]
    mae_values = [_safe_float(item.get("mae_pct"), 0.0) for item in closed_trades if item.get("mae_pct") is not None]
    actual_excursion_count = sum(1 for item in closed_trades if item.get("excursion_source") == "actual_market_history")
    rule_break_count = sum(_safe_int(item.get("rule_break_count", 0), 0) for item in closed_trades)
    summary_stats = {
        "avg_holding_days": avg_holding_days,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "rule_break_count": rule_break_count,
        "avg_mfe_pct": round(sum(mfe_values) / len(mfe_values), 2) if mfe_values else None,
        "avg_mae_pct": round(sum(mae_values) / len(mae_values), 2) if mae_values else None,
        "actual_excursion_coverage_pct": round(actual_excursion_count / len(closed_trades) * 100, 1) if closed_trades else 0.0,
    }
    attribution_summary = _portfolio_attribution_summary(closed_trades)
    summary_stats["portfolio_attribution"] = attribution_summary
    excursion_sources = {str(item.get("excursion_source", "")).strip() for item in closed_trades if str(item.get("excursion_source", "")).strip()}
    if not mfe_values:
        mfe_mae_status = "pending_market_history"
    elif excursion_sources == {"actual_market_history"}:
        mfe_mae_status = "actual_market_history"
    elif "actual_market_history" in excursion_sources:
        mfe_mae_status = "mixed_market_history"
    else:
        mfe_mae_status = "proxy_market_history"
    return {
        "scope": scope,
        "window": window,
        "closed_trade_count": len(closed_trades),
        "win_count": len(winners),
        "loss_count": len(losers),
        "win_rate": round((len(winners) / len(closed_trades) * 100), 1) if closed_trades else 0.0,
        "total_realized_pnl": total_realized_pnl,
        "open_position_count": len(open_positions),
        "closed_trades": closed_trades,
        "open_positions": list(open_positions.values()),
        "summary_stats": summary_stats,
        "portfolio_attribution_summary": attribution_summary,
        "source": activity.get("source", "structured_ledger"),
        "mfe_mae_status": mfe_mae_status,
    }


def audit_state() -> dict:
    """Check whether pool projections are in sync with the structured ledger."""
    snapshot = load_pool_snapshot()
    expected = {
        "core": {
            entry["code"]: round(float(entry.get("total_score", 0) or 0), 1)
            for entry in snapshot.get("core_pool", [])
        },
        "watch": {
            entry["code"]: round(float(entry.get("total_score", 0) or 0), 1)
            for entry in snapshot.get("watch_pool", [])
        },
    }

    stocks_cfg = get_stocks()
    config_view = {
        "core": {
            _normalize_code(item.get("code", "")): round(_safe_float(item.get("score", 0)), 1)
            for item in stocks_cfg.get("core_pool", [])
        },
        "watch": {
            _normalize_code(item.get("code", "")): round(_safe_float(item.get("score", 0)), 1)
            for item in stocks_cfg.get("watch_pool", [])
        },
    }
    def _filtered_md_scores(relative_path: str) -> dict:
        scores = {}
        for row in _load_md_rows(relative_path):
            raw_code = str(row.get("代码", "")).strip()
            code = _normalize_code(raw_code)
            if not code or code in {"—", "-", "N/A"}:
                continue
            scores[code] = round(_safe_float(row.get("四维总分", row.get("总分", 0)), 0.0), 1)
        return scores

    md_view = {
        "core": _filtered_md_scores("04-选股/核心池.md"),
        "watch": _filtered_md_scores("04-选股/观察池.md"),
    }

    def _compare_pool_view(view: dict[str, dict[str, float]], expected_view: dict[str, dict[str, float]]) -> dict:
        mismatches = []
        missing_codes = []
        extra_codes = []
        score_mismatches = []
        bucket_mismatches = []
        expected_bucket_by_code = {
            code: bucket
            for bucket, rows in expected_view.items()
            for code in rows.keys()
        }
        actual_bucket_by_code = {
            code: bucket
            for bucket, rows in view.items()
            for code in rows.keys()
        }
        for code, expected_bucket in sorted(expected_bucket_by_code.items()):
            actual_bucket = actual_bucket_by_code.get(code, "")
            if not actual_bucket:
                missing_codes.append(code)
                continue
            if actual_bucket != expected_bucket:
                bucket_mismatches.append({
                    "code": code,
                    "expected": expected_bucket,
                    "actual": actual_bucket,
                })
                continue
            expected_score = expected_view[expected_bucket][code]
            actual_score = view[actual_bucket][code]
            if actual_score != expected_score:
                score_mismatches.append({
                    "code": code,
                    "bucket": expected_bucket,
                    "expected": expected_score,
                    "actual": actual_score,
                })
        for code in sorted(set(actual_bucket_by_code.keys()) - set(expected_bucket_by_code.keys())):
            extra_codes.append(code)
        for bucket in ("core", "watch"):
            if view[bucket] != expected_view[bucket]:
                mismatches.append({
                    "bucket": bucket,
                    "expected": expected_view[bucket],
                    "actual": view[bucket],
                })
        return {
            "ok": not (missing_codes or extra_codes or score_mismatches or bucket_mismatches),
            "mismatches": mismatches,
            "missing_codes": missing_codes,
            "extra_codes": extra_codes,
            "score_mismatches": score_mismatches,
            "bucket_mismatches": bucket_mismatches,
        }

    checks = {}
    overall_ok = True
    for label, view in (("stocks_yaml", config_view), ("obsidian_projection", md_view)):
        check = _compare_pool_view(view, expected)
        checks[label] = check
        overall_ok = overall_ok and check["ok"]

    return {
        "status": "ok" if overall_ok else "drift",
        "snapshot_date": snapshot.get("snapshot_date", _today_str()),
        "checks": checks,
    }
