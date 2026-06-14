"""
market/health.py — 数据源健康聚合。

基于 market_observations 的最近观测时间做轻量健康判断。
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from astock_trading.platform.time import is_market_weekday


@dataclass(frozen=True)
class DataSourceExpectation:
    name: str
    kinds: tuple[str, ...]
    max_age_hours: int
    required: bool = False
    min_payload_count: int = 1


DEFAULT_EXPECTATIONS = (
    DataSourceExpectation("hot_stocks", ("hot_stocks",), 24, True),
    DataSourceExpectation("northbound_realtime", ("northbound_realtime",), 24, True),
    DataSourceExpectation("baidu_fund_flow", ("fund_flow", "flow"), 24, True),
    DataSourceExpectation("industry_comparison", ("industry_comparison",), 72, False),
    DataSourceExpectation("announcements", ("announcements", "market_announcements"), 72, False),
    DataSourceExpectation("research_reports", ("research_reports",), 168, False),
    DataSourceExpectation("stock_news", ("stock_news",), 72, False),
    DataSourceExpectation("basic_info", ("basic_info",), 168, False),
    DataSourceExpectation("financial", ("financial",), 168, False),
)

DEFAULT_CANDIDATE_POOL_MAX_AGE_HOURS = 24
_SUCCESS_KIND_ALIASES = {
    "cross_platform_hot_stocks": ("cross_platform_hot_stocks", "hot_stocks"),
    "fund_flow": ("fund_flow", "flow"),
    "flow": ("fund_flow", "flow"),
}
_SUCCESS_SYMBOL_ALIASES = {
    ("cross_platform_hot_stocks", "cn_a"): ("cn_a", "latest"),
    ("cross_platform_hot_stocks", "latest"): ("cn_a", "latest"),
    ("hot_stocks", "cn_a"): ("cn_a", "latest"),
    ("hot_stocks", "latest"): ("cn_a", "latest"),
}
_SUCCESS_LOOKBACK_MINUTES = {
    "cross_platform_hot_stocks": 15,
}


def _parse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _payload_count(payload_json: Optional[str]) -> int:
    if not payload_json:
        return 0
    try:
        payload = json.loads(payload_json)
    except json.JSONDecodeError:
        return 0
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        for key in ("items", "records", "data", "upcoming"):
            value = payload.get(key)
            if isinstance(value, list):
                return len(value)
        total = payload.get("total")
        if isinstance(total, int):
            return total
        return len(payload)
    return 1


def _latest_for_kinds(conn, kinds: tuple[str, ...]) -> Optional[dict]:
    latest: Optional[dict] = None
    for kind in kinds:
        row = conn.execute(
            """SELECT source, kind, symbol, observed_at, payload_json
               FROM market_observations
               WHERE kind = ?
               ORDER BY observed_at DESC
               LIMIT 1""",
            (kind,),
        ).fetchone()
        if not row:
            continue
        candidate = dict(row)
        if latest is None or _parse_dt(candidate["observed_at"]) > _parse_dt(
            latest["observed_at"]
        ):
            latest = candidate
    return latest


def _candidate_pool_health(
    conn,
    *,
    now: datetime,
    max_age_hours: int,
) -> dict[str, dict]:
    row = conn.execute(
        """SELECT
               COUNT(*) AS total_count,
               SUM(CASE WHEN pool_tier = 'core' THEN 1 ELSE 0 END) AS core_count,
               MAX(COALESCE(NULLIF(last_scored_at, ''), added_at)) AS latest_scored_at
           FROM projection_candidate_pool"""
    ).fetchone()
    total_count = int(row["total_count"] or 0)
    core_count = int(row["core_count"] or 0)
    latest_scored_at = row["latest_scored_at"]

    if latest_scored_at:
        observed = _parse_dt(latest_scored_at)
        age_hours = (now - observed).total_seconds() / 3600
        freshness_status = "healthy" if age_hours <= max_age_hours else "degraded"
        rounded_age = round(age_hours, 2)
    else:
        freshness_status = "down"
        rounded_age = None

    return {
        "candidate_pool_freshness": {
            "status": freshness_status,
            "required": False,
            "latest_scored_at": latest_scored_at,
            "age_hours": rounded_age,
            "max_age_hours": max_age_hours,
            "total_count": total_count,
            "core_count": core_count,
        },
        "core_pool": {
            "status": "healthy" if core_count > 0 else "empty",
            "required": False,
            "total_count": total_count,
            "core_count": core_count,
        },
    }


def _recent_provider_failures(
    conn,
    *,
    now: datetime,
    max_age_hours: int = 24,
    limit: int = 20,
) -> dict:
    rows = conn.execute(
        """SELECT source, symbol, observed_at, run_id, payload_json
           FROM market_observations
           WHERE kind = 'provider_failure'
           ORDER BY observed_at DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()

    recent = []
    unresolved = []
    skipped = []
    source_counter: Counter = Counter()
    unresolved_source_counter: Counter = Counter()
    skipped_source_counter: Counter = Counter()
    target_kind_counter: Counter = Counter()
    for row in rows:
        observed = _parse_dt(row["observed_at"])
        age_hours = (now - observed).total_seconds() / 3600
        if age_hours > max_age_hours:
            continue
        try:
            payload = json.loads(row["payload_json"])
        except json.JSONDecodeError:
            payload = {}

        source = str(payload.get("source") or row["source"])
        target_kind = str(payload.get("target_kind") or "")
        symbol = str(payload.get("symbol") or row["symbol"])
        status = str(payload.get("status") or "provider_error")
        error_type = str(payload.get("error_type") or "")
        skipped_by_circuit = status == "circuit_open" or error_type == "CircuitOpen"
        resolved = _provider_failure_resolution(
            conn,
            target_kind=target_kind,
            symbol=symbol,
            observed_at=row["observed_at"],
        )
        item = {
            "source": source,
            "target_kind": target_kind,
            "symbol": symbol,
            "status": status,
            "error_type": error_type,
            "error_message": str(payload.get("error_message") or ""),
            "observed_at": row["observed_at"],
            "age_hours": round(age_hours, 2),
            "run_id": row["run_id"] or "",
            "resolved_by_fallback": resolved is not None,
            "resolved_source": resolved["source"] if resolved else "",
            "resolved_observed_at": resolved["observed_at"] if resolved else "",
            "skipped_by_circuit": skipped_by_circuit,
        }
        details = payload.get("details")
        if isinstance(details, dict):
            item["details"] = details
        recent.append(item)
        source_counter.update([source])
        if target_kind:
            target_kind_counter.update([target_kind])
        if skipped_by_circuit:
            skipped.append(item)
            skipped_source_counter.update([source])
        elif not resolved:
            unresolved.append(item)
            unresolved_source_counter.update([source])

    return {
        "total_recent": len(recent),
        "unresolved_recent": len(unresolved),
        "resolved_recent": len(recent) - len(unresolved) - len(skipped),
        "skipped_recent": len(skipped),
        "max_age_hours": max_age_hours,
        "by_source": dict(sorted(source_counter.items())),
        "by_unresolved_source": dict(sorted(unresolved_source_counter.items())),
        "by_skipped_source": dict(sorted(skipped_source_counter.items())),
        "by_target_kind": dict(sorted(target_kind_counter.items())),
        "recent": recent,
        "unresolved": unresolved,
        "skipped": skipped,
    }


def _provider_failure_resolution(
    conn,
    *,
    target_kind: str,
    symbol: str,
    observed_at: str,
) -> Optional[dict]:
    if not target_kind or not symbol:
        return None
    success_kinds = _SUCCESS_KIND_ALIASES.get(target_kind, (target_kind,))
    success_symbols = _SUCCESS_SYMBOL_ALIASES.get((target_kind, symbol), (symbol,))
    success_after = observed_at
    lookback_minutes = _SUCCESS_LOOKBACK_MINUTES.get(target_kind)
    if lookback_minutes:
        success_after = (_parse_dt(observed_at) - timedelta(minutes=lookback_minutes)).isoformat()
    kind_placeholders = ",".join("?" for _ in success_kinds)
    symbol_placeholders = ",".join("?" for _ in success_symbols)
    params: list[object] = [*success_kinds, *success_symbols, success_after]
    row = conn.execute(
        f"""SELECT source, observed_at
            FROM market_observations
            WHERE kind IN ({kind_placeholders})
              AND symbol IN ({symbol_placeholders})
              AND observed_at >= ?
            ORDER BY observed_at ASC
            LIMIT 1""",
        tuple(params),
    ).fetchone()
    return dict(row) if row else None


def evaluate_data_source_health(
    conn,
    *,
    now: Optional[datetime] = None,
    max_age_hours: Optional[int] = None,
    candidate_pool_max_age_hours: Optional[int] = None,
    provider_failure_max_age_hours: int = 24,
    provider_failure_limit: int = 20,
    expectations: tuple[DataSourceExpectation, ...] = DEFAULT_EXPECTATIONS,
) -> dict:
    """汇总数据源健康状态。

    required 源缺失或过期时整体 failed；optional 源缺失或过期时 warning。
    """
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    market_weekday = is_market_weekday(now)

    checks: dict[str, dict] = {}
    required_missing: list[str] = []
    deferred_required: list[str] = []
    optional_missing: list[str] = []

    for expected in expectations:
        latest = _latest_for_kinds(conn, expected.kinds)
        age_limit = max_age_hours or expected.max_age_hours
        if not latest:
            item = {
                "status": "down",
                "required": expected.required,
                "latest_observed_at": None,
                "age_hours": None,
                "max_age_hours": age_limit,
                "source": "",
                "kind": ",".join(expected.kinds),
                "symbol": "",
                "payload_count": 0,
                "min_payload_count": expected.min_payload_count,
            }
        else:
            observed = _parse_dt(latest["observed_at"])
            age_hours = (now - observed).total_seconds() / 3600
            payload_count = _payload_count(latest["payload_json"])
            payload_ok = payload_count >= expected.min_payload_count
            stale_by_age = age_hours > age_limit
            status = "healthy" if not stale_by_age and payload_ok else "degraded"
            item = {
                "status": status,
                "required": expected.required,
                "latest_observed_at": latest["observed_at"],
                "age_hours": round(age_hours, 2),
                "max_age_hours": age_limit,
                "source": latest["source"],
                "kind": latest["kind"],
                "symbol": latest["symbol"],
                "payload_count": payload_count,
                "min_payload_count": expected.min_payload_count,
            }
            if expected.required and stale_by_age and payload_ok and not market_weekday:
                item["stale_reason"] = "non_trading_day"
                item["next_refresh_required_before_next_window"] = True
                item["blocks_new_trades"] = False

        checks[expected.name] = item
        if item["status"] != "healthy":
            if expected.required:
                if item.get("blocks_new_trades") is False:
                    deferred_required.append(expected.name)
                else:
                    required_missing.append(expected.name)
            else:
                optional_missing.append(expected.name)

    pool_checks = _candidate_pool_health(
        conn,
        now=now,
        max_age_hours=(
            candidate_pool_max_age_hours
            or max_age_hours
            or DEFAULT_CANDIDATE_POOL_MAX_AGE_HOURS
        ),
    )
    checks.update(pool_checks)
    for name, item in pool_checks.items():
        if item["status"] != "healthy":
            optional_missing.append(name)

    status = (
        "failed"
        if required_missing
        else "warning"
        if optional_missing or deferred_required
        else "ok"
    )
    return {
        "status": status,
        "calendar_context": {
            "market_weekday": market_weekday,
            "non_trading_day": not market_weekday,
        },
        "checks": checks,
        "required_missing": required_missing,
        "deferred_required": deferred_required,
        "optional_missing": optional_missing,
        "provider_failures": _recent_provider_failures(
            conn,
            now=now,
            max_age_hours=provider_failure_max_age_hours,
            limit=provider_failure_limit,
        ),
    }
