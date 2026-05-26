"""数据源诊断服务层，供 CLI、Hermes 和 Agent 输出复用。"""

from __future__ import annotations

from collections import Counter
from datetime import datetime
import json
from typing import Any, Optional

from astock_trading.market.health import evaluate_data_source_health
from astock_trading.platform.time import utc_now

SOURCE_QUALITY_DIMENSIONS = (
    ("quote", "行情", "L1", "has_quote"),
    ("technical", "技术指标", "L1", "has_technical"),
    ("financial", "基本面", "L1", "has_financial"),
    ("flow", "资金流", "L1", "has_flow"),
    ("sentiment", "舆情", "L2", "has_sentiment"),
    ("sector", "行业上下文", "L2", "has_sector"),
)
L1_SOURCE_QUALITY_KEYS = {"quote", "technical", "financial", "flow"}
L1_PROVIDER_TARGET_KINDS = {
    "",
    "quote",
    "technical",
    "financial",
    "fund_flow",
    "flow",
    "snapshot",
}
SCREENER_SOURCE_QUALITY_PHASES = ("screener", "scoring")
REPAIR_KIND_ALIASES = {
    "quote": ("quote", "snapshot"),
    "technical": ("snapshot",),
    "financial": ("financial", "snapshot"),
    "flow": ("fund_flow", "flow", "snapshot"),
    "sentiment": ("sentiment", "snapshot"),
    "sector": ("snapshot",),
}
MISSING_FIELD_DIMENSIONS = {
    "行情": "quote",
    "技术指标": "technical",
    "基本面": "financial",
    "ROE": "financial",
    "营收": "financial",
    "现金流": "financial",
    "资金流": "flow",
    "舆情": "sentiment",
    "行业上下文": "sector",
}


def _decode_payload(value: Any) -> Any:
    if value is None:
        return {}
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return {}


def _coverage_rate(available: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(available / total, 4)


def _snapshot_has(payload: dict, attr: str, completeness_key: str) -> bool:
    completeness = payload.get("completeness")
    if isinstance(completeness, dict) and completeness_key in completeness:
        return bool(completeness.get(completeness_key))
    return payload.get(attr) is not None


def _payload_code(payload: dict) -> str:
    return str(payload.get("code") or payload.get("symbol") or "")


def _source_quality_from_payloads(
    snapshot_payloads: list[dict],
    scores: list[dict],
    *,
    repairs_by_dimension: dict[str, set[str]] | None = None,
) -> dict:
    total = len(snapshot_payloads)
    coverage = {}
    warnings = []
    repairs_by_dimension = repairs_by_dimension or {}

    for attr, label, layer, completeness_key in SOURCE_QUALITY_DIMENSIONS:
        available_symbols: list[str] = []
        missing_symbols: list[str] = []
        for snapshot in snapshot_payloads:
            symbol = _payload_code(snapshot)
            if _snapshot_has(snapshot, attr, completeness_key):
                if symbol:
                    available_symbols.append(symbol)
            elif symbol:
                missing_symbols.append(symbol)
        repaired_symbols = repairs_by_dimension.get(attr, set()) & set(missing_symbols)
        unresolved_missing_symbols = sorted(set(missing_symbols) - repaired_symbols)
        available = total - len(unresolved_missing_symbols)
        row = {
            "label": label,
            "layer": layer,
            "available": available,
            "missing": len(unresolved_missing_symbols),
            "total": total,
            "rate": _coverage_rate(available, total),
            "available_symbols": sorted(set(available_symbols) | repaired_symbols),
            "missing_symbols": unresolved_missing_symbols,
            "raw_missing_symbols": sorted(set(missing_symbols)),
            "repaired_symbols": sorted(repaired_symbols),
        }
        coverage[attr] = row
        if total and layer == "L1" and row["missing"] > 0:
            warnings.append(
                f"最近筛选逐票{label}覆盖率 {row['rate']:.1%}，可能影响评分和买入门禁。"
            )

    quality_counter: Counter = Counter()
    score_quality_items = []
    missing_counter: Counter = Counter()
    for score in scores:
        fields = score.get("data_missing_fields") or []
        if isinstance(fields, str):
            fields = [fields]
        code = str(score.get("code") or score.get("symbol") or "")
        unresolved_fields = []
        for item in fields:
            field = str(item)
            dimension = MISSING_FIELD_DIMENSIONS.get(field)
            if dimension and code in repairs_by_dimension.get(dimension, set()):
                continue
            unresolved_fields.append(field)
        missing_counter.update(unresolved_fields)
        quality = str(score.get("data_quality", "ok"))
        effective_quality = "ok" if quality == "degraded" and fields and not unresolved_fields else quality
        quality_counter.update([effective_quality])
        if effective_quality != "ok":
            score_quality_items.append({
                "code": code,
                "name": str(score.get("name") or ""),
                "data_quality": effective_quality,
                "missing_fields": unresolved_fields or [str(item) for item in fields],
            })

    if total == 0:
        status = "warning"
        warnings.append("最近筛选没有可回放的逐票快照，无法评估覆盖率。")
    elif coverage["quote"]["available"] == 0 or coverage["technical"]["available"] == 0:
        status = "failed"
    elif warnings or quality_counter.get("degraded", 0) or quality_counter.get("error", 0):
        status = "warning"
    else:
        status = "ok"

    if quality_counter.get("degraded", 0) or quality_counter.get("error", 0):
        warnings.append(
            f"评分数据质量存在降级 {quality_counter.get('degraded', 0)} 条、错误 {quality_counter.get('error', 0)} 条。"
        )

    return {
        "status": status,
        "sample_size": total,
        "score_count": len(scores),
        "coverage": coverage,
        "score_quality_counts": dict(sorted(quality_counter.items())),
        "score_quality_items": score_quality_items,
        "missing_fields": [
            {"field": key, "count": count}
            for key, count in sorted(missing_counter.items(), key=lambda item: (-item[1], item[0]))
        ],
        "warnings": warnings,
    }


def _empty_screener_source_quality() -> dict:
    return {
        "status": "empty",
        "run_id": "",
        "history_group_id": "",
        "phase": "",
        "created_at": "",
        "sample_size": 0,
        "score_count": 0,
        "coverage": {},
        "score_quality_counts": {},
        "score_quality_items": [],
        "missing_fields": [],
        "warnings": ["尚无可用于覆盖率回放的筛选镜像。"],
    }


def _latest_repaired_symbols(
    conn,
    *,
    attr: str,
    completeness_key: str,
    symbols: set[str],
    observed_after: str,
) -> set[str]:
    if not symbols or not observed_after:
        return set()
    kinds = REPAIR_KIND_ALIASES.get(attr, ())
    if not kinds:
        return set()
    kind_placeholders = ",".join("?" for _ in kinds)
    symbol_placeholders = ",".join("?" for _ in symbols)
    rows = conn.execute(
        f"""SELECT kind, symbol, payload_json
           FROM market_observations
           WHERE kind IN ({kind_placeholders})
             AND symbol IN ({symbol_placeholders})
             AND observed_at >= ?
           ORDER BY observed_at DESC""",
        (*kinds, *sorted(symbols), observed_after),
    ).fetchall()
    repaired: set[str] = set()
    for row in rows:
        symbol = str(row["symbol"])
        if symbol in repaired:
            continue
        kind = str(row["kind"])
        if kind == "snapshot":
            payload = _decode_payload(row["payload_json"])
            if isinstance(payload, dict) and _snapshot_has(payload, attr, completeness_key):
                repaired.add(symbol)
            continue
        repaired.add(symbol)
    return repaired


def _repairs_by_dimension(conn, snapshot_payloads: list[dict], observed_after: str) -> dict[str, set[str]]:
    repairs: dict[str, set[str]] = {}
    for attr, _label, _layer, completeness_key in SOURCE_QUALITY_DIMENSIONS:
        missing_symbols = {
            _payload_code(snapshot)
            for snapshot in snapshot_payloads
            if _payload_code(snapshot) and not _snapshot_has(snapshot, attr, completeness_key)
        }
        if not missing_symbols:
            continue
        repaired = _latest_repaired_symbols(
            conn,
            attr=attr,
            completeness_key=completeness_key,
            symbols=missing_symbols,
            observed_after=observed_after,
        )
        if repaired:
            repairs[attr] = repaired
    return repairs


def _latest_screener_source_quality(conn) -> dict:
    phase_placeholders = ",".join("?" for _ in SCREENER_SOURCE_QUALITY_PHASES)
    latest_date = conn.execute(
        f"""SELECT MAX(snapshot_date) AS snapshot_date
           FROM signal_history_snapshots
           WHERE snapshot_type = 'candidates'
             AND phase IN ({phase_placeholders})""",
        SCREENER_SOURCE_QUALITY_PHASES,
    ).fetchone()
    if not latest_date or not latest_date["snapshot_date"]:
        return _empty_screener_source_quality()

    row = conn.execute(
        f"""SELECT history_group_id, run_id, phase, created_at, payload_json
           FROM signal_history_snapshots
           WHERE snapshot_type = 'candidates' AND snapshot_date = ?
             AND phase IN ({phase_placeholders})
           ORDER BY created_at DESC
           LIMIT 1""",
        (latest_date["snapshot_date"], *SCREENER_SOURCE_QUALITY_PHASES),
    ).fetchone()
    if not row:
        return _empty_screener_source_quality()

    row_dict = dict(row)
    scores = _decode_payload(row_dict.get("payload_json"))
    if not isinstance(scores, list):
        scores = []
    snapshot_rows = conn.execute(
        """SELECT symbol, payload_json
           FROM market_observations
           WHERE kind = 'snapshot' AND run_id = ?""",
        (row_dict.get("run_id", ""),),
    ).fetchall()
    snapshots_by_symbol: dict[str, dict] = {}
    for snapshot_row in snapshot_rows:
        symbol = snapshot_row["symbol"]
        if symbol in snapshots_by_symbol:
            continue
        payload = _decode_payload(snapshot_row["payload_json"])
        if isinstance(payload, dict):
            snapshots_by_symbol[symbol] = payload

    snapshot_payloads = list(snapshots_by_symbol.values())
    quality = _source_quality_from_payloads(
        snapshot_payloads,
        scores,
        repairs_by_dimension=_repairs_by_dimension(
            conn,
            snapshot_payloads,
            str(row_dict.get("created_at", "") or ""),
        ),
    )
    return {
        "run_id": row_dict.get("run_id", ""),
        "history_group_id": row_dict.get("history_group_id", ""),
        "phase": row_dict.get("phase", ""),
        "created_at": row_dict.get("created_at", ""),
        **quality,
    }


def _active_candidate_symbols(conn) -> list[str]:
    symbols: set[str] = set()
    pool_rows = conn.execute(
        """SELECT code
           FROM projection_candidate_pool
           WHERE pool_tier IN ('core', 'watch')
           ORDER BY
               CASE pool_tier WHEN 'core' THEN 0 WHEN 'watch' THEN 1 ELSE 2 END,
               score DESC,
               code"""
    ).fetchall()
    symbols.update(str(row["code"]) for row in pool_rows if row["code"])

    decision_rows = conn.execute(
        """SELECT payload_json
           FROM event_log
           WHERE event_type = 'decision.suggested'
           ORDER BY occurred_at DESC
           LIMIT 100"""
    ).fetchall()
    for row in decision_rows:
        payload = _decode_payload(row["payload_json"])
        if not isinstance(payload, dict):
            continue
        if str(payload.get("action") or "") == "BUY":
            code = str(payload.get("code") or "")
            if code:
                symbols.add(code)

    manual_rows = conn.execute(
        """SELECT payload_json
           FROM event_log
           WHERE event_type = 'manual_trade.requested'
           ORDER BY occurred_at DESC
           LIMIT 100"""
    ).fetchall()
    for row in manual_rows:
        payload = _decode_payload(row["payload_json"])
        if not isinstance(payload, dict):
            continue
        code = str(payload.get("code") or "")
        if code:
            symbols.add(code)

    return sorted(symbols)


def _actionable_provider_failures(
    provider_failures: dict[str, Any],
    active_candidate_symbols: list[str],
) -> list[dict[str, Any]]:
    active_symbols = {str(symbol) for symbol in active_candidate_symbols if symbol}
    return [
        item
        for item in provider_failures.get("unresolved", []) or []
        if str(item.get("target_kind", "")) in L1_PROVIDER_TARGET_KINDS
        and str(item.get("symbol", "")) in active_symbols
    ]


def _source_quality_is_actionable(
    source_quality: dict[str, Any],
    active_candidate_symbols: list[str],
) -> bool:
    """判断最近逐票覆盖率 warning 是否影响当前候选/买入判断。"""
    if source_quality.get("status") not in {"warning", "failed"}:
        return False
    active_symbols = {str(symbol) for symbol in active_candidate_symbols if symbol}
    if not active_symbols:
        return True

    coverage = source_quality.get("coverage", {}) or {}
    for name, item in coverage.items():
        if name not in L1_SOURCE_QUALITY_KEYS or int(item.get("missing", 0) or 0) <= 0:
            continue
        missing_symbols = {str(symbol) for symbol in item.get("missing_symbols", []) or [] if symbol}
        if not missing_symbols or missing_symbols & active_symbols:
            return True

    for item in source_quality.get("score_quality_items", []) or []:
        if str(item.get("code") or "") in active_symbols:
            return True
    return False


def build_data_source_diagnosis(
    conn,
    *,
    now: datetime | None = None,
    max_age_hours: Optional[int] = None,
) -> dict:
    """汇总全局门禁、provider 失败和最近筛选逐票覆盖率。"""
    now = now or utc_now()
    health = evaluate_data_source_health(conn, now=now, max_age_hours=max_age_hours)
    provider_failures = health.get("provider_failures", {}) or {}
    source_quality = _latest_screener_source_quality(conn)
    active_candidate_symbols = _active_candidate_symbols(conn)
    actionable_provider_failures = _actionable_provider_failures(
        provider_failures,
        active_candidate_symbols,
    )
    source_quality_actionable = _source_quality_is_actionable(source_quality, active_candidate_symbols)
    source_quality = {
        **source_quality,
        "actionable": source_quality_actionable,
    }

    findings: list[str] = []
    recommendations: list[str] = []
    if health.get("required_missing"):
        findings.append(f"核心门禁源缺失或过期: {', '.join(health['required_missing'])}")
        recommendations.append("先运行 atrade check-data-sources --json 或对应 pipeline 修复核心源。")
    if health.get("deferred_required"):
        findings.append(f"非交易日核心源自然过期: {', '.join(health['deferred_required'])}")
        recommendations.append("当前可继续只读复核；下个买入窗口前通过候选刷新或 atrade check-data-sources --json 更新核心源。")
    if health.get("optional_missing"):
        findings.append(f"辅助源降级: {', '.join(health['optional_missing'])}")
        recommendations.append("辅助源降级时可以继续只读分析，但不要提高交易置信度。")

    unresolved_count = len(actionable_provider_failures)
    if unresolved_count:
        findings.append(f"{unresolved_count} 个当前候选 provider 失败未被 fallback 补齐")
        recommendations.append("查看 provider_failures.unresolved，先修当前候选数据源再扩大交易判断。")

    if source_quality.get("status") in {"warning", "failed"} and source_quality_actionable:
        findings.extend(source_quality.get("warnings", []))
        recommendations.append("逐票 L1 覆盖率不足时，评分可保留，但新增买入意向应暂停或人工复核。")

    if health["status"] == "failed" or (source_quality.get("status") == "failed" and source_quality_actionable):
        status = "failed"
    elif health["status"] == "warning" or unresolved_count or (
        source_quality.get("status") == "warning" and source_quality_actionable
    ):
        status = "warning"
    else:
        status = "ok"

    return {
        "diagnostic": "data_sources",
        "status": status,
        "findings": findings,
        "recommendations": recommendations,
        "health": health,
        "provider_failures": provider_failures,
        "provider_incidents": {
            "actionable_unresolved_recent": unresolved_count,
            "non_actionable_unresolved_recent": max(
                int(provider_failures.get("unresolved_recent", 0) or 0) - unresolved_count,
                0,
            ),
        },
        "latest_screener_source_quality": source_quality,
        "active_candidate_symbols": active_candidate_symbols,
    }


def data_source_blockers_for_new_trades(data_source_diagnosis: dict[str, Any]) -> list[dict[str, Any]]:
    """把全局诊断压缩成新增交易判断门禁，只拦核心源和 L1 覆盖问题。"""
    blockers: list[dict[str, Any]] = []
    health = data_source_diagnosis.get("health", {}) or {}
    required_missing = health.get("required_missing", []) or []
    if required_missing:
        blockers.append({
            "reason": "required_data_sources_unavailable",
            "label": "核心数据源不可用",
            "required_missing": required_missing,
        })

    provider_failures = data_source_diagnosis.get("provider_failures", {}) or {}
    active_candidate_symbols = {
        str(symbol)
        for symbol in data_source_diagnosis.get("active_candidate_symbols", []) or []
        if symbol
    }
    unresolved_l1 = _actionable_provider_failures(
        provider_failures,
        sorted(active_candidate_symbols),
    )
    if unresolved_l1:
        blockers.append({
            "reason": "unresolved_l1_provider_failures",
            "label": "L1 数据源失败未补齐",
            "count": len(unresolved_l1),
            "items": unresolved_l1[:5],
        })

    source_quality = data_source_diagnosis.get("latest_screener_source_quality", {}) or {}
    coverage = source_quality.get("coverage", {}) or {}
    missing_l1 = []
    for name, item in coverage.items():
        if name not in L1_SOURCE_QUALITY_KEYS or int(item.get("missing", 0) or 0) <= 0:
            continue
        missing_symbols = {
            str(symbol)
            for symbol in item.get("missing_symbols", []) or []
            if symbol
        }
        active_missing_symbols = sorted(missing_symbols & active_candidate_symbols)
        if active_candidate_symbols and missing_symbols and not active_missing_symbols:
            continue
        if active_candidate_symbols and not missing_symbols:
            continue
        missing_l1.append({
            "name": name,
            "label": item.get("label", name),
            "missing": len(active_missing_symbols) if active_missing_symbols else item.get("missing", 0),
            "total": item.get("total", 0),
            "rate": item.get("rate", 0),
            "symbols": active_missing_symbols or sorted(missing_symbols),
        })
    score_quality = source_quality.get("score_quality_counts", {}) or {}
    degraded_scores = int(score_quality.get("degraded", 0) or 0)
    errored_scores = int(score_quality.get("error", 0) or 0)
    score_quality_items = source_quality.get("score_quality_items", []) or []
    active_score_quality_items = [
        item
        for item in score_quality_items
        if str(item.get("code") or "") in active_candidate_symbols
    ]
    if active_candidate_symbols and score_quality_items:
        degraded_scores = sum(1 for item in active_score_quality_items if item.get("data_quality") == "degraded")
        errored_scores = sum(1 for item in active_score_quality_items if item.get("data_quality") == "error")
    if missing_l1:
        blockers.append({
            "reason": "latest_screener_l1_coverage_degraded",
            "label": "最近筛选逐票 L1 覆盖不足",
            "source_quality_status": source_quality.get("status", "unknown"),
            "run_id": source_quality.get("run_id", ""),
            "missing_l1": missing_l1,
            "degraded_scores": degraded_scores,
            "errored_scores": errored_scores,
            "warnings": source_quality.get("warnings", []) or [],
        })
    elif degraded_scores or errored_scores:
        blockers.append({
            "reason": "latest_screener_score_quality_degraded",
            "label": "最近筛选评分数据质量降级",
            "source_quality_status": source_quality.get("status", "unknown"),
            "run_id": source_quality.get("run_id", ""),
            "degraded_scores": degraded_scores,
            "errored_scores": errored_scores,
            "score_quality_items": active_score_quality_items or score_quality_items,
            "missing_fields": source_quality.get("missing_fields", []) or [],
            "warnings": source_quality.get("warnings", []) or [],
        })

    return blockers


def data_source_blocker_summary(blockers: list[dict[str, Any]]) -> str:
    labels = [str(item.get("label") or item.get("reason")) for item in blockers if item]
    if not labels:
        return "数据覆盖不足，先诊断数据源再看新增交易。"
    return "；".join(labels) + "，先诊断数据源再看新增交易。"
