"""数据源诊断服务层，供 CLI、Hermes 和 Agent 输出复用。"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
from typing import Any, Optional

from astock_trading.market.health import evaluate_data_source_health

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


def _source_quality_from_payloads(snapshot_payloads: list[dict], scores: list[dict]) -> dict:
    total = len(snapshot_payloads)
    coverage = {}
    warnings = []

    for attr, label, layer, completeness_key in SOURCE_QUALITY_DIMENSIONS:
        available = sum(
            1
            for snapshot in snapshot_payloads
            if _snapshot_has(snapshot, attr, completeness_key)
        )
        row = {
            "label": label,
            "layer": layer,
            "available": available,
            "missing": max(total - available, 0),
            "total": total,
            "rate": _coverage_rate(available, total),
        }
        coverage[attr] = row
        if total and layer == "L1" and available < total:
            warnings.append(
                f"最近筛选逐票{label}覆盖率 {row['rate']:.1%}，可能影响评分和买入门禁。"
            )

    quality_counter = Counter(str(score.get("data_quality", "ok")) for score in scores)
    missing_counter: Counter = Counter()
    for score in scores:
        fields = score.get("data_missing_fields") or []
        if isinstance(fields, str):
            fields = [fields]
        missing_counter.update(str(item) for item in fields)

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
        "missing_fields": [],
        "warnings": ["尚无可用于覆盖率回放的筛选镜像。"],
    }


def _latest_screener_source_quality(conn) -> dict:
    latest_date = conn.execute(
        """SELECT MAX(snapshot_date) AS snapshot_date
           FROM signal_history_snapshots
           WHERE snapshot_type = 'candidates'"""
    ).fetchone()
    if not latest_date or not latest_date["snapshot_date"]:
        return _empty_screener_source_quality()

    row = conn.execute(
        """SELECT history_group_id, run_id, phase, created_at, payload_json
           FROM signal_history_snapshots
           WHERE snapshot_type = 'candidates' AND snapshot_date = ?
           ORDER BY created_at DESC
           LIMIT 1""",
        (latest_date["snapshot_date"],),
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

    quality = _source_quality_from_payloads(list(snapshots_by_symbol.values()), scores)
    return {
        "run_id": row_dict.get("run_id", ""),
        "history_group_id": row_dict.get("history_group_id", ""),
        "phase": row_dict.get("phase", ""),
        "created_at": row_dict.get("created_at", ""),
        **quality,
    }


def build_data_source_diagnosis(
    conn,
    *,
    now: datetime | None = None,
    max_age_hours: Optional[int] = None,
) -> dict:
    """汇总全局门禁、provider 失败和最近筛选逐票覆盖率。"""
    now = now or datetime.now(timezone.utc)
    health = evaluate_data_source_health(conn, now=now, max_age_hours=max_age_hours)
    provider_failures = health.get("provider_failures", {}) or {}
    source_quality = _latest_screener_source_quality(conn)

    findings: list[str] = []
    recommendations: list[str] = []
    if health.get("required_missing"):
        findings.append(f"核心门禁源缺失或过期: {', '.join(health['required_missing'])}")
        recommendations.append("先运行 atrade check-data-sources --json 或对应 pipeline 修复核心源。")
    if health.get("optional_missing"):
        findings.append(f"辅助源降级: {', '.join(health['optional_missing'])}")
        recommendations.append("辅助源降级时可以继续只读分析，但不要提高交易置信度。")

    unresolved_count = int(provider_failures.get("unresolved_recent", 0) or 0)
    if unresolved_count:
        findings.append(f"{unresolved_count} 个 provider 失败未被 fallback 补齐")
        recommendations.append("查看 provider_failures.unresolved，先修未补齐的数据源再扩大交易判断。")

    if source_quality.get("status") in {"warning", "failed"}:
        findings.extend(source_quality.get("warnings", []))
        recommendations.append("逐票 L1 覆盖率不足时，评分可保留，但新增买入意向应暂停或人工复核。")

    if health["status"] == "failed" or source_quality.get("status") == "failed":
        status = "failed"
    elif health["status"] == "warning" or unresolved_count or source_quality.get("status") == "warning":
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
        "latest_screener_source_quality": source_quality,
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
    unresolved_l1 = [
        item
        for item in provider_failures.get("unresolved", []) or []
        if str(item.get("target_kind", "")) in L1_PROVIDER_TARGET_KINDS
    ]
    if unresolved_l1:
        blockers.append({
            "reason": "unresolved_l1_provider_failures",
            "label": "L1 数据源失败未补齐",
            "count": len(unresolved_l1),
            "items": unresolved_l1[:5],
        })

    source_quality = data_source_diagnosis.get("latest_screener_source_quality", {}) or {}
    coverage = source_quality.get("coverage", {}) or {}
    missing_l1 = [
        {
            "name": name,
            "label": item.get("label", name),
            "missing": item.get("missing", 0),
            "total": item.get("total", 0),
            "rate": item.get("rate", 0),
        }
        for name, item in coverage.items()
        if name in L1_SOURCE_QUALITY_KEYS and int(item.get("missing", 0) or 0) > 0
    ]
    score_quality = source_quality.get("score_quality_counts", {}) or {}
    degraded_scores = int(score_quality.get("degraded", 0) or 0)
    errored_scores = int(score_quality.get("error", 0) or 0)
    if source_quality.get("status") == "failed" or missing_l1 or degraded_scores or errored_scores:
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

    return blockers


def data_source_blocker_summary(blockers: list[dict[str, Any]]) -> str:
    labels = [str(item.get("label") or item.get("reason")) for item in blockers if item]
    if not labels:
        return "数据覆盖不足，先诊断数据源再看新增交易。"
    return "；".join(labels) + "，先诊断数据源再看新增交易。"
