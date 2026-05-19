"""P6-2 多策略 profile 对比。

只做配置和历史证据对比；不自动切换 ASTOCK_CONFIG_PROFILE。
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from statistics import mean
from typing import Any

from astock_trading.platform.config import ConfigRegistry
from astock_trading.platform.domain_events import (
    DECISION_SUGGESTED,
    STRATEGY_PROFILE_COMPARISON_PROPOSED,
    TRADE_REVIEW_RECORDED,
)
from astock_trading.platform.events import EventStore
from astock_trading.platform.paths import resolve_config_dir
from astock_trading.platform.time import utc_now_iso

DEFAULT_PROFILES = ("trend_swing", "short_continuation", "defensive_watch")


def compare_strategy_profiles(
    conn: Any,
    *,
    config_dir: Path | None = None,
    profiles: tuple[str, ...] = DEFAULT_PROFILES,
    record: bool = False,
) -> dict:
    """比较多个策略 profile 的配置差异和已有运行证据。"""
    config_root = config_dir or resolve_config_dir()
    store = EventStore(conn)
    rows = [
        _profile_summary(conn, store, config_root=config_root, profile=name)
        for name in profiles
    ]
    has_evidence = any(item["evidence_status"] == "has_profile_runs" for item in rows)
    payload = {
        "analysis": "strategy_profile_comparison",
        "status": "ok" if has_evidence else "needs_shadow_validation",
        "generated_at": utc_now_iso(),
        "current_profile": os.getenv("ASTOCK_CONFIG_PROFILE", "default"),
        "profiles": rows,
        "recommendations": _recommendations(rows),
        "guardrails": {
            "auto_switch_profile": False,
            "auto_allocate_capital": False,
            "manual_approval_required": True,
            "reason": "P6-2 只做多策略 profile 对比，不自动切换 ASTOCK_CONFIG_PROFILE，也不自动分配资金。",
        },
        "recorded_event_id": "",
    }
    payload["report_markdown"] = render_strategy_profile_report(payload)

    if record:
        event_id = store.append(
            "strategy:profiles",
            "strategy",
            STRATEGY_PROFILE_COMPARISON_PROPOSED,
            payload={key: value for key, value in payload.items() if key != "recorded_event_id"},
            metadata={"source": "strategy_profiles"},
        )
        payload["recorded_event_id"] = event_id
        _write_report_artifact(conn, event_id, payload["report_markdown"])

    return payload


def profile_config_hash(config: dict) -> str:
    """返回与 ConfigRegistry.freeze() 一致的配置 hash 前缀。"""
    config_json = json.dumps(config, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(config_json.encode()).hexdigest()[:16]


def render_strategy_profile_report(payload: dict) -> str:
    """渲染中文多策略 profile 对比报告。"""
    status_label = {
        "ok": "已有运行证据",
        "needs_shadow_validation": "需要影子验证",
    }.get(str(payload.get("status") or ""), str(payload.get("status") or ""))
    lines = [
        "# P6-2 多策略 Profile 对比",
        "",
        f"- 状态：{status_label}",
        f"- 当前执行 profile：{payload.get('current_profile')}",
        "- 自动切换 profile：否",
        "- 自动资金分配：否",
        "",
        "## Profile 概览",
    ]
    for item in payload.get("profiles") or []:
        review = item.get("trade_review") or {}
        params = item.get("key_parameters") or {}
        lines.extend([
            f"- {item.get('name')}：{item.get('purpose')}",
            f"  - 买入阈值：{params.get('buy_threshold')}；观察阈值：{params.get('watch_threshold')}",
            f"  - 历史运行：{item.get('run_count')} 次；复盘样本：{review.get('sample_count')} 笔",
            f"  - 平均收益：{review.get('avg_return_pct', 0):.2%}；胜率：{review.get('win_rate_pct', 0):.2%}",
        ])
    lines.extend(["", "## 建议"])
    for recommendation in payload.get("recommendations") or []:
        lines.append(f"- {recommendation}")
    return "\n".join(lines)


def _profile_summary(conn: Any, store: EventStore, *, config_root: Path, profile: str) -> dict:
    config, errors = ConfigRegistry(config_dir=config_root, profile=profile).load_and_validate()
    strategy = config.get("strategy", {})
    config_hash = profile_config_hash(config)
    versions = _matching_config_versions(conn, config_hash)
    run_count = _run_count(conn, versions)
    decisions = _decision_counts(store, versions)
    trade_review = _trade_review_stats(store, versions)
    evidence_status = "has_profile_runs" if run_count or sum(decisions.values()) or trade_review["sample_count"] else "no_profile_runs"
    return {
        "name": profile,
        "purpose": _profile_purpose(profile),
        "config_hash": config_hash,
        "matched_config_versions": versions,
        "config_errors": errors,
        "evidence_status": evidence_status,
        "run_count": run_count,
        "decision_counts": decisions,
        "trade_review": trade_review,
        "key_parameters": _key_parameters(strategy),
    }


def _matching_config_versions(conn: Any, config_hash: str) -> list[str]:
    rows = conn.execute(
        """SELECT config_version
           FROM config_versions
           WHERE config_hash = ?
           ORDER BY created_at DESC""",
        (config_hash,),
    ).fetchall()
    return [str(row["config_version"]) for row in rows]


def _run_count(conn: Any, config_versions: list[str]) -> int:
    if not config_versions:
        return 0
    placeholders = ", ".join("?" for _ in config_versions)
    row = conn.execute(
        f"""SELECT COUNT(*) AS count
            FROM run_log
            WHERE config_version IN ({placeholders}) AND status = 'completed'""",
        tuple(config_versions),
    ).fetchone()
    return int(row["count"] or 0) if row else 0


def _decision_counts(store: EventStore, config_versions: list[str]) -> dict[str, int]:
    counts = {"BUY": 0, "WATCH": 0, "CLEAR": 0, "SELL": 0, "NO_TRADE": 0}
    for version in config_versions:
        for event in store.query(event_type=DECISION_SUGGESTED, metadata_filter={"config_version": version}, limit=5000):
            action = str((event.get("payload") or {}).get("action") or "NO_TRADE")
            counts[action] = counts.get(action, 0) + 1
    return counts


def _trade_review_stats(store: EventStore, config_versions: list[str]) -> dict:
    returns = []
    for version in config_versions:
        events = store.query(event_type=TRADE_REVIEW_RECORDED, metadata_filter={"config_version": version}, limit=5000)
        for event in events:
            returns.append(_float((event.get("payload") or {}).get("latest_return_pct")))
    return {
        "sample_count": len(returns),
        "avg_return_pct": round(mean(returns), 4) if returns else 0.0,
        "win_rate_pct": round(sum(1 for value in returns if value > 0) / len(returns), 4) if returns else 0.0,
    }


def _key_parameters(strategy: dict) -> dict:
    scoring = strategy.get("scoring", {})
    thresholds = scoring.get("thresholds", {})
    gates = scoring.get("decision_gates", {})
    position = strategy.get("risk", {}).get("position", {})
    auto_trade = strategy.get("auto_trade", {})
    continuation = strategy.get("continuation", {})
    continuation_scoring = continuation.get("scoring", {})
    return {
        "buy_threshold": _float(thresholds.get("buy")),
        "watch_threshold": _float(thresholds.get("watch")),
        "reject_threshold": _float(thresholds.get("reject")),
        "require_entry_signal_for_buy": bool(gates.get("require_entry_signal_for_buy", False)),
        "min_data_quality_for_buy": str(gates.get("min_data_quality_for_buy", "degraded")),
        "max_missing_fields_for_buy": gates.get("max_missing_fields_for_buy"),
        "single_max_pct": _float(position.get("single_max")),
        "total_max_pct": _float(position.get("total_max")),
        "weekly_max": int(position.get("weekly_max", 0) or 0),
        "continuation_top_n": int(continuation_scoring.get("top_n", 0) or 0),
        "continuation_hold_days": continuation_scoring.get("hold_days", []),
        "auto_trade_enabled": bool(auto_trade.get("enabled", False)),
        "auto_trade_dry_run": bool(auto_trade.get("dry_run", True)),
    }


def _profile_purpose(profile: str) -> str:
    return {
        "trend_swing": "趋势波段候选，适合 5-20 个交易日的确认型机会。",
        "short_continuation": "短线续涨研究，适合 T+1 到 T+3 的强势延续样本验证。",
        "defensive_watch": "弱市观察模式，提高买入门槛，优先减少新开仓。",
    }.get(profile, "自定义策略 profile。")


def _recommendations(rows: list[dict]) -> list[str]:
    if not rows:
        return ["没有发现可比较的策略 profile。"]
    if not any(row["evidence_status"] == "has_profile_runs" for row in rows):
        return [
            "先做影子运行并积累每个 profile 的 run_log、decision.suggested 和 trade.review.recorded，再比较胜率与收益。",
            "在有足够样本前，不要自动切换 ASTOCK_CONFIG_PROFILE，也不要做自动资金隔离。",
        ]
    ranked = sorted(
        rows,
        key=lambda item: (
            item["trade_review"]["sample_count"],
            item["trade_review"]["avg_return_pct"],
            item["run_count"],
        ),
        reverse=True,
    )
    top = ranked[0]
    return [
        f"当前证据最多的是 {top['name']}，但仍需结合样本数量、市场状态和人工复核决定是否用于执行。",
        "profile 对比只产生建议；执行前必须显式确认 ASTOCK_CONFIG_PROFILE。",
    ]


def _write_report_artifact(conn: Any, event_id: str, markdown: str) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO report_artifacts
           (artifact_id, run_id, report_type, format, content, delivered_to, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            f"strategy_profiles_{event_id}",
            event_id,
            "strategy_profile_comparison",
            "markdown",
            markdown,
            "local",
            utc_now_iso(),
        ),
    )


def _float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
