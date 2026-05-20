"""历史信号镜像归档与诊断。"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from astock_trading.platform.time import local_today_str, utc_now_iso

SNAPSHOT_TYPES = ("market", "pool", "candidates", "decision")


def archive_signal_history(
    conn: Any,
    *,
    snapshot_date: str | None = None,
    history_group_id: str = "",
    run_id: str = "",
    phase: str = "screener",
    market: dict | None = None,
    pool: list[dict] | None = None,
    candidates: list[dict] | None = None,
    decisions: list[dict] | None = None,
) -> str:
    """归档一次信号运行看到的 market / pool / candidates / decision。"""
    date_value = snapshot_date or local_today_str()
    group_id = history_group_id or _history_group_id(date_value, phase, run_id)
    payloads = {
        "market": market or {},
        "pool": pool or [],
        "candidates": candidates or [],
        "decision": decisions or [],
    }
    created_at = utc_now_iso()

    for snapshot_type, payload in payloads.items():
        snapshot_id = _snapshot_id(group_id, snapshot_type)
        conn.execute(
            """INSERT OR REPLACE INTO signal_history_snapshots
               (snapshot_id, snapshot_date, history_group_id, run_id, phase,
                snapshot_type, payload_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                snapshot_id,
                date_value,
                group_id,
                run_id,
                phase,
                snapshot_type,
                json.dumps(payload, ensure_ascii=False, default=str),
                created_at,
            ),
        )
    return group_id


def diagnose_signal_history(
    conn: Any,
    *,
    snapshot_date: str | None = None,
    history_group_id: str = "",
    code: str = "",
) -> dict[str, Any]:
    """按日期/group/code 查看历史信号镜像。"""
    date_value = snapshot_date or local_today_str()
    groups = _history_groups(conn, date_value)
    selected_group_id = history_group_id or (groups[-1]["history_group_id"] if groups else "")
    if not selected_group_id:
        return {
            "status": "empty",
            "snapshot_date": date_value,
            "history_group_id": "",
            "groups": [],
            "sections": _empty_sections(),
            "code_analysis": _code_not_found(code),
        }

    rows = conn.execute(
        """SELECT snapshot_type, payload_json, run_id, phase, created_at
           FROM signal_history_snapshots
           WHERE snapshot_date = ? AND history_group_id = ?
           ORDER BY snapshot_type""",
        (date_value, selected_group_id),
    ).fetchall()
    sections = _empty_sections()
    meta = {"run_id": "", "phase": "", "created_at": ""}
    for row in rows:
        row_dict = dict(row)
        snapshot_type = row_dict["snapshot_type"]
        if snapshot_type in sections:
            sections[snapshot_type] = _decode_payload(row_dict.get("payload_json"))
        meta = {
            "run_id": row_dict.get("run_id", ""),
            "phase": row_dict.get("phase", ""),
            "created_at": row_dict.get("created_at", ""),
        }

    return {
        "status": "ok" if rows else "empty",
        "snapshot_date": date_value,
        "history_group_id": selected_group_id,
        "groups": groups,
        "run_id": meta["run_id"],
        "phase": meta["phase"],
        "created_at": meta["created_at"],
        "sections": sections,
        "code_analysis": _analyze_code(code, sections) if code else {},
    }


def load_signal_history_bundle(
    conn: Any,
    *,
    snapshot_date: str,
    history_group_id: str = "",
    phases: tuple[str, ...] = ("screener", "scoring"),
) -> dict[str, Any] | None:
    """读取可用于历史回放的一组信号镜像，缺失时返回 None。"""
    if history_group_id:
        payload = diagnose_signal_history(
            conn,
            snapshot_date=snapshot_date,
            history_group_id=history_group_id,
        )
        return payload if payload.get("status") == "ok" else None

    groups = [
        group
        for group in _history_groups(conn, snapshot_date)
        if not phases or group.get("phase") in phases
    ]
    if not groups:
        return None
    payload = diagnose_signal_history(
        conn,
        snapshot_date=snapshot_date,
        history_group_id=groups[-1]["history_group_id"],
    )
    return payload if payload.get("status") == "ok" else None


def archive_from_runtime_state(
    conn: Any,
    *,
    run_id: str,
    phase: str,
    candidates: list[dict] | None = None,
    decisions: list[dict] | None = None,
    history_group_id: str = "",
) -> str:
    """从当前投影和事件结果归档信号镜像。"""
    return archive_signal_history(
        conn,
        snapshot_date=local_today_str(),
        history_group_id=history_group_id,
        run_id=run_id,
        phase=phase,
        market=_market_snapshot(conn),
        pool=_pool_snapshot(conn),
        candidates=candidates or [],
        decisions=decisions or [],
    )


def archive_market_signal_snapshot(
    conn: Any,
    *,
    run_id: str,
    phase: str,
    market_state: Any,
    index_data: dict | None = None,
    history_group_id: str = "",
) -> str:
    """归档盘前/午间/收盘看到的大盘信号时点。"""
    return archive_signal_history(
        conn,
        snapshot_date=local_today_str(),
        history_group_id=history_group_id,
        run_id=run_id,
        phase=phase,
        market={
            "signal": _market_signal_value(market_state),
            "multiplier": getattr(market_state, "multiplier", 0.0),
            "detail": getattr(market_state, "detail", {}) or {},
            "indices": index_data or {},
        },
        pool=_pool_snapshot(conn),
        candidates=[],
        decisions=[],
    )


def _market_snapshot(conn: Any) -> dict:
    rows = conn.execute(
        """SELECT index_symbol, name, `signal`, price_cents, change_pct, ma20_pct, ma60_pct, updated_at
           FROM projection_market_state
           ORDER BY index_symbol"""
    ).fetchall()
    return {
        "indices": [dict(row) for row in rows],
        "signal": _dominant_signal([dict(row) for row in rows]),
    }


def _pool_snapshot(conn: Any) -> list[dict]:
    rows = conn.execute(
        """SELECT code, pool_tier, name, score, added_at, last_scored_at, streak_days, note
           FROM projection_candidate_pool
           ORDER BY pool_tier, score DESC, code"""
    ).fetchall()
    return [dict(row) for row in rows]


def _history_groups(conn: Any, snapshot_date: str) -> list[dict]:
    rows = conn.execute(
        """SELECT history_group_id, run_id, phase, MAX(created_at) AS created_at,
                  COUNT(*) AS section_count
           FROM signal_history_snapshots
           WHERE snapshot_date = ?
           GROUP BY history_group_id, run_id, phase
           ORDER BY created_at""",
        (snapshot_date,),
    ).fetchall()
    return [dict(row) for row in rows]


def _analyze_code(code: str, sections: dict[str, Any]) -> dict[str, Any]:
    candidate = _find_by_code(sections.get("candidates", []), code)
    decision = _find_by_code(sections.get("decision", []), code)
    pool_item = _find_by_code(sections.get("pool", []), code)
    miss_reason = _miss_reason(candidate, decision, pool_item)
    return {
        "code": code,
        "candidate": candidate,
        "decision": decision,
        "pool_item": pool_item,
        "decision_action": str((decision or {}).get("action", "")),
        "miss_reason": miss_reason,
    }


def _miss_reason(candidate: dict | None, decision: dict | None, pool_item: dict | None) -> str:
    if decision:
        action = str(decision.get("action", ""))
        if action == "BUY":
            return "已形成买入意向，仍需人工确认。"
        if action == "WATCH":
            notes = "；".join(str(item) for item in decision.get("notes", []) if item)
            return f"观察：{notes or '评分或门禁未达到买入意向。'}"
        if action in {"NO_TRADE", "SELL"}:
            reasons = decision.get("veto_reasons") or decision.get("notes") or []
            detail = "；".join(str(item) for item in reasons if item)
            return f"不操作：{detail or '当前决策未通过交易门槛。'}"
        return f"决策为 {action or '未知'}，未形成买入意向。"
    if candidate:
        veto = candidate.get("hard_veto_signals") or candidate.get("veto_reasons") or []
        if veto:
            return "否决：" + "；".join(str(item) for item in veto)
        if candidate.get("entry_signal") is False:
            return "缺少入场信号，未形成买入意向。"
        return "有候选评分，但未找到对应决策事件。"
    if pool_item:
        return "仅在候选池中出现，本次未进入评分候选或未生成决策。"
    return "历史镜像中未命中该股票。"


def _find_by_code(items: Any, code: str) -> dict | None:
    if not isinstance(items, list):
        return None
    for item in items:
        if isinstance(item, dict) and str(item.get("code", "")) == str(code):
            return item
    return None


def _empty_sections() -> dict[str, Any]:
    return {
        "market": {},
        "pool": [],
        "candidates": [],
        "decision": [],
    }


def _code_not_found(code: str) -> dict[str, Any]:
    return {
        "code": code,
        "candidate": None,
        "decision": None,
        "pool_item": None,
        "decision_action": "",
        "miss_reason": "历史镜像中未命中该股票。" if code else "",
    }


def _decode_payload(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _history_group_id(snapshot_date: str, phase: str, run_id: str) -> str:
    raw = "_".join(part for part in (snapshot_date, phase, run_id) if part)
    normalized = re.sub(r"[^0-9A-Za-z_\-]", "_", raw).strip("_")
    return normalized or f"{snapshot_date}_{phase}"


def _snapshot_id(history_group_id: str, snapshot_type: str) -> str:
    return hashlib.sha1(f"{history_group_id}:{snapshot_type}".encode("utf-8")).hexdigest()[:24]


def _dominant_signal(rows: list[dict]) -> str:
    for row in rows:
        signal = row.get("signal")
        if signal:
            return str(signal)
    return ""


def _market_signal_value(market_state: Any) -> str:
    signal = getattr(market_state, "signal", "")
    return str(getattr(signal, "value", signal))
