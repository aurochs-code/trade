"""候选池评分证据补全工具。"""

from __future__ import annotations

import json
from typing import Any


def enrich_candidate_rows_with_latest_scores(conn: Any, rows: list[dict[str, Any]]) -> None:
    """用最新评分事件补全候选池行，便于 CLI 和报告展示入场证据。"""
    codes = [str(row.get("code") or "") for row in rows if row.get("code")]
    if not codes:
        return

    stream_names = [stream for code in codes for stream in (f"strategy:{code}", f"score:{code}")]
    placeholders = ",".join("?" for _ in stream_names)
    try:
        score_rows = conn.execute(
            f"""SELECT stream, payload_json, occurred_at, stream_version
                FROM event_log
                WHERE event_type = 'score.calculated'
                  AND stream IN ({placeholders})
                ORDER BY occurred_at DESC, stream_version DESC""",
            tuple(stream_names),
        ).fetchall()
    except Exception:
        return

    latest_by_code: dict[str, dict[str, Any]] = {}
    for score_row in score_rows:
        stream = str(score_row["stream"])
        code = stream.split(":", 1)[1] if ":" in stream else ""
        if not code or code in latest_by_code:
            continue
        try:
            payload = json.loads(score_row["payload_json"])
        except (TypeError, json.JSONDecodeError):
            payload = {}
        if isinstance(payload, dict):
            latest_by_code[code] = payload

    for row in rows:
        score = latest_by_code.get(str(row.get("code") or ""))
        if not score:
            row.setdefault("entry_signal", None)
            row.setdefault("primary_strategy_route", None)
            row.setdefault("primary_strategy_route_label", None)
            row.setdefault("strategy_routes", [])
            row.setdefault("technical_detail", "")
            row.setdefault("data_quality", "")
            continue

        routes = score.get("strategy_routes") or []
        primary_route = score.get("primary_strategy_route")
        primary_route_label = _primary_route_label(routes, primary_route)
        row.update({
            "entry_signal": _truthy(score.get("entry_signal")),
            "primary_strategy_route": primary_route,
            "primary_strategy_route_label": primary_route_label,
            "strategy_routes": routes,
            "technical_detail": score.get("technical_detail", ""),
            "data_quality": score.get("data_quality", ""),
        })


def _primary_route_label(routes: list[Any], primary_route: object) -> str | None:
    for route in routes:
        if not isinstance(route, dict):
            continue
        if primary_route and route.get("route") != primary_route:
            continue
        label = route.get("display_name")
        if label:
            return str(label)
    return None


def _truthy(value: object) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)
