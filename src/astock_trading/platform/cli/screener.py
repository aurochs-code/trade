"""Stock screener CLI commands."""

from __future__ import annotations

import asyncio
from collections import Counter
from datetime import timedelta
from typing import Optional

import typer

from astock_trading.market.adapters import MXScreenerAdapter
from astock_trading.pipeline.context import build_context
from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.db import connect
from astock_trading.platform.events import EventStore
from astock_trading.platform.time import local_now, local_now_str
from astock_trading.reporting.projectors import ProjectionUpdater


screener_app = typer.Typer(name="screener", help="选股、评分和候选池管理")


def _split_codes(codes: str) -> list[str]:
    return [part.strip() for part in codes.replace("，", ",").split(",") if part.strip()]


def _candidate_rows(conn, tier: str = "all", limit: int = 100) -> list[dict]:
    if tier == "all":
        rows = conn.execute(
            """SELECT code, pool_tier, name, score, added_at, last_scored_at, streak_days, note
               FROM projection_candidate_pool
               ORDER BY pool_tier, score DESC, code
               LIMIT ?""",
            (limit,),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT code, pool_tier, name, score, added_at, last_scored_at, streak_days, note
               FROM projection_candidate_pool
               WHERE pool_tier = ?
               ORDER BY score DESC, code
               LIMIT ?""",
            (tier, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def _score_stock_list(ctx, stock_list: list[dict], run_id: str) -> list[dict]:
    snapshots = asyncio.run(
        ctx.market_svc.collect_batch(stock_list, run_id, include_sector_context=True)
    )
    market_state, index_data = asyncio.run(ctx.market_svc.collect_market_state(run_id))
    if index_data:
        ctx.projector.sync_market_state(index_data)
    ctx.strategy_svc.evaluate(snapshots, market_state, run_id, ctx.config_version)
    events = ctx.event_store.query(
        event_type="score.calculated",
        metadata_filter={"run_id": run_id},
    )
    scores = [event["payload"] for event in events]
    scores.sort(key=lambda item: item.get("total_score", 0), reverse=True)
    return scores


def _watch_threshold(ctx, explicit_threshold: Optional[float]) -> float:
    if explicit_threshold is not None:
        return explicit_threshold
    pool_cfg = ctx.cfg.get("pool_management", {})
    scoring_cfg = ctx.cfg.get("scoring", {})
    return float(
        pool_cfg.get("promote_min_score")
        or scoring_cfg.get("thresholds", {}).get("buy")
        or 5.5
    )


def _pool_thresholds(ctx) -> dict[str, float]:
    pool_cfg = ctx.cfg.get("pool_management", {})
    scoring_cfg = ctx.cfg.get("scoring", {})
    thresholds = scoring_cfg.get("thresholds", {})
    return {
        "promote": float(pool_cfg.get("promote_min_score") or thresholds.get("buy") or 5.5),
        "watch": float(pool_cfg.get("watch_min_score") or thresholds.get("watch") or 5.0),
        "reject": float(pool_cfg.get("remove_max_score") or thresholds.get("reject") or 4.0),
        "promote_streak_days": int(pool_cfg.get("promote_streak_days") or 1),
    }


def _scan_limit(cfg: dict, explicit_limit: Optional[int]) -> int:
    if explicit_limit is not None:
        return explicit_limit
    return int(cfg.get("market_scan_limit") or 30)


CORE_ROUTE_BLOCKER = "requires_entry_strategy_route"
ACTION_CN = {
    "BUY": "买入意向",
    "SELL": "卖出意向",
    "WATCH": "观察",
    "CLEAR": "观望",
    "NO_TRADE": "不操作",
}
MARKET_SIGNAL_CN = {
    "GREEN": "偏强",
    "YELLOW": "震荡",
    "RED": "转弱",
    "CLEAR": "观望",
}
DATA_QUALITY_CN = {
    "ok": "正常",
    "degraded": "降级",
    "error": "错误",
}
BLOCKER_CN = {
    "below_ma20": "跌破 MA20",
    "limit_up_today": "当日涨停",
    "consecutive_outflow": "连续资金流出",
    "consecutive_outflow_warn": "连续资金流出预警",
    "ma20_trend_down": "MA20 趋势下行",
    "red_market": "大盘转弱",
    "earnings_bomb": "业绩雷",
    CORE_ROUTE_BLOCKER: "缺少有效策略路线",
}


def _label(mapping: dict[str, str], value: object) -> str:
    text = str(value)
    return mapping.get(text, text)


def _score_value(payload: dict) -> float:
    return float(payload.get("total_score", payload.get("total", payload.get("score", 0))) or 0)


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _counter_rows(counter: Counter, *, labels: dict[str, str]) -> list[dict]:
    return [
        {"reason": key, "label": _label(labels, key), "count": count}
        for key, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    ]


def _quality_rows(counter: Counter) -> list[dict]:
    order = {"ok": 0, "degraded": 1, "error": 2}
    return [
        {"quality": key, "label": _label(DATA_QUALITY_CN, key), "count": count}
        for key, count in sorted(counter.items(), key=lambda item: (order.get(item[0], 99), item[0]))
    ]


def _decision_count_rows(decisions: list[dict], key: str, labels: dict[str, str]) -> list[dict]:
    groups: dict[str, list[dict]] = {}
    for decision in decisions:
        value = str(decision.get(key, "-"))
        groups.setdefault(value, []).append(decision)
    rows = []
    for value, items in groups.items():
        scores = [_score_value(item) for item in items]
        rows.append({
            key: value,
            "label": _label(labels, value),
            "count": len(items),
            "max_score": max(scores) if scores else 0,
        })
    rows.sort(key=lambda item: (-item["count"], item[key]))
    return rows


def _near_miss_blockers(score: dict, buy_threshold: float) -> list[str]:
    blockers = []
    hard_veto = score.get("hard_veto_signals") or []
    if hard_veto:
        blockers.extend(_label(BLOCKER_CN, item) for item in hard_veto)
    if not _truthy(score.get("entry_signal")):
        blockers.append("缺少入场信号")
    quality = str(score.get("data_quality", "ok"))
    if quality != "ok":
        blockers.append(f"数据质量{_label(DATA_QUALITY_CN, quality)}")
    missing = score.get("data_missing_fields") or []
    if missing:
        blockers.append(f"缺失字段: {', '.join(str(item) for item in missing)}")
    total = _score_value(score)
    if total < buy_threshold:
        blockers.append(f"分数低于买入线 {buy_threshold:.1f}")
    return blockers


def _candidate_follow_up_item(score: dict, buy_threshold: float) -> dict:
    quality = str(score.get("data_quality", "ok"))
    hard_veto = [str(item) for item in (score.get("hard_veto_signals") or [])]
    return {
        "code": score.get("code", ""),
        "name": score.get("name", ""),
        "score": _score_value(score),
        "data_quality": quality,
        "data_quality_label": _label(DATA_QUALITY_CN, quality),
        "entry_signal": _truthy(score.get("entry_signal")),
        "veto_triggered": bool(score.get("veto_triggered")),
        "hard_veto_signals": hard_veto,
        "hard_veto_labels": [_label(BLOCKER_CN, item) for item in hard_veto],
        "missing_fields": [str(item) for item in (score.get("data_missing_fields") or [])],
        "blockers": _near_miss_blockers(score, buy_threshold),
    }


def _follow_up_candidates(
    scores: list[dict],
    *,
    buy_threshold: float,
    watch_threshold: float,
    reject_threshold: float,
    limit: int = 10,
) -> dict:
    sorted_scores = sorted(scores, key=_score_value, reverse=True)
    near_watch_floor = max(reject_threshold, watch_threshold - 1.0)
    watch_candidates = []
    near_watch_candidates = []
    blocked_high_scores = []
    data_repair_candidates = []

    for score in sorted_scores:
        total = _score_value(score)
        veto = bool(score.get("veto_triggered"))
        quality = str(score.get("data_quality", "ok"))
        missing = score.get("data_missing_fields") or []
        item = _candidate_follow_up_item(score, buy_threshold)

        if not veto and watch_threshold <= total < buy_threshold:
            watch_candidates.append(item)
        if not veto and near_watch_floor <= total < watch_threshold:
            near_watch_candidates.append(item)
        if veto and total >= watch_threshold:
            blocked_high_scores.append(item)
        if quality != "ok" or missing:
            data_repair_candidates.append(item)

    return {
        "watch_candidates": watch_candidates[:limit],
        "near_watch_candidates": near_watch_candidates[:limit],
        "blocked_high_scores": blocked_high_scores[:limit],
        "data_repair_candidates": data_repair_candidates[:limit],
    }


def _next_actions(follow_up: dict) -> list[dict]:
    actions = []
    watch_candidates = follow_up.get("watch_candidates") or []
    near_watch_candidates = follow_up.get("near_watch_candidates") or []
    blocked_high_scores = follow_up.get("blocked_high_scores") or []
    data_repair_candidates = follow_up.get("data_repair_candidates") or []

    if watch_candidates:
        code = watch_candidates[0].get("code", "")
        actions.append({
            "type": "stock_analysis",
            "label": "复核观察候选",
            "command": f"atrade stock analyze {code} --json",
        })
    if near_watch_candidates:
        code = near_watch_candidates[0].get("code", "")
        actions.append({
            "type": "near_watch_review",
            "label": "复核临界观察候选",
            "command": f"atrade stock analyze {code} --json",
        })
    if blocked_high_scores:
        code = blocked_high_scores[0].get("code", "")
        actions.append({
            "type": "blocked_candidate_review",
            "label": "复核高分被拦截候选",
            "command": f"atrade stock analyze {code} --json",
        })
    if data_repair_candidates:
        code = data_repair_candidates[0].get("code", "")
        actions.append({
            "type": "data_repair_review",
            "label": "复核数据补齐候选",
            "command": f"atrade stock analyze {code} --json",
        })
    if not actions:
        actions.append({
            "type": "refresh_scores",
            "label": "刷新评分证据",
            "command": "atrade screener refresh --json",
        })
    return actions


def _build_screener_explanation(
    scores: list[dict],
    decisions: list[dict],
    *,
    thresholds: dict[str, float],
    since: str,
    run_id: str | None = None,
    near_miss_margin: float = 1.0,
    near_miss_limit: int = 20,
) -> dict:
    buy_threshold = float(thresholds.get("buy") or 6.0)
    watch_threshold = float(thresholds.get("watch") or 5.0)
    reject_threshold = float(thresholds.get("reject") or 4.0)
    near_buy_floor = max(watch_threshold, buy_threshold - near_miss_margin)

    bucket_counts = {
        "buy_ready_raw": 0,
        "near_buy": 0,
        "watch_band": 0,
        "reject_band": 0,
        "below_reject": 0,
    }
    quality_counter: Counter = Counter()
    missing_counter: Counter = Counter()
    hard_veto_counter: Counter = Counter()
    decision_veto_counter: Counter = Counter()
    entry_signal_count = 0

    for score in scores:
        total = _score_value(score)
        if total >= buy_threshold:
            bucket_counts["buy_ready_raw"] += 1
        elif total >= near_buy_floor:
            bucket_counts["near_buy"] += 1
        elif total >= watch_threshold:
            bucket_counts["watch_band"] += 1
        elif total >= reject_threshold:
            bucket_counts["reject_band"] += 1
        else:
            bucket_counts["below_reject"] += 1

        quality_counter.update([str(score.get("data_quality", "ok"))])
        missing_counter.update(str(item) for item in (score.get("data_missing_fields") or []))
        hard_veto_counter.update(str(item) for item in (score.get("hard_veto_signals") or []))
        if _truthy(score.get("entry_signal")):
            entry_signal_count += 1

    for decision in decisions:
        decision_veto_counter.update(str(item) for item in (decision.get("veto_reasons") or []))

    near_misses = []
    for score in sorted(scores, key=_score_value, reverse=True):
        total = _score_value(score)
        if len(near_misses) >= near_miss_limit:
            break
        if total < near_buy_floor or total >= buy_threshold or bool(score.get("veto_triggered")):
            continue
        near_misses.append({
            "code": score.get("code", ""),
            "name": score.get("name", ""),
            "score": total,
            "data_quality": score.get("data_quality", "ok"),
            "entry_signal": _truthy(score.get("entry_signal")),
            "blockers": _near_miss_blockers(score, buy_threshold),
        })

    top_scores = [
        {
            "code": score.get("code", ""),
            "name": score.get("name", ""),
            "score": _score_value(score),
            "data_quality": score.get("data_quality", "ok"),
            "entry_signal": _truthy(score.get("entry_signal")),
            "veto_triggered": bool(score.get("veto_triggered")),
            "hard_veto_signals": score.get("hard_veto_signals") or [],
        }
        for score in sorted(scores, key=_score_value, reverse=True)[:10]
    ]
    follow_up = _follow_up_candidates(
        scores,
        buy_threshold=buy_threshold,
        watch_threshold=watch_threshold,
        reject_threshold=reject_threshold,
    )

    recommendations = []
    if not scores:
        summary = "最近没有评分事件；先运行 screener run 或 refresh，再判断是否真没有候选。"
        recommendations.append("先执行 atrade screener refresh --json 生成新的评分证据")
    elif bucket_counts["buy_ready_raw"] == 0 and not near_misses:
        summary = "近期候选整体评分不足，当前不应通过降低买入线来制造交易。"
        recommendations.append("扩大召回或补齐数据源，但保持买入门槛不变")
    elif near_misses:
        summary = f"发现 {len(near_misses)} 个临界候选；适合进入观察池，不适合直接当作买入意向。"
        recommendations.append("把临界候选列入观察并跟踪入场信号、资金流和数据质量变化")
    else:
        summary = "近期存在原始分数达标候选，但仍需检查入场信号、数据质量和风控门禁。"
        recommendations.append("逐只查看高分候选的门禁原因，避免把观察信号误作买入意向")

    if hard_veto_counter:
        recommendations.append("优先查看硬否决最高的原因，判断是市场结构问题还是数据补齐问题")
    if quality_counter.get("degraded", 0) or quality_counter.get("error", 0):
        recommendations.append("补齐降级或错误数据源；热度源只能召回，不能替代行情和资金证据")

    return {
        "diagnostic": "screener_explain",
        "status": "ok" if scores else "warning",
        "summary": summary,
        "scope": {
            "since": since,
            "run_id": run_id,
            "score_events": len(scores),
            "decision_events": len(decisions),
        },
        "thresholds": {
            "buy": buy_threshold,
            "watch": watch_threshold,
            "reject": reject_threshold,
            "near_buy_floor": near_buy_floor,
        },
        "score_buckets": bucket_counts,
        "decision_actions": _decision_count_rows(decisions, "action", ACTION_CN),
        "market_signals": _decision_count_rows(decisions, "market_signal", MARKET_SIGNAL_CN),
        "blockers": {
            "entry_signal": {
                "triggered": entry_signal_count,
                "missing": max(len(scores) - entry_signal_count, 0),
            },
            "hard_veto_reasons": _counter_rows(hard_veto_counter, labels=BLOCKER_CN),
            "decision_veto_reasons": _counter_rows(decision_veto_counter, labels=BLOCKER_CN),
            "data_quality": _quality_rows(quality_counter),
            "missing_fields": [
                {"field": key, "count": count}
                for key, count in sorted(missing_counter.items(), key=lambda item: (-item[1], item[0]))
            ],
        },
        "near_misses": near_misses,
        "follow_up": follow_up,
        "top_scores": top_scores,
        "next_actions": _next_actions(follow_up),
        "recommendations": recommendations,
    }


def _add_watch_candidates(ctx, scores: list[dict], threshold: float, run_id: str) -> list[dict]:
    existing = {
        row["code"]
        for row in ctx.conn.execute("SELECT code FROM projection_candidate_pool").fetchall()
    }
    added = []
    entries = []
    for score in scores:
        code = score.get("code", "")
        total = float(score.get("total_score", score.get("total", 0)) or 0)
        if not code or code in existing or score.get("veto_triggered") or total < threshold:
            continue
        item = {
            "code": code,
            "name": score.get("name", ""),
            "pool_tier": "watch",
            "score": total,
            "note": "screener_auto_watch",
        }
        entries.append(item)
        added.append({"code": code, "name": item["name"], "score": total})
        existing.add(code)
    if entries:
        ctx.projector.sync_candidate_pool(entries)
        for item in entries:
            ctx.event_store.append(
                stream=f"candidate:{item['code']}",
                stream_type="candidate",
                event_type="candidate.added",
                payload=item,
                metadata={"source": "cli.screener", "run_id": run_id},
            )
    return added


def _pool_rows_by_code(ctx) -> dict[str, dict]:
    return {
        row["code"]: dict(row)
        for row in ctx.conn.execute(
            """SELECT code, pool_tier, name, score, added_at, last_scored_at,
                      streak_days, note
               FROM projection_candidate_pool"""
        ).fetchall()
    }


def _score_name(score: dict, existing: dict | None = None) -> str:
    return score.get("name") or (existing or {}).get("name", "") or score.get("code", "")


def _route_has_entry_signal(route: object) -> bool:
    if isinstance(route, dict):
        return bool(route.get("entry_signal"))
    return bool(getattr(route, "entry_signal", False))


def _core_promotion_blockers(score: dict) -> list[str]:
    routes = score.get("strategy_routes") or []
    if any(_route_has_entry_signal(route) for route in routes):
        return []
    return [CORE_ROUTE_BLOCKER]


def _pool_change(
    code: str,
    name: str,
    score: float,
    old_tier: str | None,
    tier: str,
    *,
    reason: str | None = None,
) -> dict:
    item = {"code": code, "name": name, "score": score, "from": old_tier, "to": tier}
    if reason:
        item["reason"] = reason
    return item


def _apply_candidate_pool_refresh(ctx, scores: list[dict], run_id: str) -> dict:
    thresholds = _pool_thresholds(ctx)
    existing = _pool_rows_by_code(ctx)
    promoted: list[dict] = []
    watched: list[dict] = []
    rejected: list[dict] = []
    updated: list[dict] = []
    projection_entries: list[dict] = []

    for score in scores:
        code = score.get("code", "")
        if not code:
            continue
        current = existing.get(code)
        total = float(score.get("total_score", score.get("total", 0)) or 0)
        veto = bool(score.get("veto_triggered"))
        name = _score_name(score, current)
        old_tier = (current or {}).get("pool_tier")

        if veto or total < thresholds["watch"]:
            reason = "veto" if veto else f"score<{thresholds['watch']:.1f}"
            ctx.event_store.append(
                stream=f"candidate:{code}",
                stream_type="candidate",
                event_type="candidate.rejected",
                payload={
                    "code": code,
                    "name": name,
                    "score": total,
                    "reason": reason,
                    "removed": [current] if current else [],
                },
                metadata={"source": "cli.screener.refresh", "run_id": run_id},
            )
            ctx.conn.execute("DELETE FROM projection_candidate_pool WHERE code = ?", (code,))
            rejected.append({"code": code, "name": name, "score": total, "reason": reason})
            continue

        old_streak = int((current or {}).get("streak_days", 0) or 0)
        promotion_blockers = (
            _core_promotion_blockers(score)
            if total >= thresholds["promote"] and old_tier != "core"
            else []
        )
        promotion_blocker = promotion_blockers[0] if promotion_blockers else None
        if total >= thresholds["promote"]:
            new_streak = old_streak + 1 if old_streak >= 0 else 1
            tier = (
                "core"
                if old_tier == "core"
                or (new_streak >= thresholds["promote_streak_days"] and not promotion_blockers)
                else "watch"
            )
        else:
            new_streak = 0
            tier = "watch"
        note = "screener_refresh"
        if tier == "watch" and total >= thresholds["promote"] and promotion_blocker:
            note = f"{note}:{promotion_blocker}"
        entry = {
            "code": code,
            "name": name,
            "pool_tier": tier,
            "score": total,
            "added_at": (current or {}).get("added_at") or local_now_str("%Y-%m-%d"),
            "streak_days": new_streak,
            "note": note,
        }

        if tier == "core" and old_tier != "core":
            event_type = "candidate.promoted"
            promoted.append(_pool_change(code, name, total, old_tier, tier))
        elif tier == "watch" and old_tier == "core":
            event_type = "pool.demoted"
            watched.append(_pool_change(code, name, total, old_tier, tier))
        elif tier == "watch" and total >= thresholds["promote"]:
            event_type = "candidate.updated" if current else "candidate.added"
            watched.append(_pool_change(
                code,
                name,
                total,
                old_tier,
                tier,
                reason=promotion_blocker,
            ))
        elif current:
            event_type = "candidate.updated"
            updated.append({"code": code, "name": name, "score": total, "pool_tier": tier})
        else:
            event_type = "candidate.added"
            watched.append({"code": code, "name": name, "score": total, "from": None, "to": tier})

        payload = {
            "code": code,
            "name": name,
            "pool_tier": tier,
            "score": total,
            "note": note,
            "from": old_tier,
            "to": tier,
        }
        if promotion_blockers:
            payload["promotion_blockers"] = promotion_blockers
        ctx.event_store.append(
            stream=f"candidate:{code}" if event_type != "pool.demoted" else f"strategy:{code}",
            stream_type="candidate" if event_type != "pool.demoted" else "strategy",
            event_type=event_type,
            payload=payload,
            metadata={"source": "cli.screener.refresh", "run_id": run_id},
        )
        ctx.conn.execute("DELETE FROM projection_candidate_pool WHERE code = ?", (code,))
        projection_entries.append(entry)

    if projection_entries:
        ctx.projector.sync_candidate_pool(projection_entries)

    return {
        "thresholds": thresholds,
        "promoted": promoted,
        "watched": watched,
        "updated": updated,
        "rejected": rejected,
    }


def _run_screener(
    query: str,
    limit: Optional[int],
    watch_threshold: Optional[float],
    as_json: bool,
    *,
    refresh_pool: bool = False,
) -> None:
    ctx = build_context()
    try:
        cfg = ctx.cfg.get("screening", {})
        q = query.strip() or cfg.get("mx_query", "")
        if not q:
            raise typer.BadParameter("screener run requires --query or strategy.screening.mx_query")
        score_limit = _scan_limit(cfg, limit)

        raw_results = asyncio.run(MXScreenerAdapter().search_stocks(q))
        stock_list = [
            {"code": row.get("code") or row.get("代码", ""), "name": row.get("name") or row.get("名称", "")}
            for row in raw_results
            if row.get("code") or row.get("代码")
        ][:score_limit]
        if not stock_list:
            payload = {"query": q, "screened": len(raw_results), "scored": [], "added_to_watch": []}
            json_or_text(payload, as_json)
            return

        run_id = f"screener_{local_now_str('%H%M%S')}"
        if refresh_pool:
            seen = {item["code"] for item in stock_list}
            for row in _candidate_rows(ctx.conn, tier="all", limit=1000):
                code = row.get("code", "")
                if code and code not in seen:
                    stock_list.append({"code": code, "name": row.get("name") or ""})
                    seen.add(code)

        scores = _score_stock_list(ctx, stock_list, run_id)
        threshold = _watch_threshold(ctx, watch_threshold)
        if refresh_pool:
            pool_changes = _apply_candidate_pool_refresh(ctx, scores, run_id)
            added = [item for item in pool_changes["watched"] if item.get("from") is None]
        else:
            pool_changes = {}
            added = _add_watch_candidates(ctx, scores, threshold, run_id)
        ctx.obsidian.write_screening_result(run_id, q, scores, added, buy_threshold=threshold)

        payload = {
            "query": q,
            "run_id": run_id,
            "screened": len(raw_results),
            "threshold": threshold,
            "scored": scores,
            "added_to_watch": added,
        }
        if pool_changes:
            payload["pool_changes"] = pool_changes
        json_or_text(payload, as_json)
    finally:
        ctx.conn.close()


@screener_app.command("run")
def screener_run(
    query: str = typer.Option("", "--query", "-q", help="选股条件；空值使用配置默认条件"),
    limit: Optional[int] = typer.Option(None, "--limit", help="最多评分数量；默认读取 strategy.screening.market_scan_limit"),
    watch_threshold: Optional[float] = typer.Option(None, "--watch-threshold", help="自动加入观察池的最低分；默认读取配置"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """执行选股筛选、评分，并把高分结果加入观察池。"""
    _run_screener(query, limit, watch_threshold, as_json)


@screener_app.command("refresh")
def screener_refresh(
    query: str = typer.Option("", "--query", "-q", help="选股条件；空值使用配置默认条件"),
    limit: Optional[int] = typer.Option(None, "--limit", help="最多评分数量；默认读取 strategy.screening.market_scan_limit"),
    watch_threshold: Optional[float] = typer.Option(None, "--watch-threshold", help="自动加入观察池的最低分；默认读取配置"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """刷新候选池：筛选、评分，并把达标结果写入候选池事件和投影。"""
    _run_screener(query, limit, watch_threshold, as_json, refresh_pool=True)


@screener_app.command("explain")
def screener_explain(
    since: str = typer.Option("", "--since", help="起始时间 ISO；空值使用 --days 回推"),
    days: int = typer.Option(7, "--days", help="未指定 --since 时回看天数"),
    run_id: str = typer.Option("", "--run-id", help="只分析指定 run_id 的评分/决策事件"),
    limit: int = typer.Option(1000, "--limit", help="最大读取事件数量"),
    near_miss_margin: float = typer.Option(1.0, "--near-miss-margin", help="买入线下方多少分视为临界候选"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """解释近期为什么没有合适候选，输出评分漏斗、否决原因和临界候选。"""
    if days < 1:
        raise typer.BadParameter("--days must be >= 1")
    if limit < 1:
        raise typer.BadParameter("--limit must be >= 1")

    ctx = build_context()
    try:
        since_value = since.strip() or (local_now() - timedelta(days=days)).isoformat()
        metadata_filter = {"run_id": run_id} if run_id else None
        score_events = ctx.event_store.query(
            event_type="score.calculated",
            since=since_value,
            limit=limit,
            metadata_filter=metadata_filter,
        )
        decision_events = ctx.event_store.query(
            event_type="decision.suggested",
            since=since_value,
            limit=limit,
            metadata_filter=metadata_filter,
        )
        thresholds = ctx.cfg.get("scoring", {}).get("thresholds", {})
        payload = _build_screener_explanation(
            [event["payload"] for event in score_events],
            [event["payload"] for event in decision_events],
            thresholds=thresholds,
            since=since_value,
            run_id=run_id or None,
            near_miss_margin=near_miss_margin,
        )
        json_or_text(payload, as_json)
    finally:
        ctx.conn.close()


@screener_app.command("score")
def screener_score(
    codes: str = typer.Option(..., "--codes", "-c", help="逗号分隔股票代码"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """对指定股票批量评分。"""
    stock_list = [{"code": code, "name": ""} for code in _split_codes(codes)]
    if not stock_list:
        raise typer.BadParameter("screener score requires --codes")

    ctx = build_context()
    try:
        run_id = f"screener_score_{local_now_str('%H%M%S')}"
        scores = _score_stock_list(ctx, stock_list, run_id)
        ctx.obsidian.write_scoring_report(run_id, scores)
        json_or_text({"run_id": run_id, "scores": scores, "count": len(scores)}, as_json)
    finally:
        ctx.conn.close()


@screener_app.command("candidates")
def screener_candidates(
    tier: str = typer.Option("all", "--tier", help="all / core / watch"),
    limit: int = typer.Option(100, "--limit", help="最大返回数量"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查看候选池。"""
    if tier not in {"all", "core", "watch"}:
        raise typer.BadParameter("--tier must be all, core, or watch")
    conn = connect()
    try:
        rows = _candidate_rows(conn, tier=tier, limit=limit)
        if as_json:
            json_or_text(rows, True)
        elif not rows:
            typer.echo("候选池为空")
        else:
            for row in rows:
                typer.echo(
                    f"{row['pool_tier']} {row['code']} {row.get('name') or ''} "
                    f"score={row.get('score', '-')}"
                )
    finally:
        conn.close()


@screener_app.command("promote")
def screener_promote(
    code: str = typer.Argument(..., help="股票代码"),
    to: str = typer.Option("core", "--to", help="core / watch"),
    name: str = typer.Option("", "--name", help="股票名称"),
    score: float = typer.Option(0.0, "--score", help="人工指定评分"),
    note: str = typer.Option("manual_promote", "--note", help="备注"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """把股票加入或提升到候选池。"""
    if to not in {"core", "watch"}:
        raise typer.BadParameter("--to must be core or watch")
    conn = connect()
    try:
        store = EventStore(conn)
        conn.execute("DELETE FROM projection_candidate_pool WHERE code = ?", (code,))
        ProjectionUpdater(store, conn).sync_candidate_pool(
            [{"code": code, "name": name, "pool_tier": to, "score": score, "note": note}]
        )
        event_id = store.append(
            stream=f"candidate:{code}",
            stream_type="candidate",
            event_type="candidate.promoted",
            payload={"code": code, "name": name, "pool_tier": to, "score": score, "note": note},
            metadata={"source": "cli.screener"},
        )
        json_or_text(
            {"status": "promoted", "event_id": event_id, "code": code, "pool_tier": to},
            as_json,
        )
    finally:
        conn.close()


@screener_app.command("reject")
def screener_reject(
    code: str = typer.Argument(..., help="股票代码"),
    reason: str = typer.Option("", "--reason", help="拒绝原因"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """从候选池移除股票并记录拒绝原因。"""
    conn = connect()
    try:
        store = EventStore(conn)
        removed = conn.execute(
            "SELECT code, pool_tier, name, score FROM projection_candidate_pool WHERE code = ?",
            (code,),
        ).fetchall()
        conn.execute("DELETE FROM projection_candidate_pool WHERE code = ?", (code,))
        event_id = store.append(
            stream=f"candidate:{code}",
            stream_type="candidate",
            event_type="candidate.rejected",
            payload={"code": code, "reason": reason, "removed": [dict(row) for row in removed]},
            metadata={"source": "cli.screener"},
        )
        json_or_text(
            {"status": "rejected", "event_id": event_id, "code": code, "removed": len(removed)},
            as_json,
        )
    finally:
        conn.close()
