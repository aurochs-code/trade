"""Stock screener CLI commands."""

from __future__ import annotations

import asyncio
from typing import Optional

import typer

from astock_trading.market.adapters import MXScreenerAdapter
from astock_trading.pipeline.context import build_context
from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.db import connect
from astock_trading.platform.events import EventStore
from astock_trading.platform.time import local_now_str
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
    snapshots = asyncio.run(ctx.market_svc.collect_batch(stock_list, run_id))
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
        if total >= thresholds["promote"]:
            new_streak = old_streak + 1 if old_streak >= 0 else 1
            tier = (
                "core"
                if (current or {}).get("pool_tier") == "core"
                or new_streak >= thresholds["promote_streak_days"]
                else "watch"
            )
        else:
            new_streak = 0
            tier = "watch"
        note = "screener_refresh"
        entry = {
            "code": code,
            "name": name,
            "pool_tier": tier,
            "score": total,
            "added_at": (current or {}).get("added_at") or local_now_str("%Y-%m-%d"),
            "streak_days": new_streak,
            "note": note,
        }

        old_tier = (current or {}).get("pool_tier")
        if tier == "core" and old_tier != "core":
            event_type = "candidate.promoted"
            promoted.append({"code": code, "name": name, "score": total, "from": old_tier, "to": tier})
        elif tier == "watch" and old_tier == "core":
            event_type = "pool.demoted"
            watched.append({"code": code, "name": name, "score": total, "from": old_tier, "to": tier})
        elif tier == "watch" and total >= thresholds["promote"]:
            event_type = "candidate.updated" if current else "candidate.added"
            watched.append({"code": code, "name": name, "score": total, "from": old_tier, "to": tier})
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
    limit: int,
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

        raw_results = asyncio.run(MXScreenerAdapter().search_stocks(q))
        stock_list = [
            {"code": row.get("code") or row.get("代码", ""), "name": row.get("name") or row.get("名称", "")}
            for row in raw_results
            if row.get("code") or row.get("代码")
        ][:limit]
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
    limit: int = typer.Option(30, "--limit", help="最多评分数量"),
    watch_threshold: Optional[float] = typer.Option(None, "--watch-threshold", help="自动加入观察池的最低分；默认读取配置"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """执行选股筛选、评分，并把高分结果加入观察池。"""
    _run_screener(query, limit, watch_threshold, as_json)


@screener_app.command("refresh")
def screener_refresh(
    query: str = typer.Option("", "--query", "-q", help="选股条件；空值使用配置默认条件"),
    limit: int = typer.Option(30, "--limit", help="最多评分数量"),
    watch_threshold: Optional[float] = typer.Option(None, "--watch-threshold", help="自动加入观察池的最低分；默认读取配置"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """刷新候选池：筛选、评分，并把达标结果写入候选池事件和投影。"""
    _run_screener(query, limit, watch_threshold, as_json, refresh_pool=True)


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
