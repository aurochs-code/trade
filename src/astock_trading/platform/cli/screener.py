"""Stock screener CLI commands."""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import os
import signal
import subprocess
import sys
from collections import Counter
from datetime import timedelta
from typing import Optional

import typer

from astock_trading.market.models import StockSnapshot
from astock_trading.pipeline.context import build_context
from astock_trading.platform.candidate_evidence import enrich_candidate_rows_with_latest_scores
from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.db import connect
from astock_trading.platform.events import EventStore
from astock_trading.platform.history_mirror import archive_from_runtime_state
from astock_trading.platform.time import local_now, local_now_str
from astock_trading.reporting.projectors import ProjectionUpdater


screener_app = typer.Typer(name="screener", help="选股、评分和候选池管理")
DEFAULT_SNAPSHOT_TIMEOUT_SECONDS = 20.0
DEFAULT_SCREENER_QUERY_TIMEOUT_SECONDS = 30.0
DEFAULT_SCREENER_SCORING_TIMEOUT_SECONDS = 90.0


class ScreenerSearchTimeout(Exception):
    """选股粗筛源超时。"""

    def __init__(self, query: str, timeout_seconds: float):
        super().__init__(f"screener search timeout after {timeout_seconds:.1f}s: {query}")
        self.query = query
        self.timeout_seconds = timeout_seconds


class ScreenerSearchFailed(Exception):
    """选股粗筛子进程失败。"""

    def __init__(self, query: str, *, returncode: int, stderr_tail: str):
        super().__init__(f"screener search failed with code {returncode}: {query}")
        self.query = query
        self.returncode = returncode
        self.stderr_tail = stderr_tail


class ScreenerScoringTimeout(Exception):
    """逐票评分或行情采集超时。"""

    def __init__(self, query: str, timeout_seconds: float):
        super().__init__(f"screener scoring timeout after {timeout_seconds:.1f}s: {query}")
        self.query = query
        self.timeout_seconds = timeout_seconds


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
    result = [dict(row) for row in rows]
    enrich_candidate_rows_with_latest_scores(conn, result)
    return result


def _score_stock_batch(ctx, stock_list: list[dict], run_id: str) -> dict:
    cfg = ctx.cfg.get("screening", {})
    snapshot_timeout = float(
        cfg.get("snapshot_timeout_seconds") or DEFAULT_SNAPSHOT_TIMEOUT_SECONDS
    )
    sector_context_timeout = float(cfg.get("sector_context_timeout_seconds") or 15.0)
    snapshots = asyncio.run(
        ctx.market_svc.collect_batch(
            stock_list,
            run_id,
            include_sector_context=True,
            per_snapshot_timeout_seconds=snapshot_timeout,
            sector_context_timeout_seconds=sector_context_timeout,
        )
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
    return {"scores": scores, "snapshots": snapshots}


def _score_stock_list(ctx, stock_list: list[dict], run_id: str) -> list[dict]:
    result = _score_stock_batch(ctx, stock_list, run_id)
    return result["scores"]


SOURCE_QUALITY_DIMENSIONS = (
    ("quote", "行情", "L1"),
    ("technical", "技术指标", "L1"),
    ("financial", "基本面", "L1"),
    ("flow", "资金流", "L1"),
    ("sentiment", "舆情", "L2"),
    ("sector", "行业上下文", "L2"),
)


def _coverage_rate(available: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(available / total, 4)


def _build_source_quality_summary(snapshots: list[StockSnapshot], scores: list[dict]) -> dict:
    total = len(snapshots)
    coverage = {}
    warnings = []

    for attr, label, layer in SOURCE_QUALITY_DIMENSIONS:
        available = sum(1 for snapshot in snapshots if getattr(snapshot, attr, None) is not None)
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
                f"逐票{label}覆盖率 {row['rate']:.1%}，可能影响评分和买入门禁。"
            )

    quality_counter = Counter(str(score.get("data_quality", "ok")) for score in scores)
    missing_counter: Counter = Counter()
    for score in scores:
        missing_counter.update(str(item) for item in (score.get("data_missing_fields") or []))

    if total == 0:
        status = "warning"
        warnings.append("本次没有可评分样本，无法评估逐票数据覆盖率。")
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


def _watch_threshold(ctx, explicit_threshold: Optional[float]) -> float:
    if explicit_threshold is not None:
        return explicit_threshold
    pool_cfg = ctx.cfg.get("pool_management", {})
    scoring_cfg = ctx.cfg.get("scoring", {})
    return float(
        pool_cfg.get("watch_min_score")
        or scoring_cfg.get("thresholds", {}).get("watch")
        or pool_cfg.get("promote_min_score")
        or scoring_cfg.get("thresholds", {}).get("buy")
        or 5.0
    )


def _screening_report_thresholds(ctx, watch_threshold: float) -> dict[str, float]:
    thresholds = ctx.cfg.get("scoring", {}).get("thresholds", {})
    return {
        "buy": float(thresholds.get("buy") or max(watch_threshold, 5.5)),
        "watch": float(thresholds.get("watch") or watch_threshold),
    }


def _pool_thresholds(ctx) -> dict[str, float]:
    pool_cfg = ctx.cfg.get("pool_management", {})
    scoring_cfg = ctx.cfg.get("scoring", {})
    thresholds = scoring_cfg.get("thresholds", {})
    watch = float(pool_cfg.get("watch_min_score") or thresholds.get("watch") or 5.0)
    reject = float(pool_cfg.get("remove_max_score") or thresholds.get("reject") or 4.0)
    return {
        "promote": float(pool_cfg.get("promote_min_score") or thresholds.get("buy") or 5.5),
        "watch": watch,
        "radar": float(pool_cfg.get("radar_min_score") or pool_cfg.get("near_watch_min_score") or max(reject, watch - 0.5)),
        "reject": reject,
        "promote_streak_days": int(pool_cfg.get("promote_streak_days") or 1),
        "entry_signal_promote_streak_days": int(
            pool_cfg.get("entry_signal_promote_streak_days")
            or pool_cfg.get("promote_streak_days")
            or 1
        ),
    }


DEFAULT_REFRESH_SCAN_LIMIT = 80


def _scan_limit(cfg: dict, explicit_limit: Optional[int], *, refresh_pool: bool = False) -> int:
    if explicit_limit is not None:
        return max(1, int(explicit_limit))
    market_limit = int(cfg.get("market_scan_limit") or 30)
    if not refresh_pool:
        return market_limit
    refresh_limit = int(cfg.get("refresh_scan_limit") or DEFAULT_REFRESH_SCAN_LIMIT)
    return max(1, min(market_limit, refresh_limit))


def _screener_query_timeout(cfg: dict) -> float:
    return float(
        cfg.get("screener_query_timeout_seconds")
        or DEFAULT_SCREENER_QUERY_TIMEOUT_SECONDS
    )


def _screener_scoring_timeout(cfg: dict) -> float:
    return float(
        cfg.get("screener_scoring_timeout_seconds")
        or DEFAULT_SCREENER_SCORING_TIMEOUT_SECONDS
    )


def _score_stock_batch_with_timeout(
    ctx,
    stock_list: list[dict],
    run_id: str,
    *,
    query: str,
    timeout_seconds: float,
) -> dict:
    if timeout_seconds <= 0:
        return _score_stock_batch(ctx, stock_list, run_id)

    previous_handler = signal.getsignal(signal.SIGALRM)

    def _handle_timeout(signum, frame):  # noqa: ARG001
        raise ScreenerScoringTimeout(query, timeout_seconds)

    signal.signal(signal.SIGALRM, _handle_timeout)
    signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
    try:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return _score_stock_batch(ctx, stock_list, run_id)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


def _exit_after_scoring_timeout(status_code: int) -> None:
    """真实 CLI 超时后强制退出，避免 native provider 资源清理继续卡住进程。"""
    if "PYTEST_CURRENT_TEST" in os.environ:
        raise typer.Exit(status_code)
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(status_code)


def _search_screener_results(query: str, timeout_seconds: float) -> list[dict]:
    """用独立 Python 子进程隔离粗筛源，避免 native crash 拖垮 CLI。"""
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "astock_trading.platform.cli.screener_search_worker",
                query,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=max(float(timeout_seconds), 0.1),
        )
    except subprocess.TimeoutExpired as exc:
        raise ScreenerSearchTimeout(query, timeout_seconds) from exc
    if result.returncode != 0:
        raise ScreenerSearchFailed(
            query,
            returncode=result.returncode,
            stderr_tail=(result.stderr or "")[-1200:],
        )
    try:
        rows = json.loads(result.stdout or "[]")
    except json.JSONDecodeError as exc:
        raise ScreenerSearchFailed(
            query,
            returncode=result.returncode,
            stderr_tail=f"粗筛子进程返回非 JSON 输出: {(result.stdout or '')[-400:]}",
        ) from exc
    return rows if isinstance(rows, list) else []


def _screener_search_timeout_payload(
    *,
    command: str,
    query: str,
    timeout_seconds: float,
) -> dict:
    return {
        "command": command,
        "status": "failed",
        "reason": "screener_search_timeout",
        "query": query,
        "timeout_seconds": timeout_seconds,
        "execution_allowed": False,
        "writes_state": False,
        "summary": "选股粗筛源超时，候选池未刷新；先诊断数据源或调小刷新范围。",
        "next_action": {
            "type": "diagnose_data_sources",
            "label": "诊断数据源",
            "command": "atrade data-sources diagnose --json",
            "safe_to_auto_apply": True,
        },
    }


def _screener_search_failed_payload(
    *,
    command: str,
    query: str,
    exc: ScreenerSearchFailed,
) -> dict:
    return {
        "command": command,
        "status": "failed",
        "reason": "screener_search_failed",
        "query": query,
        "execution_allowed": False,
        "writes_state": False,
        "summary": "选股粗筛源执行失败，候选池未刷新；先诊断数据源或改用缓存/热点召回证据。",
        "diagnostic": {
            "returncode": exc.returncode,
            "stderr_tail": exc.stderr_tail,
        },
        "next_action": {
            "type": "diagnose_data_sources",
            "label": "诊断数据源",
            "command": "atrade data-sources diagnose --json",
            "safe_to_auto_apply": True,
        },
    }


def _screener_scoring_timeout_payload(
    *,
    command: str,
    query: str,
    timeout_seconds: float,
) -> dict:
    return {
        "command": command,
        "status": "failed",
        "reason": "screener_scoring_timeout",
        "query": query,
        "timeout_seconds": timeout_seconds,
        "execution_allowed": False,
        "candidate_pool_refreshed": False,
        "may_have_partial_score_events": True,
        "summary": "逐票评分或行情采集超时，候选池未刷新；先诊断数据源或调小 --limit。",
        "next_action": {
            "type": "diagnose_data_sources",
            "label": "诊断数据源",
            "command": "atrade data-sources diagnose --json",
            "safe_to_auto_apply": True,
        },
    }


HOT_RECALL_KINDS = (
    "cross_platform_hot_stocks",
    "xueqiu_hot_stocks",
    "hot_stocks",
)
DEFAULT_HOT_RECALL_LIMIT = 20
DEFAULT_RECENT_SIGNAL_RECALL_LIMIT = 20
DEFAULT_RECENT_SIGNAL_RECALL_EVENT_LIMIT = 500


def _hot_recall_candidates(conn, *, limit: int = DEFAULT_HOT_RECALL_LIMIT) -> list[dict]:
    """从最近热点观测里提取额外召回股票，只用于打分和观察，不直接买入。"""
    rows = conn.execute(
        f"""SELECT kind, payload_json, observed_at
            FROM market_observations
            WHERE kind IN ({",".join("?" for _ in HOT_RECALL_KINDS)})
            ORDER BY observed_at DESC
            LIMIT 30""",
        HOT_RECALL_KINDS,
    ).fetchall()
    latest_by_kind: dict[str, dict] = {}
    for row in rows:
        kind = row["kind"]
        if kind in latest_by_kind:
            continue
        try:
            payload = json.loads(row["payload_json"]) if isinstance(row["payload_json"], str) else row["payload_json"]
        except (TypeError, json.JSONDecodeError):
            payload = {}
        latest_by_kind[kind] = {"kind": kind, "payload": payload or {}}

    seen: set[str] = set()
    candidates: list[dict] = []
    for kind in HOT_RECALL_KINDS:
        payload = latest_by_kind.get(kind, {}).get("payload", {})
        for item in _hot_recall_items(payload):
            code = _normalize_a_share_code(item.get("code") or item.get("代码") or item.get("symbol") or "")
            name = str(item.get("name") or item.get("名称") or item.get("secName") or code).strip()
            if not code or code in seen or _is_st_stock_name(name):
                continue
            candidates.append({"code": code, "name": name, "recall_source": kind})
            seen.add(code)
            if len(candidates) >= limit:
                return candidates
    return candidates


def _recent_signal_recall_candidates(
    conn,
    *,
    min_score: float,
    watch_score: float,
    limit: int = DEFAULT_RECENT_SIGNAL_RECALL_LIMIT,
    event_limit: int = DEFAULT_RECENT_SIGNAL_RECALL_EVENT_LIMIT,
) -> list[dict]:
    """把近期临界评分和入场信号重新召回到刷新评分，不直接入池或买入。"""
    if limit <= 0:
        return []
    try:
        rows = conn.execute(
            """SELECT payload_json, occurred_at, stream_version
               FROM event_log
               WHERE event_type = 'score.calculated'
               ORDER BY occurred_at DESC, stream_version DESC
               LIMIT ?""",
            (max(int(event_limit), int(limit)),),
        ).fetchall()
    except Exception:
        return []

    seen: set[str] = set()
    candidates: list[dict] = []
    for row in rows:
        try:
            payload = json.loads(row["payload_json"])
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        code = _normalize_a_share_code(payload.get("code") or "")
        if not code or code in seen:
            continue
        seen.add(code)
        if bool(payload.get("veto_triggered")):
            continue
        if str(payload.get("data_quality") or "ok").lower() == "error":
            continue

        score = _score_value(payload)
        entry_signal = _truthy(payload.get("entry_signal"))
        if entry_signal and score >= min_score:
            recall_source = "recent_entry_signal"
        elif score >= watch_score:
            recall_source = "recent_signal_score"
        else:
            continue

        candidates.append({
            "code": code,
            "name": str(payload.get("name") or code),
            "recall_source": recall_source,
            "score": score,
            "entry_signal": entry_signal,
        })
        if len(candidates) >= limit:
            break
    return candidates


def _hot_recall_items(payload: object) -> list[dict]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("stocks", "items", "data", "result"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _is_st_stock_name(name: str) -> bool:
    normalized = name.upper().replace("＊", "*")
    return "ST" in normalized


def _normalize_a_share_code(value: object) -> str:
    code = str(value or "").strip().upper()
    if not code:
        return ""
    if len(code) == 6 and code.isdigit():
        return code
    if len(code) == 8 and code[:2] in {"SH", "SZ", "BJ"} and code[2:].isdigit():
        return code[2:]
    if len(code) == 9 and code[:6].isdigit() and code[6:] in {".SH", ".SZ", ".BJ"}:
        return code[:6]
    return ""


def _append_candidate_with_budget(
    selected: list[dict],
    seen: set[str],
    candidate: dict,
    *,
    source: str,
    source_counts: Counter,
    score_limit: int,
) -> bool:
    if len(selected) >= score_limit:
        return False
    code = str(candidate.get("code") or "").strip()
    if not code or code in seen:
        return False
    selected.append({"code": code, "name": candidate.get("name") or ""})
    seen.add(code)
    source_counts[source] += 1
    return True


def _refresh_source_budgets(
    score_limit: int,
    *,
    hot_count: int,
    recent_signal_count: int = 0,
    existing_count: int,
) -> dict[str, int]:
    if score_limit <= 0:
        return {"mx": 0, "hot_stocks": 0, "recent_signals": 0, "existing_pool": 0}

    existing_budget = 0
    if existing_count:
        existing_budget = min(existing_count, max(1, 3 if score_limit >= 5 else score_limit // 2))

    recent_signal_budget = 0
    if recent_signal_count and score_limit >= 3:
        recent_signal_budget = min(recent_signal_count, max(1, score_limit // 5))

    hot_budget = 0
    if hot_count and score_limit >= 3:
        hot_budget = min(hot_count, max(1, score_limit // 5))

    reserved = min(score_limit, recent_signal_budget + hot_budget + existing_budget)
    return {
        "mx": max(score_limit - reserved, 0),
        "hot_stocks": hot_budget,
        "recent_signals": recent_signal_budget,
        "existing_pool": existing_budget,
    }


def _build_scoring_candidates(
    raw_candidates: list[dict],
    hot_candidates: list[dict],
    recent_signal_candidates: list[dict],
    existing_candidates: list[dict],
    *,
    score_limit: int,
    refresh_pool: bool,
) -> dict:
    source_counts: Counter = Counter()
    selected: list[dict] = []
    seen: set[str] = set()

    if not refresh_pool:
        for candidate in raw_candidates:
            _append_candidate_with_budget(
                selected,
                seen,
                candidate,
                source="mx",
                source_counts=source_counts,
                score_limit=score_limit,
            )
        return {"stock_list": selected, "source_counts": dict(source_counts)}

    buckets = {
        "mx": raw_candidates,
        "hot_stocks": hot_candidates,
        "recent_signals": recent_signal_candidates,
        "existing_pool": existing_candidates,
    }
    budgets = _refresh_source_budgets(
        score_limit,
        hot_count=len(hot_candidates),
        recent_signal_count=len(recent_signal_candidates),
        existing_count=len(existing_candidates),
    )
    source_order = ("existing_pool", "recent_signals", "hot_stocks", "mx")
    for source in source_order:
        for candidate in buckets[source]:
            if source_counts[source] >= budgets[source]:
                break
            _append_candidate_with_budget(
                selected,
                seen,
                candidate,
                source=source,
                source_counts=source_counts,
                score_limit=score_limit,
            )

    for source in source_order:
        for candidate in buckets[source]:
            _append_candidate_with_budget(
                selected,
                seen,
                candidate,
                source=source,
                source_counts=source_counts,
                score_limit=score_limit,
            )

    return {"stock_list": selected, "source_counts": dict(source_counts)}


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
POOL_TIER_CN = {
    "core": "核心",
    "watch": "观察",
    "radar": "强势观察",
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


def _latest_scores_by_code(scores: list[dict]) -> list[dict]:
    latest: dict[str, dict] = {}
    anonymous: list[dict] = []
    for score in scores:
        code = str(score.get("code") or "")
        if not code:
            anonymous.append(score)
            continue
        latest[code] = score
    return [*latest.values(), *anonymous]


def _candidate_pool_score(row: dict) -> float:
    return float(row.get("score", row.get("total_score", row.get("total", 0))) or 0)


def _current_candidate_pool_summary(current_candidates: list[dict]) -> dict:
    counts = {"core": 0, "watch": 0, "radar": 0}
    items = []
    tier_order = {"core": 0, "watch": 1, "radar": 2}
    for row in sorted(
        current_candidates,
        key=lambda item: (
            tier_order.get(str(item.get("pool_tier") or ""), 99),
            -_candidate_pool_score(item),
            str(item.get("code") or ""),
        ),
    ):
        tier = str(row.get("pool_tier") or "")
        if tier in counts:
            counts[tier] += 1
        items.append({
            "code": row.get("code", ""),
            "name": row.get("name", ""),
            "pool_tier": tier,
            "pool_tier_label": _label(POOL_TIER_CN, tier),
            "score": _candidate_pool_score(row),
            "entry_signal": _truthy(row.get("entry_signal")),
            "data_quality": row.get("data_quality") or "ok",
            "last_scored_at": row.get("last_scored_at", ""),
            "note": row.get("note", ""),
        })
    return {"counts": counts, "items": items}


def _merge_scores_with_current_candidates(
    scores: list[dict],
    current_candidates: list[dict],
) -> list[dict]:
    merged_by_code: dict[str, dict] = {}
    anonymous: list[dict] = []
    for score in scores:
        code = str(score.get("code") or "")
        if not code:
            anonymous.append(score)
            continue
        merged_by_code[code] = dict(score)

    for candidate in current_candidates:
        code = str(candidate.get("code") or "")
        if not code:
            continue
        existing = merged_by_code.get(code, {})
        tier = str(candidate.get("pool_tier") or "")
        entry_signal = candidate.get("entry_signal")
        if entry_signal is None:
            entry_signal = existing.get("entry_signal", False)
        data_quality = candidate.get("data_quality") or existing.get("data_quality") or "ok"
        merged_by_code[code] = {
            **existing,
            "code": code,
            "name": candidate.get("name") or existing.get("name", ""),
            "total_score": _candidate_pool_score(candidate),
            "data_quality": data_quality,
            "entry_signal": _truthy(entry_signal),
            "veto_triggered": bool(existing.get("veto_triggered", False)),
            "hard_veto_signals": existing.get("hard_veto_signals") or [],
            "data_missing_fields": existing.get("data_missing_fields") or [],
            "score_source": "current_candidate_pool",
            "pool_tier": tier,
            "pool_tier_label": _label(POOL_TIER_CN, tier),
            "last_scored_at": candidate.get("last_scored_at", ""),
            "note": candidate.get("note", ""),
        }

    return [*merged_by_code.values(), *anonymous]


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
    item = {
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
    if score.get("score_source"):
        item["score_source"] = score.get("score_source")
    if score.get("pool_tier"):
        item["pool_tier"] = score.get("pool_tier")
        item["pool_tier_label"] = score.get("pool_tier_label") or _label(
            POOL_TIER_CN,
            score.get("pool_tier"),
        )
    for key in ("score_event_id", "scored_at"):
        if score.get(key):
            item[key] = score.get(key)
    if _historical_entry_signal_recall_hint(score):
        item["recall_hint"] = _historical_entry_signal_recall_hint(score)
    return item


def _historical_entry_signal_recall_hint(score: dict) -> dict | None:
    if score.get("score_source") != "score_event":
        return None
    if not _truthy(score.get("entry_signal")):
        return None
    if score.get("pool_tier"):
        return None
    return {
        "type": "recent_entry_signal_recall",
        "label": "历史入场信号需重新评分入池",
        "command": "atrade screener refresh --json",
        "safe_to_auto_apply": True,
        "reason": "该票来自历史评分事件，不在当前候选池；先通过刷新召回重新评分，不直接当成当前可模拟候选。",
    }


def _follow_up_candidates(
    scores: list[dict],
    *,
    buy_threshold: float,
    watch_threshold: float,
    reject_threshold: float,
    limit: int = 10,
) -> tuple[dict, dict]:
    sorted_scores = sorted(scores, key=_score_value, reverse=True)
    near_watch_floor = max(reject_threshold, watch_threshold - 1.0)
    groups = {
        "watch_candidates": [],
        "near_watch_candidates": [],
        "blocked_high_scores": [],
        "data_repair_candidates": [],
    }

    for score in sorted_scores:
        total = _score_value(score)
        veto = bool(score.get("veto_triggered"))
        quality = str(score.get("data_quality", "ok"))
        missing = score.get("data_missing_fields") or []
        item = _candidate_follow_up_item(score, buy_threshold)

        if not veto and watch_threshold <= total < buy_threshold:
            groups["watch_candidates"].append(item)
        if not veto and near_watch_floor <= total < watch_threshold:
            groups["near_watch_candidates"].append(item)
        if veto and total >= watch_threshold:
            groups["blocked_high_scores"].append(item)
        if quality != "ok" or missing:
            groups["data_repair_candidates"].append(item)

    return (
        {key: values[:limit] for key, values in groups.items()},
        {key: len(values) for key, values in groups.items()},
    )


def _next_actions(follow_up: dict, current_candidate_pool: dict | None = None) -> list[dict]:
    actions = []
    current_items = (current_candidate_pool or {}).get("items") or []
    if any(item.get("pool_tier") == "core" and _truthy(item.get("entry_signal")) for item in current_items):
        actions.append({
            "type": "paper_auto_readiness",
            "label": "复核模拟盘承接",
            "command": "atrade paper auto-readiness --json",
        })

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
        recall_hint = near_watch_candidates[0].get("recall_hint") or {}
        if recall_hint:
            actions.append({
                "type": "refresh_recent_signal_recall",
                "label": "刷新历史入场信号候选",
                "command": str(recall_hint.get("command") or "atrade screener refresh --json"),
                "safe_to_auto_apply": bool(recall_hint.get("safe_to_auto_apply", True)),
            })
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
    follow_up_limit: int = 10,
    current_candidates: list[dict] | None = None,
) -> dict:
    raw_score_count = len(scores)
    scores = _latest_scores_by_code(scores)
    current_candidates = current_candidates or []
    actionable_scores = _merge_scores_with_current_candidates(scores, current_candidates)
    current_candidate_pool = _current_candidate_pool_summary(current_candidates)
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
    for score in sorted(actionable_scores, key=_score_value, reverse=True):
        total = _score_value(score)
        if len(near_misses) >= near_miss_limit:
            break
        if total < near_buy_floor or total >= buy_threshold or bool(score.get("veto_triggered")):
            continue
        item = {
            "code": score.get("code", ""),
            "name": score.get("name", ""),
            "score": total,
            "data_quality": score.get("data_quality", "ok"),
            "entry_signal": _truthy(score.get("entry_signal")),
            "blockers": _near_miss_blockers(score, buy_threshold),
        }
        for key in ("score_source", "pool_tier", "pool_tier_label", "score_event_id", "scored_at"):
            if score.get(key):
                item[key] = score[key]
        if _historical_entry_signal_recall_hint(score):
            item["recall_hint"] = _historical_entry_signal_recall_hint(score)
        near_misses.append(item)

    top_scores = []
    for score in sorted(actionable_scores, key=_score_value, reverse=True)[:10]:
        item = {
            "code": score.get("code", ""),
            "name": score.get("name", ""),
            "score": _score_value(score),
            "data_quality": score.get("data_quality", "ok"),
            "entry_signal": _truthy(score.get("entry_signal")),
            "veto_triggered": bool(score.get("veto_triggered")),
            "hard_veto_signals": score.get("hard_veto_signals") or [],
        }
        for key in ("score_source", "pool_tier", "pool_tier_label", "score_event_id", "scored_at"):
            if score.get(key):
                item[key] = score[key]
        if _historical_entry_signal_recall_hint(score):
            item["recall_hint"] = _historical_entry_signal_recall_hint(score)
        top_scores.append(item)
    follow_up, follow_up_counts = _follow_up_candidates(
        actionable_scores,
        buy_threshold=buy_threshold,
        watch_threshold=watch_threshold,
        reject_threshold=reject_threshold,
        limit=follow_up_limit,
    )

    recommendations = []
    core_entry_candidates = [
        item for item in current_candidate_pool["items"]
        if item["pool_tier"] == "core" and item["entry_signal"]
    ]
    core_count = int(current_candidate_pool["counts"].get("core") or 0)

    if core_entry_candidates:
        top = core_entry_candidates[0]
        summary = (
            f"当前候选池已有 {len(core_entry_candidates)} 个核心候选带入场信号；"
            f"最高为 {top['name'] or top['code']}({top['code']}) {top['score']:.1f} 分，"
            "下一步应检查模拟盘窗口、profile 和风控预检。"
        )
        recommendations.append("使用 atrade paper auto-readiness --json 复核模拟承接阻断项")
    elif core_count > 0:
        summary = f"当前候选池已有 {core_count} 个核心候选；继续复核入场信号、数据质量和风控门禁。"
        recommendations.append("逐只查看核心候选的入场信号和风控门禁")
    elif not scores and not current_candidates:
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
        "status": "ok" if scores or current_candidates else "warning",
        "summary": summary,
        "scope": {
            "since": since,
            "run_id": run_id,
            "score_events": raw_score_count,
            "unique_scores": len(scores),
            "decision_events": len(decisions),
            "current_candidate_pool": len(current_candidates),
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
        "current_candidate_pool": current_candidate_pool,
        "follow_up": follow_up,
        "follow_up_counts": follow_up_counts,
        "top_scores": top_scores,
        "next_actions": _next_actions(follow_up, current_candidate_pool),
        "recommendations": recommendations,
    }


def _score_event_payloads(events: list[dict]) -> list[dict]:
    payloads: list[dict] = []
    for event in events:
        payload = dict(event.get("payload") or {})
        payload.setdefault("score_source", "score_event")
        if event.get("event_id"):
            payload.setdefault("score_event_id", event.get("event_id"))
        if event.get("occurred_at"):
            payload.setdefault("scored_at", event.get("occurred_at"))
        payloads.append(payload)
    return payloads


def _first_follow_up(explanation: dict, group: str) -> dict:
    items = (explanation.get("follow_up") or {}).get(group) or []
    return items[0] if items else {}


def _first_next_action(explanation: dict, action_type: str) -> dict:
    for action in explanation.get("next_actions") or []:
        if action.get("type") == action_type:
            return action
    return {}


def _iteration_action(
    action_type: str,
    label: str,
    command: str,
    rationale: str,
    *,
    safe_to_auto_apply: bool = False,
) -> dict:
    return {
        "type": action_type,
        "label": label,
        "command": command,
        "rationale": rationale,
        "safe_to_auto_apply": safe_to_auto_apply,
    }


def _plan_has_command(plan: list[dict], command: str) -> bool:
    return any(item.get("command") == command for item in plan)


def _build_screener_iteration_plan(explanation: dict, *, record: bool = True) -> dict:
    scope = explanation.get("scope") or {}
    score_events = int(scope.get("score_events") or 0)
    follow_up_counts = explanation.get("follow_up_counts") or {}
    plan: list[dict] = []

    if score_events == 0:
        plan.append(_iteration_action(
            "refresh_scores",
            "刷新评分证据",
            "atrade screener refresh --json",
            "最近没有评分事件，先生成新证据再判断策略是否过严。",
            safe_to_auto_apply=True,
        ))

    watch_count = int(follow_up_counts.get("watch_candidates") or 0)
    if watch_count:
        plan.append(_iteration_action(
            "watch_pool_refresh",
            "刷新观察池",
            "atrade screener refresh --json",
            f"发现 {watch_count} 个观察候选，先进入观察池跟踪，不提升为买入意向。",
            safe_to_auto_apply=True,
        ))

    near_watch = _first_follow_up(explanation, "near_watch_candidates")
    if near_watch:
        action = _first_next_action(explanation, "near_watch_review")
        command = action.get("command") or f"atrade stock analyze {near_watch.get('code', '')} --json"
        recall_hint = near_watch.get("recall_hint") or {}
        if recall_hint and not _plan_has_command(
            plan,
            str(recall_hint.get("command") or "atrade screener refresh --json"),
        ):
            plan.append(_iteration_action(
                "recent_signal_recall_refresh",
                "刷新历史入场信号候选",
                str(recall_hint.get("command") or "atrade screener refresh --json"),
                "历史评分曾出现入场信号，但不在当前候选池；先重新评分入池，再判断是否进入观察或核心。",
                safe_to_auto_apply=bool(recall_hint.get("safe_to_auto_apply", True)),
            ))
        rationale = (
            "该票来自历史评分事件，刷新后如果仍接近观察线再单票复核。"
            if recall_hint
            else "分数接近观察线但还缺少入场信号或买入强度，只能复核和等待确认。"
        )
        plan.append(_iteration_action(
            "near_watch_review",
            "复核临界观察候选",
            command,
            rationale,
        ))

    blocked_high = _first_follow_up(explanation, "blocked_high_scores")
    if blocked_high:
        action = _first_next_action(explanation, "blocked_candidate_review")
        command = action.get("command") or f"atrade stock analyze {blocked_high.get('code', '')} --json"
        plan.append(_iteration_action(
            "blocked_candidate_review",
            "复核高分被拦截候选",
            command,
            "分数达标但被硬门禁拦截，优先确认是风险信号还是数据异常。",
        ))

    data_repair = _first_follow_up(explanation, "data_repair_candidates")
    data_repair_count = int(follow_up_counts.get("data_repair_candidates") or 0)
    if data_repair:
        action = _first_next_action(explanation, "data_repair_review")
        command = action.get("command") or f"atrade stock analyze {data_repair.get('code', '')} --json"
        plan.append(_iteration_action(
            "data_repair",
            "复核数据补齐候选",
            command,
            f"发现 {data_repair_count} 个数据降级或缺字段候选，先修复证据链再提高判断置信度。",
        ))

    buckets = explanation.get("score_buckets") or {}
    if score_events and not plan and int(buckets.get("below_reject") or 0) >= max(score_events * 0.8, 1):
        plan.append(_iteration_action(
            "recall_expand",
            "扩大召回后重新评分",
            "atrade screener refresh --json",
            "绝大多数候选低于拒绝线，优先扩大召回或补齐数据，而不是降低买入线。",
            safe_to_auto_apply=True,
        ))

    next_command = plan[0]["command"] if plan else "atrade screener explain --json"
    status = "needs_action" if plan else "stable_wait"
    return {
        "diagnostic": "screener_iteration",
        "status": status,
        "mode": "dry_run",
        "summary": "已生成受控迭代计划；只允许证据刷新、观察池刷新和单股复核，不自动降低买入线。",
        "closed_loop": {
            "phase": "proposal",
            "record_event": record,
            "next_command": next_command,
            "can_self_adjust_without_trade": any(item["safe_to_auto_apply"] for item in plan),
        },
        "iteration_plan": plan,
        "guardrails": {
            "manual_confirmation_required": True,
            "blocked_auto_adjustments": [
                {
                    "type": "lower_buy_threshold",
                    "reason": "买入线变化会改变交易风险收益，必须人工批准并通过回测/复盘验证。",
                },
                {
                    "type": "switch_config_profile",
                    "reason": "策略 profile 会改变执行语义，必须显式批准。",
                },
                {
                    "type": "place_real_order",
                    "reason": "系统没有实盘券商接口，真实交易边界是人工确认。",
                },
            ],
        },
        "source_explanation": {
            "summary": explanation.get("summary", ""),
            "scope": scope,
            "score_buckets": buckets,
            "follow_up_counts": follow_up_counts,
        },
    }


def _record_screener_iteration(ctx, payload: dict, *, run_id: str) -> str:
    return ctx.event_store.append(
        stream="strategy:iteration",
        stream_type="strategy",
        event_type="strategy.iteration.proposed",
        payload=payload,
        metadata={"source": "cli.screener.iterate", "run_id": run_id},
    )


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


def _score_has_entry_strategy_route(score: dict) -> bool:
    return not _core_promotion_blockers(score)


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
    radar: list[dict] = []
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

        if veto or total < thresholds["radar"]:
            reason = "veto" if veto else f"score<{thresholds['radar']:.1f}"
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

        if total < thresholds["watch"]:
            tier = "radar"
            new_streak = 0
            note = "screener_refresh:below_watch_retained"
            reason = "below_watch_retained"
            event_type = (
                "pool.demoted"
                if old_tier == "core"
                else ("candidate.updated" if current else "candidate.added")
            )
            radar.append(_pool_change(code, name, total, old_tier, tier, reason=reason))
            payload = {
                "code": code,
                "name": name,
                "pool_tier": tier,
                "score": total,
                "note": note,
                "reason": reason,
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
            projection_entries.append({
                "code": code,
                "name": name,
                "pool_tier": tier,
                "score": total,
                "added_at": (current or {}).get("added_at") or local_now_str("%Y-%m-%d"),
                "streak_days": new_streak,
                "note": note,
            })
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
            required_streak_days = thresholds["promote_streak_days"]
            if not promotion_blockers and _score_has_entry_strategy_route(score):
                required_streak_days = thresholds["entry_signal_promote_streak_days"]
            tier = (
                "core"
                if old_tier == "core"
                or (new_streak >= required_streak_days and not promotion_blockers)
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
        "radar": radar,
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
        score_limit = _scan_limit(cfg, limit, refresh_pool=refresh_pool)
        command_name = "screener refresh" if refresh_pool else "screener run"
        search_timeout = _screener_query_timeout(cfg)
        scoring_timeout = _screener_scoring_timeout(cfg)

        try:
            raw_results = _search_screener_results(q, search_timeout)
        except ScreenerSearchTimeout:
            payload = _screener_search_timeout_payload(
                command=command_name,
                query=q,
                timeout_seconds=search_timeout,
            )
            json_or_text(payload, as_json)
            raise typer.Exit(1)
        except ScreenerSearchFailed as exc:
            payload = _screener_search_failed_payload(
                command=command_name,
                query=q,
                exc=exc,
            )
            json_or_text(payload, as_json)
            raise typer.Exit(1)
        raw_candidates = [
            {"code": row.get("code") or row.get("代码", ""), "name": row.get("name") or row.get("名称", "")}
            for row in raw_results
            if row.get("code") or row.get("代码")
        ]
        hot_recall = []
        if refresh_pool and cfg.get("include_hot_recall", True):
            hot_recall = _hot_recall_candidates(
                ctx.conn,
                limit=int(cfg.get("hot_recall_limit") or DEFAULT_HOT_RECALL_LIMIT),
            )
        recent_signal_recall = []
        if refresh_pool and cfg.get("include_recent_signal_recall", True):
            pool_thresholds = _pool_thresholds(ctx)
            recent_signal_recall = _recent_signal_recall_candidates(
                ctx.conn,
                min_score=float(pool_thresholds["radar"]),
                watch_score=float(pool_thresholds["watch"]),
                limit=int(
                    cfg.get("recent_signal_recall_limit")
                    or DEFAULT_RECENT_SIGNAL_RECALL_LIMIT
                ),
                event_limit=int(
                    cfg.get("recent_signal_recall_event_limit")
                    or DEFAULT_RECENT_SIGNAL_RECALL_EVENT_LIMIT
                ),
            )
        existing_candidates = []
        if refresh_pool:
            existing_candidates = [
                {"code": row.get("code", ""), "name": row.get("name") or ""}
                for row in _candidate_rows(ctx.conn, tier="all", limit=1000)
            ]
        scoring_candidates = _build_scoring_candidates(
            raw_candidates,
            hot_recall,
            recent_signal_recall,
            existing_candidates,
            score_limit=score_limit,
            refresh_pool=refresh_pool,
        )
        stock_list = scoring_candidates["stock_list"]
        candidate_source_counts = scoring_candidates["source_counts"]
        if not stock_list:
            payload = {
                "query": q,
                "screened": len(raw_results),
                "score_limit": score_limit,
                "scored": [],
                "added_to_watch": [],
                "recall_candidates": {
                    "hot_stocks": len(hot_recall),
                    "recent_signals": len(recent_signal_recall),
                },
                "candidate_source_counts": candidate_source_counts,
                "source_quality": _build_source_quality_summary([], []),
            }
            json_or_text(payload, as_json)
            return

        run_id = f"screener_{local_now_str('%H%M%S')}"

        try:
            score_batch = _score_stock_batch_with_timeout(
                ctx,
                stock_list,
                run_id,
                query=q,
                timeout_seconds=scoring_timeout,
            )
        except ScreenerScoringTimeout:
            payload = _screener_scoring_timeout_payload(
                command=command_name,
                query=q,
                timeout_seconds=scoring_timeout,
            )
            json_or_text(payload, as_json)
            ctx.conn.close()
            _exit_after_scoring_timeout(1)
            raise typer.Exit(1)
        scores = score_batch["scores"]
        snapshots = score_batch["snapshots"]
        source_quality = _build_source_quality_summary(snapshots, scores)
        threshold = _watch_threshold(ctx, watch_threshold)
        report_thresholds = _screening_report_thresholds(ctx, threshold)
        if refresh_pool:
            pool_changes = _apply_candidate_pool_refresh(ctx, scores, run_id)
            added = [item for item in pool_changes["watched"] if item.get("from") is None]
        else:
            pool_changes = {}
            added = _add_watch_candidates(ctx, scores, threshold, run_id)
        ctx.obsidian.write_screening_result(
            run_id,
            q,
            scores,
            added,
            buy_threshold=report_thresholds["buy"],
            watch_threshold=report_thresholds["watch"],
        )
        decision_events = ctx.event_store.query(
            event_type="decision.suggested",
            metadata_filter={"run_id": run_id},
        )
        history_group_id = archive_from_runtime_state(
            ctx.conn,
            run_id=run_id,
            phase="screener",
            candidates=scores,
            decisions=[event["payload"] for event in decision_events],
        )

        payload = {
            "query": q,
            "run_id": run_id,
            "history_group_id": history_group_id,
            "screened": len(raw_results),
            "score_limit": score_limit,
            "threshold": threshold,
            "report_thresholds": report_thresholds,
            "scored": scores,
            "added_to_watch": added,
            "recall_candidates": {
                "hot_stocks": len(hot_recall),
                "recent_signals": len(recent_signal_recall),
            },
            "candidate_source_counts": candidate_source_counts,
            "source_quality": source_quality,
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
    limit: Optional[int] = typer.Option(None, "--limit", help="最多评分数量；默认读取 strategy.screening.refresh_scan_limit"),
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
    follow_up_limit: int = typer.Option(10, "--follow-up-limit", help="每类跟进候选最多返回数量"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """解释近期为什么没有合适候选，输出评分漏斗、否决原因和临界候选。"""
    if days < 1:
        raise typer.BadParameter("--days must be >= 1")
    if limit < 1:
        raise typer.BadParameter("--limit must be >= 1")
    if follow_up_limit < 1:
        raise typer.BadParameter("--follow-up-limit must be >= 1")

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
            _score_event_payloads(score_events),
            [event["payload"] for event in decision_events],
            thresholds=thresholds,
            since=since_value,
            run_id=run_id or None,
            near_miss_margin=near_miss_margin,
            follow_up_limit=follow_up_limit,
            current_candidates=_candidate_rows(ctx.conn, limit=200),
        )
        json_or_text(payload, as_json)
    finally:
        ctx.conn.close()


@screener_app.command("iterate")
def screener_iterate(
    since: str = typer.Option("", "--since", help="起始时间 ISO；空值使用 --days 回推"),
    days: int = typer.Option(7, "--days", help="未指定 --since 时回看天数"),
    run_id: str = typer.Option("", "--run-id", help="只分析指定 run_id 的评分/决策事件"),
    limit: int = typer.Option(1000, "--limit", help="最大读取事件数量"),
    near_miss_margin: float = typer.Option(1.0, "--near-miss-margin", help="买入线下方多少分视为临界候选"),
    follow_up_limit: int = typer.Option(10, "--follow-up-limit", help="每类跟进候选最多返回数量"),
    record: bool = typer.Option(True, "--record/--no-record", help="是否写入 strategy.iteration.proposed 证据事件"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """生成选股自我迭代计划，并按受控边界记录建议事件。"""
    if days < 1:
        raise typer.BadParameter("--days must be >= 1")
    if limit < 1:
        raise typer.BadParameter("--limit must be >= 1")
    if follow_up_limit < 1:
        raise typer.BadParameter("--follow-up-limit must be >= 1")

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
        explanation = _build_screener_explanation(
            _score_event_payloads(score_events),
            [event["payload"] for event in decision_events],
            thresholds=thresholds,
            since=since_value,
            run_id=run_id or None,
            near_miss_margin=near_miss_margin,
            follow_up_limit=follow_up_limit,
            current_candidates=_candidate_rows(ctx.conn, limit=200),
        )
        payload = _build_screener_iteration_plan(explanation, record=record)
        iteration_run_id = f"screener_iterate_{local_now_str('%H%M%S')}"
        if record:
            payload["event_id"] = _record_screener_iteration(
                ctx,
                payload,
                run_id=iteration_run_id,
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
        score_batch = _score_stock_batch(ctx, stock_list, run_id)
        scores = score_batch["scores"]
        ctx.obsidian.write_scoring_report(run_id, scores)
        json_or_text(
            {
                "run_id": run_id,
                "scores": scores,
                "count": len(scores),
                "source_quality": _build_source_quality_summary(score_batch["snapshots"], scores),
            },
            as_json,
        )
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
