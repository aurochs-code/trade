#!/usr/bin/env python3
"""
pipeline/stock_screener.py — 选股流水线

职责：
  1. 读取 config/stocks.yaml 的核心池/观察池/黑名单
  2. 优先调用妙想智能选股 API（mx_xuangu）进行自然语言筛选
  3. mx skill 不可用或失败时，fallback 到 akshare 原生接口
  4. 对候选池进行四维评分，写入 Obsidian 筛选记录

用法：
  python -m scripts.pipeline.stock_screener
  python -m scripts.pipeline.stock_screener --pool core
  python -m scripts.pipeline.stock_screener --pool watch
"""

import os
import sys
import warnings
from datetime import datetime
from pathlib import Path
from time import sleep

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

os.environ["TQDM_DISABLE"] = "1"
warnings.filterwarnings("ignore")

import akshare as ak

from scripts.engine.scorer import batch_score, data_quality_blocks_auto_buy, get_recommendation
from scripts.engine.composite import build_today_decision
from scripts.mx.cli_tools import MXCommandError, dispatch_mx_command, list_mx_command_metadata, mx_command_groups
from scripts.state import (
    load_market_snapshot,
    save_candidate_snapshot_history,
    save_decision_snapshot_history,
    save_market_snapshot_history,
)
from scripts.utils.obsidian import ObsidianVault
from scripts.utils.cache import load_json_cache, save_json_cache
from scripts.utils.config_loader import get_stocks, get_strategy
from scripts.utils.logger import get_logger
from scripts.utils.pool_manager import evaluate_pool_actions, load_pool_snapshot, save_pool_snapshot
from scripts.utils.runtime_state import update_pipeline_state

_logger = get_logger("pipeline.stock_screener")

# ---------------------------------------------------------------------------
# mx-xuangu 调用（优先，通过妙想智能选股 API）
# ---------------------------------------------------------------------------

def _mx_health_snapshot(include_unavailable: bool = False) -> dict:
    items = list_mx_command_metadata(include_unavailable=include_unavailable)
    groups = mx_command_groups(include_unavailable=include_unavailable)
    unavailable_items = [item for item in items if not item.get("available", False)]
    available_items = [item for item in items if item.get("available", False)]
    required_commands = ["mx.xuangu.search", "mx.zixuan.query", "mx.zixuan.manage"]
    item_lookup = {item.get("id", ""): item for item in items}
    required = {
        command_id: {
            "available": bool(item_lookup.get(command_id, {}).get("available", False)),
            "availability_note": item_lookup.get(command_id, {}).get("availability_note", ""),
        }
        for command_id in required_commands
    }
    return {
        "status": "ok" if not unavailable_items else "warning",
        "available_count": len(available_items),
        "unavailable_count": len(unavailable_items),
        "command_count": len(items),
        "group_count": len(groups),
        "groups": {name: len(values) for name, values in groups.items()},
        "required": required,
        "unavailable_commands": [item.get("id", "") for item in unavailable_items],
        "source": "scripts.mx.cli_tools",
    }

def _call_mx_screener(query: str, select_type: str = "A股") -> list:
    """
    调用妙想智能选股 API 进行自然语言筛选。

    优先使用，失败时返回空列表（触发 fallback）。

    Returns:
        list of {"code": str, "name": str, "mx_data": dict}
        mx_data 包含妙想返回的完整行数据（最新价/MA20/ROE/涨跌幅等），
        供 scorer 直接使用，避免再逐个调 akshare。
    """
    try:
        _logger.info(f"[mx-xuangu] 调用: query='{query}'")
        result = dispatch_mx_command("mx.xuangu.search", query=query)
    except MXCommandError as e:
        _logger.warning(f"[mx-xuangu] 调用失败: {e}")
        return []

    try:
        rows = []
        if isinstance(result, dict):
            rows = (
                result.get("data", {})
                .get("data", {})
                .get("searchDataResultDTO", {})
                .get("dataTableDTOList", [])
            )
        elif isinstance(result, list):
            rows = result
        if not rows:
            _logger.warning("[mx-xuangu] 返回结果为空")
            return []
        candidates = []
        for row in rows:
            code = (
                row.get("代码", "") or row.get("SECURITY_CODE", "") or
                row.get("股票代码", "") or ""
            ).strip()
            name = (
                row.get("简称", "") or row.get("SECURITY_SHORT_NAME", "") or
                row.get("股票简称", "") or row.get("名称", "") or ""
            ).strip()
            if code and name:
                code = code.split(".")[0]
                candidates.append({"code": code, "name": name, "mx_data": row})

        _logger.info(f"[mx-xuangu] 成功，返回 {len(candidates)} 条结果")
        return candidates

    except Exception as e:
        _logger.warning(f"[mx-xuangu] 调用异常: {e}")
        return []


# ---------------------------------------------------------------------------
# fallback：akshare 原生筛选
# ---------------------------------------------------------------------------

def _get_blacklist(stocks_cfg: dict) -> set:
    """读取永久/临时黑名单代码。"""
    blacklist = set(stocks_cfg.get("blacklist", {}).get("permanent", []))
    blacklist.update(stocks_cfg.get("blacklist", {}).get("temporary", []))
    return blacklist


def _dedupe_candidates(candidates: list) -> list:
    """按代码去重并保留原始顺序，保留 mx_data 等附加字段。"""
    deduped = []
    seen = set()
    for item in candidates:
        code = str(item.get("code", "")).strip()
        name = str(item.get("name", "")).strip()
        if not code or code in seen:
            continue
        seen.add(code)
        entry = {"code": code, "name": name}
        if "mx_data" in item:
            entry["mx_data"] = item["mx_data"]
        deduped.append(entry)
    return deduped


def _tracked_candidates(pool: str, stocks_cfg: dict, blacklist: set, current_snapshot: dict | None = None) -> tuple[list, str]:
    """返回现有核心池/观察池候选集。"""
    snapshot_entries = []
    if current_snapshot:
        snapshot_entries = current_snapshot.get("entries", [])

    if snapshot_entries:
        if pool == "core":
            candidates = [
                {"code": item.get("code", ""), "name": item.get("name", "")}
                for item in snapshot_entries
                if str(item.get("bucket", "")).strip() == "core"
            ]
            pool_name = "核心"
        elif pool == "watch":
            candidates = [
                {"code": item.get("code", ""), "name": item.get("name", "")}
                for item in snapshot_entries
                if str(item.get("bucket", "")).strip() == "watch"
            ]
            pool_name = "观察"
        else:
            candidates = [
                {"code": item.get("code", ""), "name": item.get("name", "")}
                for item in snapshot_entries
                if str(item.get("bucket", "")).strip() in {"core", "watch"}
            ]
            pool_name = "综合"
    else:
        if pool == "core":
            candidates = stocks_cfg.get("core_pool", [])
            pool_name = "核心"
        elif pool == "watch":
            candidates = stocks_cfg.get("watch_pool", [])
            pool_name = "观察"
        else:
            candidates = stocks_cfg.get("core_pool", []) + stocks_cfg.get("watch_pool", [])
            pool_name = "综合"

    candidates = [c for c in candidates if c.get("code") not in blacklist]
    return _dedupe_candidates(candidates), pool_name


def _fallback_tracked_candidates(stocks_cfg: dict, blacklist: set, current_snapshot: dict | None = None) -> list:
    """tracked 模式下的 fallback，直接回退到已跟踪股票池。"""
    _logger.info("[fallback] 使用已跟踪股票池作为候选")
    snapshot_entries = current_snapshot.get("entries", []) if current_snapshot else []
    if snapshot_entries:
        candidates = [
            {"code": item.get("code", ""), "name": item.get("name", "")}
            for item in snapshot_entries
            if str(item.get("bucket", "")).strip() in {"core", "watch"}
        ]
    else:
        candidates = stocks_cfg.get("core_pool", []) + stocks_cfg.get("watch_pool", [])
    candidates = [c for c in candidates if c.get("code") not in blacklist]
    candidates = _dedupe_candidates(candidates)
    _logger.info(f"[fallback] 候选股票: {len(candidates)} 只")
    return candidates


def _safe_float(value, default=0.0) -> float:
    try:
        return float(value) if value is not None and value != "" else default
    except (ValueError, TypeError):
        return default


def _fallback_market_candidates(strategy_cfg: dict, blacklist: set) -> list:
    """
    当 mx skill 不可用时，使用 akshare 全市场快照做一轮轻筛。
    先用价格/成交额/PE/ST 过滤，再按成交额排序截断到固定数量。
    """
    screening_cfg = strategy_cfg.get("screening", {})
    filters = screening_cfg.get("fallback_filters", {})
    min_price = filters.get("min_price", 5)
    max_price = filters.get("max_price", 200)
    min_amount = filters.get("min_amount", 100_000_000)
    max_pe = filters.get("max_pe_ttm", 40)
    exclude_st = filters.get("exclude_st", True)
    market_limit = int(screening_cfg.get("market_scan_limit", 30))

    last_error = None
    for attempt in range(1, 4):
        try:
            _logger.info(
                "[fallback-market] 使用 akshare 全市场快照初筛 "
                f"(attempt {attempt}/3, limit={market_limit})"
            )
            df = ak.stock_zh_a_spot_em()
            break
        except Exception as e:
            last_error = e
            if attempt < 3:
                sleep(1)
    else:
        _logger.warning(f"[fallback-market] 获取全市场快照失败: {last_error}")
        return []

    candidates = []
    for _, row in df.iterrows():
        code = str(row.get("代码", "")).strip()
        name = str(row.get("名称", "")).strip()
        if not code or code in blacklist:
            continue
        if exclude_st and ("ST" in name.upper() or "退" in name):
            continue

        price = _safe_float(row.get("最新价"))
        amount = _safe_float(row.get("成交额"))
        pe = _safe_float(row.get("市盈率-动态"), default=-1)

        if not (min_price <= price <= max_price):
            continue
        if amount < min_amount:
            continue
        if pe > 0 and pe > max_pe:
            continue

        candidates.append({
            "code": code,
            "name": name,
            "_amount": amount,
        })

    candidates.sort(key=lambda item: item.get("_amount", 0), reverse=True)
    trimmed = [{"code": item["code"], "name": item["name"]} for item in candidates[:market_limit]]
    _logger.info(f"[fallback-market] 初筛通过 {len(candidates)} 只，送评估 {len(trimmed)} 只")
    return trimmed


def _load_cached_candidates(cache_key: str, max_age_seconds: int) -> tuple[list, dict]:
    """读取最近一次成功候选缓存。"""
    payload = load_json_cache("screening_candidates", cache_key, max_age_seconds=max_age_seconds)
    if not payload:
        return [], {}

    cached_candidates = _dedupe_candidates(payload.get("data", []))
    if not cached_candidates:
        return [], {}

    meta = payload.get("meta", {})
    meta["cached_at"] = payload.get("cached_at", "")
    _logger.info(
        f"[cache] 命中 {cache_key}: {len(cached_candidates)} 只 "
        f"(cached_at={payload.get('cached_at', 'unknown')})"
    )
    return cached_candidates, meta


def _save_candidate_cache(cache_key: str, candidates: list, source: str, extra_meta=None) -> str:
    """缓存候选列表，供外部接口失败时兜底。"""
    meta = {"source": source, "count": len(candidates)}
    if extra_meta:
        meta.update(extra_meta)
    return save_json_cache("screening_candidates", cache_key, _dedupe_candidates(candidates), meta=meta)


def _write_market_scan_watchlist(results: list) -> str:
    """将市场扫描中可操作的股票写入观察池候选文件。"""
    vault = ObsidianVault()
    report_dir = Path(vault.vault_path) / "04-选股" / "筛选结果"
    report_dir.mkdir(parents=True, exist_ok=True)
    path = report_dir / f"市场扫描候选_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"

    actionable = [r for r in results if not r.get("veto_triggered") and r.get("total_score", 0) >= 5]
    lines = [
        f"# 市场扫描候选 — {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "",
        "| # | 股票 | 代码 | 总分 | 建议 |",
        "|---|------|------|------|------|",
    ]
    for i, row in enumerate(actionable, 1):
        suggestion = get_recommendation(row)
        lines.append(f"| {i} | {row.get('name','')} | {row.get('code','')} | {row.get('total_score',0):.1f} | {suggestion} |")
    if not actionable:
        lines.append("| — | — | — | — | 本次无满足条件候选 |")

    path.write_text("\n".join(lines), encoding="utf-8")
    _logger.info(f"  市场扫描候选已写入: {path.name}")
    return str(path)


def _write_pool_suggestions(suggestions: dict, meta: dict | None = None) -> str:
    vault = ObsidianVault()
    report_dir = Path(vault.vault_path) / "04-选股" / "筛选结果"
    report_dir.mkdir(parents=True, exist_ok=True)
    path = report_dir / f"池子调整建议_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    meta = meta or {}
    rules = meta.get("rules", {})

    sections = [
        ("建议晋级核心池", suggestions.get("promote_to_core", [])),
        ("建议保留观察", suggestions.get("keep_watch", [])),
        ("建议加入观察池", suggestions.get("add_to_watch", [])),
        ("需要人工复核", suggestions.get("manual_review", [])),
        ("建议降级移出核心池", suggestions.get("demote_from_core", [])),
        ("建议规避", suggestions.get("remove_or_avoid", [])),
    ]

    lines = [f"# 池子调整建议 — {datetime.now().strftime('%Y-%m-%d %H:%M')}", ""]
    if rules:
        lines.extend([
            "## 当前规则",
            "",
            f"- 晋级核心池：连续 {rules.get('promote_streak_days', 0)} 天分数 >= {rules.get('promote_min_score', 0):.1f}",
            f"- 加入观察池：连续 {rules.get('add_to_watch_streak_days', 0)} 天分数 >= {rules.get('watch_min_score', 0):.1f}",
            f"- 降级核心池：连续 {rules.get('demote_streak_days', 0)} 天分数 < {rules.get('demote_max_score', 0):.1f}",
            f"- 移出观察池：连续 {rules.get('remove_streak_days', 0)} 天分数 < {rules.get('remove_max_score', 0):.1f}",
            "",
        ])
    for title, rows in sections:
        lines.extend([f"## {title}", "", "| 股票 | 代码 | 分数 | 原因 |", "|------|------|------|------|"])
        if rows:
            for row in rows:
                lines.append(f"| {row['name']} | {row['code']} | {row['score']:.1f} | {row['reason']} |")
        else:
            lines.append("| — | — | — | 暂无 |")
        lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")
    return str(path)


def _sync_to_zixuan(actionable: list) -> None:
    """
    将可操作的股票同步到东方财富自选股列表。
    新入池的添加，不在池子里的删除，保持自选和池子一致。
    """
    if not actionable:
        return
    actionable = [item for item in actionable if not data_quality_blocks_auto_buy(item)]
    if not actionable:
        return
    try:
        # 1. 查当前自选股列表
        current = dispatch_mx_command("mx.zixuan.query")
        current_data = current.get("data", {}).get("allResults", {}).get("result", {}).get("dataList", [])
        current_codes = {str(s.get("SECURITY_CODE", "")).strip() for s in current_data}
        current_names = {str(s.get("SECURITY_CODE", "")).strip(): str(s.get("SECURITY_SHORT_NAME", "")).strip()
                         for s in current_data}

        # 2. 目标池子（本次可操作的股票）
        target_codes = set()
        target_map = {}
        for r in actionable:
            code = str(r.get("code", "")).strip()
            name = r.get("name", "")
            if code:
                target_codes.add(code)
                target_map[code] = name

        # 3. 需要添加的（在目标池但不在自选）
        to_add = target_codes - current_codes
        # 4. 需要删除的（在自选但不在目标池）
        to_remove = current_codes - target_codes

        added = 0
        for code in to_add:
            name = target_map.get(code, code)
            try:
                result = dispatch_mx_command("mx.zixuan.manage", query=f"把{name}添加到我的自选股列表")
                if result.get("status") == 0:
                    added += 1
            except Exception:
                pass

        removed = 0
        for code in to_remove:
            name = current_names.get(code, code)
            try:
                result = dispatch_mx_command("mx.zixuan.manage", query=f"把{name}从我的自选股列表删除")
                if result.get("status") == 0:
                    removed += 1
            except Exception:
                pass

        _logger.info(
            f">> 自选股同步: 添加{added}只 删除{removed}只 "
            f"(目标{len(target_codes)}只 自选原有{len(current_codes)}只)"
        )
    except Exception as e:
        _logger.warning(f">> 自选股同步失败: {e}")


def _resolve_market_candidates(default_query: str, select_type: str,
                               strategy_cfg: dict, blacklist: set,
                               tracked_candidates: list) -> tuple[list, str, dict]:
    """全市场模式：优先 mx-screener，再试 akshare，再回退缓存和 tracked。"""
    screening_cfg = strategy_cfg.get("screening", {})
    cache_ttl_hours = int(screening_cfg.get("candidate_cache_ttl_hours", 24))
    cache_ttl_seconds = max(cache_ttl_hours, 1) * 3600
    fallback_meta = {
        "source_chain": [],
        "used_cache": False,
        "cache_path": "",
        "cache_hit_source": "",
    }

    if default_query:
        mx_results = _call_mx_screener(default_query, select_type)
        if mx_results:
            candidates = [c for c in _dedupe_candidates(mx_results) if c.get("code") not in blacklist]
            _logger.info(f">> mx-screener 全市场初筛返回 {len(candidates)} 只")
            cache_path = _save_candidate_cache(
                "market_latest",
                candidates,
                "妙想智能选股（全市场）",
                {"query": default_query, "select_type": select_type},
            )
            fallback_meta["source_chain"].append("mx_market")
            fallback_meta["cache_path"] = cache_path
            return candidates, "妙想智能选股（全市场）", fallback_meta

        _logger.warning(">> mx-screener 调用失败，fallback 到 akshare 全市场轻筛")
        fallback_meta["source_chain"].append("mx_market_failed")

    candidates = _fallback_market_candidates(strategy_cfg, blacklist)
    if candidates:
        cache_path = _save_candidate_cache("market_latest", candidates, "akshare 全市场轻筛")
        fallback_meta["source_chain"].append("akshare_market")
        fallback_meta["cache_path"] = cache_path
        return candidates, "akshare 全市场轻筛", fallback_meta

    fallback_meta["source_chain"].append("akshare_market_failed")
    cached_candidates, cache_meta = _load_cached_candidates("market_latest", cache_ttl_seconds)
    if cached_candidates:
        fallback_meta["source_chain"].append("cache_market")
        fallback_meta["used_cache"] = True
        fallback_meta["cache_hit_source"] = str(cache_meta.get("source", ""))
        fallback_meta["cache_cached_at"] = str(cache_meta.get("cached_at", ""))
        return cached_candidates, f"本地缓存回退（{cache_meta.get('source', 'market_latest')})", fallback_meta

    if tracked_candidates:
        fallback_meta["source_chain"].append("tracked_fallback")
        _logger.warning(">> 全市场候选为空，回退到已跟踪股票池")
        return tracked_candidates, "已跟踪股票池回退", fallback_meta

    return [], "无可用候选来源", fallback_meta


def _resolve_tracked_candidates(pool: str, stocks_cfg: dict, blacklist: set,
                                default_query: str, select_type: str,
                                current_snapshot: dict | None = None) -> tuple[list, str, str]:
    """已跟踪模式：先从 core/watch 取池子，再做命中筛选。"""
    base_candidates, pool_name = _tracked_candidates(pool, stocks_cfg, blacklist, current_snapshot=current_snapshot)
    _logger.info(f">> 候选股票: {len(base_candidates)} 只")
    if not base_candidates:
        return [], pool_name, ""

    if default_query:
        mx_results = _call_mx_screener(default_query, select_type)
        if mx_results:
            mx_codes = {r["code"] for r in mx_results}
            candidates = [c for c in base_candidates if c.get("code") in mx_codes]
            _logger.info(f">> mx-screener 初筛: {len(mx_results)} 只通过，候选池命中 {len(candidates)} 只")
            if candidates:
                return candidates, pool_name, "妙想智能选股"
            _logger.warning(">> mx 筛选结果为空，fallback")
        else:
            _logger.warning(">> mx-screener 调用失败，fallback 到 akshare")

    return _fallback_tracked_candidates(stocks_cfg, blacklist, current_snapshot=current_snapshot), pool_name, "akshare 原生接口"


# ---------------------------------------------------------------------------
# 报告写入
# ---------------------------------------------------------------------------

def _write_screening_result(results: list, pool_name: str, source: str) -> str:
    """写入筛选结果到 Obsidian"""
    vault = ObsidianVault()
    date_str = datetime.now().strftime("%Y%m%d")
    time_str = datetime.now().strftime("%H%M%S")

    report_dir = Path(vault.vault_path) / "04-选股" / "筛选结果"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"筛选结果_{pool_name}_{date_str}_{time_str}.md"

    lines = [
        f"# 选股筛选报告 — {pool_name}池 {date_str}",
        "",
        f"筛选时间：{datetime.now().strftime('%H:%M')}",
        f"数据来源：{source}",
        "",
        "---",
        "",
        "| # | 股票 | 代码 | 四维总分 | 技术 | 基本面 | 资金 | 舆情 | 建议 |",
        "|---|------|------|---------|------|--------|------|------|------|",
    ]

    for i, r in enumerate(results, 1):
        total = r.get("total_score", 0)
        suggestion = get_recommendation(r)

        lines.append(
            f"| {i} | {r.get('name','')} | {r.get('code','')} | "
            f"**{total:.1f}** | {r.get('technical_score',0):.1f} | "
            f"{r.get('fundamental_score',0):.1f} | {r.get('flow_score',0):.1f} | "
            f"{r.get('sentiment_score',0):.1f} | {suggestion} |"
        )

    if not results:
        lines.append("| — | — | — | — | — | — | — | — | 暂无筛选结果 |")

    lines.extend(["", "---", "", "## 筛选条件", ""])
    if source == "妙想智能选股":
        lines.append("- 由妙想智能选股 API 东方财富自然语言筛选")
    elif "全市场" in source:
        lines.append("- 全市场候选池初筛（价格 / 成交额 / 市盈率 / ST 过滤）")
        lines.append(f"- 数据来源：{source}")
    else:
        lines.append("- 核心池候选股票（akshare 原生接口）")
        lines.append("- 黑名单过滤（永久+临时）")
    lines.append("")
    lines.append(f"> 本报告由 A股交易系统 自动生成")

    content = "\n".join(lines)
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(content)

    _logger.info(f"  已写入: {report_path.name}")
    return str(report_path)


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------

def run(pool: str = "watch", universe: str = "tracked") -> list:
    """
    执行选股流水线

    Args:
        pool: "core" | "watch" | "all"
        universe: "tracked" | "market"

    Returns:
        评分结果列表
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    _logger.info(f"[SCREENER] 选股流水线 {today_str} pool={pool} universe={universe}")
    try:
        vault = ObsidianVault()
        stocks_cfg = get_stocks()
        strategy_cfg = get_strategy()
        current_snapshot = load_pool_snapshot()
        mx_health = _mx_health_snapshot(include_unavailable=True)

        blacklist = _get_blacklist(stocks_cfg)

        screening_cfg = strategy_cfg.get("screening", {})
        default_query = screening_cfg.get("mx_query", "")
        select_type = screening_cfg.get("mx_select_type", "A股")

        tracked_fallback = _fallback_tracked_candidates(stocks_cfg, blacklist, current_snapshot=current_snapshot)
        resolution_meta = {"source_chain": []}

        if universe == "market":
            candidates, source, resolution_meta = _resolve_market_candidates(
                default_query, select_type, strategy_cfg, blacklist, tracked_fallback
            )
            pool_name = "市场扫描"
            _logger.info(f">> 市场扫描候选: {len(candidates)} 只")
        else:
            candidates, pool_name, source = _resolve_tracked_candidates(
                pool, stocks_cfg, blacklist, default_query, select_type, current_snapshot=current_snapshot
            )
            resolution_meta = {
                "source_chain": ["tracked_pool"],
                "used_cache": False,
                "cache_path": "",
                "cache_hit_source": "",
            }

        if not candidates:
            _logger.warning("无候选股票，退出")
            update_pipeline_state(
                "screener",
                "skipped",
                {
                    "pool": pool,
                    "universe": universe,
                    "reason": "no_candidates",
                    "source": source,
                    "source_chain": resolution_meta.get("source_chain", []),
                },
                today_str,
            )
            return []

        _logger.info(f">> 四维评分（来源: {source}）...")
        scored = batch_score(candidates)
        actionable = [
            r for r in scored
            if not r.get("veto_triggered", False) and not data_quality_blocks_auto_buy(r)
        ]
        today_decision = build_today_decision(strategy_cfg)
        history_group_id = f"screener:{today_str}:{datetime.now().strftime('%H%M%S')}"
        pool_suggestions, pool_meta = evaluate_pool_actions(
            scored,
            stocks_cfg,
            strategy_cfg,
            current_snapshot=current_snapshot,
            source=source,
        )
        snapshot_entries = pool_meta.get("snapshot_entries", [])
        snapshot_path = ""
        if snapshot_entries:
            snapshot_path = save_pool_snapshot(snapshot_entries, {
                **pool_meta,
                "pool": pool,
                "universe": universe,
                "pipeline": "stock_screener",
                "history_group_id": history_group_id,
                "source": source,
            })

        history_meta = {
            "snapshot_date": today_str,
            "updated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "history_group_id": history_group_id,
            "pipeline": "stock_screener",
            "pool": pool,
            "universe": universe,
            "source": source,
            "pool_snapshot_path": snapshot_path,
            "pool_snapshot_summary": pool_meta.get("snapshot_summary", {}),
            "candidate_count": len(candidates),
            "scored_count": len(scored),
            "actionable_count": len(actionable),
            "used_cache": resolution_meta.get("used_cache", False),
            "cache_path": resolution_meta.get("cache_path", ""),
            "cache_hit_source": resolution_meta.get("cache_hit_source", ""),
            "source_chain": resolution_meta.get("source_chain", []),
        }
        market_snapshot = load_market_snapshot()
        save_market_snapshot_history(
            market_snapshot,
            pipeline="stock_screener",
            history_group_id=history_group_id,
            metadata=history_meta,
        )
        save_decision_snapshot_history(
            today_decision,
            snapshot_date=today_str,
            pipeline="stock_screener",
            history_group_id=history_group_id,
            metadata=history_meta,
        )
        save_candidate_snapshot_history(
            scored,
            snapshot_date=today_str,
            pipeline="stock_screener",
            history_group_id=history_group_id,
            pool=pool,
            universe=universe,
            source=source,
            actionable_count=len(actionable),
            metadata=history_meta,
        )

        _logger.info(">> 写入筛选报告...")
        report_path = _write_screening_result(scored, pool_name, source)
        suggestion_path = _write_pool_suggestions(pool_suggestions, pool_meta)

        _logger.info(
            f"[SCREENER] 完成: {len(scored)} 只评分, "
            f"{len(actionable)} 只可操作 → {report_path}"
        )

        if universe == "market":
            market_watch_path = _write_market_scan_watchlist(scored)
        else:
            market_watch_path = None

        _sync_to_zixuan(actionable)

        if universe == "market":
            try:
                from scripts.pipeline.shadow_trade import buy_new_picks
                shadow_results = buy_new_picks()
                bought = [r for r in shadow_results if r.get("status") == "成功"]
                if bought:
                    _logger.info(f">> 影子交易: {len(bought)} 只已在模拟盘买入")
            except Exception as e:
                _logger.warning(f">> 影子交易买入失败: {e}")

        update_pipeline_state(
            "screener",
            "success",
            {
                "pool": pool,
                "universe": universe,
                "source": source,
                "candidate_count": len(candidates),
                "scored_count": len(scored),
                "actionable_count": len(actionable),
                "report_path": report_path,
                "suggestion_path": suggestion_path,
                "market_watch_path": market_watch_path,
                "used_fallback": "akshare" in source.lower() or "fallback" in source.lower(),
                "used_cache": resolution_meta.get("used_cache", False),
                "cache_path": resolution_meta.get("cache_path", ""),
                "cache_hit_source": resolution_meta.get("cache_hit_source", ""),
                "source_chain": resolution_meta.get("source_chain", []),
                "today_decision": today_decision,
                "history_group_id": history_group_id,
                "pool_suggestions": {
                    key: len(value)
                    for key, value in pool_suggestions.items()
                },
                "pool_state_path": pool_meta.get("state_path", ""),
                "pool_rules": pool_meta.get("rules", {}),
                "pool_snapshot_path": snapshot_path,
                "pool_snapshot_summary": pool_meta.get("snapshot_summary", {}),
                "pool_snapshot_count": len(snapshot_entries),
                "mx_health": mx_health,
            },
            today_str,
        )

        return scored
    except Exception as e:
        update_pipeline_state(
            "screener",
            "error",
            {
                "pool": pool,
                "universe": universe,
                "error": str(e),
            },
            today_str,
        )
        raise


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser(description="选股流水线")
    parser.add_argument(
        "--pool",
        choices=["core", "watch", "all"],
        default="watch",
        help="筛选哪个池（默认 watch）"
    )
    parser.add_argument(
        "--universe",
        choices=["tracked", "market"],
        default="tracked",
        help="候选集范围：tracked=现有池，market=全市场扫描"
    )
    args = parser.parse_args()

    results = run(pool=args.pool, universe=args.universe)
    print(f"\n筛选完成: {len(results)} 只")
    if results:
        print("\nTOP 5:")
        for i, r in enumerate(results[:5], 1):
            veto = " ❌" if r.get("veto_triggered") else ""
            print(f"  {i}. {r['name']}({r['code']}): {r['total_score']:.1f}{veto}")


if __name__ == "__main__":
    main()
