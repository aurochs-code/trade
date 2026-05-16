"""
pipeline/sentiment.py — 舆情监控

流程：
1. 读持仓股 + 核心池股票
2. 对每只票调 mx-search 搜最新资讯
3. 过滤重要事件（评级变动、重大公告）
4. 去重（跟已推送的比对）
5. 写事件 + Obsidian 日志
6. 格式化 Discord embed 推送
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import datetime, timezone

from astock_trading.pipeline.context import PipelineContext

_logger = logging.getLogger(__name__)

# 重要事件的关键词
_RATING_KEYWORDS = {"买入", "增持", "推荐", "强烈推荐", "优于大市"}
_NEGATIVE_KEYWORDS = {"减持", "卖出", "回避", "中性"}
_EVENT_KEYWORDS = {"业绩预告", "业绩快报", "重大合同", "股权变动", "回购", "增减持",
                   "停牌", "退市", "立案", "处罚", "诉讼", "违规"}
_OPENCLI_NEGATIVE_KEYWORDS = {"暴跌", "跌停", "爆雷", "违规", "处罚", "退市", "立案", "调查", "减持", "亏损"}
_OPENCLI_EVENT_KEYWORDS = _EVENT_KEYWORDS | {"中标", "复牌", "控制权", "关税", "制裁", "禁令", "供应中断"}
_OPENCLI_SOURCE_CN = {
    "eastmoney": "东财",
    "sinafinance": "新浪",
    "xueqiu": "雪球",
    "bloomberg": "彭博",
    "reuters": "路透",
}


def _item_hash(item: dict) -> str:
    """生成资讯去重 hash（含日期避免同标题不同日期误去重）。"""
    key = f"{item.get('code', '')}{item.get('title', '')}{item.get('date', '')[:10]}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _extract_brief(text: str, max_len: int = 120) -> str:
    """从研报/新闻内容中提取关键摘要。"""
    if not text:
        return ""
    # 去掉全角空格和多余换行
    text = text.replace("\u3000", "").replace("\r\n", " ").replace("\n", " ").strip()

    # 优先找盈利预测/投资建议相关的句子
    priority_keywords = ["预计", "盈利预测", "目标价", "维持", "首次覆盖", "上调", "下调",
                         "营收", "净利润", "CAGR", "增长", "PE", "估值"]
    sentences = [s.strip() for s in text.replace("。", "。\n").split("\n") if s.strip()]

    for sent in sentences:
        if any(kw in sent for kw in priority_keywords) and len(sent) > 10:
            return sent[:max_len] + ("…" if len(sent) > max_len else "")

    # 没找到就取前 max_len 字
    return text[:max_len] + ("…" if len(text) > max_len else "")


def _classify_item(item: dict) -> dict | None:
    """
    分类资讯，返回 None 表示不重要（跳过）。
    返回 dict 包含 level/emoji/summary。
    """
    info_type = item.get("informationType", "")
    rating = str(item.get("rating", ""))
    title = item.get("title", "")
    content = item.get("content", "")[:200]
    ins_name = item.get("insName", "")
    entity = item.get("entityFullName", "")

    # 研报评级
    if info_type == "REPORT" and rating:
        # 提取摘要：取 content 前 150 字，去掉开头的股票名和空白
        brief = content.strip()
        for prefix in [entity, f"{entity}({entity})", "\u3000", "\t"]:
            brief = brief.lstrip(prefix).strip()
        # 取投资要点或盈利预测相关的句子
        brief = _extract_brief(brief, 120)

        if any(k in rating for k in _RATING_KEYWORDS):
            return {
                "level": "positive",
                "emoji": "🟢",
                "summary": f"{ins_name}「{rating}」",
                "brief": brief,
                "date": item.get("date", "")[:10],
            }
        if any(k in rating for k in _NEGATIVE_KEYWORDS):
            return {
                "level": "negative",
                "emoji": "🔴",
                "summary": f"{ins_name}「{rating}」",
                "brief": brief,
                "date": item.get("date", "")[:10],
            }

    # 重大公告
    if info_type == "ANNOUNCEMENT":
        for kw in _EVENT_KEYWORDS:
            if kw in title or kw in content:
                brief = _extract_brief(content.strip(), 120)
                return {
                    "level": "event",
                    "emoji": "📢",
                    "summary": title[:60],
                    "brief": brief,
                }

    # 负面新闻
    if info_type == "NEWS":
        neg_words = {"暴跌", "跌停", "爆雷", "违规", "处罚", "退市", "立案", "亏损"}
        for w in neg_words:
            if w in title:
                brief = _extract_brief(content.strip(), 120)
                return {
                    "level": "negative",
                    "emoji": "🔴",
                    "summary": title[:60],
                    "brief": brief,
                }

    return None


def _classify_opencli_watch_item(item: dict, code: str, name: str, source_kind: str) -> dict | None:
    """Classify opencli watch-stock items for low-noise sentiment alerts."""
    title = str(item.get("title") or "").strip()
    text = str(item.get("summary") or item.get("content") or item.get("text") or "").strip()
    combined = f"{title} {text}".strip()
    if not combined:
        return None

    code_hit = bool(code and code in combined)
    name_hit = bool(name and name in combined)
    if not (code_hit or name_hit):
        return None

    source = str(item.get("source") or "")
    source_cn = _OPENCLI_SOURCE_CN.get(source, source or "opencli")
    if any(word in combined for word in _OPENCLI_NEGATIVE_KEYWORDS):
        return {
            "level": "negative",
            "emoji": "🔴",
            "summary": f"{source_cn if source_kind != 'xueqiu_comments' else '雪球评论'}负面线索",
            "brief": _extract_brief(combined, 120),
            "date": str(item.get("time") or item.get("created_at") or "")[:10],
        }
    if any(word in combined for word in _OPENCLI_EVENT_KEYWORDS):
        return {
            "level": "event",
            "emoji": "📢",
            "summary": f"{source_cn if source_kind != 'xueqiu_comments' else '雪球评论'}事件线索",
            "brief": _extract_brief(combined, 120),
            "date": str(item.get("time") or item.get("created_at") or "")[:10],
        }
    return None


def _get_pushed_hashes(conn, hours: int = 24) -> set[str]:
    """获取最近已推送的资讯 hash。"""
    try:
        rows = conn.execute(
            "SELECT payload_json FROM market_observations "
            "WHERE source = 'sentiment_monitor' AND kind = 'news_alert' "
            "ORDER BY observed_at DESC LIMIT 500"
        ).fetchall()
        hashes = set()
        for r in rows:
            try:
                p = json.loads(r["payload_json"])
                hashes.add(p.get("hash", ""))
            except Exception:
                pass
        return hashes
    except Exception as e:
        _logger.warning(f"[sentiment] 读取已推送 hash 失败: {e}")
        return set()


def _save_pushed(conn, symbol: str, item_hash: str, summary: str, run_id: str):
    """记录已推送的资讯。"""
    import uuid
    now = datetime.now(timezone.utc).isoformat()
    try:
        conn.execute(
            """INSERT OR IGNORE INTO market_observations
               (observation_id, source, kind, symbol, observed_at, run_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (uuid.uuid4().hex[:16], "sentiment_monitor", "news_alert",
             symbol, now, run_id,
             json.dumps({"hash": item_hash, "summary": summary}, ensure_ascii=False)),
        )
    except Exception as e:
        _logger.warning(f"[sentiment] save_pushed failed: {e}")


def _get_cached_items(conn, symbol: str, ttl_minutes: int = 30) -> list[dict] | None:
    """获取缓存的搜索结果，未过期返回 items 列表，过期或无缓存返回 None。"""
    try:
        row = conn.execute(
            "SELECT observed_at, payload_json FROM market_observations "
            "WHERE source = 'sentiment_monitor' AND kind = 'news_cache' AND symbol = ? "
            "ORDER BY observed_at DESC LIMIT 1",
            (symbol,),
        ).fetchone()
        if not row:
            return None
        cached_at = datetime.fromisoformat(row["observed_at"])
        age = (datetime.now(timezone.utc) - cached_at).total_seconds() / 60
        if age > ttl_minutes:
            return None
        payload = json.loads(row["payload_json"])
        return payload.get("items", [])
    except Exception as e:
        _logger.debug(f"[sentiment] 缓存读取失败 {symbol}: {e}")
        return None


def _save_cache(conn, symbol: str, items: list[dict], run_id: str):
    """缓存搜索结果。"""
    import uuid
    now = datetime.now(timezone.utc).isoformat()
    try:
        # 删除旧缓存
        conn.execute(
            "DELETE FROM market_observations "
            "WHERE source = 'sentiment_monitor' AND kind = 'news_cache' AND symbol = ?",
            (symbol,),
        )
        conn.execute(
            """INSERT INTO market_observations
               (observation_id, source, kind, symbol, observed_at, run_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (uuid.uuid4().hex[:16], "sentiment_monitor", "news_cache",
             symbol, now, run_id,
             json.dumps({"items": items}, ensure_ascii=False)),
        )
    except Exception as e:
        _logger.warning(f"[sentiment] save_cache failed: {e}")


def _opencli_watch_item_hash(code: str, item: dict) -> str:
    normalized = {
        "code": code,
        "title": item.get("title") or str(item.get("text") or item.get("summary") or "")[:80],
        "date": str(item.get("time") or item.get("created_at") or "")[:10],
    }
    return _item_hash(normalized)


def _collect_opencli_watch_alerts(
    ctx: PipelineContext,
    watch_stocks: dict[str, str],
    pushed_hashes: set[str],
    run_id: str,
) -> list[dict]:
    alerts: list[dict] = []

    try:
        finance_items = asyncio.run(ctx.market_svc.collect_finance_flash(limit=30, run_id=run_id))
    except Exception as e:
        _logger.warning(f"[sentiment] opencli finance flash failed: {e}")
        finance_items = []

    for code, name in watch_stocks.items():
        for item in finance_items:
            h = _opencli_watch_item_hash(code, item)
            if h in pushed_hashes:
                continue
            classified = _classify_opencli_watch_item(item, code, name, "finance_flash")
            if classified is None:
                continue
            alerts.append({"code": code, "name": name, "hash": h, **classified})
            _save_pushed(ctx.conn, code, h, classified["summary"], run_id)
            pushed_hashes.add(h)

    for code, name in list(watch_stocks.items())[:5]:
        try:
            comments = asyncio.run(ctx.market_svc.collect_xueqiu_comments(code, limit=5, run_id=run_id))
        except Exception as e:
            _logger.warning(f"[sentiment] xueqiu comments failed: {name}({code}) {e}")
            comments = []
        for item in comments:
            h = _opencli_watch_item_hash(code, item)
            if h in pushed_hashes:
                continue
            classified = _classify_opencli_watch_item(item, code, name, "xueqiu_comments")
            if classified is None:
                continue
            alerts.append({"code": code, "name": name, "hash": h, **classified})
            _save_pushed(ctx.conn, code, h, classified["summary"], run_id)
            pushed_hashes.add(h)

    return alerts


def run(ctx: PipelineContext, run_id: str) -> dict:
    """执行舆情监控 pipeline。"""
    from astock_trading.market.mx.search import MXSearch

    # 1. 收集监控股票列表（持仓 + 核心池，去重）
    positions = ctx.exec_svc.get_positions()
    pool_rows = ctx.conn.execute(
        "SELECT code, name FROM projection_candidate_pool WHERE pool_tier = 'core' ORDER BY score DESC"
    ).fetchall()

    watch_stocks: dict[str, str] = {}  # code → name
    for p in positions:
        watch_stocks[p.code] = p.name
    for r in pool_rows:
        if r["code"] not in watch_stocks:
            watch_stocks[r["code"]] = r["name"] or r["code"]

    if not watch_stocks:
        _logger.info("[sentiment] 无持仓和核心池，跳过")
        return {"monitored": 0, "alerts": []}

    # 2. 获取已推送 hash
    pushed_hashes = _get_pushed_hashes(ctx.conn)

    # 3. 并发搜索资讯（带缓存 + semaphore 限流）
    cfg = ctx.cfg.get("sentiment", {})
    cache_ttl = cfg.get("dedup_hours", 24) * 60 // 24  # 默认约 30 分钟
    if cache_ttl < 10:
        cache_ttl = 30

    mx = MXSearch()
    alerts = []
    cache_hits = 0

    async def _search_one(code: str, name: str, sem: asyncio.Semaphore) -> tuple[str, list[dict], bool]:
        """单票搜索，返回 (code, items, is_cache_hit)。"""
        cached_items = _get_cached_items(ctx.conn, code, ttl_minutes=cache_ttl)
        if cached_items is not None:
            return code, cached_items, True
        async with sem:
            try:
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(None, mx.search, f"{name} 最新资讯")
                items = mx.extract_items(result)
                _save_cache(ctx.conn, code, items, run_id)
                return code, items, False
            except Exception as e:
                _logger.warning(f"[sentiment] {name}({code}) search failed: {e}")
                return code, [], False

    async def _search_all():
        sem = asyncio.Semaphore(5)
        tasks = [
            _search_one(code, name, sem)
            for code, name in watch_stocks.items()
        ]
        return await asyncio.gather(*tasks)

    search_results = asyncio.run(_search_all())

    for code, items, is_hit in search_results:
        if is_hit:
            cache_hits += 1
        name = watch_stocks.get(code, code)
        for item in items:
            h = _item_hash(item)
            if h in pushed_hashes:
                continue

            classified = _classify_item(item)
            if classified is None:
                continue

            alert = {
                "code": code,
                "name": name,
                "hash": h,
                "date": item.get("date", "")[:10],
                **classified,
            }
            alerts.append(alert)

            # 记录已推送
            _save_pushed(ctx.conn, code, h, classified["summary"], run_id)
            pushed_hashes.add(h)

    opencli_alerts = _collect_opencli_watch_alerts(ctx, watch_stocks, pushed_hashes, run_id)
    alerts.extend(opencli_alerts)

    # 4. Obsidian 日志
    if alerts:
        lines = ["## 舆情监控", ""]
        for a in alerts:
            lines.append(f"- {a['emoji']} **{a['name']}({a['code']})**: {a['summary']}")
            if a.get("brief"):
                lines.append(f"  > {a['brief']}")
        ctx.obsidian.write_daily_log(run_id, "\n".join(lines))

    # 5. Discord 推送
    if alerts:
        from astock_trading.reporting.discord import format_sentiment_embed
        embed = format_sentiment_embed(alerts)
        try:
            from astock_trading.reporting.discord_sender import send_embed
            ok, err = send_embed(embed)
            if not ok:
                _logger.warning(f"[sentiment] Discord 推送失败: {err}")
        except Exception as e:
            _logger.warning(f"[sentiment] Discord 推送异常: {e}")

    _logger.info(f"[sentiment] 完成: 监控 {len(watch_stocks)} 只, {len(alerts)} 条告警, 缓存命中 {cache_hits}")

    return {
        "monitored": len(watch_stocks),
        "alerts": alerts,
        "opencli_alerts": len(opencli_alerts),
        "cache_hits": cache_hits,
    }
