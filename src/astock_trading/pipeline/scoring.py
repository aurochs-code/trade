"""
pipeline/scoring.py — 核心池评分

流程：
1. 读核心池股票列表
2. MarketService 批量抓取数据
3. 抓大盘信号
4. StrategyService 评分 + 决策 → 事件写入
5. 更新 projection_candidate_pool
6. 写 Obsidian（评分报告 + 核心池 + 观察池）
7. 格式化 Discord embed
"""

from __future__ import annotations

import asyncio
import logging

from astock_trading.pipeline.context import PipelineContext
from astock_trading.pipeline.helpers import get_current_exposure
from astock_trading.platform.time import local_today_str
from astock_trading.reporting.discord import format_scoring_embed

_logger = logging.getLogger(__name__)


def run(ctx: PipelineContext, run_id: str) -> dict:
    """执行核心池评分 pipeline。"""

    # 1. 读核心池
    rows = ctx.conn.execute(
        "SELECT code, name FROM projection_candidate_pool WHERE pool_tier = 'core' ORDER BY score DESC"
    ).fetchall()
    stock_list = [{"code": r["code"], "name": r["name"] or ""} for r in rows]

    if not stock_list:
        _logger.warning("[scoring] 核心池为空")
        return {"scored": 0, "stock_list": []}

    # 2. 批量抓取
    snapshots = asyncio.run(
        ctx.market_svc.collect_batch(stock_list, run_id, include_sector_context=True)
    )

    # 3. 大盘信号
    market_state, index_data = asyncio.run(ctx.market_svc.collect_market_state(run_id))

    # 同步指数数据到 projection_market_state 表
    if index_data:
        ctx.projector.sync_market_state(index_data)

    # 3.5 获取当前仓位和本周买入次数（修复 #3：之前始终为 0）
    current_exposure_pct, weekly_buy_count = get_current_exposure(ctx)

    # 4. 评分 + 决策（传入真实的仓位数据）
    ctx.strategy_svc.evaluate(
        snapshots, market_state, run_id, ctx.config_version,
        current_exposure_pct=current_exposure_pct,
        weekly_buy_count=weekly_buy_count,
    )

    # 收集评分结果（使用 metadata_filter 在 SQL 层过滤，避免全量拉取）
    score_events = ctx.event_store.query(
        event_type="score.calculated",
        metadata_filter={"run_id": run_id},
    )
    run_scores = [e["payload"] for e in score_events]
    run_scores.sort(key=lambda x: x.get("total_score", 0), reverse=True)

    # 5. 更新 projection_candidate_pool（含降级逻辑）
    # 读取现有 streak_days
    existing_rows = {r["code"]: dict(r) for r in ctx.conn.execute(
        "SELECT code, streak_days, pool_tier FROM projection_candidate_pool WHERE pool_tier = 'core'"
    ).fetchall()}

    pool_entries = []
    demoted = []
    removed = []
    for s in run_scores:
        code = s.get("code", "")
        total = s.get("total_score", 0)
        prev = existing_rows.get(code, {})
        old_streak = prev.get("streak_days", 0)

        # streak_days: 正数=连续达标天数, 负数=连续低分天数
        if s.get("veto_triggered"):
            # 一票否决 → 立即降级到观察池
            demoted.append({"code": code, "name": s.get("name", ""), "reason": "veto"})
            pool_entries.append({
                "code": code, "name": s.get("name", ""),
                "pool_tier": "watch", "score": total,
                "streak_days": 0, "note": "veto_demoted",
            })
            # 删除核心池记录
            ctx.conn.execute(
                "DELETE FROM projection_candidate_pool WHERE code = ? AND pool_tier = 'core'", (code,)
            )
        elif total < 4:
            new_streak = (old_streak - 1) if old_streak < 0 else -1
            if new_streak <= -2:
                # 连续2天<4 → 移出池子
                removed.append({"code": code, "name": s.get("name", ""), "score": total})
                ctx.conn.execute(
                    "DELETE FROM projection_candidate_pool WHERE code = ? AND pool_tier = 'core'", (code,)
                )
            else:
                pool_entries.append({
                    "code": code, "name": s.get("name", ""),
                    "pool_tier": "core", "score": total,
                    "streak_days": new_streak, "note": f"low_score_streak={new_streak}",
                })
        elif total < 5:
            new_streak = (old_streak - 1) if old_streak < 0 else -1
            if new_streak <= -2:
                # 连续2天<5 → 降级到观察池
                demoted.append({"code": code, "name": s.get("name", ""), "reason": f"score<5 x{abs(new_streak)}d"})
                pool_entries.append({
                    "code": code, "name": s.get("name", ""),
                    "pool_tier": "watch", "score": total,
                    "streak_days": 0, "note": "demoted_from_core",
                })
                ctx.conn.execute(
                    "DELETE FROM projection_candidate_pool WHERE code = ? AND pool_tier = 'core'", (code,)
                )
            else:
                pool_entries.append({
                    "code": code, "name": s.get("name", ""),
                    "pool_tier": "core", "score": total,
                    "streak_days": new_streak, "note": f"low_score_streak={new_streak}",
                })
        else:
            # 评分正常，重置 streak
            pool_entries.append({
                "code": code, "name": s.get("name", ""),
                "pool_tier": "core", "score": total,
                "streak_days": max(old_streak + 1, 1) if old_streak >= 0 else 1,
                "note": "veto" if s.get("veto_triggered") else "",
            })
    ctx.projector.sync_candidate_pool(pool_entries)

    # 写池子变动事件
    for d in demoted:
        ctx.event_store.append(
            stream=f"strategy:{d['code']}", stream_type="strategy",
            event_type="pool.demoted",
            payload={"code": d["code"], "name": d.get("name", ""), "from": "core", "to": "watch", "reason": d["reason"]},
            metadata={"run_id": run_id},
        )
    for r in removed:
        ctx.event_store.append(
            stream=f"strategy:{r['code']}", stream_type="strategy",
            event_type="pool.removed",
            payload={"code": r["code"], "name": r.get("name", ""), "from": "core", "score": r["score"]},
            metadata={"run_id": run_id},
        )

    # 6. Obsidian
    ctx.obsidian.write_scoring_report(run_id, run_scores)
    ctx.obsidian.write_core_pool()
    ctx.obsidian.write_watch_pool()
    ctx.obsidian.write_decision_pool()
    ctx.obsidian.write_candidate_pool_overview()

    # 池子调整建议（有变动时自动写入）
    ctx.obsidian.write_pool_adjustment(run_id, demoted, removed)

    # 日志追加
    lines = ["## 核心池评分", ""]
    for s in run_scores:
        total = s.get("total_score", 0)
        emoji = "✅" if total >= 7 else ("🟡" if total >= 5 else "❌")
        lines.append(f"- {s.get('name', '')}({s.get('code', '')}) {emoji} {total:.1f}")
    if demoted:
        lines.extend(["", "### 池子变动"])
        for d in demoted:
            lines.append(f"- ⬇️ {d.get('name', '')}({d['code']}) 降级观察池（{d['reason']}）")
    if removed:
        for r in removed:
            lines.append(f"- ❌ {r.get('name', '')}({r['code']}) 移出池子（评分 {r['score']:.1f}）")
    ctx.obsidian.write_daily_log(run_id, "\n".join(lines))

    # 刷新每日巡检报告
    ctx.obsidian.write_daily_output_index(run_id)

    # 7. Discord embed
    embed = format_scoring_embed(run_scores, local_today_str())

    _logger.info(f"[scoring] 完成: {len(run_scores)} 只评分")

    # 8. Discord 推送
    try:
        from astock_trading.reporting.discord_sender import send_embed
        ok, err = send_embed(embed)
        if not ok:
            _logger.warning(f"[scoring] Discord 推送失败: {err}")
    except Exception as e:
        _logger.warning(f"[scoring] Discord 推送异常: {e}")

    return {
        "scored": len(run_scores),
        "scores": run_scores,
        "demoted": demoted,
        "removed": removed,
        "discord_embed": embed,
    }
