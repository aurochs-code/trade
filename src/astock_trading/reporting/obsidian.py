"""
reporting/obsidian.py — Obsidian vault 投影

对齐 trade-vault/ 目录结构：
  01-状态/持仓/持仓概览.md         ← write_portfolio_status()
  01-状态/池子/核心池.md           ← write_core_pool()
  01-状态/池子/观察池.md           ← write_watch_pool()
  01-状态/池子/决策池.md           ← write_decision_pool()
  01-状态/账户/账户总览.md         ← write_account_overview()
  02-巡检/{date}.md                ← write_daily_log()/write_daily_output_index()
  04-决策/候选池/最新评分.md        ← write_scoring_report()
  03-分析/历史模拟盘交易记录.md      ← append_paper_trade_log()
  04-决策/今日决策.md              ← write_today_decision()
  04-决策/候选池/候选池总览.md     ← write_candidate_pool_overview()

trade-vault 只保留人读摘要；原始事实以 MySQL 为准。
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Optional

from astock_trading.platform.events import EventStore
from astock_trading.platform.time import iso_to_local, local_date_bounds_utc
from astock_trading.platform.time import local_now_str, local_today_str
from astock_trading.reporting.screening_result import render_screening_result

_logger = logging.getLogger(__name__)


_CANDIDATE_NOTE_LABELS = {
    "requires_entry_strategy_route": "缺少有效策略路线，暂留观察",
}


def _candidate_note_display(note: str | None) -> str:
    raw = note or ""
    for marker, label in _CANDIDATE_NOTE_LABELS.items():
        if marker in raw:
            return label
    return raw


class ObsidianProjector:
    """从投影表生成 Obsidian markdown 文件。"""

    def __init__(
        self,
        event_store: EventStore,
        conn: Any,
        vault_path: Optional[str] = None,
    ):
        self._events = event_store
        self._conn = conn
        self._vault = Path(vault_path) if vault_path else None

    def _write(self, relative_path: str, content: str) -> Optional[str]:
        if not self._vault:
            _logger.debug("[obsidian] vault 路径未配置，跳过写入: %s", relative_path)
            return None
        full = self._vault / relative_path
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_text(content, encoding="utf-8")
        return str(full)

    def _now(self) -> str:
        return local_now_str()

    def _today(self) -> str:
        return local_today_str()

    def _clean_section_body(self, body: str) -> str:
        lines = body.strip().splitlines()
        if lines and lines[0] == "---":
            for idx in range(1, len(lines)):
                if lines[idx] == "---":
                    lines = lines[idx + 1:]
                    break
        while lines and not lines[0].strip():
            lines.pop(0)
        if lines and lines[0].startswith("# "):
            lines.pop(0)
        while lines and not lines[0].strip():
            lines.pop(0)
        return "\n".join(lines).strip()

    def _daily_inspection_base(self, run_id: str | None = None) -> tuple[str, str]:
        today = self._today()
        now = self._now()
        path = f"02-巡检/{today}.md"
        header = "\n".join(
            [
                "---",
                f"date: {today}",
                f"updated_at: {now}",
                "type: daily_inspection",
                "tags: [巡检, 自动更新]",
                "---",
                "",
                f"# 收盘巡检 - {today}",
                "",
                "## 结论",
                "",
                "- 待 Agent 收盘后补充综合结论。",
            ]
        )

        existing = ""
        if self._vault:
            full = self._vault / path
            if full.exists():
                existing = full.read_text(encoding="utf-8").rstrip()
        if not existing:
            existing = header
        return path, existing

    def _append_daily_inspection(self, title: str, body: str, run_id: str | None = None) -> str:
        path, existing = self._daily_inspection_base(run_id)
        run_line = f"\n\nrun_id: `{run_id}`\n" if run_id else ""
        content = f"{existing}\n\n## {title}\n{run_line}\n{self._clean_section_body(body)}\n"
        self._write(path, content)
        return content

    def _upsert_daily_inspection(self, title: str, body: str, run_id: str | None = None) -> str:
        path, existing = self._daily_inspection_base(run_id)
        run_line = f"\n\nrun_id: `{run_id}`\n" if run_id else ""
        section = f"## {title}\n{run_line}\n{self._clean_section_body(body)}\n"
        pattern = re.compile(rf"\n## {re.escape(title)}\n.*?(?=\n## |\Z)", re.DOTALL)
        if pattern.search(existing):
            content = pattern.sub("\n" + section.rstrip(), existing).rstrip() + "\n"
        else:
            content = f"{existing}\n\n{section}"
        self._write(path, content)
        return content

    # ------------------------------------------------------------------
    # 01-状态/持仓/持仓概览.md
    # ------------------------------------------------------------------

    def write_portfolio_status(self) -> str:
        """从 projection_positions 生成持仓概览（A 股/港股分区）。"""
        rows = self._conn.execute(
            "SELECT * FROM projection_positions ORDER BY entry_date"
        ).fetchall()

        now = self._now()
        lines = [
            "---",
            f"updated_at: {now}",
            "type: portfolio_overview",
            "tags: [持仓, 状态, 自动更新]",
            "---",
            "",
            "# 持仓概览",
            "",
        ]

        if not rows:
            lines.append("当前无持仓。")
        else:
            total_cost = sum(r["avg_cost_cents"] * r["shares"] for r in rows) / 100
            total_market = sum((r["current_price_cents"] or r["avg_cost_cents"]) * r["shares"] for r in rows) / 100
            total_pnl = total_market - total_cost

            # 分区：港股代码以 0 开头且为 5 位数字
            a_rows = [r for r in rows if not (r["code"].isdigit() and len(r["code"]) == 5 and r["code"][0] == "0")]
            hk_rows = [r for r in rows if r["code"].isdigit() and len(r["code"]) == 5 and r["code"][0] == "0"]

            lines.extend([
                f"持仓 **{len(rows)}** 只 | 总成本 ¥{total_cost:,.0f} | 总市值 ¥{total_market:,.0f} | 浮动盈亏 ¥{total_pnl:+,.0f}",
                "",
            ])

            def _pos_table(title: str, pos_rows: list) -> list[str]:
                if not pos_rows:
                    return []
                out = [f"## {title}", ""]
                out.append("| 代码 | 名称 | 风格 | 股数 | 成本 | 现价 | 盈亏 | 入场日 |")
                out.append("|------|------|------|------|------|------|------|--------|")
                for r in pos_rows:
                    cost = r["avg_cost_cents"] / 100
                    price = (r["current_price_cents"] or r["avg_cost_cents"]) / 100
                    pnl = (price - cost) * r["shares"]
                    out.append(
                        f"| {r['code']} | {r['name']} | {r['style']} "
                        f"| {r['shares']} | ¥{cost:.2f} | ¥{price:.2f} "
                        f"| ¥{pnl:+,.0f} | {r['entry_date']} |"
                    )
                out.append("")
                return out

            # 只有一个市场时不分区
            if a_rows and hk_rows:
                lines.extend(_pos_table("A 股", a_rows))
                lines.extend(_pos_table("港股", hk_rows))
            else:
                # 单市场，不加子标题
                all_rows = a_rows or hk_rows
                lines.append("| 代码 | 名称 | 风格 | 股数 | 成本 | 现价 | 盈亏 | 入场日 |")
                lines.append("|------|------|------|------|------|------|------|--------|")
                for r in all_rows:
                    cost = r["avg_cost_cents"] / 100
                    price = (r["current_price_cents"] or r["avg_cost_cents"]) / 100
                    pnl = (price - cost) * r["shares"]
                    lines.append(
                        f"| {r['code']} | {r['name']} | {r['style']} "
                        f"| {r['shares']} | ¥{cost:.2f} | ¥{price:.2f} "
                        f"| ¥{pnl:+,.0f} | {r['entry_date']} |"
                    )

        content = "\n".join(lines) + "\n"
        self._write("01-状态/持仓/持仓概览.md", content)
        return content

    # ------------------------------------------------------------------
    # 01-状态/池子/核心池.md + 观察池.md
    # ------------------------------------------------------------------

    def write_core_pool(self) -> str:
        """从 projection_candidate_pool 生成核心池页。"""
        rows = self._conn.execute(
            "SELECT * FROM projection_candidate_pool WHERE pool_tier = 'core' ORDER BY score DESC"
        ).fetchall()

        now = self._now()
        lines = [
            "---",
            f"date: {self._today()}",
            "type: watchlist_core",
            "tags: [核心池, 选股]",
            f"updated_at: {now}",
            "---",
            "",
            "# 核心池（结构化投影）",
            "",
            f"> 更新时间：{now}",
            "",
            "| # | 股票 | 代码 | 四维总分 | 加入日 | 连续天数 | 备注 |",
            "|---|------|------|---------|--------|----------|------|",
        ]

        for i, r in enumerate(rows, 1):
            score = r["score"] or 0
            emoji = "✅" if score >= 7 else ("🟡" if score >= 5 else "❌")
            lines.append(
                f"| {i} | {r['name'] or ''} | {r['code']} "
                f"| {emoji} **{score:.1f}** "
                f"| {(r['added_at'] or '')[:10]} | {r['streak_days'] or 0} "
                f"| {_candidate_note_display(r['note'])} |"
            )
        if not rows:
            lines.append("| — | — | — | — | — | — | 暂无 |")

        content = "\n".join(lines) + "\n"
        self._write("01-状态/池子/核心池.md", content)
        return content

    def write_watch_pool(self) -> str:
        """从 projection_candidate_pool 生成观察池页。"""
        rows = self._conn.execute(
            "SELECT * FROM projection_candidate_pool WHERE pool_tier = 'watch' ORDER BY score DESC"
        ).fetchall()

        now = self._now()
        lines = [
            "---",
            f"date: {self._today()}",
            "type: watchlist_observe",
            "tags: [观察池, 选股]",
            f"updated_at: {now}",
            "---",
            "",
            "# 观察池（结构化投影）",
            "",
            f"> 更新时间：{now}",
            "",
            "| # | 股票 | 代码 | 四维总分 | 加入日 | 备注 |",
            "|---|------|------|---------|--------|------|",
        ]

        for i, r in enumerate(rows, 1):
            score = r["score"] or 0
            lines.append(
                f"| {i} | {r['name'] or ''} | {r['code']} "
                f"| **{score:.1f}** | {(r['added_at'] or '')[:10]} "
                f"| {_candidate_note_display(r['note'])} |"
            )
        if not rows:
            lines.append("| — | — | — | — | — | 暂无 |")

        content = "\n".join(lines) + "\n"
        self._write("01-状态/池子/观察池.md", content)
        return content

    # 兼容旧接口
    def write_pool_status(self) -> str:
        self.write_core_pool()
        return self.write_watch_pool()

    # ------------------------------------------------------------------
    # 01-状态/账户/账户总览.md
    # ------------------------------------------------------------------

    def write_account_overview(self, capital: float = 0, cash: float = 0) -> str:
        """生成账户总览。
        
        如果 cash 未传入，从 projection_balances 读；
        若 balance 也为空，则用总资产 - 持仓市值估算现金。
        """
        rows = self._conn.execute(
            "SELECT * FROM projection_positions ORDER BY entry_date"
        ).fetchall()

        now = self._now()
        total_market = sum((r["current_price_cents"] or r["avg_cost_cents"]) * r["shares"] for r in rows) / 100

        # 尝试从余额表读现金
        if cash == 0:
            row = self._conn.execute("SELECT cash_cents, total_asset_cents FROM projection_balances LIMIT 1").fetchone()
            if row and row["cash_cents"]:
                cash = row["cash_cents"] / 100
            else:
                # 无 balance 数据时，用总资产估算（需要在调用时传入 capital）
                cash = (capital - total_market) if capital else 0

        total_asset = cash + total_market

        lines = [
            "---",
            f"updated_at: {now}",
            "type: account_overview",
            "tags: [账户, 状态, 自动更新]",
            "---",
            "",
            "# 账户总览",
            "",
            "| 项目 | 金额 |",
            "|------|------|",
            f"| 现金 | ¥{cash:,.0f} |",
            f"| 持仓市值 | ¥{total_market:,.0f} |",
            f"| 总资产 | ¥{total_asset:,.0f} |",
            f"| 持仓数 | {len(rows)} 只 |",
        ]

        content = "\n".join(lines) + "\n"
        self._write("01-状态/账户/账户总览.md", content)
        return content

    # ------------------------------------------------------------------
    # 02-巡检/{date}.md
    # ------------------------------------------------------------------

    def write_daily_log(self, run_id: str, report: str) -> str:
        """把运行日志追加进当天巡检报告。"""
        return self._append_daily_inspection("运行日志", report, run_id)

    # ------------------------------------------------------------------
    # 04-决策/候选池/最新评分.md
    # ------------------------------------------------------------------

    def write_scoring_report(self, run_id: str, scores: list[dict]) -> str:
        """写入评分报告。"""
        today = self._today()
        now = self._now()

        lines = [
            "---",
            f"date: {today}",
            f"updated_at: {now}",
            f"run_id: {run_id}",
            "type: scoring_report",
            "tags: [评分, 核心池, 自动更新]",
            "---",
            "",
            f"# 核心池评分 — {today}",
            "",
            "| # | 名称 | 代码 | 总分 | 技术 | 基本面 | 资金 | 舆情 | 风格 | 状态 |",
            "|---|------|------|------|------|--------|------|------|------|------|",
        ]

        for i, s in enumerate(scores, 1):
            total = float(s.get("total_score", s.get("total", 0)) or 0)
            veto = s.get("veto_triggered", False)
            status = "❌" if veto else ("✅" if total >= 7 else ("🟡" if total >= 5 else "❌"))
            lines.append(
                f"| {i} | {s.get('name', '')} | {s.get('code', '')} "
                f"| **{total:.1f}** "
                f"| {float(s.get('technical_score', 0) or 0):.1f} "
                f"| {float(s.get('fundamental_score', 0) or 0):.1f} "
                f"| {float(s.get('flow_score', 0) or 0):.1f} "
                f"| {float(s.get('sentiment_score', 0) or 0):.1f} "
                f"| {s.get('style', '')} | {status} |"
            )

        content = "\n".join(lines) + "\n"
        self._write("04-决策/候选池/最新评分.md", content)
        return content

    # ------------------------------------------------------------------
    # 04-决策/今日决策.md
    # ------------------------------------------------------------------

    def write_today_decision(
        self,
        market_signal: str = "",
        multiplier: float = 0.0,
        can_buy: bool = False,
        weekly_buys: int = 0,
        holding_count: int = 0,
        exposure_pct: float = 0.0,
        reasons: list[str] = None,
    ) -> str:
        """生成今日决策页。"""
        now = self._now()
        reasons = reasons or []

        if market_signal in ("RED", "CLEAR"):
            action = "NO_TRADE"
        elif not can_buy:
            action = "NO_TRADE"
        elif market_signal == "YELLOW":
            action = "REDUCED_BUY"
        else:
            action = "BUY_ALLOWED"

        lines = [
            "---",
            f"updated_at: {now}",
            "type: today_decision",
            "tags: [决策, 自动更新]",
            "---",
            "",
            "# 今日决策",
            "",
            "## 决策摘要",
            "",
            "| 项目 | 数值 |",
            "|------|------|",
            f"| 决策动作 | {action} |",
            f"| 市场信号 | {market_signal or '—'} |",
            f"| 仓位系数 | {multiplier:.2f} |",
            f"| 当前仓位 | {exposure_pct:.1%} |",
            f"| 本周买入次数 | {weekly_buys} |",
            f"| 当前持仓数 | {holding_count} |",
            "",
            "## 原因说明",
            "",
        ]
        if reasons:
            lines.extend([f"- {r}" for r in reasons])
        else:
            lines.append("- 无")

        content = "\n".join(lines) + "\n"
        self._write("04-决策/今日决策.md", content)
        return content

    # ------------------------------------------------------------------
    # 01-状态/池子/决策池.md
    # ------------------------------------------------------------------

    def write_decision_pool(self) -> str:
        """从 projection_candidate_pool 生成决策池页（全部 tier 汇总）。"""
        rows = self._conn.execute(
            "SELECT * FROM projection_candidate_pool ORDER BY pool_tier, score DESC"
        ).fetchall()

        now = self._now()
        lines = [
            "---",
            f"date: {self._today()}",
            "type: watchlist_decision",
            "tags: [决策池, 选股, 自动更新]",
            f"updated_at: {now}",
            "---",
            "",
            "# 决策池（结构化投影）",
            "",
            f"> 更新时间：{now}",
            "",
        ]

        # 按 tier 分组
        tiers: dict[str, list] = {}
        for r in rows:
            tiers.setdefault(r["pool_tier"], []).append(r)

        tier_labels = {"core": "核心池", "watch": "观察池"}
        for tier_key in ["core", "watch"]:
            tier_rows = tiers.get(tier_key, [])
            label = tier_labels.get(tier_key, tier_key)
            lines.extend([f"## {label}（{len(tier_rows)} 只）", ""])
            if tier_rows:
                lines.append("| # | 股票 | 代码 | 四维总分 | 加入日 | 备注 |")
                lines.append("|---|------|------|---------|--------|------|")
                for i, r in enumerate(tier_rows, 1):
                    score = r["score"] or 0
                    emoji = "✅" if score >= 7 else ("🟡" if score >= 5 else "❌")
                    lines.append(
                        f"| {i} | {r['name'] or ''} | {r['code']} "
                        f"| {emoji} **{score:.1f}** "
                        f"| {(r['added_at'] or '')[:10]} | {_candidate_note_display(r['note'])} |"
                    )
            else:
                lines.append("暂无")
            lines.append("")

        # 其他 tier
        other_tiers = {k: v for k, v in tiers.items() if k not in tier_labels}
        for tier_key, tier_rows in other_tiers.items():
            lines.extend([f"## {tier_key}（{len(tier_rows)} 只）", ""])
            lines.append("| # | 股票 | 代码 | 总分 | 备注 |")
            lines.append("|---|------|------|------|------|")
            for i, r in enumerate(tier_rows, 1):
                score = r["score"] or 0
                lines.append(
                    f"| {i} | {r['name'] or ''} | {r['code']} "
                    f"| {score:.1f} | {_candidate_note_display(r['note'])} |"
                )
            lines.append("")

        lines.append(f"---\n*更新时间：{now}*\n")
        content = "\n".join(lines) + "\n"
        self._write("01-状态/池子/决策池.md", content)
        return content

    # ------------------------------------------------------------------
    # 02-巡检/{date}.md
    # ------------------------------------------------------------------

    def write_signal_snapshot(
        self,
        run_id: str,
        market_state_detail: dict,
        market_signal: str = "",
        decision: dict | None = None,
    ) -> str:
        """从大盘信号 + 池子 + 决策生成巡检中的信号摘要。"""
        today = self._today()
        now = self._now()

        # 池子摘要
        pool_rows = self._conn.execute(
            "SELECT pool_tier, COUNT(*) as cnt FROM projection_candidate_pool GROUP BY pool_tier"
        ).fetchall()
        pool_counts = {r["pool_tier"]: r["cnt"] for r in pool_rows}
        core_count = pool_counts.get("core", 0)
        watch_count = pool_counts.get("watch", 0)
        other_count = sum(v for k, v in pool_counts.items() if k not in ("core", "watch"))

        # 指数数据
        indices = market_state_detail.get("indices", {})

        lines = [
            "---",
            f"updated_at: {now}",
            "type: signal_snapshot",
            f"date: {today}",
            "tags: [巡检, 信号, 自动更新]",
            "---",
            "",
            f"# 信号摘要 · {today}",
            "",
            "## 大盘信号",
            "",
            f"整体信号: **{market_signal}**",
            "",
            "| 指数 | 最新 | 涨跌% | 信号 |",
            "|------|------|------|------|",
        ]

        for name, data in indices.items():
            if isinstance(data, dict) and "error" not in data:
                close = data.get("close", 0)
                chg = data.get("change_pct", 0)
                sig = data.get("signal", "")
                lines.append(f"| {name} | {close:.2f} | {chg:+.2f}% | {sig} |")

        # 今日决策
        lines.extend(["", "## 今日决策", "", "| 项目 | 数值 |", "|------|------|"])
        if decision:
            lines.append(f"| 决策动作 | {decision.get('action', '—')} |")
            lines.append(f"| 市场信号 | {market_signal} |")
        else:
            lines.append("| 决策动作 | — |")
            lines.append(f"| 市场信号 | {market_signal} |")

        # 池子摘要
        lines.extend([
            "", "## 池子摘要", "",
            "| 类别 | 数量 |", "|------|------|",
            f"| 核心池 | {core_count} |",
            f"| 观察池 | {watch_count} |",
            f"| 其他 | {other_count} |",
        ])

        # 候选股 Top5
        top5 = self._conn.execute(
            "SELECT code, name, score FROM projection_candidate_pool "
            "WHERE pool_tier = 'core' ORDER BY score DESC LIMIT 5"
        ).fetchall()
        lines.extend(["", "## 候选股 Top5", ""])
        if top5:
            lines.append("| 股票 | 代码 | 评分 |")
            lines.append("|------|------|------|")
            for r in top5:
                lines.append(f"| {r['name'] or ''} | {r['code']} | {r['score'] or 0:.1f} |")
        else:
            lines.append("_暂无候选股数据_")

        blocked = self._conn.execute(
            "SELECT code, name, score, note FROM projection_candidate_pool "
            "WHERE pool_tier = 'watch' AND note LIKE '%requires_entry_strategy_route%' "
            "ORDER BY score DESC LIMIT 5"
        ).fetchall()
        if blocked:
            lines.extend(["", "## 观察池阻断", ""])
            lines.append("| 股票 | 代码 | 评分 | 原因 |")
            lines.append("|------|------|------|------|")
            for r in blocked:
                lines.append(
                    f"| {r['name'] or ''} | {r['code']} | {r['score'] or 0:.1f} "
                    f"| {_candidate_note_display(r['note'])} |"
                )

        lines.extend([
            "", "---", "",
            f"> 自动生成于 {now}，run_id: `{run_id}`",
        ])

        content = "\n".join(lines) + "\n"
        return self._upsert_daily_inspection("信号与候选池", content, run_id)

    # ------------------------------------------------------------------
    # 02-巡检/{date}.md
    # ------------------------------------------------------------------

    def write_daily_output_index(self, run_id: str) -> str:
        """从 run_log 聚合当日所有 pipeline 运行，生成每日巡检报告。"""
        today = self._today()
        now = self._now()

        start_utc, end_utc = local_date_bounds_utc(today)
        runs = self._conn.execute(
            "SELECT run_id, run_type, status, started_at, finished_at, error_message "
            "FROM run_log WHERE started_at >= ? AND started_at < ? ORDER BY started_at",
            (start_utc, end_utc),
        ).fetchall()

        # 按 run_type 聚合，取最新一次
        latest: dict[str, dict] = {}
        total_runs = 0
        for r in runs:
            total_runs += 1
            rt = r["run_type"]
            latest[rt] = dict(r)

        type_labels = {
            "morning": "盘前", "noon": "午休", "evening": "收盘",
            "scoring": "评分", "sentiment": "舆情", "weekly": "周报",
            "auto_trade": "自动交易",
        }

        lines = [
            "---",
            f"updated_at: {now}",
            "type: daily_inspection",
            f"date: {today}",
            "tags: [巡检, 自动更新]",
            "---",
            "",
            f"# 收盘巡检 - {today}",
            "",
            f"共 {total_runs} 个运行记录，{len(latest)} 个 pipeline。",
            "",
            "## Pipeline 运行状态",
            "",
            "| Pipeline | 状态 | 开始时间 | 结束时间 | 错误 |",
            "|----------|------|----------|----------|------|",
        ]

        for rt, info in latest.items():
            label = type_labels.get(rt, rt)
            status = info["status"]
            status_emoji = "✅" if status == "completed" else ("⚠️" if status == "warning" else ("🔴" if status == "failed" else "🔄"))
            started = iso_to_local(info["started_at"]).strftime("%Y-%m-%d %H:%M") if info["started_at"] else "—"
            finished = iso_to_local(info["finished_at"]).strftime("%Y-%m-%d %H:%M") if info["finished_at"] else "—"
            err = info["error_message"] or "—"
            lines.append(f"| {label} | {status_emoji} {status} | {started} | {finished} | {err} |")

        lines.extend([
            "", "## 相关文件", "",
            "- 今日决策: [[今日决策]]",
            "- 账户总览: [[账户总览]]",
            "- 核心池: [[核心池]]",
            "", "---", "",
            f"> 自动生成于 {now}",
        ])

        content = "\n".join(lines) + "\n"
        return self._upsert_daily_inspection("Pipeline 运行状态", content, run_id)

    # ------------------------------------------------------------------
    # 04-决策/候选池/候选池总览.md
    # ------------------------------------------------------------------

    def write_candidate_pool_overview(self) -> str:
        """从 projection_candidate_pool 聚合生成候选池总览。"""
        now = self._now()
        today = self._today()

        # 分类统计
        rows = self._conn.execute(
            "SELECT code, name, pool_tier, score, streak_days, note "
            "FROM projection_candidate_pool ORDER BY pool_tier, score DESC"
        ).fetchall()

        core_stable = []    # 核心池 score >= 7
        core_edge = []      # 核心池 5.5 <= score < 7
        watch_stable = []   # 观察池 score >= 5.5
        watch_edge = []     # 观察池 4.5 <= score < 5.5
        vetoed = []         # note 含 veto
        low = []            # score < 4.5

        for r in rows:
            score = r["score"] or 0
            note = r["note"] or ""
            tier = r["pool_tier"]
            entry = {"code": r["code"], "name": r["name"] or "", "score": score,
                     "tier": tier, "streak": r["streak_days"] or 0,
                     "note": _candidate_note_display(note)}

            if "veto" in note.lower():
                vetoed.append(entry)
            elif tier == "core":
                if score >= 7:
                    core_stable.append(entry)
                elif score >= 5.5:
                    core_edge.append(entry)
                else:
                    low.append(entry)
            elif tier == "watch":
                if score >= 5.5:
                    watch_stable.append(entry)
                elif score >= 4.5:
                    watch_edge.append(entry)
                else:
                    low.append(entry)
            else:
                low.append(entry)

        lines = [
            "---",
            f"updated_at: {now}",
            "type: candidate_pool_overview",
            "tags: [候选池, 自动更新]",
            "---",
            "",
            "# 候选池总览",
            "",
            f"> 统计日期: {today}",
            "",
            "## 池子摘要",
            "",
            "| 类别 | 数量 |",
            "|------|------|",
            f"| 核心池·稳定 | {len(core_stable)} |",
            f"| 核心池·买入边缘 | {len(core_edge)} |",
            f"| 观察池·稳定 | {len(watch_stable)} |",
            f"| 观察池·边缘 | {len(watch_edge)} |",
            f"| 否决池 | {len(vetoed)} |",
            f"| 低分/规避 | {len(low)} |",
            "",
            "> 买入边缘：核心池评分 5.5–6.9，距买入阈值 < 2 分",
            "> 观察边缘：观察池评分 4.5–5.4，接近但未达买入线",
        ]

        # 详细列表（只列有内容的分组）
        def _table(title: str, entries: list[dict]) -> list[str]:
            if not entries:
                return []
            out = ["", f"## {title}", "",
                   "| 股票 | 代码 | 评分 | 连续天数 | 备注 |",
                   "|------|------|------|----------|------|"]
            for e in entries:
                emoji = "✅" if e["score"] >= 7 else ("🟡" if e["score"] >= 5 else "❌")
                out.append(
                    f"| {e['name']} | {e['code']} | {emoji} {e['score']:.1f} "
                    f"| {e['streak']} | {e['note']} |"
                )
            return out

        lines.extend(_table("核心池·稳定", core_stable))
        lines.extend(_table("核心池·买入边缘", core_edge))
        lines.extend(_table("观察池·稳定", watch_stable))
        lines.extend(_table("观察池·边缘", watch_edge))
        lines.extend(_table("否决", vetoed))

        lines.extend(["", "---", "", f"> 自动生成于 {now}"])

        content = "\n".join(lines) + "\n"
        self._write("04-决策/候选池/候选池总览.md", content)
        return content

    # ------------------------------------------------------------------
    # 04-决策/候选池/最新筛选.md — 筛选结果
    # ------------------------------------------------------------------

    def write_screening_result(
        self,
        run_id: str,
        query: str,
        scores: list[dict],
        added_to_watch: list[dict] | None = None,
        buy_threshold: float = 5.5,
        watch_threshold: float = 5.0,
    ) -> str:
        """写入筛选结果（综合 + 市场扫描候选）。"""
        today = self._today()
        now = self._now()
        content, candidate_content = render_screening_result(
            today=today,
            now=now,
            run_id=run_id,
            query=query,
            scores=scores,
            added_to_watch=added_to_watch,
            buy_threshold=buy_threshold,
            watch_threshold=watch_threshold,
        )
        self._write("04-决策/候选池/最新筛选.md", content)
        if candidate_content:
            self._write("04-决策/候选池/市场扫描候选.md", candidate_content)

        return content

    # ------------------------------------------------------------------
    # 04-决策/候选池/池子调整建议.md
    # ------------------------------------------------------------------

    def write_pool_adjustment(
        self,
        run_id: str,
        demoted: list[dict],
        removed: list[dict],
        promoted: list[dict] | None = None,
    ) -> str | None:
        """写入池子调整建议（降级/移出/晋升）。"""
        promoted = promoted or []
        if not demoted and not removed and not promoted:
            return None

        today = self._today()
        now = self._now()

        lines = [
            "---",
            f"date: {today}",
            f"updated_at: {now}",
            f"run_id: {run_id}",
            "type: pool_adjustment",
            "tags: [池子调整, 自动更新]",
            "---",
            "",
            f"# 池子调整建议 — {today}",
            "",
        ]

        if promoted:
            lines.extend(["## ⬆️ 晋升", ""])
            for p in promoted:
                lines.append(f"- {p.get('name', '')}（{p.get('code', '')}）→ 核心池")
            lines.append("")

        if demoted:
            lines.extend(["## ⬇️ 降级", ""])
            for d in demoted:
                reason = d.get("reason", "")
                lines.append(f"- {d.get('name', '')}（{d.get('code', '')}）核心池 → 观察池（{reason}）")
            lines.append("")

        if removed:
            lines.extend(["## ❌ 移出", ""])
            for r in removed:
                score = r.get("score", 0)
                lines.append(f"- {r.get('name', '')}（{r.get('code', '')}）移出池子（评分 {score:.1f}）")
            lines.append("")

        lines.extend(["---", "", f"> 自动生成于 {now}"])

        content = "\n".join(lines) + "\n"
        self._write("04-决策/候选池/池子调整建议.md", content)
        return content

    # ------------------------------------------------------------------
    # 02-巡检/{date}.md — 模拟盘摘要
    # ------------------------------------------------------------------

    def write_paper_report(
        self,
        run_id: str,
        positions: list,
        balance: dict,
        buys: list[dict],
        sells: list[dict],
        market_signal: str = "",
        market_indices: dict | None = None,
        dry_run: bool = False,
    ) -> str:
        """生成模拟盘收盘报告。

        Args:
            positions: PaperPosition 列表（需有 code/name/shares/avg_cost/current_price/pnl/pnl_pct）
            balance: dict with total_asset/available_cash/market_value
            buys/sells: 交易记录列表
            market_indices: 大盘指数数据
        """
        today = self._today()
        now = self._now()
        market_indices = market_indices or {}

        total_asset = balance.get("total_asset", 0)
        available_cash = balance.get("available_cash", 0)
        market_value = balance.get("market_value", 0)
        exposure_pct = market_value / total_asset if total_asset > 0 else 0
        # 假设初始资金 20 万（模拟盘默认）
        initial_capital = 200000
        net_value = total_asset / initial_capital if initial_capital > 0 else 1
        total_return_pct = (net_value - 1) * 100

        mode = "[DRY RUN] " if dry_run else ""

        lines = [
            "---",
            f"date: {today}",
            f"updated_at: {now}",
            f"run_id: {run_id}",
            "type: paper_report",
            "tags: [模拟盘, 自动更新]",
            "---",
            "",
            f"# {mode}模拟盘收盘报告 — {today}",
            "",
            f"> 生成时间：{now}",
            "",
        ]

        # 大盘情绪
        if market_indices:
            lines.extend([
                "---", "", "## 大盘情绪", "",
                "| 指数 | 收盘 | 涨跌 |",
                "|------|------|------|",
            ])
            for name, data in market_indices.items():
                if isinstance(data, dict) and "error" not in data:
                    close = data.get("close", 0)
                    chg = data.get("change_pct", 0)
                    lines.append(f"| {name} | {close:.2f} | {chg:+.2f}% |")
            if market_signal:
                lines.append(f"\n> 整体信号：**{market_signal}**")
            lines.append("")

        # 账户概览
        lines.extend([
            "---", "", "## 账户概览", "",
            "| 项目 | 数值 |",
            "|------|------|",
            f"| 初始资金 | ¥{initial_capital:,.2f} |",
            f"| 总资产 | ¥{total_asset:,.2f} |",
            f"| 可用资金 | ¥{available_cash:,.2f} |",
            f"| 持仓市值 | ¥{market_value:,.2f} |",
            f"| 持仓占比 | {exposure_pct:.2%} |",
            f"| 账户净值 | **{net_value:.4f} ({total_return_pct:+.2f}%)** |",
            "",
        ])

        # 持仓明细
        if positions:
            lines.extend(["---", "", "## 持仓明细", ""])
            for pos in positions:
                code = pos.code if hasattr(pos, "code") else pos.get("code", "")
                name = pos.name if hasattr(pos, "name") else pos.get("name", "")
                shares = pos.shares if hasattr(pos, "shares") else pos.get("shares", 0)
                avg_cost = pos.avg_cost if hasattr(pos, "avg_cost") else pos.get("avg_cost", 0)
                current_price = pos.current_price if hasattr(pos, "current_price") else pos.get("current_price", 0)
                pnl = pos.pnl if hasattr(pos, "pnl") else pos.get("pnl", 0)
                pnl_pct = pos.pnl_pct if hasattr(pos, "pnl_pct") else pos.get("pnl_pct", 0)
                mv = pos.market_value if hasattr(pos, "market_value") else pos.get("market_value", 0)
                pos_pct = mv / total_asset if total_asset > 0 else 0

                emoji = "🟢" if pnl >= 0 else "🔴"
                lines.extend([
                    f"### {name} ({code})", "",
                    "| 项目 | 数值 |",
                    "|------|------|",
                    f"| 持仓数量 | {shares} 股 |",
                    f"| 成本价 | ¥{avg_cost:.3f} |",
                    f"| 当前价 | ¥{current_price:.3f} |",
                    f"| 持仓价值 | ¥{mv:,.2f} |",
                    f"| 总收益 | {emoji} ¥{pnl:+,.2f} ({pnl_pct:+.2f}%) |",
                    f"| 持仓占比 | {pos_pct:.2%} |",
                    "",
                ])

        # 今日交易
        if buys or sells:
            lines.extend(["---", "", "## 今日交易", ""])
            if sells:
                lines.append("### 卖出")
                for s in sells:
                    status = "✅" if s.get("status") == "filled" else ("🧪" if s.get("status") == "dry_run" else "❌")
                    lines.append(
                        f"- {status} 🔴 {s.get('name', '')}({s.get('code', '')}) "
                        f"{s.get('shares', 0)}股 | {s.get('reason', '')}"
                    )
                lines.append("")
            if buys:
                lines.append("### 买入")
                for b in buys:
                    status = "✅" if b.get("status") == "filled" else ("🧪" if b.get("status") == "dry_run" else "❌")
                    lines.append(
                        f"- {status} 🟢 {b.get('name', '')}({b.get('code', '')}) "
                        f"{b.get('shares', 0)}股 @ ¥{b.get('price', 0):.2f} | 评分 {b.get('score', 0):.1f}"
                    )
                lines.append("")
        else:
            lines.extend(["---", "", "## 今日交易", "", "无交易信号", ""])

        # 池子状态
        pool_rows = self._conn.execute(
            "SELECT pool_tier, COUNT(*) as cnt FROM projection_candidate_pool GROUP BY pool_tier"
        ).fetchall()
        if pool_rows:
            lines.extend([
                "---", "", "## 池子状态", "",
                "| 池子 | 数量 |",
                "|------|------|",
            ])
            for r in pool_rows:
                lines.append(f"| {r['pool_tier']} | {r['cnt']} |")
            lines.append("")

        lines.extend(["---", "", "> 本报告由模拟盘自动交易引擎自动生成", ""])

        content = "\n".join(lines) + "\n"
        return self._upsert_daily_inspection("模拟盘", content, run_id)

    # ------------------------------------------------------------------
    # 03-分析/历史模拟盘交易记录.md — 追加交易记录
    # ------------------------------------------------------------------

    def append_paper_trade_log(
        self,
        trades: list[dict],
    ) -> str | None:
        """追加模拟盘交易记录到历史留档。

        Args:
            trades: list of {"time", "side", "name", "code", "shares", "price", "amount", "reason"}
        """
        if not trades or not self._vault:
            return None

        path = "03-分析/历史模拟盘交易记录.md"
        full = self._vault / path

        # 如果文件不存在，创建表头
        if not full.exists():
            header = (
                "# 模拟盘交易记录\n\n"
                "| 时间 | 操作 | 股票 | 代码 | 数量 | 价格 | 金额 | 原因 |\n"
                "|------|------|------|------|------|------|------|------|\n"
            )
            self._write(path, header)

        existing = full.read_text(encoding="utf-8")
        new_rows = []
        for t in trades:
            time_str = t.get("time", self._now())
            side = t.get("side", "")
            side_label = "买入" if side == "buy" else ("卖出" if side == "sell" else side)
            name = t.get("name", "")
            code = t.get("code", "")
            shares = t.get("shares", 0)
            price = t.get("price", 0)
            amount = t.get("amount", shares * price)
            reason = t.get("reason", "")
            new_rows.append(
                f"| {time_str} | {side_label} | {name} | {code} "
                f"| {shares} | ¥{price:.2f} | ¥{amount:,.0f} | {reason} |"
            )

        content = existing.rstrip() + "\n" + "\n".join(new_rows) + "\n"
        self._write(path, content)
        return content

    # ------------------------------------------------------------------
    # 03-分析/周复盘/{week}.md
    # ------------------------------------------------------------------

    def write_weekly_review(self, week_stats: dict) -> str:
        """生成周复盘，数据部分自动填充，反思部分预留手动填写。

        Args:
            week_stats: {
                week_str, week_start, week_end,
                buy_count, sell_count, wins, losses, win_rate,
                profit_loss_ratio, net_pnl_cents, total_profit_cents, total_loss_cents,
                trades: [{date, code, name, side, price, shares, pnl_cents, note}],
                positions: [{code, name, shares, avg_cost, style}],
                core_pool: [{code, name, score}],
                pool_changes: [{code, name, change_type, reason}],
                paper_stats: {buy_count, sell_count, net_pnl_cents} | None,
            }
        """
        now = self._now()
        today = self._today()
        ws = week_stats
        week_str = ws.get("week_str", "")
        week_start = ws.get("week_start", "")
        week_end = ws.get("week_end", "")

        wins = ws.get("wins", 0)
        losses = ws.get("losses", 0)
        win_rate = ws.get("win_rate", 0)
        buy_count = ws.get("buy_count", 0)
        sell_count = ws.get("sell_count", 0)
        net_pnl = ws.get("net_pnl_cents", 0) / 100
        profit_loss_ratio = ws.get("profit_loss_ratio", 0)
        trades = ws.get("trades", [])
        positions = ws.get("positions", [])
        core_pool = ws.get("core_pool", [])
        pool_changes = ws.get("pool_changes", [])
        paper_stats = ws.get("paper_stats")

        lines = [
            "---",
            f"date: {today}",
            "type: weekly_review",
            "tags: [周复盘, 自动更新]",
            f"week: {week_str}",
            f"updated_at: {now}",
            "---",
            "",
            f"# {week_str} 周复盘（{week_start} - {week_end}）",
            "",
            "---",
            "",
            "## 本周概览（自动统计）",
            "",
            "| 项目 | 数据 |",
            "|------|------|",
            "| 周初资产 | ¥<!-- 手动填写 --> |",
            "| 周末资产 | ¥<!-- 手动填写 --> |",
            f"| 本周盈亏 | ¥{net_pnl:+,.0f} |",
            "| 收益率 | <!-- 填写资产后自动计算 --> |",
            f"| 买入次数 | {buy_count} 次 |",
            f"| 卖出次数 | {sell_count} 次 |",
            f"| 胜率 | {win_rate:.0%}（{wins}胜 {losses}负） |",
            f"| 盈亏比 | {profit_loss_ratio:.2f} |",
        ]

        if paper_stats:
            p = paper_stats
            paper_pnl = p.get("net_pnl_cents", 0) / 100
            lines.extend([
                "",
                "### 模拟盘",
                "",
                "| 项目 | 数据 |",
                "|------|------|",
                f"| 买入次数 | {p.get('buy_count', 0)} 次 |",
                f"| 卖出次数 | {p.get('sell_count', 0)} 次 |",
                f"| 净盈亏 | ¥{paper_pnl:+,.0f} |",
            ])

        # 交易明细
        lines.extend([
            "",
            "---",
            "",
            "## 本周交易明细（自动统计）",
            "",
            "| 日期 | 股票 | 操作 | 价格 | 数量 | 盈亏 | 备注 |",
            "|------|------|------|------|------|------|------|",
        ])
        if trades:
            for t in trades:
                side_label = "买入" if t.get("side") == "buy" else "卖出"
                pnl = t.get("pnl_cents", 0) / 100
                pnl_str = f"¥{pnl:+,.0f}" if pnl != 0 else "—"
                lines.append(
                    f"| {t.get('date', '')} | {t.get('name', '')}({t.get('code', '')}) "
                    f"| {side_label} | ¥{t.get('price', 0):.2f} | {t.get('shares', 0)} "
                    f"| {pnl_str} | {t.get('note', '')} |"
                )
        else:
            lines.append("| — | — | — | — | — | — | 本周无交易 |")

        # 当前持仓
        lines.extend(["", "---", "", "## 当前持仓", ""])
        if positions:
            lines.append("| 代码 | 名称 | 股数 | 成本 | 风格 |")
            lines.append("|------|------|------|------|------|")
            for p in positions:
                lines.append(
                    f"| {p.get('code', '')} | {p.get('name', '')} "
                    f"| {p.get('shares', 0)} | ¥{p.get('avg_cost', 0):.2f} | {p.get('style', '')} |"
                )
        else:
            lines.append("空仓")

        # 核心池变化
        lines.extend(["", "---", "", "## 核心池变化（自动统计）", ""])
        if pool_changes:
            for pc in pool_changes:
                ct = pc.get("change_type", "")
                emoji = "⬆️" if ct == "promoted" else ("⬇️" if ct == "demoted" else "❌")
                lines.append(f"- {emoji} {pc.get('name', '')}({pc.get('code', '')}) {pc.get('reason', '')}")
        else:
            lines.append("本周无池子变动")

        # ── 手动填写区 ──
        lines.extend([
            "",
            "---",
            "",
            "## 规则执行检查",
            "",
            "- [ ] 是否遵守了所有买入规则？",
            "- [ ] 是否遵守了止损纪律？",
            "- [ ] 加仓操作是否合理？",
            "- [ ] 是否触发过冷却机制？",
            "- [ ] 是否有情绪化交易？",
            "",
            "**违反规则记录：**",
            "",
            "<!-- 手动填写：记录本周违反规则的操作 -->",
            "",
            "---",
            "",
            "## 下周计划",
            "",
            "### 核心池",
            "",
        ])
        if core_pool:
            lines.append("| 股票 | 代码 | 评分 | 计划操作 | 技术面状态 |")
            lines.append("|------|------|------|----------|-----------|")
            for c in core_pool:
                score = c.get("score", 0)
                emoji = "✅" if score >= 7 else ("🟡" if score >= 5 else "❌")
                lines.append(
                    f"| {c.get('name', '')} | {c.get('code', '')} "
                    f"| {emoji} {score:.1f} | <!-- 填写 --> | <!-- 填写 --> |"
                )
        else:
            lines.append("核心池为空")

        lines.extend([
            "",
            "### 下周买入计划（最多2次）",
            "",
            "1. <!-- 手动填写 -->",
            "2. <!-- 手动填写 -->",
            "",
            "---",
            "",
            "## 下周改进点（只写1条）",
            "",
            "> <!-- 手动填写：本周最大的一个教训或改进方向 -->",
            "",
        ])

        content = "\n".join(lines) + "\n"
        self._write(f"03-分析/周复盘/{week_str}.md", content)
        return content

    # ------------------------------------------------------------------
    # 03-分析/月复盘/{month}.md
    # ------------------------------------------------------------------

    def write_monthly_review(self, month_stats: dict) -> str:
        """生成月复盘，数据部分自动填充，反思部分预留手动填写。

        Args:
            month_stats: {
                month_str (e.g. "2026-04"),
                trading_days,
                buy_count, sell_count, wins, losses, win_rate,
                profit_loss_ratio, net_pnl_cents,
                total_profit_cents, total_loss_cents,
                max_drawdown_cents,
                avg_profit_cents, avg_loss_cents,
                weekly_summaries: [{week, pnl_cents, buy_count, sell_count, wins, losses}],
                worst_trades: [{code, name, pnl_cents, date}],
                pool_changes: [{code, name, change_type, reason, date}],
                paper_stats: {buy_count, sell_count, net_pnl_cents} | None,
                risk_params: {stop_loss, trailing_stop, time_stop_days, weekly_max, total_max, single_max},
            }
        """
        now = self._now()
        today = self._today()
        ms = month_stats
        month_str = ms.get("month_str", "")

        buy_count = ms.get("buy_count", 0)
        sell_count = ms.get("sell_count", 0)
        wins = ms.get("wins", 0)
        losses = ms.get("losses", 0)
        win_rate = ms.get("win_rate", 0)
        plr = ms.get("profit_loss_ratio", 0)
        net_pnl = ms.get("net_pnl_cents", 0) / 100
        max_dd = ms.get("max_drawdown_cents", 0) / 100
        avg_profit = ms.get("avg_profit_cents", 0) / 100
        avg_loss = ms.get("avg_loss_cents", 0) / 100
        trading_days = ms.get("trading_days", 0)
        weekly_summaries = ms.get("weekly_summaries", [])
        worst_trades = ms.get("worst_trades", [])
        pool_changes = ms.get("pool_changes", [])
        paper = ms.get("paper_stats")
        risk_params = ms.get("risk_params", {})

        # 年月标题
        try:
            from datetime import datetime as dt
            d = dt.strptime(month_str, "%Y-%m")
            title = f"{d.year}年{d.month}月 月度复盘"
        except Exception:
            title = f"{month_str} 月度复盘"

        lines = [
            "---",
            f"date: {today}",
            "type: monthly_review",
            "tags: [月复盘, 自动更新]",
            f"month: {month_str}",
            f"updated_at: {now}",
            "---",
            "",
            f"# {title}",
            "",
        ]

        # 月度概览
        lines.extend([
            "## 月度概览",
            "",
            "| 项目 | 实盘 |",
            "|------|------|",
            f"| 交易日数 | {trading_days} |",
            f"| 买入次数 | {buy_count} |",
            f"| 卖出次数 | {sell_count} |",
            f"| 已实现盈亏 | ¥{net_pnl:+,.0f} |",
            f"| 胜率 | {win_rate:.0%}（{wins}胜{losses}负） |",
            f"| 盈亏比 | {plr:.2f} |",
            f"| 最大回撤 | ¥{max_dd:,.0f} |",
            f"| 平均盈利 | ¥{avg_profit:+,.0f} |",
            f"| 平均亏损 | ¥{avg_loss:+,.0f} |",
        ])

        if paper:
            p_pnl = paper.get("net_pnl_cents", 0) / 100
            lines.extend([
                "",
                "### 模拟盘",
                "",
                "| 项目 | 数据 |",
                "|------|------|",
                f"| 买入次数 | {paper.get('buy_count', 0)} |",
                f"| 卖出次数 | {paper.get('sell_count', 0)} |",
                f"| 净盈亏 | ¥{p_pnl:+,.0f} |",
            ])

        # 周度汇总
        lines.extend(["", "## 周度汇总", ""])
        if weekly_summaries:
            lines.append("| 周次 | 盈亏 | 买入 | 卖出 | 胜/负 |")
            lines.append("|------|------|------|------|-------|")
            for w in weekly_summaries:
                wpnl = w.get("pnl_cents", 0) / 100
                lines.append(
                    f"| {w.get('week', '')} | ¥{wpnl:+,.0f} "
                    f"| {w.get('buy_count', 0)} | {w.get('sell_count', 0)} "
                    f"| {w.get('wins', 0)}/{w.get('losses', 0)} |"
                )
        else:
            lines.append("本月无交易记录")

        # 亏损最大的 3 笔
        lines.extend(["", "## 亏损最大的 3 笔交易", ""])
        if worst_trades:
            lines.append("| 日期 | 股票 | 代码 | 亏损 |")
            lines.append("|------|------|------|------|")
            for t in worst_trades[:3]:
                loss = t.get("pnl_cents", 0) / 100
                lines.append(
                    f"| {t.get('date', '')} | {t.get('name', '')} "
                    f"| {t.get('code', '')} | ¥{loss:+,.0f} |"
                )
        else:
            lines.append("本月无亏损交易。")

        # 模拟盘 vs 实盘对比
        lines.extend([
            "",
            "## 模拟盘 vs 实盘对比",
            "",
        ])
        if paper:
            paper_buys = paper.get("buy_count", 0)
            deviation = paper_buys - buy_count
            lines.extend([
                f"- 模拟盘发出 **{paper_buys}** 个买入信号",
                f"- 实盘执行了 **{buy_count}** 个买入",
                f"- 偏离：**{abs(deviation)}** 个信号{'未执行（实盘更保守）' if deviation > 0 else '多执行' if deviation < 0 else '完全一致'}",
            ])
        else:
            lines.append("本月无模拟盘数据")

        # 核心池月度变化
        lines.extend(["", "## 核心池月度变化", ""])
        if pool_changes:
            for pc in pool_changes:
                ct = pc.get("change_type", "")
                emoji = "⬆️" if ct == "promoted" else ("⬇️" if ct == "demoted" else "❌")
                lines.append(
                    f"- {emoji} {pc.get('date', '')} {pc.get('name', '')}({pc.get('code', '')}) {pc.get('reason', '')}"
                )
        else:
            lines.append("本月无核心池变动。")

        # 系统参数检查
        rp = risk_params
        lines.extend([
            "",
            "## 系统参数检查",
            "",
            "| 参数 | 当前值 | 是否需要调整 | 备注 |",
            "|------|--------|-------------|------|",
            f"| 止损线 | {rp.get('stop_loss', '8%')} | <!-- 填写 --> | |",
            f"| 移动止盈 | {rp.get('trailing_stop', '10%')} | <!-- 填写 --> | |",
            f"| 时间止损 | {rp.get('time_stop_days', '15')}天 | <!-- 填写 --> | |",
            f"| 每周买入上限 | {rp.get('weekly_max', 2)}次 | <!-- 填写 --> | |",
            f"| 总仓位上限 | {rp.get('total_max', '60%')} | <!-- 填写 --> | |",
            f"| 单票上限 | {rp.get('single_max', '20%')} | <!-- 填写 --> | |",
            "",
            "> 参数调整需基于至少 20 笔闭合交易的 MFE/MAE 分布，当前样本量不足时保持默认值。",
        ])

        # ── 手动填写区 ──
        lines.extend([
            "",
            "---",
            "",
            "## 本月反思",
            "",
            "### 做得好的地方",
            "",
            "<!-- 手动填写：本月执行最好的 1-2 个方面 -->",
            "",
            "### 做得不好的地方",
            "",
            "<!-- 手动填写：本月最大的 1-2 个问题 -->",
            "",
            "### 系统改进建议",
            "",
            "<!-- 手动填写：对交易系统/策略参数的改进想法 -->",
            "",
            "---",
            "",
            "## 下月计划",
            "",
            "> <!-- 手动填写：下月的核心目标和操作方向 -->",
            "",
        ])

        content = "\n".join(lines) + "\n"
        self._write(f"03-分析/月复盘/{month_str}.md", content)
        return content

    # ------------------------------------------------------------------
    # 全量刷新
    # ------------------------------------------------------------------

    def refresh_all(self, capital: float = 0, cash: float = 0) -> dict:
        """刷新所有 vault 投影页。"""
        results = {}
        results["portfolio"] = self.write_portfolio_status()
        results["core_pool"] = self.write_core_pool()
        results["watch_pool"] = self.write_watch_pool()
        results["decision_pool"] = self.write_decision_pool()
        results["account"] = self.write_account_overview(capital, cash)
        results["candidate_overview"] = self.write_candidate_pool_overview()
        return results
