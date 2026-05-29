"""
reporting/reports.py — 报告生成

所有报告从 event_log + projection 表消费数据。
不 import 任何业务 service。
"""

from __future__ import annotations

import uuid
from typing import Any

from astock_trading.execution.reconciliation import TradeReconciliationService
from astock_trading.platform.events import EventStore
from astock_trading.platform.time import local_date_bounds_utc, local_now_str
from astock_trading.platform.time import local_today, utc_now_iso


def _now_iso() -> str:
    return utc_now_iso()


def _position_cost_basis_cents(row: Any) -> int:
    if "cost_basis_cents" in row.keys() and row["cost_basis_cents"]:
        return row["cost_basis_cents"]
    return row["avg_cost_cents"] * row["shares"]


def _position_cost_price(row: Any) -> float:
    shares = row["shares"] or 0
    if shares <= 0:
        return row["avg_cost_cents"] / 100
    return _position_cost_basis_cents(row) / shares / 100


class ReportGenerator:
    """报告生成器 — 只读消费事实和投影。"""

    def __init__(self, event_store: EventStore, conn: Any):
        self._events = event_store
        self._conn = conn

    def generate_scoring_report(self, run_id: str) -> str:
        """评分报告：从 score.calculated 事件生成。"""
        events = self._events.query(event_type="score.calculated")
        # 过滤当前 run
        scores = [e for e in events if e.get("metadata", {}).get("run_id") == run_id]

        if not scores:
            return f"# 评分报告\n\n> run_id: {run_id}\n\n无评分数据。\n"

        lines = ["# 评分报告", "", f"> run_id: {run_id}", f"> 时间: {local_now_str('%Y-%m-%d %H:%M:%S')}", ""]
        lines.append("| 代码 | 名称 | 总分 | 技术 | 基本面 | 资金 | 舆情 | 风格 | 否决 |")
        lines.append("|------|------|------|------|--------|------|------|------|------|")

        for ev in sorted(scores, key=lambda e: e["payload"].get("total_score", 0), reverse=True):
            p = ev["payload"]
            veto = "❌" if p.get("veto_triggered") else ""
            lines.append(
                f"| {p.get('code', '')} | {p.get('name', '')} "
                f"| {p.get('total_score', 0):.1f} "
                f"| {p.get('technical_score', 0):.1f} "
                f"| {p.get('fundamental_score', 0):.1f} "
                f"| {p.get('flow_score', 0):.1f} "
                f"| {p.get('sentiment_score', 0):.1f} "
                f"| {p.get('style', '')} "
                f"| {veto} |"
            )

        report = "\n".join(lines) + "\n"
        self._save_artifact(run_id, "scoring", "markdown", report)
        return report

    def generate_portfolio_report(self) -> str:
        """持仓报告：从 projection_positions 生成。"""
        rows = self._conn.execute(
            "SELECT * FROM projection_positions ORDER BY entry_date"
        ).fetchall()

        lines = ["# 持仓报告", "", f"> 时间: {local_now_str('%Y-%m-%d %H:%M:%S')}", ""]

        if not rows:
            lines.append("当前无持仓。")
            return "\n".join(lines) + "\n"

        lines.append("| 代码 | 名称 | 风格 | 股数 | 成本 | 现价 | 盈亏 | 入场日 |")
        lines.append("|------|------|------|------|------|------|------|--------|")

        for r in rows:
            cost = _position_cost_price(r)
            price = (r["current_price_cents"] or 0) / 100
            pnl = ((r["current_price_cents"] or 0) * r["shares"] - _position_cost_basis_cents(r)) / 100
            lines.append(
                f"| {r['code']} | {r['name']} | {r['style']} "
                f"| {r['shares']} | {cost:.3f} | {price:.2f} "
                f"| {pnl:+.0f} | {r['entry_date']} |"
            )

        return "\n".join(lines) + "\n"

    def generate_trade_history(self, days: int = 7) -> str:
        """交易记录：从 order.filled 事件生成。"""
        events = self._events.query(event_type="order.filled")

        lines = ["# 交易记录", "", f"> 最近 {days} 天", ""]
        lines.append("| 代码 | 方向 | 股数 | 成交价 | 时间 |")
        lines.append("|------|------|------|--------|------|")

        for ev in events[-50:]:  # 最近 50 条
            p = ev["payload"]
            price = p.get("fill_price_cents", 0) / 100
            lines.append(
                f"| {p.get('code', '')} | {p.get('side', '')} "
                f"| {p.get('shares', 0)} | {price:.2f} "
                f"| {ev.get('occurred_at', '')[:16]} |"
            )

        return "\n".join(lines) + "\n"

    def _save_artifact(
        self, run_id: str, report_type: str, fmt: str, content: str,
        delivered_to: str = "",
    ) -> str:
        """写入 report_artifacts 表。"""
        artifact_id = uuid.uuid4().hex[:16]
        self._conn.execute(
            """INSERT INTO report_artifacts
               (artifact_id, run_id, report_type, format, content, delivered_to, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (artifact_id, run_id, report_type, fmt, content, delivered_to, _now_iso()),
        )
        return artifact_id

    # ------------------------------------------------------------------
    # 盘前 / 收盘 / 周报
    # ------------------------------------------------------------------

    def generate_morning_report(self, run_id: str) -> str:
        """盘前摘要：从 projection 表 + 最近事件生成。"""
        lines = ["# 盘前摘要", "", f"> run_id: {run_id}", f"> 时间: {local_now_str('%Y-%m-%d %H:%M:%S')}", ""]

        # 大盘状态
        market_rows = self._conn.execute(
            "SELECT * FROM projection_market_state ORDER BY index_symbol"
        ).fetchall()
        if market_rows:
            lines.append("## 大盘信号")
            lines.append("")
            lines.append("| 指数 | 信号 | 涨跌 |")
            lines.append("|------|------|------|")
            for r in market_rows:
                chg = f"{r['change_pct']:+.2f}%" if r["change_pct"] else "—"
                lines.append(f"| {r['name']} | {r['signal'] or '—'} | {chg} |")
            lines.append("")

        # 持仓
        lines.append("## 当前持仓")
        lines.append("")
        pos_report = self.generate_portfolio_report()
        # 去掉标题行
        for line in pos_report.split("\n"):
            if line.startswith("#") or line.startswith(">"):
                continue
            lines.append(line)

        report = "\n".join(lines) + "\n"
        self._save_artifact(run_id, "morning", "markdown", report)
        return report

    def generate_evening_report(self, run_id: str) -> str:
        """收盘报告：从当日事件 + 投影生成。"""
        lines = ["# 收盘报告", "", f"> run_id: {run_id}", f"> 时间: {local_now_str('%Y-%m-%d %H:%M:%S')}", ""]

        # 持仓
        lines.append("## 持仓状态")
        lines.append("")
        pos_report = self.generate_portfolio_report()
        for line in pos_report.split("\n"):
            if line.startswith("#") or line.startswith(">"):
                continue
            lines.append(line)

        # 今日交易
        filled = self._events.query(event_type="order.filled")
        start_utc, end_utc = local_date_bounds_utc()
        today_fills = [
            e for e in filled
            if start_utc <= e.get("occurred_at", "") < end_utc
        ]
        if today_fills:
            lines.append("## 今日成交")
            lines.append("")
            lines.append("| 代码 | 方向 | 股数 | 成交价 |")
            lines.append("|------|------|------|--------|")
            for ev in today_fills:
                p = ev["payload"]
                price = p.get("fill_price_cents", 0) / 100
                lines.append(f"| {p.get('code', '')} | {p.get('side', '')} | {p.get('shares', 0)} | {price:.2f} |")
            lines.append("")

        self._append_shadow_reconciliation(lines)

        # 风控事件
        risk_events = self._events.query(stream_type="risk")
        today_risks = [
            e for e in risk_events
            if start_utc <= e.get("occurred_at", "") < end_utc
        ]
        if today_risks:
            lines.append("## 风控事件")
            lines.append("")
            for ev in today_risks:
                lines.append(f"- [{ev['event_type']}] {ev['payload'].get('description', ev['payload'].get('code', ''))}")
            lines.append("")

        report = "\n".join(lines) + "\n"
        self._save_artifact(run_id, "evening", "markdown", report)
        return report

    def _append_shadow_reconciliation(self, lines: list[str]) -> None:
        reconciliation = TradeReconciliationService(self._events).reconcile(date=local_today().isoformat())
        summary = reconciliation.get("summary", {})
        paper_count = int(summary.get("paper_trades") or 0)
        real_count = int(summary.get("real_trades") or 0)
        deviation_count = int(summary.get("deviation_count") or 0)
        if paper_count == 0 and real_count == 0 and deviation_count == 0:
            return

        lines.append("## 模拟盘 vs 实盘对账")
        lines.append("")
        lines.append(
            f"- 模拟盘 {paper_count} / 实盘 {real_count} / "
            f"匹配 {summary.get('matched', 0)} / 偏离 {deviation_count}"
        )
        deviation_types = summary.get("deviation_types") or {}
        if deviation_types:
            labels = [
                f"{_deviation_type_label(kind)} {count}"
                for kind, count in deviation_types.items()
            ]
            lines.append(f"- 偏离类型：{'，'.join(labels)}")

        items = [
            item for item in reconciliation.get("items", [])
            if item.get("deviation_type") != "matched"
        ][:5]
        if items:
            lines.append("")
            lines.append("| 类型 | 代码 | 信号 | 说明 |")
            lines.append("|------|------|------|------|")
            for item in items:
                join_key = item.get("join_key") or {}
                details = item.get("details") or {}
                lines.append(
                    f"| {_deviation_type_label(item.get('deviation_type', ''))} "
                    f"| {join_key.get('code', '')} "
                    f"| {join_key.get('signal_id', '') or '—'} "
                    f"| {details.get('message', '') or _deviation_type_label(item.get('deviation_type', ''))} |"
                )
        lines.append("")

    def generate_weekly_report(self, week: str = "") -> str:
        """周报：从本周事件汇总生成。"""
        if not week:
            week = local_today().strftime("%Y-W%W")

        lines = ["# 周报", "", f"> {week}", f"> 生成时间: {local_now_str('%Y-%m-%d %H:%M:%S')}", ""]

        # 本周交易
        filled = self._events.query(event_type="order.filled")
        lines.append("## 本周交易")
        lines.append("")
        if filled:
            lines.append("| 代码 | 方向 | 股数 | 成交价 | 时间 |")
            lines.append("|------|------|------|--------|------|")
            for ev in filled[-20:]:
                p = ev["payload"]
                price = p.get("fill_price_cents", 0) / 100
                lines.append(
                    f"| {p.get('code', '')} | {p.get('side', '')} "
                    f"| {p.get('shares', 0)} | {price:.2f} "
                    f"| {ev.get('occurred_at', '')[:10]} |"
                )
        else:
            lines.append("本周无交易。")
        lines.append("")

        # 当前持仓
        lines.append("## 当前持仓")
        lines.append("")
        pos_report = self.generate_portfolio_report()
        for line in pos_report.split("\n"):
            if line.startswith("#") or line.startswith(">"):
                continue
            lines.append(line)

        report = "\n".join(lines) + "\n"
        self._save_artifact("weekly", "weekly", "markdown", report)
        return report


def _deviation_type_label(kind: str) -> str:
    return {
        "not_executed": "未执行",
        "extra_real_trade": "实盘额外交易",
        "partial_fill": "部分成交",
        "price_slippage": "价格偏离",
        "manual_override": "人工覆盖",
        "matched": "一致",
    }.get(kind, kind or "未知")
