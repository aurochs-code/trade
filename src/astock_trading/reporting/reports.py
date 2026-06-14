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

        self._append_candidate_action_preview(lines)

        report = "\n".join(lines) + "\n"
        self._save_artifact(run_id, "morning", "markdown", report)
        return report

    def _append_candidate_action_preview(self, lines: list[str]) -> None:
        rows = self._conn.execute(
            """SELECT pool_tier, COUNT(*) AS count
               FROM projection_candidate_pool
               WHERE pool_tier IN ('core', 'watch', 'radar')
               GROUP BY pool_tier"""
        ).fetchall()
        counts = {"core": 0, "watch": 0, "radar": 0}
        for row in rows:
            counts[str(row["pool_tier"] or "")] = int(row["count"] or 0)
        candidates = self._conn.execute(
            """SELECT code, pool_tier, name, score, last_scored_at, note
               FROM projection_candidate_pool
               WHERE pool_tier IN ('core', 'watch', 'radar')
               ORDER BY CASE pool_tier WHEN 'core' THEN 0 WHEN 'watch' THEN 1 ELSE 2 END,
                        score DESC,
                        last_scored_at DESC,
                        code
               LIMIT 5"""
        ).fetchall()

        lines.append("## 今日操作预览")
        lines.append("")
        lines.append(
            f"- 候选池：核心 {counts['core']} / 观察 {counts['watch']} / 强势观察 {counts['radar']}"
        )
        if counts["core"] > 0:
            lines.append("- 下一步：`atrade paper auto-readiness --json`，只读确认模拟承接阻断项。")
        elif counts["watch"] or counts["radar"]:
            lines.append("- 下一步：`atrade opportunity --json`，复核观察候选，不降低买入门槛。")
        else:
            lines.append("- 下一步：`atrade screener refresh --json`，先刷新候选和评分证据。")
        if candidates:
            lines.append("")
            lines.append("| 层级 | 代码 | 名称 | 分数 | 复核 |")
            lines.append("|------|------|------|------|------|")
            for row in candidates:
                code = str(row["code"] or "")
                name = row["name"] or code
                tier = _pool_tier_label(row["pool_tier"])
                score = float(row["score"] or 0.0)
                lines.append(
                    f"| {tier} | {code} | {name} | {score:.1f} | "
                    f"`atrade stock analyze {code} --json` |"
                )
        lines.append("")

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

        self._append_today_trade_attribution(lines, start_utc=start_utc, end_utc=end_utc)
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

    def _append_today_trade_attribution(
        self,
        lines: list[str],
        *,
        start_utc: str,
        end_utc: str,
    ) -> None:
        decisions = self._events.query(
            event_type="decision.suggested",
            since=start_utc,
            until=end_utc,
            limit=200,
        )
        actionable = [
            event for event in decisions
            if str((event.get("payload") or {}).get("action") or "") in {"BUY", "TRIAL_BUY", "WATCH", "SELL"}
        ]
        if not actionable:
            return

        lines.append("## 今日交易归因")
        lines.append("")
        lines.append("| 代码 | 名称 | 判断 | 路线 | 分数 | 入场信号 | 复核 |")
        lines.append("|------|------|------|------|------|----------|------|")
        for event in actionable[-10:]:
            payload = event.get("payload") or {}
            code = str(payload.get("code") or "")
            name = payload.get("name") or code
            score = float(payload.get("score") or payload.get("confidence") or 0.0)
            entry_signal = "有" if _truthy(payload.get("entry_signal")) else "无"
            lines.append(
                f"| {code} | {name} | {_action_label(payload.get('action'))} "
                f"| {_route_label(payload)} | {score:.1f} | {entry_signal} "
                f"| `atrade stock analyze {code} --json` |"
            )
        lines.append("")

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

        self._append_weekly_route_attribution(lines)

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

    def _append_weekly_route_attribution(self, lines: list[str]) -> None:
        reviews = self._events.query(event_type="trade.review.recorded", limit=1000)
        hypotheses = {
            event["event_id"]: event
            for event in self._events.query(event_type="trade.hypothesis.recorded", limit=1000)
        }
        scores = {
            event["event_id"]: event
            for event in self._events.query(event_type="score.calculated", limit=1000)
        }

        returns_by_route: dict[str, list[float]] = {}
        for review in reviews:
            payload = review.get("payload") or {}
            hypothesis = hypotheses.get(str(payload.get("source_hypothesis_event_id") or "")) or {}
            score = scores.get(str((hypothesis.get("payload") or {}).get("source_score_event_id") or "")) or {}
            route = _route_label(score.get("payload") or {})
            if route == "—":
                route = str(((hypothesis.get("payload") or {}).get("hypothesis") or {}).get("entry_signal_type") or "未知路线")
            returns_by_route.setdefault(route, []).append(_return_pct(payload.get("latest_return_pct")))

        if returns_by_route:
            lines.append("## 路线收益归因")
            lines.append("")
            for route, values in sorted(
                returns_by_route.items(),
                key=lambda item: (len(item[1]), sum(item[1]) / len(item[1])),
                reverse=True,
            ):
                avg_return = sum(values) / len(values)
                win_rate = sum(1 for value in values if value > 0) / len(values)
                lines.append(
                    f"- {route}：样本 {len(values)}，平均收益 {_pct_text(avg_return)}，胜率 {win_rate:.0%}"
                )
            lines.append("")
            return

        decisions = self._events.query(event_type="decision.suggested", limit=1000)
        signal_counts: dict[str, dict[str, Any]] = {}
        for event in decisions:
            payload = event.get("payload") or {}
            route = _route_label(payload)
            bucket = signal_counts.setdefault(route, {"count": 0, "buy": 0, "trial": 0, "watch": 0, "scores": []})
            action = str(payload.get("action") or "")
            bucket["count"] += 1
            bucket["scores"].append(float(payload.get("score") or payload.get("confidence") or 0.0))
            if action == "BUY":
                bucket["buy"] += 1
            elif action == "TRIAL_BUY":
                bucket["trial"] += 1
            elif action == "WATCH":
                bucket["watch"] += 1
        if not signal_counts:
            return

        lines.append("## 路线信号归因")
        lines.append("")
        for route, stats in sorted(signal_counts.items(), key=lambda item: item[1]["count"], reverse=True)[:8]:
            scores = stats["scores"]
            avg_score = sum(scores) / len(scores) if scores else 0.0
            lines.append(
                f"- {route}：信号 {stats['count']}，买入意向 {stats['buy']}，"
                f"试买意向 {stats['trial']}，观察 {stats['watch']}，平均分 {avg_score:.1f}"
            )
        lines.append("")


def _deviation_type_label(kind: str) -> str:
    return {
        "not_executed": "未执行",
        "extra_real_trade": "实盘额外交易",
        "partial_fill": "部分成交",
        "price_slippage": "价格偏离",
        "manual_override": "人工覆盖",
        "matched": "一致",
    }.get(kind, kind or "未知")


def _pool_tier_label(value: Any) -> str:
    return {
        "core": "核心",
        "watch": "观察",
        "radar": "强势观察",
    }.get(str(value or ""), str(value or "未知"))


def _action_label(value: Any) -> str:
    return {
        "BUY": "买入意向",
        "TRIAL_BUY": "试买意向",
        "SELL": "卖出意向",
        "WATCH": "观察",
        "NO_TRADE": "不操作",
    }.get(str(value or ""), str(value or "未知"))


def _route_label(payload: dict[str, Any]) -> str:
    direct = payload.get("primary_strategy_route_label")
    if direct:
        return str(direct)
    route = payload.get("primary_strategy_route")
    for item in payload.get("strategy_routes") or []:
        if route and item.get("route") != route:
            continue
        label = item.get("display_name")
        if label:
            return str(label)
    return str(route or "—")


def _return_pct(value: Any) -> float:
    try:
        number = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    if abs(number) <= 1:
        return number * 100
    return number


def _pct_text(value: float) -> str:
    return f"{value:+.2f}%"


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)
