"""Pure markdown rendering for screening results."""

from __future__ import annotations


def render_screening_result(
    *,
    today: str,
    now: str,
    run_id: str,
    query: str,
    scores: list[dict],
    added_to_watch: list[dict] | None = None,
    buy_threshold: float = 5.5,
    watch_threshold: float = 5.0,
) -> tuple[str, str | None]:
    """Render screening result markdown and optional market-scan candidate markdown."""
    added_to_watch = added_to_watch or []

    lines = [
        "---",
        f"date: {today}",
        f"updated_at: {now}",
        f"run_id: {run_id}",
        "type: screening_result",
        "tags: [筛选结果, 自动更新]",
        "---",
        "",
        f"# 筛选结果 — {today}",
        "",
        f"筛选条件：{query}",
        f"命中 {len(scores)} 只",
        "",
        "| # | 名称 | 代码 | 总分 | 技术 | 基本面 | 资金 | 舆情 | 风格 | 路线 | 状态 |",
        "|---|------|------|------|------|--------|------|------|------|------|------|",
    ]

    for i, s in enumerate(scores, 1):
        total = float(s.get("total_score", s.get("total", 0)) or 0)
        veto = s.get("veto_triggered", False)
        if veto:
            status = "🚫否决"
        elif total >= buy_threshold:
            status = "✅可买"
        elif total >= watch_threshold:
            status = "🟡观察"
        else:
            status = "❌规避"
        lines.append(
            f"| {i} | {s.get('name', '')} | {s.get('code', '')} "
            f"| **{total:.1f}** "
            f"| {float(s.get('technical_score', 0) or 0):.1f} "
            f"| {float(s.get('fundamental_score', 0) or 0):.1f} "
            f"| {float(s.get('flow_score', 0) or 0):.1f} "
            f"| {float(s.get('sentiment_score', 0) or 0):.1f} "
            f"| {s.get('style', '')} | {_route_labels(s)} | {status} |"
        )

    if added_to_watch:
        lines.extend(["", "## 新增观察池", ""])
        for a in added_to_watch:
            lines.append(
                f"- {a.get('name', '')}（{a.get('code', '')}）"
                f"评分 {a.get('score', 0):.1f}"
            )

    lines.extend(["", "---", "", f"> 自动生成于 {now}"])

    content = "\n".join(lines) + "\n"

    candidates = [
        s
        for s in scores
        if float(s.get("total_score", 0) or 0) >= watch_threshold
        and not s.get("veto_triggered")
    ]
    if not candidates:
        return content, None

    cand_lines = [
        "---",
        f"date: {today}",
        f"updated_at: {now}",
        "type: market_scan_candidate",
        "tags: [市场扫描, 候选, 自动更新]",
        "---",
        "",
        f"# 市场扫描候选 — {today}",
        "",
        "| # | 名称 | 代码 | 总分 | 路线 | 建议 |",
        "|---|------|------|------|------|------|",
    ]
    for i, s in enumerate(candidates, 1):
        total = float(s.get("total_score", 0) or 0)
        suggestion = "可买入" if total >= buy_threshold else "观察"
        cand_lines.append(
            f"| {i} | {s.get('name', '')} | {s.get('code', '')} "
            f"| {total:.1f} | {_route_labels(s)} | {suggestion} |"
        )
    cand_lines.extend(["", "---", "", f"> 自动生成于 {now}"])
    return content, "\n".join(cand_lines) + "\n"


def _route_labels(score: dict, limit: int = 2) -> str:
    routes = score.get("strategy_routes") or []
    labels = []
    for route in routes[:limit]:
        if not isinstance(route, dict):
            continue
        label = route.get("display_name") or route.get("route")
        if label:
            labels.append(str(label))
    return ",".join(labels)
