"""MX 自选股同步计划。

只负责把“最新候选池 + 当前自选 + 持仓保护”合成为可审计计划；
实际调用 MX API 的动作由 CLI 层显式执行。
"""

from __future__ import annotations

from typing import Any


TIER_LABELS = {
    "core": "核心",
    "watch": "观察",
    "radar": "强势观察",
}

TIER_ORDER = {"core": 0, "watch": 1, "radar": 2}
DEFAULT_TIERS = ("core", "watch", "radar")


def load_candidate_pool_items(conn: Any, *, include_tiers: tuple[str, ...] = DEFAULT_TIERS) -> list[dict[str, Any]]:
    """读取当前候选池，按核心、观察、强势观察排序。"""
    rows = conn.execute(
        """SELECT code, pool_tier, name, score, added_at, last_scored_at, streak_days, note
           FROM projection_candidate_pool
           ORDER BY
             CASE pool_tier WHEN 'core' THEN 0 WHEN 'watch' THEN 1 WHEN 'radar' THEN 2 ELSE 9 END,
             score DESC,
             code"""
    ).fetchall()
    allowed = set(include_tiers)
    items = []
    for row in rows:
        item = dict(row)
        tier = str(item.get("pool_tier") or "")
        if tier not in allowed:
            continue
        item["pool_tier_label"] = TIER_LABELS.get(tier, tier or "未分层")
        items.append(item)
    return items


def load_local_position_items(conn: Any) -> list[dict[str, Any]]:
    """读取本地投影持仓，用于保护手动记录的真实持仓。"""
    rows = conn.execute(
        """SELECT code, name, shares
           FROM projection_positions
           WHERE shares > 0
           ORDER BY updated_at DESC, code"""
    ).fetchall()
    return [_stock_item(dict(row), source="local_projection") for row in rows if _stock_code(dict(row))]


def watchlist_items_from_mx_result(result: dict[str, Any]) -> list[dict[str, Any]]:
    """把 MX 自选接口响应转成统一股票列表。"""
    data = result.get("data", {}) if isinstance(result, dict) else {}
    all_results = data.get("allResults", {}) if isinstance(data, dict) else {}
    result_data = all_results.get("result", {}) if isinstance(all_results, dict) else {}
    data_list = result_data.get("dataList", []) if isinstance(result_data, dict) else []
    items = []
    for raw in data_list:
        if not isinstance(raw, dict):
            continue
        items.append(_stock_item({
            "code": raw.get("SECURITY_CODE", ""),
            "name": raw.get("SECURITY_SHORT_NAME", ""),
            "price": raw.get("NEWEST_PRICE"),
            "change_pct": raw.get("CHG"),
        }, source="mx_watchlist"))
    return [item for item in items if item["code"]]


def mx_position_items(positions: list[Any]) -> list[dict[str, Any]]:
    """把 PaperPosition 或 dict 持仓转成统一股票列表。"""
    items = []
    for pos in positions:
        if isinstance(pos, dict):
            raw = pos
        else:
            raw = {
                "code": getattr(pos, "code", ""),
                "name": getattr(pos, "name", ""),
                "shares": getattr(pos, "shares", 0),
            }
        item = _stock_item(raw, source="mx_paper_position")
        if item["code"] and _positive_shares(item):
            items.append(item)
    return items


def build_watchlist_sync_plan(
    *,
    candidates: list[dict[str, Any]],
    current_watchlist: list[dict[str, Any]],
    mx_positions: list[dict[str, Any]],
    local_positions: list[dict[str, Any]],
    preserve_holdings: bool = True,
) -> dict[str, Any]:
    """生成 MX 自选股同步计划。

    目标自选 = MX/本地正持仓 + 最新候选池里的非持仓标的。
    """
    candidate_items = _dedupe([
        _stock_item(item, source="candidate_pool")
        | {
            "pool_tier": str(item.get("pool_tier") or ""),
            "pool_tier_label": str(item.get("pool_tier_label") or TIER_LABELS.get(str(item.get("pool_tier") or ""), "")),
            "score": item.get("score"),
            "last_scored_at": item.get("last_scored_at", ""),
        }
        for item in candidates
        if _stock_code(item)
    ])
    candidate_items.sort(key=lambda item: (TIER_ORDER.get(item.get("pool_tier", ""), 9), -_float(item.get("score")), item["code"]))

    current_items = _dedupe([_stock_item(item, source="mx_watchlist") for item in current_watchlist if _stock_code(item)])
    holding_items = _dedupe([
        *(
            _stock_item(item, source="mx_paper_position")
            for item in mx_positions
            if _stock_code(item) and _positive_shares(item)
        ),
        *(
            _stock_item(item, source="local_projection")
            for item in local_positions
            if _stock_code(item) and _positive_shares(item)
        ),
    ])
    holding_codes = {item["code"] for item in holding_items} if preserve_holdings else set()
    current_codes = {item["code"] for item in current_items}

    skipped_candidate_holdings = [item for item in candidate_items if item["code"] in holding_codes]
    target_candidates = [item for item in candidate_items if item["code"] not in holding_codes]
    target_codes = {item["code"] for item in target_candidates}

    keep_positions = [item for item in current_items if item["code"] in holding_codes]
    keep_candidates = [item for item in current_items if item["code"] in target_codes and item["code"] not in holding_codes]
    remove = [item for item in current_items if item["code"] not in holding_codes and item["code"] not in target_codes]
    add_positions = [item for item in holding_items if item["code"] not in current_codes]
    add_candidates = [item for item in target_candidates if item["code"] not in current_codes]
    add = _dedupe([*add_positions, *add_candidates])

    desired = _dedupe([*keep_positions, *add_positions, *keep_candidates, *add_candidates])
    return {
        "status": "changes_required" if add or remove else "up_to_date",
        "source": "candidate-pool",
        "preserve_holdings": preserve_holdings,
        "target_count": len(target_candidates),
        "current_count": len(current_items),
        "holding_count": len(holding_items),
        "add_count": len(add),
        "remove_count": len(remove),
        "keep_position_count": len(keep_positions),
        "keep_candidate_count": len(keep_candidates),
        "add_position_count": len(add_positions),
        "add_candidate_count": len(add_candidates),
        "skipped_holding_candidate_count": len(skipped_candidate_holdings),
        "target_candidates": target_candidates,
        "current_watchlist": current_items,
        "protected_holdings": holding_items,
        "skipped_candidate_holdings": skipped_candidate_holdings,
        "keep_positions": keep_positions,
        "keep_candidates": keep_candidates,
        "remove": remove,
        "add_positions": add_positions,
        "add_candidates": add_candidates,
        "add": add,
        "desired_watchlist": desired,
        "summary": _plan_summary(add, remove, keep_positions, add_positions, skipped_candidate_holdings),
        "guardrails": {
            "writes_order": False,
            "real_trade": False,
            "external_state": "mx_watchlist",
            "preserves_mx_and_local_positions": preserve_holdings,
        },
    }


def watchlist_manage_action(action: str, item: dict[str, Any]) -> str:
    """生成 MX 自选管理自然语言动作。"""
    name = item.get("name") or item.get("code", "")
    code = item.get("code", "")
    if action == "add":
        return f"把{name}({code})加入自选股"
    if action == "remove":
        return f"把{name}({code})从自选股删除"
    raise ValueError(f"unknown watchlist action: {action}")


def _stock_code(item: dict[str, Any]) -> str:
    for key in ("code", "secCode", "SECURITY_CODE", "stock_code"):
        value = str(item.get(key) or "").strip()
        if value:
            return value
    return ""


def _stock_name(item: dict[str, Any], code: str) -> str:
    for key in ("name", "secName", "SECURITY_SHORT_NAME", "stock_name"):
        value = str(item.get(key) or "").strip()
        if value:
            return value
    return code


def _stock_item(item: dict[str, Any], *, source: str) -> dict[str, Any]:
    code = _stock_code(item)
    return {
        "code": code,
        "name": _stock_name(item, code),
        "source": source,
        **{key: value for key, value in item.items() if key not in {"code", "name"}},
    }


def _dedupe(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    result = []
    for item in items:
        code = str(item.get("code") or "").strip()
        if not code or code in seen:
            continue
        seen.add(code)
        result.append(item)
    return result


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _positive_shares(item: dict[str, Any]) -> bool:
    for key in ("shares", "count", "position_shares"):
        if key in item:
            return _float(item.get(key)) > 0
    return False


def _plan_summary(
    add: list[dict[str, Any]],
    remove: list[dict[str, Any]],
    keep_positions: list[dict[str, Any]],
    add_positions: list[dict[str, Any]],
    skipped_candidate_holdings: list[dict[str, Any]],
) -> str:
    return (
        f"MX 自选同步计划：新增 {len(add)} 只，删除 {len(remove)} 只，"
        f"保留持仓自选 {len(keep_positions)} 只，补加持仓 {len(add_positions)} 只，"
        f"候选中跳过持仓 {len(skipped_candidate_holdings)} 只。"
    )
