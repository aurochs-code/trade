"""
market/mx/xuangu.py — 妙想智能选股
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Tuple

from astock_trading.market.mx.client import MXBaseClient


class MXXuangu(MXBaseClient):
    """妙想智能选股客户端。"""

    def search(self, query: str) -> Dict[str, Any]:
        return self._post("/api/claw/stock-screen", {"keyword": query})

    @staticmethod
    def extract_data(result: Dict[str, Any]) -> Tuple[List[Dict[str, str]], str, Optional[str]]:
        status = result.get("status")
        if status != 0:
            return [], "", f"状态码 {status} - {result.get('message', '')}"

        data = result.get("data", {})
        inner = data.get("data", {})
        data_list = inner.get("allResults", {}).get("result", {}).get("dataList", [])
        columns = inner.get("allResults", {}).get("result", {}).get("columns", [])

        if isinstance(data_list, list) and data_list:
            col_map = _build_column_map(columns)
            order = _columns_order(columns)
            rows = _datalist_to_rows(data_list, col_map, order)
            return rows, "dataList", None

        partial = inner.get("partialResults", "")
        if partial:
            rows = _parse_partial_table(partial)
            return rows, "partialResults", None

        return [], "", "无有效数据"


def _build_column_map(columns: list) -> Dict[str, str]:
    m: Dict[str, str] = {}
    for col in columns or []:
        if not isinstance(col, dict):
            continue
        en = col.get("field", "") or col.get("name", "") or col.get("key", "")
        cn = col.get("displayName", "") or col.get("title", "") or col.get("label", "")
        date_msg = col.get("dateMsg", "")
        if date_msg:
            cn = f"{cn} {date_msg}"
        if en is not None and cn is not None:
            m[str(en)] = str(cn)
    return m


def _columns_order(columns: list) -> List[str]:
    return [str(col.get("field") or col.get("name") or col.get("key"))
            for col in (columns or []) if isinstance(col, dict)]


def _datalist_to_rows(datalist, col_map, col_order) -> List[Dict[str, str]]:
    if not datalist:
        return []
    first = datalist[0]
    extra = [k for k in first if k not in col_order]
    header = col_order + extra
    rows = []
    for row in datalist:
        if not isinstance(row, dict):
            continue
        cn_row = {}
        for en_key in header:
            if en_key not in row:
                continue
            cn_name = col_map.get(en_key, en_key)
            val = row[en_key]
            cn_row[cn_name] = "" if val is None else (json.dumps(val, ensure_ascii=False) if isinstance(val, (dict, list)) else str(val))
        rows.append(cn_row)
    return rows


def _parse_partial_table(text: str) -> List[Dict[str, str]]:
    if not text:
        return []
    lines = [ln.strip() for ln in text.strip().splitlines() if ln.strip()]
    if not lines:
        return []

    def split(line: str) -> list[str]:
        return [c.strip() for c in line.split("|") if c.strip()]

    headers = split(lines[0])
    if not headers:
        return []
    start = 2 if len(lines) > 1 and re.match(r"^[\s|\-]+$", lines[1]) else 1
    rows = []
    for i in range(start, len(lines)):
        cells = split(lines[i])
        cells = (cells + [""] * len(headers))[:len(headers)]
        rows.append(dict(zip(headers, cells)))
    return rows
