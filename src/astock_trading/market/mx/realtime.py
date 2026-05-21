"""
market/mx/realtime.py — 妙想 MX API 行情客户端

使用 mkapi2.dfcfs.com/finskillshub/api/claw/query 接口获取：
  - 指数行情（上证/深证/创业板/科创50）
  - 个股行情（实时 OHLCV）

注意：返回的是日线级别历史数据（取 rawTable 第 0 项为最新交易日数据），
非盘中分时。适合日线级别评分系统。
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Env 加载
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent


def _load_env():
    """从项目根目录 .env 加载环境变量（不覆盖已有值）。"""
    env_path = _PROJECT_ROOT / ".env"
    if not env_path.exists():
        return
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key, value = key.strip(), value.strip()
            if key and key not in os.environ:
                os.environ[key] = value


_load_env()

MX_APIKEY = os.environ.get("MX_APIKEY", "")
MX_QUERY_URL = "https://mkapi2.dfcfs.com/finskillshub/api/claw/query"

# MX finskillshub API 固定字段 ID
_FID_PRICE = "325898"      # 收盘价 / 当前价格
_FID_CHG_PCT = "326865"    # 涨跌幅（decimal，需 ×100 转为 %）
_FID_VOLUME = "324785"     # 成交量
_FID_AMOUNT = "327483"     # 成交额
_FID_OPEN = None           # 无固定 ID，通过 nameMap 解析
_FID_HIGH = "326339"       # 最高价
_FID_LOW = "326386"        # 最低价
_FID_TURNOVER = "326699"   # 换手率
_FID_PE = None             # 市盈率
_FID_PB = None             # 市净率

_CACHE: Dict[str, tuple] = {}


def _now_min() -> str:
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%dT%H:%M")


def _http_post(query: str, timeout: float = 20.0) -> Optional[Dict[str, Any]]:
    """调用 MX finskillshub query 接口。返回内层 data（剥掉 success 外层）。"""
    if not MX_APIKEY:
        _logger.warning("[mx] MX_APIKEY not configured, MX API disabled")
        return None
    try:
        body = {"toolQuery": query}
        with httpx.Client(timeout=timeout) as client:
            resp = client.post(
                MX_QUERY_URL,
                json=body,
                headers={"Content-Type": "application/json", "apikey": MX_APIKEY},
            )
            resp.raise_for_status()
            result = resp.json()
            # finskillshub 返回 {"success": true, "data": {"data": {"searchDataResultDTO": {...}}}}
            if result and result.get("success") is True:
                outer = result.get("data", {})
                inner = outer.get("data", {}) if isinstance(outer, dict) else {}
                return inner  # 已是 searchDataResultDTO 层
            msg = result.get("message", "") if result else "empty response"
            _logger.debug(f"[mx] query failed: {msg}")
            return None
    except Exception as e:
        _logger.debug(f"[mx] request failed: {e}")
        return None


def _num(v, default=None):
    try:
        if v is None:
            return default
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip().replace("%", "").replace(",", "").replace("万", "").replace("亿", "").replace("元", "")
        return float(s) if s and s not in ("-", "") else default
    except Exception:
        return default


def _expected_mx_entity_code(code: str | None) -> str | None:
    """把外部代码转换成 MX 表里的标准实体代码，如 601127.SH / 09927.HK。"""
    raw = str(code or "").strip().upper()
    if not raw:
        return None

    explicit_market = None
    if raw.startswith(("SH", "SZ", "BJ", "HK")) and len(raw) > 2:
        explicit_market = raw[:2]
        raw = raw[2:]
    if "." in raw:
        raw, suffix = raw.split(".", 1)
        suffix = suffix.strip().upper()
        if suffix in {"SH", "SZ", "BJ", "HK"}:
            explicit_market = suffix

    digits = "".join(ch for ch in raw if ch.isdigit())
    if not digits:
        return None

    if explicit_market:
        return f"{digits}.{explicit_market}"
    if len(digits) == 5 and digits.startswith("0"):
        return f"{digits}.HK"
    if len(digits) == 6:
        if digits.startswith(("6", "9")):
            return f"{digits}.SH"
        if digits.startswith(("4", "8")):
            return f"{digits}.BJ"
        return f"{digits}.SZ"
    return None


def _entity_code_from_tag(tag: Any) -> str | None:
    if not isinstance(tag, dict):
        return None

    direct = tag.get("entityCode") or tag.get("code") or tag.get("unifiedCode")
    if direct:
        direct_text = str(direct).strip().upper()
        if "." in direct_text:
            return direct_text

    secu_code = tag.get("secuCode") or tag.get("securityCode") or tag.get("stockCode")
    market = tag.get("marketChar") or tag.get("market") or tag.get("marketType")
    if secu_code and market:
        market_text = str(market).strip().upper().replace(".", "")
        if market_text in {"SH", "SZ", "BJ", "HK"}:
            return f"{str(secu_code).strip().upper()}.{market_text}"

    return None


def _select_mx_row_index(dto: Dict[str, Any], target_code: str | None) -> int | None:
    """从 MX 多实体表中选择与请求代码匹配的列；不确定时保持旧行为取第 0 列。"""
    expected = _expected_mx_entity_code(target_code)
    if not expected:
        return 0

    entity_codes = dto.get("entityCodes") or []
    if isinstance(entity_codes, list):
        normalized_codes = [str(item).strip().upper() for item in entity_codes]
        if expected in normalized_codes:
            return normalized_codes.index(expected)

    entity_tags = dto.get("entityTagDTOList") or []
    if isinstance(entity_tags, list):
        for idx, tag in enumerate(entity_tags):
            if _entity_code_from_tag(tag) == expected:
                return idx

    raw_table = dto.get("rawTable", {})
    head_names = raw_table.get("headName", []) if isinstance(raw_table, dict) else []
    if isinstance(head_names, list):
        for idx, head in enumerate(head_names):
            head_text = str(head).strip().upper()
            if expected in head_text:
                return idx

    primary_entity = _entity_code_from_tag(dto.get("entityTagDTO"))
    if primary_entity == expected:
        return 0

    # 如果请求 A 股而当前 DTO 明确是港股，不能回退到第 0 列，否则会把
    # 09927.HK 价格写到 601127 这类 A 股代码上。
    if expected.endswith((".SH", ".SZ", ".BJ")):
        if primary_entity and primary_entity.endswith(".HK"):
            return None
        primary_tag = dto.get("entityTagDTO")
        if isinstance(primary_tag, dict):
            tag_text = " ".join(
                str(primary_tag.get(key, ""))
                for key in ("className", "entityTypeName", "marketChar", "market")
            )
            tag_upper = tag_text.upper()
            if "港股" in tag_text or "H股" in tag_text or ".HK" in tag_upper or " HK" in f" {tag_upper} ":
                return None

    return 0


def _value_at(raw_table: Dict[str, Any], field: str, row_index: int) -> Any:
    vals = raw_table.get(field, [])
    if not isinstance(vals, list) or not vals:
        return None
    if row_index < len(vals):
        return vals[row_index]
    if row_index == 0:
        return vals[0]
    return None


def _parse_mx_table(data: Dict, target_code: str | None = None) -> Dict[str, Any]:
    """
    解析 MX finskillshub 返回的 dataTableDTOList。

    策略：优先用固定 field ID 读取 rawTable，再用 nameMap 补充其他字段。
    这是因为 MX 的 nameMap 每次查询返回的 field ID 不固定（如上证指数用 f2，
    深证成指用 326865），但 field ID 的语义是稳定的。

    返回 {"字段名": 最新值}。单实体日线表默认取 rawTable[field_id][0]；
    多实体实时表会先按 target_code 选择对应列，避免 A/H 同名标的串价。
    涨跌幅（326865/f3 等）需要 ×100 转为百分比。
    """
    # 固定 field ID → (标准字段名, 是否需要 ×100 转为 %)
    # f2/f3: 已是百分比格式（"0.32" = 0.32%），直接用
    # 326865: 是 decimal fraction（0.0032 = 0.32%），需要 ×100
    _FID_MAP: Dict[str, tuple] = {
        "f2": ("最新价", False),
        "f3": ("涨跌幅", False),
        _FID_PRICE: ("收盘价", False),
        _FID_CHG_PCT: ("涨跌幅", True),   # 326865 decimal fraction → ×100
    }

    try:
        dto_list = data.get("searchDataResultDTO", {}).get("dataTableDTOList", [])
    except Exception:
        return {}

    for dto in dto_list:
        name_map = dto.get("nameMap", {})
        raw_table = dto.get("rawTable", {})
        row_index = _select_mx_row_index(dto, target_code)
        if row_index is None:
            continue
        result: Dict[str, Any] = {}

        # Step 1：用固定 field ID 读取已知字段（不依赖 nameMap）
        for fid, (std_name, multiply) in _FID_MAP.items():
            val = _value_at(raw_table, fid, row_index)
            if val is None:
                continue
            if isinstance(val, str):
                val = val.strip().replace("%", "").replace(",", "").replace("元", "")
                if val in ("-", ""):
                    val = None
                else:
                    try:
                        val = float(val)
                    except ValueError:
                        pass
            if val is not None and std_name not in result:
                if multiply:
                    val = val * 100
                result[std_name] = val

        # Step 2：用 nameMap 补充固定 ID 没覆盖到的字段
        if name_map:
            for fk, fn in name_map.items():
                if fn in ("数据来源", "headNameSub"):
                    continue
                if fn in result:  # 已由固定 ID 填充
                    continue
                vals = raw_table.get(fk, [])
                if not isinstance(vals, list) or not vals:
                    continue
                val = _value_at(raw_table, fk, row_index)
                if val is None:
                    continue
                if isinstance(val, str):
                    val = val.strip().replace("%", "").replace(",", "").replace("元", "")
                    if val in ("-", ""):
                        val = None
                    else:
                        try:
                            val = float(val)
                        except ValueError:
                            pass
                if val is not None:
                    result[fn] = val

        if result:
            ts_list = raw_table.get("headName", [])
            if isinstance(ts_list, list) and ts_list:
                if row_index < len(ts_list):
                    result["_ts"] = ts_list[row_index]
                elif row_index == 0:
                    result["_ts"] = ts_list[0]
            return result

    return {}


def get_realtime_mx(codes: List[str]) -> Dict[str, Dict[str, Any]]:
    """批量获取个股日线行情（最新交易日收盘数据）。"""
    results = {}
    for code in codes:
        code = str(code).strip()
        if not code:
            continue
        cache_key = f"rt_{code}"
        now = _now_min()
        if cache_key in _CACHE and _CACHE[cache_key][0] == now:
            results[code] = _CACHE[cache_key][1]
            continue

        query = (f"{code} 股票最新价格 涨跌幅 成交量 成交额 "
                 f"今开 最高 最低 市盈率 市净率 换手率")
        data = _http_post(query)
        if not data:
            continue

        p = _parse_mx_table(data, target_code=code)
        if not p:
            continue

        def get(fields, default=None):
            for f in fields:
                if f in p and p[f] is not None:
                    return p[f]
            return default

        entry = {
            "code": code,
            "name": get(["股票名称", "证券名称", "名称"], code),
            "price": _num(get(["收盘价", "最新价", "现价"])),
            "change_pct": _num(get(["涨跌幅"])),
            "volume": _num(get(["成交量"])),
            "amount": _num(get(["成交额"])),
            "open": _num(get(["今开", "开盘价"])),
            "high": _num(get(["最高价", "最高"])),
            "low": _num(get(["最低价", "最低"])),
            "pe": _num(get(["市盈率-动态", "市盈率(TTM)", "市盈率"])),
            "pb": _num(get(["市净率"])),
            "turnover_rate": _num(get(["换手率"])),
            "source": "mx_api",
        }
        results[code] = entry
        _CACHE[cache_key] = (now, entry)
    return results


_INDEX_MAP = {
    "上证指数": "sh000001",
    "深证成指": "sz399001",
    "创业板指": "sz399006",
    "科创50": "sh000688",
}


def get_market_index_mx() -> Dict[str, Dict[str, Any]]:
    """获取四大指数日线行情（最新交易日收盘数据）。"""
    results = {}
    for name, symbol in _INDEX_MAP.items():
        cache_key = f"idx_{name}"
        now = _now_min()
        if cache_key in _CACHE and _CACHE[cache_key][0] == now:
            results[name] = _CACHE[cache_key][1]
            continue

        query = f"{name} 当前点位 涨跌幅"
        data = _http_post(query)
        if not data:
            results[name] = {"close": None, "change_pct": None, "source": "mx_api"}
            _CACHE[cache_key] = (now, results[name])
            continue

        p = _parse_mx_table(data)
        price = p.get("最新价") if p else None
        chg_pct = p.get("涨跌幅") if p else None

        entry = {
            "close": _num(price),
            "change_pct": _num(chg_pct),
            "source": "mx_api",
        }
        results[name] = entry
        _CACHE[cache_key] = (now, entry)
    return results
