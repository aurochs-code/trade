"""iwencai 数据源接入层

使用方式：
    from data.sources.pywencai import query, get_financial

优先级设计：
    MX → iwencai → akshare → sina

接入点：
    1. get_financial() — 基本面（ROE/营收增长/现金流），作为 financial.py 的补充
    2. query()           — 自然语言选股，作为 stock_screener.py 的补充
"""
import logging

import pandas as pd

from .wencai import fetch_stocks as fetch_stocks
from .wencai import query as query

logger = logging.getLogger("pywencai")


def get_financial(name: str) -> dict:
    """
    通过 iwencai 查询个股财务数据（预测PE、ROE、营收、现金流等）。

    适用场景：MX skill 不可用或缺失字段时，用 iwencai 做补充。
    单只股票查询返回 ~130 行（不同维度的字段拼在一起），
    解析时取第一个有值的单元格。

    Returns:
        {
            "roe": float (%),
            "revenue_growth": float (%),
            "operating_cash_flow": float,
            "cash_flow_positive": bool,
            "pe_forward": float,          # 预测PE（2025/2026/2027）
            "pe_forward_year": str,
            "pe_ttm": float,
            "pb": float,
            "gross_margin": float,         # 销售毛利率
            "net_margin": float,            # 销售净利率
            "source": "iwencai",
        }
        失败返回空 dict {}
    """
    try:
        df = query(name, loop=False)
    except Exception as e:
        logger.warning(f"[iwencai.get_financial] 查询失败 name={name}: {e}")
        return {}

    if df is None or df.empty:
        return {}

    # 行方向：同一股票各维度字段平铺，取第一个非空值
    def _first_num(col: str, default=None):
        """取列中第一个有效数字。"""
        if col not in df.columns:
            return default
        for val in df[col]:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                continue
            s = str(val).strip().replace("%", "").replace(",", "")
            try:
                return float(s)
            except (ValueError, TypeError):
                continue
        return default

    def _first_str(col: str, default=None):
        if col not in df.columns:
            return default
        for val in df[col]:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                continue
            return str(val).strip()
        return default

    result = {"source": "iwencai"}

    # ROE
    roe = _first_num("净资产收益率roe(加权,公布值)")
    if roe is None:
        roe = _first_num("净资产收益率roe")
    if roe is not None:
        result["roe"] = round(roe, 2)

    # 营收增长率（同比）
    rev_growth = _first_num("营业收入(同比增长率)")
    if rev_growth is not None:
        result["revenue_growth"] = round(rev_growth, 2)

    # 经营现金流
    cf = _first_num("经营现金流")
    if cf is None:
        cf = _first_num("每股现金流量净额")
        if cf is not None:
            # 每股现金流 → 总现金流粗估（乘以股本，这里只用作正负判断）
            result["operating_cash_flow"] = cf
            result["cash_flow_positive"] = cf > 0
    else:
        result["operating_cash_flow"] = cf
        result["cash_flow_positive"] = cf > 0

    # 预测PE（优先 2025，其次 2026）
    pe_fwd_2025 = _first_num("预测市盈率(pe,最新预测)[20251231]")
    pe_fwd_2026 = _first_num("预测市盈率(pe,最新预测)[20261231]")
    if pe_fwd_2025 is not None and pe_fwd_2025 > 0:
        result["pe_forward"] = round(pe_fwd_2025, 2)
        result["pe_forward_year"] = "2025"
    elif pe_fwd_2026 is not None and pe_fwd_2026 > 0:
        result["pe_forward"] = round(pe_fwd_2026, 2)
        result["pe_forward_year"] = "2026"

    # TTM PE
    pe_ttm = _first_num("市盈率(pe)[20260414]")
    if pe_ttm is None:
        pe_ttm = _first_num("市盈率-pe")
    if pe_ttm is not None and pe_ttm > 0:
        result["pe_ttm"] = round(pe_ttm, 2)

    # PB
    pb = _first_num("市净率(pb)[20260414]")
    if pb is None:
        pb = _first_num("市净率-pb")
    if pb is not None and pb > 0:
        result["pb"] = round(pb, 2)

    # 毛利率
    gm = _first_num("销售毛利率")
    if gm is not None:
        result["gross_margin"] = round(gm, 2)

    # 净利率
    nm = _first_num("销售净利率[20251231]")
    if nm is None:
        nm = _first_num("销售净利率")
    if nm is not None:
        result["net_margin"] = round(nm, 2)

    has_any = any(k in result for k in [
        "roe", "revenue_growth", "operating_cash_flow",
        "pe_forward", "pe_ttm", "pb",
    ])
    if has_any:
        logger.info(f"[iwencai.get_financial] {name} 成功: {list(result.keys())}")
    else:
        logger.warning(f"[iwencai.get_financial] {name} 解析后无有效字段（共 {len(df)} 行）")
        return {}

    return result
