"""
market/adapters.py - compatibility facade for market data adapters.

The concrete adapter implementations live in focused modules by provider family.
Keep this module as the stable import surface for existing code.
"""

from __future__ import annotations

import shutil as shutil
import subprocess as subprocess

from .a_stock_adapters import (
    AStockSignalAdapter,
    BaiduFundFlowAdapter,
    MootdxMarketAdapter,
    TencentFinancialAdapter,
    _parse_tencent_quote_payload as _parse_tencent_quote_payload,
)
from .adapter_utils import (
    _a_stock_prefix as _a_stock_prefix,
    _extract_a_stock_code as _extract_a_stock_code,
    _normalize_a_stock_code as _normalize_a_stock_code,
    _normalize_opencli_a_stock_symbol as _normalize_opencli_a_stock_symbol,
    _normalize_xueqiu_symbol as _normalize_xueqiu_symbol,
    _parse_heat_value as _parse_heat_value,
    _split_tags as _split_tags,
    _to_float as _to_float,
    _to_int as _to_int,
    _xueqiu_symbol as _xueqiu_symbol,
    is_hk_code,
    normalize_hk_code,
)
from .akshare_adapters import (
    AkShareFinancialAdapter,
    AkShareFlowAdapter,
    AkShareMarketAdapter,
    MXMarketAdapter,
    MXScreenerAdapter,
    MXSentimentAdapter,
)
from .baostock_adapters import (
    BaoStockMarketAdapter,
    _bs_ensure_login as _bs_ensure_login,
    _bs_logout as _bs_logout,
    _normalize_baostock_code as _normalize_baostock_code,
    _to_baostock_code as _to_baostock_code,
)
from .hk_adapters import AkShareHKFinancialAdapter, AkShareHKMarketAdapter
from .opencli_adapters import OpenCliFinanceAdapter, OpenCliXueqiuAdapter
from .protocols import (
    FinancialDataProvider,
    FlowDataProvider,
    MarketDataProvider,
    ScreenerProvider,
    SentimentProvider,
)
from .tushare_adapters import (
    TushareClient,
    TushareFinancialAdapter,
    TushareFlowAdapter,
    TushareMarketAdapter,
    tushare_provider_diagnostic,
)

__all__ = [
    "AStockSignalAdapter",
    "AkShareFinancialAdapter",
    "AkShareFlowAdapter",
    "AkShareHKFinancialAdapter",
    "AkShareHKMarketAdapter",
    "AkShareMarketAdapter",
    "BaoStockMarketAdapter",
    "BaiduFundFlowAdapter",
    "FinancialDataProvider",
    "FlowDataProvider",
    "MXMarketAdapter",
    "MXScreenerAdapter",
    "MXSentimentAdapter",
    "MarketDataProvider",
    "MootdxMarketAdapter",
    "OpenCliFinanceAdapter",
    "OpenCliXueqiuAdapter",
    "ScreenerProvider",
    "SentimentProvider",
    "TencentFinancialAdapter",
    "TushareClient",
    "TushareFinancialAdapter",
    "TushareFlowAdapter",
    "TushareMarketAdapter",
    "is_hk_code",
    "normalize_hk_code",
    "tushare_provider_diagnostic",
]
