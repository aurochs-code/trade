"""BaoStock-backed market data adapter."""

from __future__ import annotations

import asyncio
from contextlib import redirect_stdout
import io
import threading
from typing import Optional

import pandas as pd

from astock_trading.market.models import StockQuote
from astock_trading.platform.time import local_now

from .adapter_utils import is_hk_code


# 模块级 session 管理，避免频繁 login/logout
_bs_lock = threading.RLock()
_bs_logged_in = False


def _bs_ensure_login():
    """确保已登录 baostock（线程安全）。"""
    global _bs_logged_in
    with _bs_lock:
        if not _bs_logged_in:
            import baostock as bs
            with redirect_stdout(io.StringIO()):
                lg = bs.login()
            if lg.error_code != "0":
                raise RuntimeError(f"baostock login failed: {lg.error_msg} {lg.error_msg}")
            _bs_logged_in = True


def _bs_logout():
    """登出 baostock。"""
    global _bs_logged_in
    with _bs_lock:
        if _bs_logged_in:
            import baostock as bs
            with redirect_stdout(io.StringIO()):
                bs.logout()
            _bs_logged_in = False


def _normalize_baostock_code(code: str) -> str:
    """将纯数字 A 股代码标准化为 baostock 格式（sh.600000 / sz.000001）。

    baostock 代码规则：
    - 沪市：sh.6xxxxx
    - 深市：sz.0xxxxx / sz.3xxxxx（创业板）
    - 北交所：bj.8xxxxx
    """
    code = code.strip()
    if "." in code:
        return code.lower()
    if code.startswith(("6", "9")):
        return f"sh.{code}"
    if code.startswith(("0", "3")):
        return f"sz.{code}"
    if code.startswith("8"):
        return f"bj.{code}"
    # 兜底：加 sh
    return f"sh.{code}"


def _to_baostock_code(code: str) -> str:
    """兼容处理：code 可能已经是 sh.600000 格式或纯数字。"""
    return _normalize_baostock_code(code)


class BaoStockMarketAdapter:
    """BaoStock 历史数据 adapter（主要用于回测场景）。

    支持：
    - 日/周/月 K 线（前复权、后复权、不复权）
    - 5/15/30/60 分钟 K 线
    - 指数 K 线

    注意：baostock 不提供实时行情，get_realtime() 返回最新一日快照
    （等同于当日收盘价），仅作兜底使用。
    """

    async def get_realtime(self, codes: list[str]) -> dict[str, StockQuote]:
        # baostock 无真正实时，此处用最新日K 快照兜底
        result = {}
        for code in codes:
            if is_hk_code(code):
                continue
            bs_code = _to_baostock_code(code)
            df = await self.get_kline(bs_code, period="daily", count=1)
            if df is not None and not df.empty:
                row = df.iloc[-1]
                result[code] = StockQuote(
                    code=code,
                    name=str(row.get("名称", code)),
                    price=float(row.get("收盘", 0)),
                    open=float(row.get("开盘", 0)),
                    high=float(row.get("最高", 0)),
                    low=float(row.get("最低", 0)),
                    close=float(row.get("收盘", 0)),
                    volume=int(float(row.get("成交量", 0))),
                    amount=float(row.get("成交额", 0)),
                    change_pct=float(row.get("涨跌幅", 0) or 0),
                )
        return result

    async def get_kline(
        self,
        code: str,
        period: str = "daily",
        count: int = 120,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        adjustflag: str = "2",
    ) -> Optional[pd.DataFrame]:
        """获取 K 线数据。

        Args:
            code: 股票代码（支持 sh.600000 / sz.000001 / 600000 等格式）
            period: 日线周期
                - "daily" / "d": 日 K（默认）
                - "weekly" / "w": 周 K
                - "monthly" / "m": 月 K
                - "5"/"15"/"30"/"60": 分钟 K
            count: 获取最近 N 条（与 start_date 二选一）
            start_date: 开始日期 "YYYY-MM-DD"（与 count 二选一）
            end_date: 结束日期 "YYYY-MM-DD"
            adjustflag: 复权类型
                - "3": 不复权（默认）
                - "2": 前复权（回测推荐）
                - "1": 后复权

        Returns:
            DataFrame，列名对齐 MarketStore.save_bars()：
            日期, 开盘, 最高, 最低, 收盘, 成交量, 成交额, 涨跌幅, 证券名称
        """
        return await asyncio.to_thread(
            self._get_kline_sync, code, period, count, start_date, end_date, adjustflag
        )

    def _get_kline_sync(
        self,
        code: str,
        period: str,
        count: int,
        start_date: Optional[str],
        end_date: Optional[str],
        adjustflag: str,
    ) -> Optional[pd.DataFrame]:
        try:
            import baostock as bs

            bs_code = _to_baostock_code(code)
            _bs_ensure_login()

            # period 标准化
            freq_map = {"daily": "d", "d": "d", "weekly": "w", "w": "w", "monthly": "m", "m": "m",
                        "5": "5", "15": "15", "30": "30", "60": "60"}
            freq = freq_map.get(period, "d")

            # 字段列表
            # 注意：分钟线不支持 pctChg 和 turn
            is_minute = freq not in ("d", "w", "m")
            fields = "date,code,open,high,low,close,volume,amount,adjustflag"
            if not is_minute:
                fields += ",pctChg"
            if freq in ("w", "m"):
                fields += ",turn"
            if is_minute:
                fields += ",time"

            # 计算日期范围（当指定 count 而非起止日期时）
            if start_date is None and count > 0:
                from datetime import timedelta
                end_dt = local_now().replace(tzinfo=None)
                if freq == "d":
                    days = min(count * 3, 5000)
                elif freq in ("w", "m"):
                    # 周/月线：每单元跨度大
                    weeks_per_bar = 1 if freq == "w" else 4
                    days = min(count * weeks_per_bar * 14, 5000)
                else:
                    # 分钟线：每交易日约 240 条，估算所需交易日
                    trading_days = (count // 240) + 2
                    days = trading_days * 7 + 7
                start_dt = end_dt - timedelta(days=days)
                start_date = start_dt.strftime("%Y-%m-%d")
                end_date = end_dt.strftime("%Y-%m-%d")

            rs = bs.query_history_k_data_plus(
                bs_code,
                fields,
                start_date=start_date or "",
                end_date=end_date or "",
                frequency=freq,
                adjustflag=adjustflag,
            )

            if rs.error_code != "0":
                return None

            data_list = []
            while rs.error_code == "0" and rs.next():
                data_list.append(rs.get_row_data())

            if not data_list:
                return None

            df = pd.DataFrame(data_list, columns=rs.fields)

            # 取最近 count 条
            if count > 0 and len(df) > count:
                df = df.tail(count).reset_index(drop=True)

            # 列名对齐 MarketStore.save_bars()
            rename = {
                "date": "日期",
                "open": "开盘",
                "high": "最高",
                "low": "最低",
                "close": "收盘",
                "volume": "成交量",
                "amount": "成交额",
                "pctChg": "涨跌幅",
                "adjustflag": "复权类型",
                "code": "证券代码",
                "turn": "换手率",
            }
            df.rename(columns=rename, inplace=True)

            # 分钟数据：组合 date + time → 日期（baostock 分钟 time=YYYYMMDDHHMMSSmmm）
            if is_minute and "time" in df.columns:
                def _combine_dt(row):
                    t = str(row.get("time", ""))
                    if len(t) >= 14:
                        return f"{t[0:4]}-{t[4:6]}-{t[6:8]} {t[8:10]}:{t[10:12]}:{t[12:14]}"
                    return str(row.get("日期", ""))
                df["日期"] = df.apply(_combine_dt, axis=1)
                df.drop(columns=["time"], inplace=True)

            # 数值类型转换（baostock 全返回字符串）
            numeric_cols = ["开盘", "最高", "最低", "收盘", "成交量", "成交额", "涨跌幅", "换手率"]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

            # 获取证券名称
            name = self._get_stock_name(bs_code)
            df["证券名称"] = name
            df["名称"] = name

            return df

        except Exception:
            return None

    def _get_stock_name(self, bs_code: str) -> str:
        """通过 query_trade_dates 获取证券简称（近似）。"""
        try:
            import baostock as bs

            _bs_ensure_login()
            rs = bs.query_stock_basic(code=bs_code)
            if rs.error_code == "0":
                while rs.next():
                    row = rs.get_row_data()
                    if len(row) >= 3:
                        return row[3] or row[1] or bs_code
            return bs_code
        except Exception:
            return bs_code

    def __del__(self):
        """全局 session 不在此关闭，由模块级函数统一管理。"""
        pass


# ---------------------------------------------------------------------------
# 港股代码识别
