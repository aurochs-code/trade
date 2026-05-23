"""Shared time helpers for audit timestamps and market-local business dates."""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import akshare as ak

_logger = logging.getLogger(__name__)

MARKET_TZ = ZoneInfo("Asia/Shanghai")

# A股全年交易日缓存（从 AkShare 一次性拉取，全量历史约242天/年）
# Key: "YYYY-MM-DD" string, Value: True
_TRADING_DATE_CACHE: set[str] | None = None
_CACHE_LOADED = False


def _load_trading_dates() -> set[str]:
    """从 AkShare 加载 A 股全年交易日历，按需缓存。"""
    global _CACHE_LOADED, _TRADING_DATE_CACHE
    if _CACHE_LOADED:
        return _TRADING_DATE_CACHE or set()
    try:
        df = ak.tool_trade_date_hist_sina()
        dates = set(df["trade_date"].astype(str).tolist())
        _TRADING_DATE_CACHE = dates
        _CACHE_LOADED = True
        _logger.info(f"[time] A股日历已加载，共 {len(dates)} 个交易日")
        return dates
    except Exception as e:
        _logger.debug("[time] 无法从 AkShare 加载日历，回退到 chinese-calendar: %s", e)
        _CACHE_LOADED = True  # 只失败一次，不重试
        return set()


def _env_now() -> datetime | None:
    value = os.getenv("ASTOCK_TEST_NOW", "").strip()
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=MARKET_TZ)
    return parsed


def utc_now() -> datetime:
    fixed = _env_now()
    if fixed:
        return fixed.astimezone(timezone.utc)
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def local_now() -> datetime:
    fixed = _env_now()
    if fixed:
        return fixed.astimezone(MARKET_TZ)
    return datetime.now(MARKET_TZ)


def local_now_iso() -> str:
    return local_now().isoformat()


def local_now_str(fmt: str = "%Y-%m-%d %H:%M") -> str:
    return local_now().strftime(fmt)


def local_today() -> date:
    return local_now().date()


def local_today_str() -> str:
    return local_today().isoformat()


def is_market_weekday(target: date | datetime | str | None = None) -> bool:
    """只做无网络的 A 股周内交易日预判，主要用于阻断周末窗口。"""
    if target is None:
        check_date = local_today()
    elif isinstance(target, datetime):
        check_date = target.astimezone(MARKET_TZ).date() if target.tzinfo else target.date()
    elif isinstance(target, str):
        check_date = date.fromisoformat(target)
    else:
        check_date = target
    return check_date.weekday() < 5


def local_date_bounds_utc(target: date | str | None = None) -> tuple[str, str]:
    if target is None:
        target_date = local_today()
    elif isinstance(target, str):
        target_date = date.fromisoformat(target)
    else:
        target_date = target

    start_local = datetime.combine(target_date, time.min, tzinfo=MARKET_TZ)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc).isoformat(), end_local.astimezone(timezone.utc).isoformat()


def iso_to_local(iso_value: str) -> datetime:
    dt = datetime.fromisoformat(iso_value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(MARKET_TZ)


def iso_to_local_date_str(iso_value: str) -> str:
    return iso_to_local(iso_value).date().isoformat()


def is_trading_day(target: date | str | None = None) -> bool:
    """
    判断指定日期是否为 A 股交易日。

    数据来源：AkShare（tool_trade_date_hist_sina），包含历史全部交易日。
    涵盖所有中国法定节假日（含调休上班日），比 chinese-calendar 更准确。
    """
    if target is None:
        check_date = local_today()
    elif isinstance(target, str):
        check_date = date.fromisoformat(target)
    else:
        check_date = target

    date_str = check_date.isoformat()
    trading_dates = _load_trading_dates()

    if date_str in trading_dates:
        return True

    # 不在 AkShare 日历里（说明不是交易日）
    # 但如果缓存为空（AkShare 加载失败），fallback 到 chinese-calendar
    if not _TRADING_DATE_CACHE:
        return _fallback_chinese_calendar(check_date)

    return False


def is_holiday(target: date | str | None = None) -> bool:
    """判断是否为 A 股非交易日（含周末及法定假日）。"""
    return not is_trading_day(target)


def _fallback_chinese_calendar(check_date: date) -> bool:
    """AkShare 加载失败时的 fallback：使用 chinese-calendar。"""
    try:
        import chinese_calendar as cc

        if check_date.weekday() >= 5:
            return False
        return cc.is_workday(check_date)
    except ImportError:
        # 无任何日历库：保守返回（交易日内允许运行）
        return False
