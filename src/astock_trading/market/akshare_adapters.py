"""AkShare and MX-backed market data adapters."""

from __future__ import annotations

import asyncio
import logging
import warnings
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import pandas as pd

from astock_trading.market.models import FinancialReport, FundFlow, IndexQuote, SentimentData, StockQuote
from astock_trading.platform.time import local_today

from .adapter_utils import is_hk_code

_logger = logging.getLogger(__name__)

def _patch_py_mini_racer_destructor() -> None:
    """Guard AkShare's optional py_mini_racer dependency against noisy partial init cleanup."""
    try:
        from py_mini_racer import py_mini_racer
    except Exception:
        return

    cls = getattr(py_mini_racer, "MiniRacer", None)
    if cls is None or getattr(cls, "_astock_safe_del", False):
        return

    def _safe_del(self) -> None:
        ext = getattr(self, "ext", None)
        ctx = getattr(self, "ctx", None)
        free_context = getattr(ext, "mr_free_context", None) if ext is not None else None
        if ctx is None or free_context is None:
            return
        try:
            free_context(ctx)
        except Exception:
            return

    cls.__del__ = _safe_del
    cls._astock_safe_del = True


_patch_py_mini_racer_destructor()

class AkShareMarketAdapter:
    """AkShare 行情 adapter（仅 A 股）。"""

    async def get_realtime(self, codes: list[str]) -> dict[str, StockQuote]:
        # 港股代码不处理（避免把 09927 当成 A 股 601127）
        cn_codes = [c for c in codes if not is_hk_code(c)]
        if not cn_codes:
            return {}
        return await asyncio.to_thread(self._get_realtime_sync, cn_codes)

    async def get_kline(self, code: str, period: str = "daily", count: int = 120) -> Optional[pd.DataFrame]:
        if is_hk_code(code):
            return None
        return await asyncio.to_thread(self._get_kline_sync, code, period, count)

    async def get_index(self, symbols: list[str]) -> dict[str, IndexQuote]:
        # AkShare 不负责指数行情，指数由 MXMarketAdapter 提供（见 _get_index_sync stub）
        return await asyncio.to_thread(self._get_index_sync, symbols)

    async def get_sector_heatmap(self) -> list[dict]:
        """获取行业板块热力图数据。

        优先东财（stock_board_industry_name_em），失败则降级新浪行业
        （stock_sector_spot），两者独立数据源。
        返回字段：板块名称、涨跌幅、成交额、上涨家数、下跌家数。
        按涨跌幅降序排列。
        """
        return await asyncio.to_thread(self._get_sector_heatmap_sync)

    async def get_market_stats(self) -> dict:
        """获取全市场涨跌家数统计（新浪A股批量并发拉取）。

        Returns:
            {"up": int, "down": int, "flat": int, "total": int}
        """
        return await asyncio.to_thread(self._get_market_stats_sync)

    def _get_sector_heatmap_sync(self) -> list[dict]:
        """同步实现：拉东财行业板块行情，失败则降级到新浪行业。"""
        # 方式1：东财行业板块
        df = self._fetch_em_industry()
        if df is not None:
            return df

        # 方式2：新浪行业（独立数据源）
        df2 = self._fetch_sina_industry()
        if df2 is not None:
            return df2

        _logger.warning("[AkShareMarket] 行业板块：东财和新浪均获取失败")
        return []

    def _fetch_em_industry(self) -> Optional[list[dict]]:
        """拉东财行业板块（stock_board_industry_name_em）。"""
        try:
            import akshare as ak
            df = ak.stock_board_industry_name_em()
            if df is None or df.empty:
                return None
            rename = {
                "板块名称": "name",
                "涨跌幅": "change_pct",
                "成交额": "amount",
                "上涨家数": "up_count",
                "下跌家数": "down_count",
            }
            col_map = {}
            for old, new in rename.items():
                for col in df.columns:
                    if old in col:
                        col_map[col] = new
                        break
            if not col_map:
                return None
            df = df.rename(columns=col_map)
            keep = ["name", "change_pct", "amount", "up_count", "down_count"]
            df = df[[c for c in keep if c in df.columns]]
            df = df.sort_values("change_pct", ascending=False).reset_index(drop=True)
            records = df.to_dict("records")
            for r in records:
                r["change_pct"] = float(r.get("change_pct", 0) or 0)
                r["amount"] = float(r.get("amount", 0) or 0)
                r["up_count"] = int(r.get("up_count", 0) or 0)
                r["down_count"] = int(r.get("down_count", 0) or 0)
            return records
        except Exception as e:
            _logger.warning(f"[AkShareMarket] 东财行业板块获取失败: {e}")
            return None

    def _fetch_sina_industry(self) -> Optional[list[dict]]:
        """拉新浪行业板块（stock_sector_spot indicator=行业），与东财独立。

        注意：stock_sector_spot 返回的是个股数据，需按板块聚合后才能得到板块行情。
        原始字段：板块、涨跌幅、总成交额 等。
        """
        try:
            import akshare as ak

            df = ak.stock_sector_spot(indicator="行业")
            if df is None or df.empty:
                return None

            # 转换为数值（有些行可能是字符串或带百分号）
            def _to_float(val):
                try:
                    s = str(val).strip().replace("%", "").replace(",", "")
                    return float(s) if s not in ("-", "", "nan") else 0.0
                except (ValueError, TypeError):
                    return 0.0

            df["涨跌幅"] = df["涨跌幅"].apply(_to_float)
            df["总成交额"] = pd.to_numeric(df["总成交额"], errors="coerce").fillna(0)

            # 按板块聚合：涨跌幅取均值，成交额取总和
            # 注意：stock_sector_spot 每个板块只返回1只代表性股票，
            # up_count/down_count 实际意义有限，仅展示代表性股票方向
            agg = (
                df.groupby("板块", as_index=False)
                .agg(
                    change_pct=("涨跌幅", "mean"),
                    amount=("总成交额", "sum"),
                    _rep_direction=("涨跌幅", lambda x: "up" if x.iloc[0] > 0 else "down"),
                )
                .rename(columns={"板块": "name"})
                .sort_values("change_pct", ascending=False)
                .reset_index(drop=True)
            )

            records = agg.to_dict("records")
            for r in records:
                r["change_pct"] = float(r.get("change_pct", 0) or 0)
                r["amount"] = float(r.get("amount", 0) or 0)
                # stock_sector_spot 每个板块仅1只代表性股票，涨跌家数无意义，置0
                r["up_count"] = 0
                r["down_count"] = 0
                r.pop("_rep_direction", None)

            return records
        except Exception as e:
            _logger.warning(f"[AkShareMarket] 新浪行业板块获取失败: {e}")
            return None

    def _get_market_stats_sync(self) -> dict:
        """并发拉取新浪A股全量数据，统计全市场升降家数。

        约5500只A股，每页100只，并发10线程，预期3-5s内完成。
        """
        import requests
        from concurrent.futures import ThreadPoolExecutor, as_completed

        SINA_BASE = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
        HEADERS = {"Referer": "https://finance.sina.com.cn"}

        def fetch_page(page: int) -> list:
            params = {
                "page": page, "num": 100, "sort": "symbol",
                "asc": 1, "node": "hs_a", "_s_r_a": "page",
            }
            try:
                r = requests.get(SINA_BASE, params=params, headers=HEADERS, timeout=15)
                data = r.json()
                return data if isinstance(data, list) else []
            except Exception:
                return []

        # 先拿一页确认总数
        first_page = fetch_page(1)
        if not first_page:
            return {"up": 0, "down": 0, "flat": 0, "total": 0}

        all_data = list(first_page)
        total_approx = 5600  # 保守估计
        pages_needed = min((total_approx // 100) + 1, 60)

        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = [ex.submit(fetch_page, p) for p in range(2, pages_needed + 1)]
            for fut in as_completed(futures):
                for item in fut.result():
                    all_data.append(item)

        up = sum(1 for d in all_data if float(d.get("changepercent") or 0) > 0)
        down = sum(1 for d in all_data if float(d.get("changepercent") or 0) < 0)
        flat = len(all_data) - up - down
        return {"up": up, "down": down, "flat": flat, "total": len(all_data)}

    def _get_realtime_sync(self, codes: list[str]) -> dict[str, StockQuote]:
        """主入口：优先东财全量，失败则降级到新浪分时。"""
        try:
            import akshare as ak

            # 过滤掉港股代码
            a_codes = [c for c in codes if not is_hk_code(c)]
            if not a_codes:
                return {}

            df = ak.stock_zh_a_spot_em()
            if df is not None and not df.empty:
                code_set = set(a_codes)
                result = {}
                for _, row in df.iterrows():
                    code = str(row.get("代码", "")).strip()
                    if code not in code_set:
                        continue
                    result[code] = StockQuote(
                        code=code,
                        name=str(row.get("名称", "")),
                        price=float(row.get("最新价", 0) or 0),
                        open=float(row.get("今开", 0) or 0),
                        high=float(row.get("最高", 0) or 0),
                        low=float(row.get("最低", 0) or 0),
                        close=float(row.get("最新价", 0) or 0),
                        volume=int(row.get("成交量", 0) or 0),
                        amount=float(row.get("成交额", 0) or 0),
                        change_pct=float(row.get("涨跌幅", 0) or 0),
                    )
                if result:
                    return result
        except Exception:
            pass

        # 东财断线，降级到新浪分时（收盘后场景，每只股票单独请求）
        return self._get_realtime_from_sina_tick(codes)

    def _get_realtime_from_sina_tick(self, codes: list[str]) -> dict[str, StockQuote]:
        """新浪分时降级：用当日最后一笔的价格和成交额估算实时行情。

        仅在东财 stock_zh_a_spot_em 断线时使用。
        收盘后可用，日内数据有限（无盘前集合竞价价格）。
        """

        result = {}
        today = local_today().strftime("%Y%m%d")

        for code in codes:
            if is_hk_code(code):
                continue
            try:
                import akshare as ak

                # 标准化新浪格式
                symbol = f"sh{code}" if code.startswith(("6", "9")) else f"sz{code}"
                df = ak.stock_intraday_sina(symbol=symbol, date=today)
                if df is None or df.empty:
                    continue

                last_row = df.iloc[-1]
                price = float(last_row["price"])
                prev_price = float(last_row["prev_price"]) if last_row["prev_price"] else price
                change_pct = ((price - prev_price) / prev_price * 100) if prev_price > 0 else 0.0

                # 成交额 = sum(price * volume)，新浪分时 volume 是累计成交量
                total_volume = float(df["volume"].sum()) if "volume" in df.columns else 0.0
                amount = float(last_row["volume"]) * price if "volume" in df.columns else 0.0

                result[code] = StockQuote(
                    code=code,
                    name=str(df.iloc[0]["name"]) if "name" in df.columns else code,
                    price=price,
                    open=0.0,  # 新浪分时无开盘价
                    high=float(df["price"].max()),
                    low=float(df["price"].min()),
                    close=price,
                    volume=int(total_volume),
                    amount=amount,
                    change_pct=round(change_pct, 2),
                )
            except Exception:
                continue

        return result

    def _get_kline_sync(self, code: str, period: str, count: int) -> Optional[pd.DataFrame]:
        """主入口：优先东财日线，失败则降级到腾讯日线。"""
        result = self._get_kline_from_em(code, count)
        if result is not None and not result.empty:
            return result
        return self._get_kline_from_tx(code, count)

    def _get_kline_from_em(self, code: str, count: int) -> Optional[pd.DataFrame]:
        """东财日线（复权）。"""
        try:
            import akshare as ak

            if code.startswith(("6", "9")):
                symbol = f"sh{code}"
            else:
                symbol = f"sz{code}"

            df = ak.stock_zh_a_daily(symbol=symbol, adjust="qfq")
            if df is None or df.empty:
                return None

            df = df.sort_values("date").tail(count * 2).reset_index(drop=True)
            df["涨跌幅"] = df["close"].pct_change() * 100
            return df
        except Exception:
            return None

    def _get_kline_from_tx(self, code: str, count: int) -> Optional[pd.DataFrame]:
        """腾讯日线降级：无成交量（volume=0），其他字段齐全。

        适用：东财 stock_zh_a_daily 断线时。
        """
        try:
            import akshare as ak
            from datetime import datetime, timedelta

            if code.startswith(("6", "9")):
                symbol = f"sh{code}"
            else:
                symbol = f"sz{code}"

            end = datetime.today().strftime("%Y%m%d")
            start = (datetime.today() - timedelta(days=count * 4)).strftime("%Y%m%d")
            df = ak.stock_zh_a_hist_tx(symbol=symbol, start_date=start, end_date=end)
            if df is None or df.empty:
                return None

            df = df.sort_values("date").tail(count * 2).reset_index(drop=True)

            # 腾讯日线无 volume，用 amount/close 估算成交量（单位一致时有效）
            # 保留 amount 列，volume 置 0（不影响技术指标计算，仅影响 amount 相关维度）
            if "volume" not in df.columns:
                df["volume"] = 0
            if "amount" not in df.columns:
                df["amount"] = 0.0

            df["涨跌幅"] = df["close"].pct_change() * 100
            return df
        except Exception:
            return None

    def _get_index_sync(self, symbols: list[str]) -> dict[str, IndexQuote]:
        # 指数行情未实现，由 MXMarketAdapter 提供，此处仅作兜底
        _logger.debug("[AkShareMarket] get_index 未实现，返回空")
        return {}


class AkShareFinancialAdapter:
    """AkShare 财务 adapter。"""

    async def get_financial(self, code: str) -> Optional[FinancialReport]:
        return await asyncio.to_thread(self._get_financial_sync, code)

    def _get_financial_sync(self, code: str) -> Optional[FinancialReport]:
        try:
            if is_hk_code(code):
                return None
            import akshare as ak
            df = ak.stock_financial_analysis_indicator(symbol=code, start_year="2024")
            if df is None or df.empty:
                return None

            def _latest(col_name_pattern: str) -> Optional[float]:
                col = next((c for c in df.columns if col_name_pattern in str(c)), None)
                if not col:
                    return None
                vals = df[col].dropna().head(4).tolist()
                return round(float(vals[0]), 2) if vals else None

            roe = _latest("净资产收益率")  # 取加权净资产收益率更准确
            if roe is None:
                roe = _latest("总资产净利润率")

            # 营收增长：主营业务收入增长率（最新一期）
            rev_growth = _latest("主营业务收入增长率")

            # 现金流：每股经营性现金流
            cash_flow = _latest("每股经营性现金流")

            # 也尝试总资产净利润率作为备选
            if roe is None:
                roe = _latest("总资产净利润率")

            return FinancialReport(
                roe=roe,
                revenue_growth=rev_growth,
                operating_cash_flow=cash_flow,
            )
        except Exception:
            return None


class AkShareFlowAdapter:
    """AkShare 资金流向 adapter。"""

    def __init__(self) -> None:
        self._last_errors: dict[str, dict] = {}

    async def get_fund_flow(self, code: str, days: int = 5) -> Optional[FundFlow]:
        return await asyncio.to_thread(self._get_flow_sync, code, days)

    def get_last_error(self, code: str) -> dict | None:
        return self._last_errors.get(code)

    def _get_flow_sync(self, code: str, days: int) -> Optional[FundFlow]:
        """主入口：优先东财，失败则降级到腾讯 tick。"""
        self._last_errors.pop(code, None)
        if is_hk_code(code):
            return None
        subsource_errors: list[dict] = []

        # 1. 优先东财资金流
        result = self._get_flow_from_em(code, days, subsource_errors)
        if result is not None:
            return result
        # 2. 东财断线，降级到腾讯分笔
        result = self._get_flow_from_tx_tick(code, subsource_errors)
        if result is not None:
            return result

        self._last_errors[code] = self._build_flow_error(subsource_errors)
        return None

    def _get_flow_from_em(
        self,
        code: str,
        days: int,
        subsource_errors: list[dict] | None = None,
    ) -> Optional[FundFlow]:
        """东财资金流接口，失败时返回 None（ caller 会尝试腾讯降级）。"""
        try:
            import akshare as ak
            market = "sh" if code.startswith(("6", "9")) else "sz"
            df = ak.stock_individual_fund_flow(stock=code, market=market)
            if df is None or df.empty:
                self._append_subsource_error(
                    subsource_errors,
                    subsource="eastmoney_fund_flow",
                    status="em_fund_flow_empty",
                    error_type="EmptyResult",
                    error_message="东财资金流返回空结果",
                )
                return None

            recent = df.tail(days)
            total_net = 0
            outflow_streak = 0
            for _, row in recent.iterrows():
                main_net = 0
                for col in row.index:
                    if "主力" in str(col) and "净" in str(col):
                        main_net = float(row[col]) if pd.notna(row[col]) else 0
                        break
                total_net += main_net
                if main_net < -5_000_000:
                    outflow_streak += 1

            return FundFlow(
                net_inflow_1d=total_net,
                consecutive_outflow_days=outflow_streak,
            )
        except Exception as exc:
            self._append_subsource_error(
                subsource_errors,
                subsource="eastmoney_fund_flow",
                status="em_fund_flow_failed",
                error_type=exc.__class__.__name__,
                error_message=str(exc),
            )
            return None

    def _get_flow_from_tx_tick(
        self,
        code: str,
        subsource_errors: list[dict] | None = None,
    ) -> Optional[FundFlow]:
        """腾讯分笔成交降级：按买盘/卖盘汇总估算主力净流入。

        适用场景：东财 stock_individual_fund_flow 接口断线时。
        注意：腾讯 tick 只有当日数据，days 参数被忽略。
        """
        try:
            import akshare as ak

            # 标准化为腾讯格式：sh600415 / sz000001
            if code.startswith(("6", "9")):
                symbol = f"sh{code}"
            else:
                symbol = f"sz{code}"

            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message="正在下载数据，请稍等")
                df = ak.stock_zh_a_tick_tx_js(symbol=symbol)
            if df is None or df.empty:
                self._append_subsource_error(
                    subsource_errors,
                    subsource="tencent_tick",
                    status="tx_tick_empty",
                    error_type="EmptyResult",
                    error_message="腾讯 tick 返回空结果",
                )
                return None

            buy_mask = df["性质"].str.contains("买", na=False)
            sell_mask = df["性质"].str.contains("卖", na=False)

            buy_amount = df.loc[buy_mask, "成交金额"].sum()
            sell_amount = df.loc[sell_mask, "成交金额"].sum()
            total_amount = buy_amount + sell_amount

            net_inflow = buy_amount - sell_amount  # 正=净流入，负=净流出

            # 主买占比（主力参与度）
            main_force_ratio = buy_amount / total_amount if total_amount > 0 else 0.0

            # 连续流出判断：今日净流出且金额 > 5000万 → outflow_streak=1（腾讯tick只当日）
            # 明日需要重新获取才知道是否连续，所以最多记1天
            outflow_streak = 1 if (net_inflow < -5_000_000) else 0

            return FundFlow(
                net_inflow_1d=net_inflow,
                net_inflow_5d=net_inflow,  # 腾讯tick仅当日，退化为单日
                main_force_ratio=main_force_ratio,
                consecutive_outflow_days=outflow_streak,
            )
        except Exception as exc:
            self._append_subsource_error(
                subsource_errors,
                subsource="tencent_tick",
                status="tx_tick_failed",
                error_type=exc.__class__.__name__,
                error_message=str(exc),
            )
            return None

    def _append_subsource_error(
        self,
        subsource_errors: list[dict] | None,
        *,
        subsource: str,
        status: str,
        error_type: str,
        error_message: str,
    ) -> None:
        if subsource_errors is None:
            return
        subsource_errors.append({
            "subsource": subsource,
            "status": status,
            "error_type": error_type,
            "error_message": error_message,
        })

    def _build_flow_error(self, subsource_errors: list[dict]) -> dict:
        statuses = [str(item.get("status", "")) for item in subsource_errors if item]
        has_failed = any(status.endswith("_failed") for status in statuses)
        return {
            "status": "provider_error" if has_failed else "empty",
            "error_type": "AkShareFlowSubsourcesFailed" if has_failed else "EmptyResult",
            "error_message": (
                "AkShare 资金流子源全部失败: "
                + (", ".join(statuses) if statuses else "no_subsource_result")
            ),
            "subsource_errors": subsource_errors,
        }


# ---------------------------------------------------------------------------
# MX Adapters
# ---------------------------------------------------------------------------

class MXSentimentAdapter:
    """妙想舆情 adapter。"""

    async def search_news(self, query: str) -> Optional[SentimentData]:
        return await asyncio.to_thread(self._sync, query)

    def _sync(self, query: str) -> Optional[SentimentData]:
        try:
            from astock_trading.market.mx.search import MXSearch
            mx = MXSearch()
            result = mx.search(query)

            data = result.get("data", {})
            inner = data.get("data", {})
            search_resp = inner.get("llmSearchResponse", {})
            items = search_resp.get("data", [])

            if not items:
                return SentimentData(score=1.5, detail="无相关资讯")

            report_count = 0
            positive_count = 0
            negative_count = 0
            for item in items:
                info_type = item.get("informationType", "")
                rating = str(item.get("rating", "")).lower()
                if info_type == "REPORT":
                    report_count += 1
                if any(w in rating for w in ["买入", "增持", "推荐"]):
                    positive_count += 1
                elif any(w in rating for w in ["减持", "卖出"]):
                    negative_count += 1

            score = 1.5
            if report_count >= 5:
                score += 0.5
            elif report_count >= 2:
                score += 0.3
            if positive_count >= 2:
                score += 0.5
            elif positive_count >= 1:
                score += 0.3
            if negative_count >= 2:
                score -= 0.5
            score = max(0, min(score, 3.0))

            return SentimentData(
                score=round(score, 1),
                news_count=len(items),
                positive_ratio=positive_count / max(len(items), 1),
                detail=f"研报{report_count}篇 买入{positive_count} 减持{negative_count}",
            )
        except Exception:
            return None


class MXScreenerAdapter:
    """妙想选股 adapter。"""

    async def search_stocks(self, query: str) -> list[dict]:
        return await asyncio.to_thread(self._sync, query)

    def _sync(self, query: str) -> list[dict]:
        try:
            from astock_trading.market.mx.xuangu import MXXuangu
            mx = MXXuangu()
            result = mx.search(query)
            rows, _, err = mx.extract_data(result)
            if err:
                return []
            return rows
        except Exception:
            return []


class MXMarketAdapter:
    """妙想行情 adapter。"""

    async def get_realtime(self, codes: list[str]) -> dict[str, StockQuote]:
        a_codes = [code for code in codes if not is_hk_code(code)]
        if not a_codes:
            return {}
        return await asyncio.to_thread(self._get_realtime_sync, a_codes)

    async def get_index(self, symbols: list[str]) -> dict[str, IndexQuote]:
        """获取 A 股指数行情（内部 hardcode 四大指数，无视 symbols 参数）。"""
        return await asyncio.to_thread(self._get_index_sync)

    async def get_kline(self, code: str, period: str = "daily", count: int = 120) -> Optional[pd.DataFrame]:
        if is_hk_code(code):
            return None
        return await asyncio.to_thread(self._get_kline_sync, code, period, count)

    def _get_kline_sync(self, code: str, period: str = "daily", count: int = 120) -> Optional[pd.DataFrame]:
        """从 MX finskillshub API 获取日K线历史数据。

        策略：query= "code 近N个交易日 日K线"，DTO 0 含：
          - headName[]    → 日期列（最新→最旧，降序）
          - 100000000017969 → 开盘价
          - 100000000017975 → 收盘价
          - 100000000019180 → 是否涨停（辅助）
        数据只有开/收，没有 high/low/vol，compute_technical_indicators
        用收盘价计算均线/R SI/MACD，只需 close 列存在即可（其他列填0）。

        兜底：AkShare 东财日线。
        """
        if is_hk_code(code):
            _logger.debug(f"[MXMarket] get_kline 跳过港股代码: {code}")
            return None

        if period != "daily":
            _logger.warning(f"[MXMarket] get_kline 不支持 period={period}，仅支持 daily")
            return None

        import akshare as ak

        # 先试 MX
        mx_df = self._get_kline_from_mx(code, count)
        if mx_df is not None and not mx_df.empty:
            return mx_df

        # MX 失败 → AkShare 东财
        try:
            symbol = f"sh{code}" if code.startswith(("6", "9")) else f"sz{code}"
            df = ak.stock_zh_a_daily(symbol=symbol, adjust="qfq")
            if df is None or df.empty:
                return None
            df = df.sort_values("date").tail(count * 2).reset_index(drop=True)
            df["涨跌幅"] = df["close"].pct_change() * 100
            return df
        except Exception:
            return None

    def _get_kline_from_mx(self, code: str, count: int) -> Optional[pd.DataFrame]:
        """调用 MX finskillshub query 接口拉日K线。"""
        try:
            import httpx
        except ImportError:
            return None

        # 加载 MX API key（与 realtime.py 相同逻辑）
        import os
        from pathlib import Path

        def _load_key():
            key = os.environ.get("MX_APIKEY", "").strip()
            if key:
                return key
            p = Path(__file__).resolve().parent.parent.parent.parent / ".env"
            if p.exists():
                for line in open(p, encoding="utf-8"):
                    line = line.strip()
                    if line.startswith("MX_APIKEY=") and len(line) > 10:
                        return line.split("=", 1)[1].strip()
            return ""

        api_key = _load_key()
        if not api_key:
            return None

        # MX finnhub 返回结构：
        # DTO 0: headName=日期[], 100000000017969=开盘[], 100000000017975=收盘[]
        # 日期列从新→旧排列，需要反转
        _FID_OPEN = "100000000017969"
        _FID_CLOSE = "100000000017975"

        try:
            resp = httpx.post(
                "https://mkapi2.dfcfs.com/finskillshub/api/claw/query",
                json={"toolQuery": f"{code} 近{count}个交易日 日K线"},
                headers={"Content-Type": "application/json", "apikey": api_key},
                timeout=15.0,
            )
            data = resp.json()
        except Exception:
            return None

        try:
            inner = data.get("data", {}).get("data", {})
            dto_list = inner.get("searchDataResultDTO", {}).get("dataTableDTOList", [])
        except Exception:
            return None

        dto = None
        for d in dto_list:
            raw = d.get("rawTable", {})
            if _FID_OPEN in raw and _FID_CLOSE in raw:
                dto = d
                break
        if dto is None:
            return None

        raw = dto.get("rawTable", {})
        head = raw.get("headName", [])

        if not isinstance(head, list) or len(head) < 2:
            return None

        dates = list(reversed(head))            # 新→旧 → 旧→新
        opens = list(reversed(raw.get(_FID_OPEN, [])))
        closes = list(reversed(raw.get(_FID_CLOSE, [])))

        rows = min(len(dates), count * 2)
        rows = max(rows, 5)

        import pandas as pd
        df = pd.DataFrame({
            "date":   dates[-rows:],
            "open":   [float(o) for o in opens[-rows:]],
            "close":  [float(c) for c in closes[-rows:]],
            # MX 没有这些字段，akshare adapter 的 compute_technical_indicators
            # 只强制要求 close，其余列填 0 或 NaN
            "high":   [float(c) for c in closes[-rows:]],   # 近似
            "low":    [float(c) for c in closes[-rows:]],   # 近似
            "volume": [0] * rows,
        })
        df["涨跌幅"] = df["close"].pct_change() * 100
        return df

    def _get_realtime_sync(self, codes: list[str]) -> dict[str, StockQuote]:
        try:
            codes = [code for code in codes if not is_hk_code(code)]
            if not codes:
                return {}
            from astock_trading.market.mx.realtime import get_realtime_mx
            raw = get_realtime_mx(codes)
            result = {}
            for code, data in raw.items():
                if "error" in data:
                    continue
                result[code] = StockQuote(
                    code=code,
                    name=data.get("name", code),
                    price=data.get("price", 0),
                    open=data.get("open", 0),
                    high=data.get("high", 0),
                    low=data.get("low", 0),
                    close=data.get("price", 0),
                    volume=int(data.get("volume", 0)),
                    amount=data.get("amount", 0),
                    change_pct=data.get("change_pct", 0),
                )
            return result
        except Exception:
            return {}

    def _get_index_sync(self) -> dict[str, IndexQuote]:
        """获取指数行情，优先 MX，失败则用 akshare 兜底。
        均线/above_ma20/below_ma60_days 由 akshare 日线数据计算。"""
        import akshare as ak
        import pandas as pd

        # 日线代码映射
        code_map = {
            "上证指数": "sh000001",
            "深证成指": "sz399001",
            "创业板指": "sz399006",
            "科创50": "sh000688",
        }

        def _compute_ma(symbol: str) -> tuple[float, float, bool, int]:
            """计算 MA20、MA60、above_ma20、below_ma60_days。"""
            try:
                df = ak.stock_zh_index_daily(symbol=symbol)
                df = df.sort_values("date")
                close = df["close"].astype(float)
                ma20_val = close.rolling(20).mean().iloc[-1] if len(close) >= 20 else 0
                ma60_val = close.rolling(60).mean().iloc[-1] if len(close) >= 60 else 0
                latest_price = close.iloc[-1]
                above = bool(latest_price > ma20_val > 0)
                # below_ma60_days：最近多少个交易日连续低于 MA60
                below_ma60 = (close < ma60_val).iloc[-20:] if ma60_val > 0 else pd.Series(False, index=close.index[-20:])
                count = 0
                for v in reversed(below_ma60.tolist()):
                    if v:
                        count += 1
                    else:
                        break
                return float(ma20_val), float(ma60_val), above, count
            except Exception:
                return 0.0, 0.0, False, 0

        result = {}

        # 优先 MX（获取实时价格/涨跌幅）
        try:
            from astock_trading.market.mx.realtime import get_market_index_mx
            raw = get_market_index_mx()
            for name, data in raw.items():
                if "error" in data:
                    continue
                symbol = code_map.get(name, name)
                ma20, ma60, above_ma20, below_days = _compute_ma(symbol)
                result[name] = IndexQuote(
                    symbol=symbol,
                    name=name,
                    price=data.get("close") or data.get("price", 0) or 0,
                    change_pct=data.get("change_pct", 0) or 0,
                    ma20=ma20,
                    ma60=ma60,
                    above_ma20=above_ma20,
                    below_ma60_days=below_days,
                )
            if result and any(v.price > 0 for v in result.values()):
                # MX 可能返回完整指数名称但部分价格为 null/0（深证/创业板/科创常见）。
                # 只有全部指数都有有效价格时才直接返回；否则继续走 akshare 日线兜底，
                # 补齐缺失或无效价格，避免 market_state 出现 price=0。
                missing_or_invalid = [
                    n for n in code_map
                    if n not in result or (result[n].price or 0) <= 0
                ]
                if not missing_or_invalid:
                    return result
                # 有缺失或价格无效，继续走 akshare 兜底补全
        except Exception as e:
            _logger.warning(f"[MXMarket] MX 指数行情获取失败: {e}")

        # akshare 兜底（价格 + 均线）
        # 注意：stock_zh_index_spot_sina 在批量请求时对部分指数（深证/创业板）返回 0，
        # 改用 stock_zh_index_daily 的最新收盘价作为价格来源，更稳定
        try:
            # Phase 1: 并行拉取所有指数日线数据
            def _fetch_daily(code):
                try:
                    return code, ak.stock_zh_index_daily(symbol=code)
                except Exception:
                    return code, None

            codes = list(code_map.values())
            with ThreadPoolExecutor(max_workers=min(len(codes), 4)) as executor:
                daily_map: dict[str, Optional[pd.DataFrame]] = {
                    code: df for code, df in executor.map(_fetch_daily, codes)
                }

            # Phase 2: 主线程更新 result（已有 MX 数据的补充价格/MA，没有的创建新记录）
            for name, code in code_map.items():
                daily_df = daily_map.get(code)
                if name in result:
                    # MX 已返回数据，用日线补充/替换价格（如果 MX 价格无效）和 MA
                    # 注意：IndexQuote 是 frozen dataclass，不能原地修改，需重建对象
                    existing = result[name]
                    price = existing.price
                    change_pct = existing.change_pct
                    ma20 = existing.ma20
                    ma60 = existing.ma60
                    above_ma20 = existing.above_ma20
                    below_ma60_days = existing.below_ma60_days
                    if daily_df is not None and not daily_df.empty:
                        latest_close = float(daily_df["close"].iloc[-1])
                        prev_close = float(daily_df["close"].iloc[-2]) if len(daily_df) >= 2 else latest_close
                        if (price or 0) <= 0 or price != latest_close:
                            price = latest_close
                        daily_chg = ((latest_close - prev_close) / prev_close * 100) if prev_close > 0 else 0
                        if abs(change_pct) < 0.01 and abs(daily_chg) > 0.01:
                            change_pct = daily_chg
                    # 补全 MA（如果 MX 没有的话）
                    if not ma20 and not ma60:
                        ma20, ma60, above_ma20, below_days = _compute_ma(code)
                        below_ma60_days = below_days
                    # 重建 IndexQuote（替换整个对象）
                    result[name] = IndexQuote(
                        symbol=existing.symbol,
                        name=name,
                        price=price,
                        change_pct=change_pct,
                        ma20=ma20,
                        ma60=ma60,
                        above_ma20=above_ma20,
                        below_ma60_days=below_ma60_days,
                        timestamp=existing.timestamp,
                    )
                else:
                    # 完全没有数据，用日线构建完整记录
                    price_val = 0.0
                    change_val = 0.0
                    if daily_df is not None and not daily_df.empty:
                        latest_close = float(daily_df["close"].iloc[-1])
                        prev_close = float(daily_df["close"].iloc[-2]) if len(daily_df) >= 2 else latest_close
                        price_val = latest_close
                        change_val = ((latest_close - prev_close) / prev_close * 100) if prev_close > 0 else 0.0
                    ma20, ma60, above_ma20, below_days = _compute_ma(code)
                    result[name] = IndexQuote(
                        symbol=code,
                        name=name,
                        price=price_val,
                        change_pct=change_val,
                        ma20=ma20,
                        ma60=ma60,
                        above_ma20=above_ma20,
                        below_ma60_days=below_days,
                    )
            return result
        except Exception:
            return {}


# ---------------------------------------------------------------------------
