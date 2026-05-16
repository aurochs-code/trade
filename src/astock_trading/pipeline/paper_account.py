"""
pipeline/paper_account.py — 模拟盘账户抽象

封装 MX 模拟盘 API，提供统一的持仓/资金/下单接口。
模拟盘数据以 MX API 为 source of truth，不落本地 projection 表。
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional

_logger = logging.getLogger(__name__)


@dataclass
class PaperPosition:
    """模拟盘持仓。"""
    code: str
    name: str
    shares: int
    avg_cost: float
    current_price: float
    market_value: float
    pnl: float
    pnl_pct: float

    @property
    def avg_cost_cents(self) -> int:
        return int(self.avg_cost * 100)

    @property
    def current_price_cents(self) -> int:
        return int(self.current_price * 100)


@dataclass
class PaperBalance:
    """模拟盘资金。"""
    total_asset: float = 0.0
    available_cash: float = 0.0
    market_value: float = 0.0
    frozen: float = 0.0


@dataclass
class PaperTradeResult:
    """模拟盘下单结果。"""
    success: bool
    order_id: str = ""
    error: str = ""
    raw: dict = field(default_factory=dict)


async def _mx_call(coro_fn) -> dict:
    """复用 MX API 调用模式。"""
    from astock_trading.market.mx_async import MXAsyncClient
    client = MXAsyncClient()
    try:
        return await coro_fn(client)
    finally:
        await client.close()


class PaperAccount:
    """模拟盘账户 — 封装 MX API。"""

    def get_positions(self) -> list[PaperPosition]:
        """查询模拟盘持仓。"""
        try:
            result = asyncio.run(_mx_call(lambda c: c.mock_positions()))
            return self._parse_positions(result)
        except Exception as e:
            _logger.error(f"[paper] 查询持仓失败: {e}")
            return []

    def get_balance(self) -> PaperBalance:
        """查询模拟盘资金。"""
        try:
            result = asyncio.run(_mx_call(lambda c: c.mock_balance()))
            return self._parse_balance(result)
        except Exception as e:
            _logger.error(f"[paper] 查询资金失败: {e}")
            return PaperBalance()

    def get_exposure(self) -> tuple[float, float]:
        """
        计算模拟盘仓位占比。

        Returns:
            (exposure_pct, available_cash)
        """
        balance = self.get_balance()
        if balance.total_asset <= 0:
            return 0.0, 0.0
        exposure = balance.market_value / balance.total_asset
        return exposure, balance.available_cash

    def buy(self, code: str, shares: int, price: float = 0) -> PaperTradeResult:
        """模拟盘买入。price=0 为市价。"""
        if shares % 100 != 0:
            return PaperTradeResult(success=False, error="shares 必须为 100 的整数倍")
        try:
            use_market = price <= 0
            result = asyncio.run(_mx_call(
                lambda c: c.mock_trade("buy", code, shares, price if not use_market else None, use_market)
            ))
            return self._parse_trade_result(result)
        except Exception as e:
            return PaperTradeResult(success=False, error=str(e))

    def sell(self, code: str, shares: int, price: float = 0) -> PaperTradeResult:
        """模拟盘卖出。price=0 为市价。"""
        if shares % 100 != 0:
            return PaperTradeResult(success=False, error="shares 必须为 100 的整数倍")
        try:
            use_market = price <= 0
            result = asyncio.run(_mx_call(
                lambda c: c.mock_trade("sell", code, shares, price if not use_market else None, use_market)
            ))
            return self._parse_trade_result(result)
        except Exception as e:
            return PaperTradeResult(success=False, error=str(e))

    def get_position(self, code: str) -> Optional[PaperPosition]:
        """查询单只股票的模拟盘持仓。"""
        positions = self.get_positions()
        for p in positions:
            if p.code == code or code in p.code:
                return p
        return None

    # ------------------------------------------------------------------
    # 解析
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_positions(result: dict) -> list[PaperPosition]:
        """解析 MX API 持仓响应。"""
        positions = []
        code = str(result.get("code", ""))
        if code != "200":
            _logger.warning(f"[paper] 持仓查询返回 code={code}: {result.get('message', '')}")
            return positions

        data = result.get("data", {})
        items = data.get("posList", [])

        for item in items:
            try:
                stock_code = str(item.get("secCode", ""))
                # 价格需要根据 priceDec 还原小数
                price_dec = int(item.get("priceDec", 2))
                cost_dec = int(item.get("costPriceDec", 3))
                raw_price = float(item.get("price", 0))
                raw_cost = float(item.get("costPrice", 0))
                current_price = raw_price / (10 ** price_dec)
                avg_cost = raw_cost / (10 ** cost_dec)

                positions.append(PaperPosition(
                    code=stock_code,
                    name=str(item.get("secName", stock_code)),
                    shares=int(item.get("count", 0)),
                    avg_cost=avg_cost,
                    current_price=current_price,
                    market_value=float(item.get("value", 0)),
                    pnl=float(item.get("profit", 0)),
                    pnl_pct=float(item.get("profitPct", 0)),
                ))
            except (ValueError, TypeError) as e:
                _logger.warning(f"[paper] 解析持仓项失败: {e}, item={item}")
        return positions

    @staticmethod
    def _parse_balance(result: dict) -> PaperBalance:
        """解析 MX API 资金响应。"""
        code = str(result.get("code", ""))
        if code != "200":
            _logger.warning(f"[paper] 资金查询返回 code={code}: {result.get('message', '')}")
            return PaperBalance()

        data = result.get("data", {})
        # balance 接口的核心数据在 data 或 data.result 中
        # positions 接口也返回 totalAssets/availBalance
        # 兼容两种结构
        inner = data.get("result", data) if "result" in data else data
        return PaperBalance(
            total_asset=float(inner.get("totalAssets", data.get("totalAssets", 0))),
            available_cash=float(inner.get("availBalance", data.get("availBalance", 0))),
            market_value=float(inner.get("totalPosValue", data.get("totalPosValue", 0))),
            frozen=float(inner.get("frozenMoney", data.get("frozenMoney", 0))),
        )

    @staticmethod
    def _parse_trade_result(result: dict) -> PaperTradeResult:
        """解析 MX API 下单响应。"""
        code = str(result.get("code", ""))
        if code == "200":
            data = result.get("data", {})
            return PaperTradeResult(
                success=True,
                order_id=str(data.get("orderId", "")),
                raw=result,
            )
        return PaperTradeResult(
            success=False,
            error=result.get("message", f"MX API code={code}"),
            raw=result,
        )
