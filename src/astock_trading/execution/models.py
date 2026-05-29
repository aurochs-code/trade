"""
execution/models.py — 执行领域模型

订单、持仓、资金、交易事件。
金额用 _cents 整数存储，领域层提供 float 属性便捷访问。
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderStatus(str, Enum):
    PENDING = "pending"
    FILLED = "filled"
    CANCELLED = "cancelled"


@dataclass
class Order:
    order_id: str
    code: str
    name: str
    side: OrderSide
    shares: int
    price_cents: int
    status: OrderStatus = OrderStatus.PENDING
    broker: str = ""
    fill_price_cents: int = 0
    fee_cents: int = 0
    created_at: str = ""
    filled_at: Optional[str] = None

    @property
    def price(self) -> float:
        return self.price_cents / 100

    @property
    def fill_price(self) -> float:
        return self.fill_price_cents / 100

    @property
    def fee(self) -> float:
        return self.fee_cents / 100

    def to_dict(self) -> dict:
        return {
            "order_id": self.order_id,
            "code": self.code,
            "name": self.name,
            "side": self.side.value,
            "shares": self.shares,
            "price_cents": self.price_cents,
            "status": self.status.value,
            "broker": self.broker,
            "fill_price_cents": self.fill_price_cents,
            "fee_cents": self.fee_cents,
            "created_at": self.created_at,
            "filled_at": self.filled_at,
        }


@dataclass
class Position:
    code: str
    name: str
    style: str  # "slow_bull" | "momentum"
    shares: int
    avg_cost_cents: int
    entry_date: str
    cost_basis_cents: int = 0
    entry_day_low_cents: int = 0
    stop_loss_cents: int = 0
    take_profit_cents: int = 0
    highest_since_entry_cents: int = 0
    current_price_cents: int = 0
    unrealized_pnl_cents: int = 0
    updated_at: str = ""
    currency: str = "CNY"  # "CNY" | "HKD"

    @property
    def effective_cost_basis_cents(self) -> int:
        if self.cost_basis_cents > 0:
            return self.cost_basis_cents
        return self.avg_cost_cents * self.shares

    @property
    def avg_cost(self) -> float:
        if self.shares > 0:
            return self.effective_cost_basis_cents / self.shares / 100
        return self.avg_cost_cents / 100

    @property
    def trade_price(self) -> float:
        return self.avg_cost_cents / 100

    @property
    def cost_price(self) -> float:
        return self.avg_cost

    @property
    def current_price(self) -> float:
        return self.current_price_cents / 100

    @property
    def unrealized_pnl(self) -> float:
        return self.unrealized_pnl_cents / 100

    @property
    def unrealized_pnl_pct(self) -> float:
        cost_basis = self.effective_cost_basis_cents
        if cost_basis <= 0:
            return 0.0
        return self.unrealized_pnl_cents / cost_basis * 100

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "name": self.name,
            "style": self.style,
            "shares": self.shares,
            "avg_cost_cents": self.avg_cost_cents,
            "trade_price": self.trade_price,
            "cost_basis_cents": self.effective_cost_basis_cents,
            "cost_price": self.cost_price,
            "entry_date": self.entry_date,
            "entry_day_low_cents": self.entry_day_low_cents,
            "stop_loss_cents": self.stop_loss_cents,
            "take_profit_cents": self.take_profit_cents,
            "highest_since_entry_cents": self.highest_since_entry_cents,
            "current_price_cents": self.current_price_cents,
            "unrealized_pnl_cents": self.unrealized_pnl_cents,
            "unrealized_pnl_pct": self.unrealized_pnl_pct,
        }


@dataclass
class Balance:
    scope: str  # "real" | "paper"
    cash_cents: int
    total_asset_cents: int
    weekly_buy_count: int = 0
    daily_pnl_cents: int = 0
    consecutive_loss_days: int = 0
    updated_at: str = ""

    @property
    def cash(self) -> float:
        return self.cash_cents / 100

    @property
    def total_asset(self) -> float:
        return self.total_asset_cents / 100


@dataclass
class TradeEvent:
    """统一交易事件记录。"""
    code: str
    name: str
    side: OrderSide
    shares: int
    price_cents: int
    realized_pnl_cents: int = 0
    reason: str = ""  # "score_buy" | "stop_loss" | "trailing_stop" | "manual"
    run_id: str = ""
    occurred_at: str = ""
