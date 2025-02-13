from decimal import Decimal
from enum import Enum

from typing import Callable, Protocol

from hummingbot.core.data_type.in_flight_order import InFlightOrder
from hummingbot.strategy_v2.executors.data_types import ConnectorPair, ExecutorConfigBase
from hummingbot.strategy_v2.models.executors import TrackedOrder
from dataclasses import dataclass

class ArbitrageDirection(Enum):
    FORWARD = 0
    BACKWARD = 1


class Strategy(Protocol):
    def confirm_round(self) -> None: ...

class TriangularArbExecutorConfig(ExecutorConfigBase):
    type: str = "triangular_arb_executor"
    arb_asset: str
    arb_asset_wrapped: str
    proxy_asset: str
    stable_asset: str
    buying_market: ConnectorPair
    proxy_market: ConnectorPair
    selling_market: ConnectorPair
    order_amount: Decimal
    min_profitability_percent: Decimal = Decimal("1.5")
    buy_amount: Decimal
    proxy_amount: Decimal
    sell_amount: Decimal
    max_retries: int = 12 # if we consider 2 mins max per poll in splash, 2 min in 15 sec = 8 + 2(+-) ext threshold = 10 => the order is oor
    confirm_round_callback: Callable[[Strategy], None] 

@dataclass
class ArbitragePercent:
    percent: Decimal
    buy_amount: Decimal
    proxy_amount: Decimal
    sell_amount: Decimal


class Idle:
    pass

class GraceFullStop:
    pass

class InProgress:
    def __init__(self, buy_order: TrackedOrder, proxy_order: TrackedOrder, sell_order: TrackedOrder):
        self._buy_order: TrackedOrder = buy_order
        self._proxy_order: TrackedOrder = proxy_order
        self._sell_order: TrackedOrder = sell_order

    @property
    def buy_order(self) -> TrackedOrder:
        return self._buy_order

    @buy_order.setter
    def buy_order(self, order: TrackedOrder):
        self._buy_order = order

    def update_buy_order(self, order: InFlightOrder):
        self._buy_order.order = order

    @property
    def proxy_order(self) -> TrackedOrder:
        return self._proxy_order

    @proxy_order.setter
    def proxy_order(self, order: TrackedOrder):
        self._proxy_order = order

    def update_proxy_order(self, order: InFlightOrder):
        self._proxy_order.order = order

    @property
    def sell_order(self) -> TrackedOrder:
        return self._sell_order

    @sell_order.setter
    def sell_order(self, order: TrackedOrder):
        self._sell_order = order

    def update_sell_order(self, order: InFlightOrder):
        self._sell_order.order = order


@dataclass
class Completed:
    buy_order_exec_price: Decimal
    proxy_order_exec_price: Decimal
    sell_order_exec_price: Decimal


class FailureReason(Enum):
    INSUFFICIENT_BALANCE = 0
    TOO_MANY_FAILURES = 1


class Failed:
    def __init__(self, reason: FailureReason):
        self.reason: FailureReason = reason