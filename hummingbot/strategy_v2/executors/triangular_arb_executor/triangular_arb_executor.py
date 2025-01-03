import asyncio
from decimal import Decimal
import logging
from typing import Optional, Union, cast

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.core.event.events import BuyOrderCreatedEvent, MarketOrderFailureEvent, SellOrderCreatedEvent
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.executors.triangular_arb_executor.data_types import (
    ArbitrageDirection,
    Completed,
    Failed,
    FailureReason,
    Idle,
    InProgress,
    TriangularArbExecutorConfig,
)
from hummingbot.strategy_v2.models.executors import TrackedOrder


class TriangularArbExecutor(ExecutorBase):
    _logger = None
    _cumulative_failures: int = 0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @property
    def is_closed(self):
        return type(self.state) is Completed or type(self.state) is Failed

    def __init__(self, strategy: ScriptStrategyBase, config: TriangularArbExecutorConfig, update_interval: float = 1.0):
        super().__init__(strategy=strategy,
                         connectors=[config.buying_market.connector_name,
                                     config.proxy_market.connector_name,
                                     config.selling_market.connector_name],
                         config=config, update_interval=update_interval)

        arb_direction = is_valid_arbitrage(config.arb_asset, config.arb_asset_wrapped, config.proxy_asset,
                                           config.stable_asset, config.buying_market, config.proxy_market,
                                           config.selling_market)
        if arb_direction:
            self.arb_direction: ArbitrageDirection = arb_direction

            self._buying_market = config.buying_market
            self._proxy_market = config.proxy_market
            self._selling_market = config.selling_market

            self.arb_asset = config.arb_asset
            self.arb_asset_wrapped = config.arb_asset_wrapped
            self.proxy_asset = config.proxy_asset
            self.stable_asset = config.stable_asset
            self.order_amount = config.order_amount
            self.min_profitability_percent = config.min_profitability_percent
            self.max_retries = config.max_retries
            self.buy_amount = config.buy_amount
            self.proxy_amount = config.proxy_amount
            self.sell_amount = config.sell_amount
            self.state: Idle | InProgress | Completed | Failed = Idle()
        else:
            raise Exception("Arbitrage is not valid.")

    def buying_market(self) -> ConnectorBase:
        return self.connectors[self._buying_market.connector_name]

    def proxy_market(self) -> ConnectorBase:
        return self.connectors[self._proxy_market.connector_name]

    def selling_market(self) -> ConnectorBase:
        return self.connectors[self._selling_market.connector_name]

    def validate_sufficient_balance(self):
        if self.arb_direction is ArbitrageDirection.FORWARD:
            buying_account_not_ok = self.buying_market().get_balance(self.stable_asset) < self.order_amount
            proxy_account_not_ok = self.proxy_market().get_balance(self.proxy_asset) < self.order_amount
            selling_account_not_ok = self.selling_market().get_balance(self.arb_asset_wrapped) < self.order_amount

            if buying_account_not_ok or proxy_account_not_ok or selling_account_not_ok:
                self.state = Failed(FailureReason.INSUFFICIENT_BALANCE)
                self.logger().error("Not enough budget to open position.")
        else:
            buying_account_not_ok = self.buying_market().get_balance(self.proxy_asset) < self.order_amount
            proxy_account_not_ok = self.selling_market().get_balance(self.stable_asset) < self.order_amount
            selling_account_not_ok = self.proxy_market().get_balance(self.arb_asset) < self.order_amount

            if buying_account_not_ok or proxy_account_not_ok or selling_account_not_ok:
                self.state = Failed(FailureReason.INSUFFICIENT_BALANCE)
                self.logger().error("Not enough budget to open position.")

    async def control_task(self):
        if type(self.state) is Idle:
            await self.init_arbitrage()
        elif type(self.state) is InProgress:
            state = self.state
            if self._cumulative_failures > self.max_retries:
                self.state = Failed(FailureReason.TOO_MANY_FAILURES)
                self.stop()
            elif state.buy_order.is_filled and state.proxy_order.is_filled and state.sell_order.is_filled:
                self.state = Completed(buy_order_exec_price=state.buy_order.average_executed_price,
                                       proxy_order_exec_price=state.proxy_order.average_executed_price,
                                       sell_order_exec_price=state.sell_order.average_executed_price)
                self.stop()

    async def init_arbitrage(self):
        buy_order = asyncio.create_task(self.place_buy_order())
        proxy_order = asyncio.create_task(self.place_proxy_order())
        sell_order = asyncio.create_task(self.place_sell_order())
        buy_order, proxy_order, sell_order = await asyncio.gather(buy_order, proxy_order, sell_order)
        self.state = InProgress(
            buy_order=buy_order,
            proxy_order=proxy_order,
            sell_order=sell_order,
        )
    # forward = erg -> usdt -> ada -> rserg
    # backward rserg -> ada -> usdt -> erg 
    # main = (erg-usdt) sell erg -> usdt
    # proxy = (ada-usdt) buy usdt -> ada
    # dex = (rsERG-ada) sell ada -> rserg
    async def place_buy_order(self) -> TrackedOrder:
        market = self._buying_market
        order_id = self.place_order(connector_name=market.connector_name, trading_pair=market.trading_pair,
                                    order_type=OrderType.MARKET, side=TradeType.SELL if self.arb_direction is ArbitrageDirection.FORWARD else TradeType.BUY, amount=self.buy_amount)
        return TrackedOrder(order_id) # erg -> usdt

    async def place_proxy_order(self) -> TrackedOrder:
        market = self._proxy_market
        order_id = self.place_order(connector_name=market.connector_name, trading_pair=market.trading_pair,
                                    order_type=OrderType.MARKET,
                                    side=TradeType.BUY if self.arb_direction is ArbitrageDirection.FORWARD else TradeType.SELL,
                                    amount=self.proxy_amount)
        return TrackedOrder(order_id)
     
    async def place_sell_order(self) -> TrackedOrder:
        market = self._selling_market
        
        order_id = self.place_order(connector_name=market.connector_name, trading_pair=market.trading_pair,
                                    order_type=OrderType.MARKET, side=TradeType.SELL if self.arb_direction is ArbitrageDirection.FORWARD else TradeType.BUY, amount=self.sell_amount)
        return TrackedOrder(order_id)

    def process_order_created_event(self,
                                    event_tag: int,
                                    market: ConnectorBase,
                                    event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        if type(self.state) is InProgress:
            order_id = event.order_id
            if order_id == self.state.buy_order.order_id:
                self.logger().info("Buy order created")
                self.state.update_buy_order(self.get_in_flight_order(self._buying_market.connector_name, order_id))
            elif order_id == self.state.proxy_order.order_id:
                self.logger().info("Proxy order created")
                self.state.update_proxy_order(self.get_in_flight_order(self._proxy_market.connector_name, order_id))
            elif order_id == self.state.sell_order.order_id:
                self.logger().info("Sell order created")
                self.state.update_sell_order(self.get_in_flight_order(self._selling_market.connector_name, order_id))

    def process_order_failed_event(self, _, market, event: MarketOrderFailureEvent):
        self._cumulative_failures += 1
        if type(self.state) is InProgress and self._cumulative_failures < self.max_retries:
            order_id = event.order_id
            if order_id == self.state.buy_order.order_id:
                self.state.buy_order = asyncio.run(self.place_sell_order())
            elif order_id == self.state.proxy_order.order_id:
                self.state.proxy_order = asyncio.run(self.place_proxy_order())
            elif order_id == self.state.sell_order.order_id:
                self.state.sell_order = asyncio.run(self.place_sell_order())

    def place_order(self,
                    connector_name: str,
                    trading_pair: str,
                    order_type: OrderType,
                    side: TradeType,
                    amount: Decimal,
                    position_action: PositionAction = PositionAction.NIL,
                    price=Decimal("NaN"),
                    ):
        """
        Places an order with the specified parameters.

        :param connector_name: The name of the connector.
        :param trading_pair: The trading pair for the order.
        :param order_type: The type of the order.
        :param side: The side of the order (buy or sell).
        :param amount: The amount for the order.
        :param position_action: The position action for the order.
        :param price: The price for the order.
        :return: The result of the order placement.
        """
        self.arb_direction
        price = Decimal("1") if connector_name == "splash_cardano_mainnet" else Decimal("NaN")
        if side == TradeType.BUY:
            return self._strategy.buy(connector_name, trading_pair, amount, order_type, price, position_action)
        else:
            return self._strategy.sell(connector_name, trading_pair, amount, order_type, price, position_action)

def is_valid_arbitrage(arb_asset: str,
                       arb_asset_wrapped: str,
                       proxy_asset: str,
                       stable_asset: str,
                       buying_market: ConnectorPair,
                       proxy_market: ConnectorPair,
                       selling_market: ConnectorPair) -> Optional[ArbitrageDirection]:
    buying_pair_assets = buying_market.trading_pair.split("-")
    proxy_pair_assets = proxy_market.trading_pair.split("-")
    selling_pair_assets = selling_market.trading_pair.split("-")
    proxy_market_ok = proxy_asset in proxy_pair_assets and stable_asset in proxy_pair_assets

    if arb_asset in buying_pair_assets:
        buying_market_ok = stable_asset in buying_pair_assets and arb_asset == buying_pair_assets[0]
        selling_market_ok = proxy_asset in selling_pair_assets and arb_asset_wrapped in selling_pair_assets
        if buying_market_ok and proxy_market_ok and selling_market_ok:
            return ArbitrageDirection.FORWARD
        
    elif arb_asset in selling_pair_assets:
        buying_market_ok = proxy_asset in buying_pair_assets and arb_asset_wrapped == buying_pair_assets[0]
        selling_market_ok = stable_asset in selling_pair_assets
        if buying_market_ok and proxy_market_ok and selling_market_ok:
            return ArbitrageDirection.BACKWARD
        
    return None
