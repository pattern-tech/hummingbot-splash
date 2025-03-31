import asyncio
from decimal import Decimal
import logging
from typing import Coroutine, Dict, Optional, Union, cast

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.core.event.events import BuyOrderCreatedEvent, MarketOrderFailureEvent, SellOrderCreatedEvent
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.executors.triangular_arb_executor.data_types import (
    ArbitrageDirection,
    AsyncTrackedOrderFunction,
    Canceled,
    Completed,
    Failed,
    FailureReason,
    GraceFullStop,
    Idle,
    InProgress,
    TriangularArbExecutorConfig,
)
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


class TriangularArbExecutor(ExecutorBase):
    _logger = None
    _cumulative_failures: int = 0
    _splash_failures: int = 0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @property
    def is_closed(self):
        return type(self.state) is Completed or type(self.state) is Failed

    def __init__(self, strategy: ScriptStrategyBase, config: TriangularArbExecutorConfig, update_interval: float = 1.0):
        super().__init__(
            strategy=strategy,
            connectors=[
                config.buying_market.connector_name,
                config.proxy_market.connector_name,
                config.selling_market.connector_name,
            ],
            config=config,
            update_interval=update_interval,
        )

        arb_direction = is_valid_arbitrage(
            config.arb_asset,
            config.arb_asset_wrapped,
            config.proxy_asset,
            config.stable_asset,
            config.buying_market,
            config.proxy_market,
            config.selling_market,
        )
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
            self.confirm_round_callback = config.confirm_round_callback
            config.set_stop(self.early_stop)
            self.stopper_initiated = False
            self.real_arbitrage_percentage = config.real_arbitrage_percentage
            self.rollbacks: AsyncTrackedOrderFunction = []
            self.state: Idle | InProgress | Completed | Canceled | Failed | GraceFullStop = Idle()
        else:
            raise Exception("Arbitrage is not valid.")

    def buying_market(self) -> ConnectorBase:
        return self.connectors[self._buying_market.connector_name]

    def proxy_market(self) -> ConnectorBase:
        return self.connectors[self._proxy_market.connector_name]

    def selling_market(self) -> ConnectorBase:
        return self.connectors[self._selling_market.connector_name]

    async def validate_sufficient_balance(self):
        if self.arb_direction is ArbitrageDirection.FORWARD:
            buying_account_not_ok = self.buying_market().get_balance(self.proxy_asset) < self.buy_amount
            proxy_account_not_ok = self.proxy_market().get_balance(self.stable_asset) < self.proxy_amount
            selling_account_not_ok = self.selling_market().get_balance(self.arb_asset) < self.sell_amount

            if buying_account_not_ok or proxy_account_not_ok or selling_account_not_ok:
                self.state = Failed(FailureReason.INSUFFICIENT_BALANCE)
                self.logger().error("Not enough budget to open position.")
        else:
            buying_account_not_ok = self.buying_market().get_balance(self.stable_asset) < self.buy_amount
            selling_account_not_ok = self.proxy_market().get_balance(self.proxy_asset) < self.proxy_amount
            proxy_account_not_ok = self.selling_market().get_balance(self.arb_asset_wrapped) < self.sell_amount

            if buying_account_not_ok or proxy_account_not_ok or selling_account_not_ok:
                self.state = Failed(FailureReason.INSUFFICIENT_BALANCE)
                self.logger().error("Not enough budget to open position.")

    async def control_task(self):
        if self.config.buy_amount + self.config.sell_amount + self.config.proxy_amount == Decimal(0)  and self.stopper_initiated != True and self.real_arbitrage_percentage == Decimal(0):
            self.logger().info("initiating the stopper")
            self.config.set_stop(self.early_stop)
            self.stopper_initiated = True
            return
        
        if isinstance(self.state, GraceFullStop):
            self.logger().error("the bot is stopped, check the logs for errors !")
            return

        if isinstance(self.state, Idle):
            await self.init_arbitrage()

        if isinstance(self.state, Canceled):
            try:
                if not self.state.rollbacks:
                    self.logger().error("No rollback functions available!")
                    return  # Avoid unnecessary execution

                # Execute all rollbacks concurrently
                results = await asyncio.gather(*(fn() for fn in self.state.rollbacks), return_exceptions=True)

                # Handle exceptions from gather
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        self.logger().error(f"Rollback {i} failed: {result}")

                self.state = Idle()
                self.logger().error("OOR happened and all orders rolled back and cancelled !!")
                self.confirm_round_callback()
                self.stop()
            except Exception as e:
                self.logger().error(f"Rollback execution error: {e}")
                self.state = Failed(2)
                self.early_stop()
                self.stop()

        if isinstance(self.state, InProgress):
            # we check before the update order function to have access to the failed order hash and cancel and replace it

            if self._cumulative_failures > self.max_retries - 2 and self._splash_failures > self.max_retries - 2:
                self.logger().error("{err(OOR), msg(maximum splash errors threshold is reached), code(429)}")

                markets_map = [
                    {
                        self._buying_market.connector_name: (
                            "buy_order",
                            self.rollback_buy_order,
                            self._buying_market.trading_pair,
                        )
                    },
                    {
                        self._proxy_market.connector_name: (
                            "proxy_order",
                            self.rollback_proxy_order,
                            self._proxy_market.trading_pair,
                        )
                    },
                    {
                        self._selling_market.connector_name: (
                            "sell_order",
                            self.rollback_sell_order,
                            self._selling_market.trading_pair,
                        )
                    },
                ]

                for market_dict in markets_map:  # Loop through the list of dictionaries
                    for connector_name, (state_attr, rollback_func, pair) in market_dict.items():
                        if connector_name == "splash_cardano_mainnet":
                            order = getattr(self.state, state_attr)
                            tx_hash = order.order.exchange_order_id

                            # Cancel order
                            self.logger().info(f"Cancelling splash order, order id(tx hash): {tx_hash}")
                            cancel_hash = await self.cancel_splash_order("splash_cardano_mainnet", pair, tx_hash)
                            self.logger().info(f"Splash OOR order cancelled, canceller tx hash: {cancel_hash}")
                            self.logger().info("Confirming the cancel...")

                            # Handle cancellation result
                            if await self.confirm_cancel(cancel_hash):
                                self.logger().info("Cancel was successful ✔️")
                                order._order.current_state = OrderState.CANCELED
                                setattr(self.state, state_attr, order)
                                self.logger().info("Rolling back other trades ...")

                                rollbacks_coros: AsyncTrackedOrderFunction = []
                                for market_dict_inner in markets_map:
                                    for connector_name_inner, (
                                        state_attr_inner,
                                        rollback_func_inner,
                                        _,
                                    ) in market_dict_inner.items():
                                        if connector_name_inner != "splash_cardano_mainnet":
                                            rollbacks_coros.append(rollback_func_inner)

                                self._splash_failures = 0
                                self.state = Canceled(rollbacks_coros)
                            else:
                                self.logger().info(
                                    """Cancel was unsuccessful ❌, stopping...
                                       HINT: Check the logs and try to recover funds manually if there is a loss.
                                       You can restart the bot after manual checks are completed."""
                                )
                                self.stop()
                                self.early_stop()
                            return
        
        if self.state.buy_order.is_filled and self.state.proxy_order.is_filled and self.state.sell_order.is_filled:
            self.state = Completed(
                buy_order_exec_price=self.state.buy_order.average_executed_price,
                proxy_order_exec_price=self.state.proxy_order.average_executed_price,
                sell_order_exec_price=self.state.sell_order.average_executed_price,
            )
            self.confirm_round_callback()
            self.stop()

    # async def on_start(self):
    #     self.logger().info("the on start on executer")
    #     """
    #     This method is responsible for starting the executor and validating if the position is expired. The base method
    #     validates if there is enough balance to place the open order.

    #     :return: None
    #     """
    #     await self.validate_sufficient_balance()
        

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

    ## --------------------------------
    async def place_buy_order(self) -> TrackedOrder:
        market = self._buying_market
        order_id = self.place_order(
            connector_name=market.connector_name,
            trading_pair=market.trading_pair,
            order_type=OrderType.MARKET,
            side=TradeType.BUY,
            amount=self.buy_amount,
        )
        return TrackedOrder(order_id)

    async def rollback_buy_order(self) -> TrackedOrder:
        market = self._buying_market
        self.logger().info("rolling back the %s on %s", market.trading_pair, market.connector_name)
        order_id = self.place_order(
            connector_name=market.connector_name,
            trading_pair=market.trading_pair,
            order_type=OrderType.MARKET,
            side=TradeType.SELL,
            amount=self.buy_amount,
        )

        return TrackedOrder(order_id)

    ## --------------------------------
    async def place_proxy_order(self) -> TrackedOrder:
        market = self._proxy_market
        order_id = self.place_order(
            connector_name=market.connector_name,
            trading_pair=market.trading_pair,
            order_type=OrderType.MARKET,
            side=TradeType.BUY if self.arb_direction is ArbitrageDirection.FORWARD else TradeType.SELL,
            amount=self.proxy_amount,
        )
        return TrackedOrder(order_id)

    async def rollback_proxy_order(self) -> TrackedOrder:
        market = self._proxy_market
        self.logger().info("rolling back the %s on %s", market.trading_pair, market.connector_name)
        order_id = self.place_order(
            connector_name=market.connector_name,
            trading_pair=market.trading_pair,
            order_type=OrderType.MARKET,
            side=TradeType.SELL if self.arb_direction is ArbitrageDirection.FORWARD else TradeType.BUY,
            amount=self.proxy_amount,
        )

        return TrackedOrder(order_id)

    ## --------------------------------
    async def place_sell_order(self) -> TrackedOrder:
        market = self._selling_market

        order_id = self.place_order(
            connector_name=market.connector_name,
            trading_pair=market.trading_pair,
            order_type=OrderType.MARKET,
            side=TradeType.SELL,
            amount=self.sell_amount,
        )
        return TrackedOrder(order_id)

    async def rollback_sell_order(self) -> TrackedOrder:
        market = self._selling_market
        self.logger().info("rolling back the %s on %s", market.trading_pair, market.connector_name)
        order_id = self.place_order(
            connector_name=market.connector_name,
            trading_pair=market.trading_pair,
            order_type=OrderType.MARKET,
            side=TradeType.BUY,
            amount=self.sell_amount,
        )

        return TrackedOrder(order_id)

    ## --------------------------------

    async def cancel_splash_order(self, connector_name: str, pair: str, order_id: str) -> str:

        cancel_tx = self._strategy.cancel(connector_name, pair, order_id)
        cancel_hash = await cancel_tx
        return cancel_hash

    async def confirm_cancel(
        self, tx_hash: str, connector_name: str = "splash_cardano_mainnet", max_attempts: int = 20, delay: float = 5.0
    ) -> bool:
        """
        Confirms transaction cancellation by repeatedly checking until confirmed or max attempts are reached.

        Args:
            connector_name (str): Name of the connector.
            tx_hash (str): Transaction hash.
            max_attempts (int): Maximum number of attempts before giving up.
            delay (float): Delay between attempts in seconds.

        Returns:
            bool: True if cancellation is confirmed, False otherwise.
        """
        for attempt in range(max_attempts):
            cancel_res = await self.connectors[connector_name].confirm_cancel(tx_hash)
            if cancel_res:
                return True
            await asyncio.sleep(delay)

        return False

    def process_order_created_event(
        self, event_tag: int, market: ConnectorBase, event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]
    ):
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
        """
        Process a failed order event and attempt to recover by placing a new order
        if within retry limits.

        Args:
            market: The market where the order failed
            event (MarketOrderFailureEvent): The failure event details
        """
        self._cumulative_failures += 1

        if not isinstance(self.state, InProgress) or self._cumulative_failures >= self.max_retries:
            return

        order_mapping = {
            self.state.buy_order.order_id: ("buy_order", self.place_buy_order, self._buying_market),
            self.state.proxy_order.order_id: ("proxy_order", self.place_proxy_order, self._proxy_market),
            self.state.sell_order.order_id: ("sell_order", self.place_sell_order, self._selling_market),
        }

        if event.order_id not in order_mapping:
            return

        state_attr, place_order_func, relevant_market = order_mapping[event.order_id]

        if relevant_market.connector_name == "splash_cardano_mainnet":
            self._splash_failures += 1
        else:
            setattr(self.state, state_attr, asyncio.create_task(place_order_func()))

    def place_order(
        self,
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

        price = Decimal("1") if connector_name == "splash_cardano_mainnet" else Decimal("NaN")
        if side == TradeType.BUY:
            return self._strategy.buy(connector_name, trading_pair, amount, order_type, price, position_action)
        else:
            return self._strategy.sell(connector_name, trading_pair, amount, order_type, price, position_action)

    def on_stop(self):
        if self._status != RunnableStatus.TERMINATED:
            self.state = GraceFullStop
            self.close_type = CloseType.EARLY_STOP
            self.close_timestamp = self._strategy.current_timestamp
            self._cancel_order_forwarder
            super().stop()
            self.stop()

    def early_stop(self):
        self.on_stop()

    def get_net_pnl_quote(self) -> Decimal:
        return Decimal("0")

    def get_net_pnl_pct(self) -> Decimal:
        return Decimal("0")

    def get_cum_fees_quote(self) -> Decimal:
        return Decimal("0")


def is_valid_arbitrage(
    arb_asset: str,
    arb_asset_wrapped: str,
    proxy_asset: str,
    stable_asset: str,
    buying_market: ConnectorPair,
    proxy_market: ConnectorPair,
    selling_market: ConnectorPair,
) -> Optional[ArbitrageDirection]:
    buying_pair_assets = buying_market.trading_pair.split("-")
    proxy_pair_assets = proxy_market.trading_pair.split("-")
    selling_pair_assets = selling_market.trading_pair.split("-")
    proxy_market_ok = proxy_asset in proxy_pair_assets and stable_asset in proxy_pair_assets

    if arb_asset in buying_pair_assets:
        buying_market_ok = stable_asset in buying_pair_assets and arb_asset == buying_pair_assets[0]
        selling_market_ok = proxy_asset in selling_pair_assets and arb_asset_wrapped in selling_pair_assets
        if buying_market_ok and proxy_market_ok and selling_market_ok:
            return ArbitrageDirection.BACKWARD

    elif arb_asset in selling_pair_assets:
        buying_market_ok = proxy_asset in buying_pair_assets and arb_asset_wrapped == buying_pair_assets[0]
        selling_market_ok = stable_asset in selling_pair_assets
        if buying_market_ok and proxy_market_ok and selling_market_ok:
            return ArbitrageDirection.FORWARD

    return None
