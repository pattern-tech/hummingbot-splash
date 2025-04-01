import asyncio
import os
import time
from asyncio import Future
from decimal import Decimal
from typing import Callable, Dict, List, Set, Union, cast

from pydantic import Field
from dataclasses import dataclass

from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.triangular_arb_executor.data_types import (
    ArbitrageDirection,
    ArbitragePercent,
    GraceFullStop,
    TriExecuter,
    TriangularArbExecutorConfig,
)
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction, StopExecutorAction


class TriangularArbV2Config(StrategyV2ConfigBase):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    candles_config: List[CandlesConfig] = []
    controllers_config: List[str] = []
    markets: Dict[str, Set[str]] = {}
    cex_connector_main: str = Field(
        default="kucoin", client_data=ClientFieldData(prompt=lambda e: "Enter main CEX connector: ", prompt_on_new=True)
    )
    cex_connector_proxy: str = Field(
        default="kucoin",
        client_data=ClientFieldData(prompt=lambda e: "Enter proxy CEX connector: ", prompt_on_new=True),
    )
    dex_connector: str = Field(
        default="splash_cardano_mainnet",
        client_data=ClientFieldData(prompt=lambda e: "Enter DEX connector: ", prompt_on_new=True),
    )
    arb_asset: str = Field(
        default="ERG", client_data=ClientFieldData(prompt=lambda e: "Enter main arbitrage asset: ", prompt_on_new=True)
    )
    arb_asset_wrapped: str = Field(
        default="RSERG",
        client_data=ClientFieldData(prompt=lambda e: "Enter wrapped main arbitrage asset: ", prompt_on_new=True),
    )
    proxy_asset: str = Field(
        default="ADA", client_data=ClientFieldData(prompt=lambda e: "Enter proxy asset: ", prompt_on_new=True)
    )
    stable_asset: str = Field(
        default="USDT", client_data=ClientFieldData(prompt=lambda e: "Enter enter stable asset: ", prompt_on_new=True)
    )
    min_arbitrage_percent: Decimal = Field(
        default=Decimal("0.01"),
        client_data=ClientFieldData(prompt=lambda e: "Enter profitability percentage: ", prompt_on_new=True),
    )
    # In stable asset
    min_arbitrage_volume: Decimal = Field(
        default=Decimal("2"),
        client_data=ClientFieldData(prompt=lambda e: "Enter trading amount (in arb asset): ", prompt_on_new=True),
    )


class TriangularArbV2(StrategyV2Base):
    _arb_task: Future = None
    one_time_trade: bool = False

    @classmethod
    def init_markets(cls, config: TriangularArbV2Config):
        markets = {}
        main_trading_pair = f"{config.arb_asset}-{config.stable_asset}"
        proxy_trading_pair = f"{config.proxy_asset}-{config.stable_asset}"
        dex_trading_pair = f"{config.arb_asset_wrapped}-{config.proxy_asset}"

        if config.cex_connector_proxy == config.cex_connector_main:
            markets = {
                config.cex_connector_main: {main_trading_pair, proxy_trading_pair},
                config.dex_connector: {dex_trading_pair},
            }
        else:
            markets = {
                config.cex_connector_proxy: {proxy_trading_pair},
                config.cex_connector_main: {main_trading_pair},
                config.dex_connector: {dex_trading_pair},
            }

        cls.markets = markets

    def __init__(self, connectors: Dict[str, ConnectorBase], config: TriangularArbV2Config):
        super().__init__(connectors, config)
        self.config = config
        self.main_trading_pair = f"{config.arb_asset}-{config.stable_asset}"
        self.proxy_trading_pair = f"{config.proxy_asset}-{config.stable_asset}"
        self.dex_trading_pair = f"{config.arb_asset_wrapped}-{config.proxy_asset}"
        self.previous_round_confirmed = True
        self.executor_stopper: Union[Callable[[TriExecuter], None], None] = None
        self.executor_called: bool = False
        self.latest_action_exec_id: str = ""
        
        
    def arbitrage_config(self, direction: ArbitrageDirection, amounts: ArbitragePercent) -> TriangularArbExecutorConfig:

        cex_main = ConnectorPair(connector_name=self.config.cex_connector_main, trading_pair=self.main_trading_pair)

        dex = ConnectorPair(connector_name=self.config.dex_connector, trading_pair=self.dex_trading_pair)

        return TriangularArbExecutorConfig(
            type="triangular_arb_executor",
            arb_asset=self.config.arb_asset,
            arb_asset_wrapped=self.config.arb_asset_wrapped,
            proxy_asset=self.config.proxy_asset,
            stable_asset=self.config.stable_asset,
            buying_market=dex if direction is ArbitrageDirection.FORWARD else cex_main,
            proxy_market=ConnectorPair(
                connector_name=self.config.cex_connector_proxy, trading_pair=self.proxy_trading_pair
            ),
            selling_market=cex_main if direction is ArbitrageDirection.FORWARD else dex,
            order_amount=self.config.min_arbitrage_volume,
            min_profitability_percent=cast(Decimal, self.config.min_arbitrage_percent),
            max_retries=12,  # default for splash OOR recognition, lower number are not suggested !
            timestamp=time.time(),
            buy_amount=amounts.buy_amount,
            proxy_amount=amounts.proxy_amount,
            sell_amount=amounts.sell_amount,
            confirm_round_callback=self.confirm_round,
            set_stop=self.set_stop,
            stopper_init=True if self.executor_stopper == None else False,
            real_arbitrage_percentage=amounts.percent
        )

    def on_tick(self):
        if self.executor_called == False:
            self.update_executors_info()
            self.update_controllers_configs()
            if self.market_data_provider.ready:
                executor_actions: List[ExecutorAction] = self.determine_executor_actions()
                for action in executor_actions:
                    self.executor_orchestrator.execute_action(action)
    
    def determine_executor_actions(self) -> List[ExecutorAction]:
        executor_actions = []
        if self._arb_task is None:
            self._arb_task = safe_ensure_future(self.try_create_arbitrage_action())
        elif self._arb_task.done():
            executor_actions.append(self._arb_task.result())
            self._arb_task = safe_ensure_future(self.try_create_arbitrage_action())
            self.logger().debug("sending this action %s and this called %s and this executer %s", executor_actions,self.executor_called,self.executor_stopper)
        return executor_actions

    async def try_create_arbitrage_action(self) -> List[ExecutorAction]:
        executor_actions = []
        active_executors = self.filter_executors(
            executors=self.get_all_executors(), filter_func=lambda e: not e.is_done
        )


        if self.executor_called == True:
            return [StopExecutorAction(executor_id= self.latest_action_exec_id)]
        if self.executor_stopper == None:
            fake_action = CreateExecutorAction(
                executor_config=self.arbitrage_config(
                    ArbitrageDirection.FORWARD,
                    ArbitragePercent(
                        Decimal(0),
                        Decimal(0),
                        Decimal(0),
                        Decimal(0),
                    ),
                )
            )
            
            fake_action.controller_id = fake_action.executor_config.id
            self.latest_action_exec_id = fake_action.executor_config.id
            return fake_action
        #
        # Uncomment this if you want to test the stats and do only one trade once found an opportunity
        #
        # if self.one_time_trade:
        #     print("traded one time, not trading anymore")
        #     return []

        if not self.previous_round_confirmed:
            print("Wait until next round gets confirmed")
            return []

        if len(active_executors) == 0:
            forward_arbitrage_percent = await self.estimate_arbitrage_percent(ArbitrageDirection.FORWARD)
            if forward_arbitrage_percent.percent >= self.config.min_arbitrage_percent:
                x = CreateExecutorAction(
                    executor_config=self.arbitrage_config(ArbitrageDirection.FORWARD, forward_arbitrage_percent)
                )
                x.controller_id = x.executor_config.id
                self.latest_action_exec_id = x.executor_config.id
                executor_actions.append(x)
            else:
                backward_arbitrage_percent = await self.estimate_arbitrage_percent(ArbitrageDirection.BACKWARD)
                if backward_arbitrage_percent.percent >= self.config.min_arbitrage_percent:
                    x = CreateExecutorAction(
                        executor_config=self.arbitrage_config(ArbitrageDirection.BACKWARD, backward_arbitrage_percent)
                    )
                    x.controller_id = x.executor_config.id
                    self.latest_action_exec_id = x.executor_config.id
                    executor_actions.append(x)

        if len(executor_actions) == 0:
            return executor_actions
        else:
            self.one_time_trade = True
            self.previous_round_confirmed = False
            return executor_actions[0]

    async def estimate_arbitrage_percent(self, direction: ArbitrageDirection) -> ArbitragePercent:
        forward = direction is ArbitrageDirection.FORWARD
        result = None
        self.logger().info("is direction forward: %s", forward)
        try:
            # forward
            if forward:
                p_arb_asset_wrapped_asset_in_proxy_asset = await self.connectors[
                    self.config.dex_connector
                ].get_quote_price(
                    trading_pair=self.dex_trading_pair, is_buy=True, amount=self.config.min_arbitrage_volume
                )
                proxy_amount = self.config.min_arbitrage_volume * p_arb_asset_wrapped_asset_in_proxy_asset

                p_proxy_asset_in_stable_asset = await self.connectors[self.config.cex_connector_proxy].get_quote_price(
                    trading_pair=self.proxy_trading_pair, is_buy=True, amount=proxy_amount
                )
                stable_amount = proxy_amount * p_proxy_asset_in_stable_asset

                p_arb_asset_in_stable_asset = await self.connectors[self.config.cex_connector_main].get_quote_price(
                    trading_pair=self.main_trading_pair, is_buy=False, amount=stable_amount
                )
                arb_asset_amount = stable_amount / p_arb_asset_in_stable_asset

                # ToDo: Must be changed for backward compatibility. Must be changed on the Splash SDK side.
                # right now if we say BUY we must pass in the proxy asset instead of arb asset
                # but the correct way is to say buy me x amount of arb asset not saying buy me x amount of arb asset using y amount of proxy asset.
                buying_amount = proxy_amount
                selling_amount = arb_asset_amount  # wrapped asset amount
                self.logger().info("FORWARD | arb price in stable: %s", p_arb_asset_in_stable_asset)
                self.logger().info("FORWARD | proxy price in stable: %s", p_proxy_asset_in_stable_asset)
                self.logger().info("FORWARD | wrapped price in proxy: %s", p_arb_asset_wrapped_asset_in_proxy_asset)
                self.logger().info("FORWARD | proxy amount: %s", proxy_amount)
                self.logger().info("FORWARD | stable amount: %s", stable_amount)
                self.logger().info("FORWARD | arb amount: %s", arb_asset_amount)

                result = ArbitragePercent(
                    get_arbitrage_percent(
                        p_arb_asset_in_stable_asset,
                        p_proxy_asset_in_stable_asset,
                        p_arb_asset_wrapped_asset_in_proxy_asset,
                        forward,
                    ),
                    proxy_amount,  # buying_market amount
                    proxy_amount,  # proxy_market amount
                    selling_amount,  # buying_market amount
                )
                self.logger().info("FORWARD  : %s", result)
                return result

            if not forward:
                p_arb_asset_wrapped_asset_in_proxy_asset = await self.connectors[
                    self.config.dex_connector
                ].get_quote_price(
                    trading_pair=self.dex_trading_pair, is_buy=False, amount=self.config.min_arbitrage_volume
                )
                proxy_amount = p_arb_asset_wrapped_asset_in_proxy_asset * self.config.min_arbitrage_volume

                p_proxy_asset_in_stable_asset = await self.connectors[self.config.cex_connector_proxy].get_quote_price(
                    trading_pair=self.proxy_trading_pair, is_buy=False, amount=proxy_amount
                )
                stable_amount = proxy_amount * p_proxy_asset_in_stable_asset

                p_arb_asset_in_stable_asset = await self.connectors[self.config.cex_connector_main].get_quote_price(
                    trading_pair=self.main_trading_pair, is_buy=True, amount=stable_amount
                )
                arb_asset_amount = stable_amount / p_arb_asset_in_stable_asset

                self.logger().info("BACKWARD | arb price in stable: %s", p_arb_asset_in_stable_asset)
                self.logger().info("BACKWARD | proxy price in stable: %s", p_proxy_asset_in_stable_asset)
                self.logger().info("BACKWARD | wrapped price in proxy: %s", p_arb_asset_wrapped_asset_in_proxy_asset)
                self.logger().info("BACKWARD | proxy amount: %s", proxy_amount)
                self.logger().info("BACKWARD | stable amount: %s", stable_amount)
                self.logger().info("BACKWARD | arb amount: %s", arb_asset_amount)

                buying_amount = arb_asset_amount  # wrapped asset amount
                selling_amount = self.config.min_arbitrage_volume
                result = ArbitragePercent(
                    get_arbitrage_percent(
                        p_arb_asset_in_stable_asset,
                        p_proxy_asset_in_stable_asset,
                        p_arb_asset_wrapped_asset_in_proxy_asset,
                        forward,
                    ),
                    buying_amount,  # buying_market amount
                    proxy_amount,  # proxy_market amount
                    selling_amount,  # buying_market amount
                )
                self.logger().info("BACKWARD  : %s", result)
                return result
        except Exception as e:
            msg = f"estimating the arb percent failed due to {str(e)}"
            print(msg)
            self.logger().error("%s", msg)
            return ArbitragePercent(
                Decimal(0),
                Decimal(0),
                Decimal(0),
                Decimal(0),
            )

    def cancel(self, connector_name: str, trading_pair: str, order_id: str):

        return self.connectors[connector_name].cancel(order_id)

    def set_stop(self, early_stop: Callable[[TriExecuter], None]):
        self.executor_stopper = early_stop

    async def on_stop(self):
        self.executor_stopper()
        self.executor_called = True
        await super().on_stop()
            
    def confirm_round(self):
        print("All orders have been filled")
        self.previous_round_confirmed = True


# Important: all prices must be given in Base/Quote format, assuming
# arb_asset, proxy_asset, arb_asset_wrapped are quote assets in corresponding pairs.
def get_arbitrage_percent(
    p_arb_asset_in_stable_asset: Decimal,
    p_proxy_asset_in_stable_asset: Decimal,
    p_arb_asset_wrapped_in_proxy_asset: Decimal,
    forward: bool,
) -> Decimal:
    p_arb_asset_wrapped_in_stable_asset = p_proxy_asset_in_stable_asset * p_arb_asset_wrapped_in_proxy_asset
    price_diff = (
        p_arb_asset_wrapped_in_stable_asset - p_arb_asset_in_stable_asset
        if not forward
        else p_arb_asset_in_stable_asset - p_arb_asset_wrapped_in_stable_asset
    )
    return price_diff * Decimal(100) / (p_arb_asset_wrapped_in_stable_asset if forward else p_arb_asset_in_stable_asset)
