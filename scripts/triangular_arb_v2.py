import asyncio
import os
import time
from asyncio import Future
from decimal import Decimal
from typing import Dict, List, Set, cast

from pydantic import Field

from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.triangular_arb_executor.data_types import (
    ArbitrageDirection,
    TriangularArbExecutorConfig,
)
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction


class TriangularArbV2Config(StrategyV2ConfigBase):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    candles_config: List[CandlesConfig] = []
    controllers_config: List[str] = []
    markets: Dict[str, Set[str]] = {}
    cex_connector_main: str = Field(
        default="kucoin",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter main CEX connector: ",
            prompt_on_new=True
        ))
    cex_main_trading_pair: str = Field(
        default="ERG-USDT",
        client_data=ClientFieldData(
            prompt=lambda e: "Which pair would you like to use on main CEX connector( arb asset & stable asset): ",
            prompt_on_new=True
        ))
    cex_connector_proxy: str = Field(
        default="mexc",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter proxy CEX connector: ",
            prompt_on_new=True
        ))
    cex_proxy_trading_pair: str = Field(
        default="ADA-USDT",
        client_data=ClientFieldData(
            prompt=lambda e: "Which pair would you like to use on proxy CEX connector( proxy asset & stable asset): ",
            prompt_on_new=True
        ))
    dex_connector: str = Field(
        default="splash_cardano_mainnet",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter DEX connector: ",
            prompt_on_new=True
        ))
    dex_proxy_trading_pair: str = Field(
        default="rsERG-ADA",
        client_data=ClientFieldData(
            prompt=lambda e: "Which pair would you like to use on proxy DEX connector( arb asset wrapped & proxy asset): ",
            prompt_on_new=True
        ))
    arb_asset: str = Field(default="ERG")
    arb_asset_wrapped: str = Field(default="RSERG")
    proxy_asset: str = Field(default="ADA")
    stable_asset: str = Field(default="USDT")
    min_arbitrage_percent: Decimal = Field(default=Decimal("0.01"))   # We set a -10% profit threshold as a filter to help the program quickly identify potential opportunities
    # In stable asset
    min_arbitrage_volume: Decimal = Field(default=Decimal("2"))


class TriangularArbV2(StrategyV2Base):
    _arb_task: Future = None
    one_time_trade: bool = False

    @classmethod
    def init_markets(cls, config: TriangularArbV2Config):
        cls.cex_connector_proxy = "_kucoin"
        config.cex_connector_proxy = "_kucoin"
        cls.markets = {config.cex_connector_main: {config.cex_main_trading_pair},
                       config.cex_connector_proxy: {config.cex_proxy_trading_pair},
                       config.dex_connector: {config.dex_proxy_trading_pair}, }

    def __init__(self, connectors: Dict[str, ConnectorBase], config: TriangularArbV2Config):
        super().__init__(connectors, config)
        self.config = config

    def arbitrage_config(self, direction: ArbitrageDirection) -> TriangularArbExecutorConfig:
        dex_trading_pair = next(iter(self.markets[self.config.dex_connector]))

        cex_main = ConnectorPair(connector_name=self.config.cex_connector_main,
                                 trading_pair=self.config.cex_main_trading_pair)
        dex = ConnectorPair(connector_name=self.config.dex_connector,
                            trading_pair=dex_trading_pair)

        return TriangularArbExecutorConfig(
            type="triangular_arb_executor",
            arb_asset=self.config.arb_asset,
            arb_asset_wrapped=self.config.arb_asset_wrapped,
            proxy_asset=self.config.proxy_asset,
            stable_asset=self.config.stable_asset,
            buying_market=cex_main if direction is ArbitrageDirection.FORWARD else dex,
            proxy_market=ConnectorPair(connector_name=self.config.cex_connector_proxy,
                                       trading_pair=self.config.dex_proxy_trading_pair),
            selling_market=dex if direction is ArbitrageDirection.FORWARD else cex_main,
            order_amount=self.config.min_arbitrage_volume,
            min_profitability_percent=cast(Decimal, 1),
            max_retries=3,
            timestamp=time.time(),
        )

    def determine_executor_actions(self) -> List[ExecutorAction]:
        executor_actions = []
        if self._arb_task is None:
            self._arb_task = safe_ensure_future(self.try_create_arbitrage_action())

        elif self._arb_task.done():
            executor_actions.append(self._arb_task.result())
            self._arb_task = safe_ensure_future(self.try_create_arbitrage_action())

        return executor_actions

    async def try_create_arbitrage_action(self) -> List[ExecutorAction]:
        if self.one_time_trade:
            self.logger().error("one action already created, check the balances")
            return []

        executor_actions = []
        active_executors = self.filter_executors(
            executors=self.get_all_executors(),
            filter_func=lambda e: not e.is_done
        )

        if len(active_executors) == 0:
            forward_arbitrage_percent = await self.estimate_arbitrage_percent(ArbitrageDirection.FORWARD)
            backward_arbitrage_percent = await self.estimate_arbitrage_percent(ArbitrageDirection.BACKWARD)
            forward_arbitrage_percent = await self.estimate_arbitrage_percent(ArbitrageDirection.FORWARD)
            self.logger().info("this is the forward percentage %s and this is the backward percentage %s", forward_arbitrage_percent, backward_arbitrage_percent)
            if forward_arbitrage_percent >= self.config.min_arbitrage_percent:
                x = CreateExecutorAction(executor_config=self.arbitrage_config(ArbitrageDirection.FORWARD))
                executor_actions.append(x)
            else:
                backward_arbitrage_percent = await self.estimate_arbitrage_percent(ArbitrageDirection.BACKWARD)
                if backward_arbitrage_percent >= self.config.min_arbitrage_percent:
                    x = CreateExecutorAction(executor_config=self.arbitrage_config(ArbitrageDirection.BACKWARD))
                    executor_actions.append(x)
        if len(executor_actions) == 0:
            return executor_actions
        else:
            self.one_time_trade = True
            return executor_actions[0]

    async def estimate_arbitrage_percent(self, direction: ArbitrageDirection) -> Decimal:
        forward = direction is ArbitrageDirection.FORWARD

        # Get the trading pairs as strings from the sets
        main_trading_pair = f"{self.config.arb_asset}-{self.config.stable_asset}"
        proxy_trading_pair = f"{self.config.proxy_asset}-{self.config.stable_asset}"
        dex_trading_pair = next(iter(self.markets[self.config.dex_connector]))

        p_arb_asset_in_stable_asset = self.connectors[self.config.cex_connector_main].get_quote_price(
            trading_pair=main_trading_pair,
            is_buy=forward,
            amount=self.config.min_arbitrage_volume)

        p_proxy_asset_in_stable_asset = self.connectors[self.config.cex_connector_proxy].get_quote_price(
            trading_pair=proxy_trading_pair,
            is_buy=not forward,
            amount=self.config.min_arbitrage_volume)

        p_arb_asset_in_stable_asset, p_proxy_asset_in_stable_asset = await asyncio.gather(
            p_arb_asset_in_stable_asset,
            p_proxy_asset_in_stable_asset)
        arb_vol_in_proxy_asset = self.config.min_arbitrage_volume / p_proxy_asset_in_stable_asset
        p_arb_asset_wrapped_asset_in_proxy_asset = await self.connectors[self.config.dex_connector].get_quote_price(
            trading_pair=dex_trading_pair,
            is_buy=not forward,
            amount=arb_vol_in_proxy_asset)

        return get_arbitrage_percent(
            p_arb_asset_in_stable_asset,
            p_proxy_asset_in_stable_asset,
            p_arb_asset_wrapped_asset_in_proxy_asset
        )
# Important: all prices must be given in Base/Quote format, assuming
# arb_asset, proxy_asset, arb_asset_wrapped are quote assets in corresponding pairs.


def get_arbitrage_percent(p_arb_asset_in_stable_asset: Decimal, p_proxy_asset_in_stable_asset: Decimal,
                          p_arb_asset_wrapped_in_proxy_asset: Decimal) -> Decimal:
    p_arb_asset_wrapped_in_stable_asset = p_proxy_asset_in_stable_asset * p_arb_asset_wrapped_in_proxy_asset
    price_diff = p_arb_asset_wrapped_in_stable_asset - p_arb_asset_in_stable_asset
    return price_diff * Decimal(100) / (
        p_arb_asset_wrapped_in_stable_asset if price_diff >= Decimal(0) else p_arb_asset_in_stable_asset)
