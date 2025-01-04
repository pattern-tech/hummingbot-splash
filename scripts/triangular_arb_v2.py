import asyncio
import os
import time
from asyncio import Future
from decimal import Decimal
from typing import Dict, List, Set, cast

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
    cex_connector_proxy: str = Field(
        default="kucoin",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter proxy CEX connector: ",
            prompt_on_new=True
        ))
    dex_connector: str = Field(
        default="splash_cardano_mainnet",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter DEX connector: ",
            prompt_on_new=True
        ))
    arb_asset: str = Field(
        default="ERG",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter main arbitrage asset: ",
            prompt_on_new=True
        ))
    arb_asset_wrapped: str = Field(
        default="RSERG",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter wrapped main arbitrage asset: ",
            prompt_on_new=True
        ))
    proxy_asset: str = Field(
        default="ADA",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter proxy asset: ",
            prompt_on_new=True
        ))
    stable_asset: str = Field(
        default="USDT",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter enter stable asset: ",
            prompt_on_new=True
        ))
    min_arbitrage_percent: Decimal = Field(
        default=Decimal("0.01"),
        client_data=ClientFieldData(
            prompt=lambda e: "Enter profitability percentage: ",
            prompt_on_new=True
        ))
    # In stable asset
    min_arbitrage_volume: Decimal = Field(
        default=Decimal("2"),
        client_data=ClientFieldData(
            prompt=lambda e: "Enter trading amount in (stable assets): ",
            prompt_on_new=True
        ))


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
                config.dex_connector: {dex_trading_pair}
                }
        else:
            markets = {
                config.cex_connector_proxy: {proxy_trading_pair},
                config.cex_connector_main: {main_trading_pair},
                config.dex_connector: {dex_trading_pair}
                   }
            
        cls.markets = markets

    def __init__(self, connectors: Dict[str, ConnectorBase], config: TriangularArbV2Config):
        super().__init__(connectors, config)
        self.config = config
        self.main_trading_pair = f"{config.arb_asset}-{config.stable_asset}" 
        self.proxy_trading_pair = f"{config.proxy_asset}-{config.stable_asset}" 
        self.dex_trading_pair = f"{config.arb_asset_wrapped}-{config.proxy_asset}"

    def arbitrage_config(self, direction: ArbitrageDirection, amounts: ArbitragePercent) -> TriangularArbExecutorConfig:

        cex_main = ConnectorPair(connector_name=self.config.cex_connector_main,
                                 trading_pair=self.main_trading_pair)

        dex = ConnectorPair(connector_name=self.config.dex_connector,
                            trading_pair=self.dex_trading_pair)

        return TriangularArbExecutorConfig(
            type="triangular_arb_executor",
            arb_asset=self.config.arb_asset,
            arb_asset_wrapped=self.config.arb_asset_wrapped,
            proxy_asset=self.config.proxy_asset,
            stable_asset=self.config.stable_asset,
            buying_market=cex_main if direction is ArbitrageDirection.FORWARD else dex,
            proxy_market=ConnectorPair(connector_name=self.config.cex_connector_proxy,
                                       trading_pair=self.proxy_trading_pair),
            selling_market=dex if direction is ArbitrageDirection.FORWARD else cex_main,
            order_amount=self.config.min_arbitrage_volume,
            min_profitability_percent=cast(Decimal, self.config.min_arbitrage_percent),
            max_retries=3,
            timestamp=time.time(),
            buy_amount=amounts.buy_amount,
            proxy_amount=amounts.proxy_amount,
            sell_amount=amounts.sell_amount
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
        executor_actions = []
        active_executors = self.filter_executors(
            executors=self.get_all_executors(),
            filter_func=lambda e: not e.is_done
        )
        
        #
        # Uncomment this if you want to test the stats and do only one trade once found an opportunity
        #
        # if self.one_time_trade:
        #     print("traded one time, not trading anymore")
        #     return []
        
        if len(active_executors) == 0:
            forward_arbitrage_percent = await self.estimate_arbitrage_percent(ArbitrageDirection.FORWARD)
            if forward_arbitrage_percent.percent >= self.config.min_arbitrage_percent:
                x = CreateExecutorAction(executor_config=self.arbitrage_config(ArbitrageDirection.FORWARD, forward_arbitrage_percent))
                executor_actions.append(x)
            else:
                backward_arbitrage_percent = await self.estimate_arbitrage_percent(ArbitrageDirection.BACKWARD)
                if backward_arbitrage_percent.percent >= self.config.min_arbitrage_percent:
                    x = CreateExecutorAction(executor_config=self.arbitrage_config(ArbitrageDirection.BACKWARD, backward_arbitrage_percent))
                    executor_actions.append(x)

        if len(executor_actions) == 0:
            return executor_actions
        else:
            self.one_time_trade = True
            return executor_actions[0]

    async def estimate_arbitrage_percent(self, direction: ArbitrageDirection) -> ArbitragePercent:
        forward = direction is ArbitrageDirection.FORWARD
        proxy_amount: Decimal = Decimal(0)
        p_arb_asset_wrapped_asset_in_proxy_asset: Decimal = Decimal(0)
        p_arb_asset_in_stable_asset: Decimal = Decimal(0)
    
        if forward:
            p_arb_asset_in_stable_asset = await self.connectors[self.config.cex_connector_main].get_quote_price(
                trading_pair=self.main_trading_pair,
                is_buy=not forward,
                amount=self.config.min_arbitrage_volume)
            proxy_amount = self.config.min_arbitrage_volume * p_arb_asset_in_stable_asset
        else:
            p_arb_asset_wrapped_asset_in_proxy_asset = await self.connectors[self.config.dex_connector].get_quote_price(
                trading_pair=self.dex_trading_pair,
                is_buy=forward,
                amount=self.config.min_arbitrage_volume)
            proxy_amount = p_arb_asset_wrapped_asset_in_proxy_asset * self.config.min_arbitrage_volume
    
        p_proxy_asset_in_stable_asset = await self.connectors[self.config.cex_connector_proxy].get_quote_price(
            trading_pair=self.proxy_trading_pair,
            is_buy=True if forward else False,
            amount=proxy_amount)
    
        if not forward:
            p_arb_asset_in_stable_asset = await self.connectors[self.config.cex_connector_main].get_quote_price(
                trading_pair=self.main_trading_pair,
                is_buy=not forward,
                amount=proxy_amount * p_proxy_asset_in_stable_asset)
        else:
            p_arb_asset_wrapped_asset_in_proxy_asset = await self.connectors[self.config.dex_connector].get_quote_price(
                trading_pair=self.dex_trading_pair,
                is_buy=forward,
                amount=proxy_amount * p_proxy_asset_in_stable_asset)
    
        buying_amount = self.config.min_arbitrage_volume
        selling_amount = proxy_amount * p_proxy_asset_in_stable_asset
    
        result = ArbitragePercent(
            get_arbitrage_percent(
                p_arb_asset_in_stable_asset,
                p_proxy_asset_in_stable_asset,
                p_arb_asset_wrapped_asset_in_proxy_asset
            ),
            buying_amount,
            proxy_amount,
            selling_amount,
        )
        
        return result


# Important: all prices must be given in Base/Quote format, assuming
# arb_asset, proxy_asset, arb_asset_wrapped are quote assets in corresponding pairs.
def get_arbitrage_percent(p_arb_asset_in_stable_asset: Decimal, p_proxy_asset_in_stable_asset: Decimal,
                          p_arb_asset_wrapped_in_proxy_asset: Decimal) -> Decimal:
    p_arb_asset_wrapped_in_stable_asset = p_proxy_asset_in_stable_asset * p_arb_asset_wrapped_in_proxy_asset
    price_diff = p_arb_asset_wrapped_in_stable_asset - p_arb_asset_in_stable_asset
    return price_diff * Decimal(100) / (
        p_arb_asset_wrapped_in_stable_asset if price_diff >= Decimal(0) else p_arb_asset_in_stable_asset)
