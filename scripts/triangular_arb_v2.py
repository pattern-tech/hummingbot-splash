import asyncio
import os
from asyncio import Future
from decimal import Decimal
import time
from typing import Dict, List, Set, cast

from hummingbot.client.settings import AllConnectorSettings
from hummingbot.connector.gateway.amm.gateway_cardano_amm import GatewayCardanoAMM
from hummingbot.connector.gateway.amm.gateway_evm_amm import GatewayEVMAMM
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
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
    arb_asset: str = Field(default="ERG")
    arb_asset_wrapped: str = Field(default="rsERG")
    proxy_asset: str = Field(default="ADA")
    stable_asset: str = Field(default="USDT")
    min_arbitrage_percent: Decimal = Field(default=Decimal("-10"))
    # In stable asset
    min_arbitrage_volume: Decimal = Field(default=Decimal("2"))


class TriangularArbV2(StrategyV2Base):
    _arb_task: Future = None

    @classmethod
    def init_markets(cls, config: TriangularArbV2Config):
         cls.cex_connector_proxy = "_kucoin"
         config.cex_connector_proxy = "_kucoin"
         cls.markets = {config.cex_connector_main: {f"{config.arb_asset}-{config.stable_asset}"},
                       config.cex_connector_proxy: {f"{config.proxy_asset}-{config.stable_asset}"},
                       config.dex_connector: {f"{config.arb_asset_wrapped}-{config.proxy_asset}"}, }

    def __init__(self, connectors: Dict[str, ConnectorBase], config: TriangularArbV2Config):
        super().__init__(connectors, config)
        self.config = config

    def arbitrage_config(self, direction: ArbitrageDirection) -> TriangularArbExecutorConfig:
        main_trading_pair = next(iter(self.markets[self.config.cex_connector_main]))
        proxy_trading_pair = next(iter(self.markets[self.config.cex_connector_proxy]))
        dex_trading_pair = next(iter(self.markets[self.config.dex_connector]))

        print(
            "trading pairssssssssssssssssssssssssssss",
            main_trading_pair,
            proxy_trading_pair,
            dex_trading_pair,
            "connectorr namessssssssssssssssssssssssssssssss",
            self.config.cex_connector_main,
            self.config.dex_connector,
            self.config.cex_connector_proxy,
            "end  connectorr namessssssssssssssssssssssssssssssss"
        )

        print(
            self.config.dex_connector
        )

        cex_main = ConnectorPair(connector_name=self.config.cex_connector_main,
                                 trading_pair=main_trading_pair)
        
        dex = ConnectorPair(connector_name=self.config.dex_connector,
                            trading_pair=dex_trading_pair)
        
        # splash = MarketTradingPairTuple(
        # self.markets[self.config.dex_connector]
        # )
        # dex: GatewayCardanoAMM = cast(GatewayCardanoAMM, splash.market)

        return TriangularArbExecutorConfig(
            type="triangular_arb_executor",
            arb_asset=self.config.arb_asset,
            arb_asset_wrapped=self.config.arb_asset_wrapped,
            proxy_asset=self.config.proxy_asset,
            stable_asset=self.config.stable_asset,
            buying_market=cex_main if direction is ArbitrageDirection.FORWARD else dex,
            proxy_market=ConnectorPair(connector_name="kucoin",
                                       trading_pair=proxy_trading_pair),
            selling_market=dex if direction is ArbitrageDirection.FORWARD else cex_main,
            order_amount=self.config.min_arbitrage_volume,
            min_profitability_percent=cast(Decimal, -10),
            max_retries=3,
            timestamp=time.time(),
            # controller_id="splash_cardano_mainnet" if direction is ArbitrageDirection.FORWARD else "kucoin"
        )

    def determine_executor_actions(self) -> List[ExecutorAction]:
        executor_actions = []
        if self._arb_task is None:
            print("running on none")
            self._arb_task = safe_ensure_future(self.try_create_arbitrage_action())
            print("this is the arb task")
            print(self._arb_task)
        elif self._arb_task.done():
            print("running on done")
            executor_actions.append(self._arb_task.result())
            self._arb_task = safe_ensure_future(self.try_create_arbitrage_action())
            print("this is the done executers actions")
            print(executor_actions)
        
        print("on status", self._arb_task, "returnning this", executor_actions)
        return executor_actions

    async def try_create_arbitrage_action(self) -> List[ExecutorAction]:
        print("building some executers")
        executor_actions = []
        active_executors = self.filter_executors(
            executors=self.get_all_executors(),
            filter_func=lambda e: not e.is_done
        )
        print("this is the active executers")
        print(active_executors)
        if len(active_executors) == 0:
            forward_arbitrage_percent = await self.estimate_arbitrage_percent(ArbitrageDirection.FORWARD)
            if 1 >= self.config.min_arbitrage_percent:
                x = CreateExecutorAction(executor_config=self.arbitrage_config(ArbitrageDirection.FORWARD))
                print("--------------- generated one")
                print(x)
                print("------------------end of generated one")
                executor_actions.append(x)
                print("----------executer actions")
                print(executor_actions)
            else:
                backward_arbitrage_percent = await self.estimate_arbitrage_percent(ArbitrageDirection.BACKWARD)
                if -backward_arbitrage_percent >= self.config.min_arbitrage_percent:
                    x = CreateExecutorAction(executor_config=self.arbitrage_config(ArbitrageDirection.BACKWARD))
                    print("--------------- generated one")
                    print(x)
                    print("------------------end of generated one")
                    executor_actions.append(x)
                    print("----------executer actions")
                    print(executor_actions)
        if len(executor_actions) == 0:
            print("its zero returning this", )
            return executor_actions
        else:
            return executor_actions[0]

    async def estimate_arbitrage_percent(self, direction: ArbitrageDirection) -> Decimal:
        forward = direction is ArbitrageDirection.FORWARD

        # Get the trading pairs as strings from the sets
        main_trading_pair = next(iter(self.markets[self.config.cex_connector_main]))
        proxy_trading_pair = next(iter(self.markets[self.config.cex_connector_proxy]))
        dex_trading_pair = next(iter(self.markets[self.config.dex_connector]))
        print(main_trading_pair,proxy_trading_pair,dex_trading_pair)

        p_arb_asset_in_stable_asset = self.connectors[self.config.cex_connector_main].get_quote_price(
            trading_pair=main_trading_pair, 
            is_buy=forward,
            amount=self.config.min_arbitrage_volume)
    
        p_proxy_asset_in_stable_asset = self.connectors["kucoin"].get_quote_price(
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
        
        print(
            p_arb_asset_in_stable_asset,
            p_proxy_asset_in_stable_asset,
            p_arb_asset_wrapped_asset_in_proxy_asset
        )

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
