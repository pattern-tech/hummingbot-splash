import asyncio
import itertools as it
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from hummingbot.connector.gateway.amm.gateway_amm_base import GatewayAMMBase
from hummingbot.connector.gateway.gateway_in_flight_order import GatewayInFlightOrder
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.in_flight_order import OrderState, OrderUpdate
from hummingbot.core.data_type.trade_fee import TokenAmount
from hummingbot.core.event.events import TradeType
from hummingbot.core.gateway.gateway_http_client import GatewayHttpClient
from hummingbot.core.utils import async_ttl_cache
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.utils.tracking_nonce import NonceCreator
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter

from hummingbot.core.gateway import check_transaction_exceptions


class GatewayCardanoAMM(GatewayAMMBase):
    """
    Defines basic functions common to connectors that interact with Gateway.
    """
    API_CALL_TIMEOUT = 60.0
    POLL_INTERVAL = 15.0
    def __init__(
            self,
            client_config_map: "ClientConfigAdapter",
            connector_name: str,
            chain: str,
            network: str,
            address: str,
            trading_pairs: List[str] = [],
            trading_required: bool = True,
    ):
        """
        :param connector_name: name of connector on gateway
        :param chain: refers to a block chain, e.g. ethereum or avalanche
        :param network: refers to a network of a particular blockchain e.g. mainnet or kovan
        :param address: the address of the eth wallet which has been added on gateway
        :param trading_pairs: a list of trading pairs
        :param trading_required: Whether actual trading is needed. Useful for some functionalities or commands like the balance command
        """
        super().__init__(
            client_config_map=client_config_map,
            connector_name=connector_name,
            chain=chain,
            network=network,
            address=address,
            trading_pairs=trading_pairs,
            trading_required=trading_required,
        )

        self._native_currency = "ADA"
        self._default_fee = Decimal("0.001")
        self.network_transaction_fee: Optional[TokenAmount] = TokenAmount(token=self._native_currency, amount=self._default_fee)
    
    async def get_chain_info(self):
        """
        Calls the base endpoint of the connector on Gateway to know basic info about chain being used.
        """
        try:
            self._chain_info = await self._get_gateway_instance().get_network_status(
                chain=self.chain, network=self.network
            )
            if not isinstance(self._chain_info, list):
                self._native_currency = self._chain_info.get("nativeCurrency", "ADA")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().network("Error fetching chain info", exc_info=True, app_warning_msg=str(e))
    
    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
    
        return []
    
    async def cancel(self, client_order_id:str) -> str:
        try:
            cancel_res = await self._get_gateway_instance().cancel_evm_transaction("cardano", "mainnet", client_order_id, 0)
        
            tx_hash: str = cancel_res.get("txHash")

            return tx_hash
        
        except Exception as err:
            self.logger().error(
                f"Failed to cancel order {client_order_id}: {str(err)}.",
                exc_info=True
            )
    
    async def confirm_cancel(self, cancel_hash:str) -> str:
        try:
            cancel_res = await self._get_gateway_instance().get_transaction_status(
                self.chain,
                self.network,
                cancel_hash
            )

            if cancel_res and cancel_res.get("scripts_successful"):                
                return True
            else: 
                return False
            
        except Exception as err:
            self.logger().error(
                f"Failed to fetch the cancel tx {cancel_hash}: {str(err)}.",
                exc_info=True
            )


    async def cancel_outdated_orders(self, cancel_age: int) -> List[CancellationResult]:
        """
        This is intentionally left blank, because cancellation is not supported for tezos blockchain.
        """
        return []
    def parse_price_response(
            self,
            base: str,
            quote: str,
            amount: Decimal,
            side: TradeType,
            price_response: Dict[str, Any],
            process_exception: bool = True
    ) -> Optional[Decimal]:
        """
        Parses price response
        :param base: The base asset
        :param quote: The quote asset
        :param amount: amount
        :param side: trade side
        :param price_response: Price response from Gateway.
        :param process_exception: Flag to trigger error on exception
        """
        required_items = ["price", "gasLimit", "gasPrice", "gasCost", "gasPriceToken"]
        if any(item not in price_response.keys() for item in required_items):
            if "info" in price_response.keys():
                self.logger().info(f"Unable to get price. {price_response['info']}")
            else:
                self.logger().info(f"Missing data from price result. Incomplete return result for ({price_response.keys()})")
        else:
            gas_price_token: str = price_response["gasPriceToken"]
            gas_cost: Decimal = Decimal(price_response["gasCost"])
            price: Decimal = Decimal(price_response["price"])
            self.network_transaction_fee = TokenAmount(gas_price_token, gas_cost)
            if process_exception is True:
                gas_limit: int = int(price_response["gasLimit"])
                exceptions: List[str] = check_transaction_exceptions(
                    allowances=self._allowances,
                    balances=self._account_balances,
                    base_asset=base,
                    quote_asset=quote,
                    amount=amount,
                    side=side,
                    gas_limit=gas_limit,
                    gas_cost=gas_cost,
                    gas_asset=gas_price_token,
                    swaps_count=len(price_response.get("swaps", [])),
                    chain=self.chain
                )
                for index in range(len(exceptions)):
                    self.logger().warning(
                        f"Warning! [{index + 1}/{len(exceptions)}] {side} order - {exceptions[index]}"
                    )
                if len(exceptions) > 0:
                    return None
            return Decimal(str(price))
        return None
    @async_ttl_cache(ttl=5, maxsize=10)
    async def get_quote_price(
            self,
            trading_pair: str,
            is_buy: bool,
            amount: Decimal,
            ignore_shim: bool = False
    ) -> Optional[Decimal]:
        """
        Retrieves a quote price.
        :param trading_pair: The market trading pair
        :param is_buy: True for an intention to buy, False for an intention to sell
        :param amount: The amount required (in base token unit)
        :param ignore_shim: Ignore the price shim, and return the real price on the network
        :return: The quote price.
        """
        base, quote = trading_pair.split("-")
        side: TradeType = TradeType.BUY if is_buy else TradeType.SELL
        # Pull the price from gateway.
        try:
            resp: Dict[str, Any] = await self._get_gateway_instance().get_price(
                self.chain, self.network, self.connector_name, base, quote, amount, side
            )
            return self.parse_price_response(base, quote, amount, side, price_response=resp, process_exception=False)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().network(
                f"Error getting quote price for {trading_pair} {side} order for {amount} amount.",
                exc_info=True,
                app_warning_msg=str(e)
            )
    async def load_token_data(self):
        tokens = await GatewayHttpClient.get_instance().get_tokens(network=self._network, chain=self.chain)
        for t in tokens.get("assets", []):
            self._amount_quantum_dict[t["symbol"]] = Decimal(str(10 ** -t["decimals"]))
    async def update_order_status(self, tracked_orders: List[GatewayInFlightOrder]):
        """
        Calls REST API to get status update for each in-flight amm orders.
        {
        "currentBlock": 28534865,
        "txBlock": 28512623,
        "txHash": "0xSCOCBDNHJVMA4I3VKETUHGFIM6HLTLAIALRPTL2LR6K66WPYEWUQ",
        "fee": 1000
        }
        """
        if len(tracked_orders) < 1:
            return
        # split canceled and non-canceled orders
        tx_hash_list: List[str] = await safe_gather(
            *[tracked_order.get_exchange_order_id() for tracked_order in tracked_orders]
        )
        self.logger().debug(
            "Polling for order status updates of %d orders.",
            len(tracked_orders)
        )
        update_results: List[Union[Dict[str, Any], Exception]] = await safe_gather(*[
            self._get_gateway_instance().get_transaction_status(
                self.chain,
                self.network,
                tx_hash
            )
            for tx_hash in tx_hash_list
        ], return_exceptions=True)
        
        failure_trigger : Union[GatewayInFlightOrder, None] = None
        
        for tracked_order, tx_details in zip(tracked_orders, update_results):
            if isinstance(tx_details, Exception):
                self.logger().error(f"An error occurred fetching transaction status of {tracked_order.client_order_id}. Please wait at least 2 minutes!")
                failure_trigger = tracked_order
                continue
            if "txHash" not in tx_details:
                self.logger().error(f"No txHash field for transaction status of {tracked_order.client_order_id}: "
                                    f"{tx_details}.")
                continue
            tx_block: int = tx_details["txBlock"]
            if tx_block > 0:
                fee: Decimal = tx_details["fee"]
                self.processs_trade_fill_update(tracked_order=tracked_order, fee=fee)
                order_update: OrderUpdate = OrderUpdate(
                    client_order_id=tracked_order.client_order_id,
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=self.current_timestamp,
                    new_state=OrderState.FILLED,
                )
                self._order_tracker.process_order_update(order_update)
            else:
                self.logger().network(
                    f"Error fetching transaction status for the order {tracked_order.client_order_id}: {tx_details}. Please wait at least 2 minutes!",
                    app_warning_msg=f"Failed to fetch transaction status for the order {tracked_order.client_order_id}. Please wait at least 2 minutes!"
                )
                failure_trigger = tracked_order
                await self._order_tracker.process_order_not_found(tracked_order.client_order_id)
                
                
        if failure_trigger is not None:
            self._order_tracker._trigger_failure_event(failure_trigger)    
            
    async def all_trading_pairs(self) -> List[str]:
        """
        Calls the tokens endpoint on Gateway.
        """
        try:
            tokens = await GatewayHttpClient.get_instance().get_tokens(network=self._network, chain=self.chain)
            token_symbols = [t["symbol"] for t in tokens["assets"]]
            trading_pairs = []
            for base, quote in it.permutations(token_symbols, 2):
                trading_pairs.append(f"{base}-{quote}")
            print(trading_pairs)
            return trading_pairs
        except Exception:
            print(Exception)
            return []
    def has_allowances(self) -> bool:
        return True