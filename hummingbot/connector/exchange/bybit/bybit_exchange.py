import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from bidict import bidict

import hummingbot.connector.exchange.bybit.bybit_constants as CONSTANTS
import hummingbot.connector.exchange.bybit.bybit_web_utils as web_utils
from hummingbot.connector.exchange.bybit.bybit_api_order_book_data_source import BybitAPIOrderBookDataSource
from hummingbot.connector.exchange.bybit.bybit_api_user_stream_data_source import BybitAPIUserStreamDataSource
from hummingbot.connector.exchange.bybit.bybit_auth import BybitAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter

s_logger = None
s_decimal_NaN = Decimal("nan")


class BybitExchange(ExchangePyBase):
    web_utils = web_utils

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 bybit_api_key: str,
                 bybit_api_secret: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 ):
        self.api_key = bybit_api_key
        self.secret_key = bybit_api_secret
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._last_trades_poll_bybit_timestamp = 1.0
        self._account_type = None  # To be update on firtst call to balances
        self._category = CONSTANTS.TRADE_CATEGORY  # Required by the V5 API
        super().__init__(client_config_map)

    @staticmethod
    def bybit_order_type(order_type: OrderType) -> str:
        if order_type.is_limit_type():
            return "Limit"
        else:
            return "Market"

    @staticmethod
    def to_hb_order_type(bybit_type: str) -> OrderType:
        return OrderType[bybit_type]

    @property
    def authenticator(self):
        return BybitAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self._time_synchronizer)

    @property
    def name(self) -> str:
        if self._domain == "bybit_main":
            return "bybit"
        else:
            return f"bybit_{self._domain}"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.SERVER_TIME_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self):
        return [OrderType.MARKET, OrderType.LIMIT, OrderType.LIMIT_MAKER]

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        error_description = str(request_exception)
        is_time_synchronizer_related = ("-1021" in error_description
                                        and "Timestamp for the request" in error_description)
        return is_time_synchronizer_related

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        # TODO: implement this method correctly for the connector
        # The default implementation was added when the functionality to detect not found orders was introduced in the
        # ExchangePyBase class. Also fix the unit test test_lost_order_removed_if_not_found_during_order_status_update
        # when replacing the dummy implementation
        return False

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        # TODO: implement this method correctly for the connector
        # The default implementation was added when the functionality to detect not found orders was introduced in the
        # ExchangePyBase class. Also fix the unit test test_cancel_order_not_found_in_the_exchange when replacing the
        # dummy implementation
        return False

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return BybitAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self.domain,
            api_factory=self._web_assistants_factory,
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return BybitAPIUserStreamDataSource(
            auth=self._auth,
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        is_maker = order_type is OrderType.LIMIT_MAKER
        trade_base_fee = build_trade_fee(
            exchange=self.name,
            is_maker=is_maker,
            order_side=order_side,
            order_type=order_type,
            amount=amount,
            price=price,
            base_currency=base_currency,
            quote_currency=quote_currency
        )
        return trade_base_fee

    async def _get_account_info(self):
        account_info = await self._api_get(
            path_url=CONSTANTS.ACCOUNT_INFO_PATH_URL,
            params=None,
            is_auth_required=True,
            headers={
                "referer": CONSTANTS.HBOT_BROKER_ID
            },
        )
        return account_info

    async def _get_account_type(self):
        account_info = await self._get_account_info()
        if account_info["retCode"] != 0:
            raise ValueError(f"{account_info['retMsg']}")
        account_type = 'SPOT' if account_info["result"]["unifiedMarginStatus"] == CONSTANTS.ACCOUNT_TYPE["REGULAR"] else 'UNIFIED'

        return account_type

    async def _update_account_type(self):
        self._account_type = await self._get_account_type()

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        amount_str = f"{amount:f}"
        type_str = self.bybit_order_type(order_type)

        side_str = CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        for tp, tpr in self._trading_rules.items():
            if tp == trading_pair:
                precision = f"{tpr.min_base_amount_increment}"[::-1].find('.')
                _price = round(price, precision)

        api_params = {
            "category": self._category,
            "symbol": symbol,
            "side": side_str,
            "orderType": type_str,
            "qty": amount_str,
            "price": f"{_price:f}",
            "orderLinkId": order_id
        }
        # sort params required for V5
        api_params = dict(sorted(api_params.items()))
        if order_type == OrderType.LIMIT:
            api_params["timeInForce"] = CONSTANTS.TIME_IN_FORCE_GTC

        response = await self._api_post(
            path_url=CONSTANTS.ORDER_PLACE_PATH_URL,
            data=api_params,
            is_auth_required=True,
            trading_pair=trading_pair,
            headers={"referer": CONSTANTS.HBOT_BROKER_ID},
        )
        if response["retCode"] != 0:
            raise ValueError(f"{response['retMsg']}")
        order_result = response.get("result", {})
        o_id = str(order_result["orderId"])
        transact_time = int(response["time"]) * 1e-3
        return (o_id, transact_time)

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        exchange_order_id = tracked_order.exchange_order_id
        client_order_id = tracked_order.client_order_id
        trading_pair = tracked_order.trading_pair
        api_params = {
            "category": self._category,
            "symbol": trading_pair
        }
        if exchange_order_id:
            api_params["orderId"] = exchange_order_id
        else:
            api_params["orderLinkId"] = client_order_id
        api_params = dict(sorted(api_params.items()))
        response = await self._api_post(
            path_url=CONSTANTS.ORDER_CANCEL_PATH_URL,
            data=api_params,
            is_auth_required=True,
            headers={"referer": CONSTANTS.HBOT_BROKER_ID},
        )
        if response["retCode"] != 0:
            raise ValueError(f"{response['retMsg']}")
        if isinstance(response, dict) and "orderLinkId" in response["result"]:
            return True
        return False

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        trading_pair_rules = exchange_info_dict.get("result", []).get("list", [])
        retval = []
        for rule in trading_pair_rules:
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=rule.get("symbol"))
                lot_size_filter = rule.get("lotSizeFilter", {})
                price_filter = rule.get("priceFilter", {})

                min_order_size = lot_size_filter.get("minOrderQty")
                min_price_increment = price_filter.get("tickSize")
                min_base_amount_increment = lot_size_filter.get("basePrecision")
                min_notional_size = lot_size_filter.get("minOrderAmt")
                retval.append(
                    TradingRule(
                        trading_pair,
                        min_order_size=Decimal(min_order_size),
                        min_price_increment=Decimal(min_price_increment),
                        min_base_amount_increment=Decimal(min_base_amount_increment),
                        min_notional_size=Decimal(min_notional_size)
                    )
                )

            except Exception:
                self.logger().exception(f"Error parsing the trading pair rule {rule.get('name')}. Skipping.")
        return retval

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange
        """
        pass

    async def _user_stream_event_listener(self):
        """
        This functions runs in background continuously processing the events received from the exchange by the user
        stream data source. It keeps reading events from the queue until the task is interrupted.
        The events received are balance updates, order updates and trade events.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                channel = event_message.get("channel")
                if channel == CONSTANTS.PRIVATE_TRADE_CHANNEL:
                    data = event_message.get("data")
                    for trade in data:
                        # SPOT: "", UNIFIED: "Trade"
                        if trade.get("execType") not in ("Trade", ""):  # Not a trade event
                            continue
                        client_order_id = trade.get("orderLinkId")
                        exchange_order_id = trade.get("orderId")
                        fillable_order = self._order_tracker.all_fillable_orders.get(client_order_id)
                        # fillable_order = self._order_tracker.fetch_order(client_order_id=client_order_id)
                        if fillable_order is not None:
                            trading_pair = fillable_order.trading_pair
                            ptoken = trading_pair.split("-")[1]
                            fee_amount = trade["execFee"] or "0"
                            fee = TradeFeeBase.new_spot_fee(
                                fee_schema=self.trade_fee_schema(),
                                trade_type=fillable_order.trade_type,
                                flat_fees=[
                                    TokenAmount(
                                        amount=Decimal(fee_amount),
                                        token=ptoken
                                    )
                                ]
                            )
                            trade_update = TradeUpdate(
                                trade_id=str(trade["blockTradeId"]),
                                client_order_id=client_order_id,
                                exchange_order_id=str(exchange_order_id),
                                trading_pair=fillable_order.trading_pair,
                                fee=fee,
                                fill_base_amount=Decimal(trade["execQty"]),
                                fill_quote_amount=Decimal(trade["execPrice"]) * Decimal(trade["execQty"]),
                                fill_price=Decimal(trade["execPrice"]),
                                fill_timestamp=int(trade["execTime"]) * 1e-3,
                            )
                            self._order_tracker.process_trade_update(trade_update)
                if channel == CONSTANTS.PRIVATE_ORDER_CHANNEL:
                    data = event_message.get("data")
                    for order in data:
                        client_order_id = order.get("orderLinkId")
                        exchange_order_id = order.get("orderId")
                        updatable_order = self._order_tracker.all_updatable_orders.get(client_order_id)
                        if updatable_order is not None:
                            new_state = CONSTANTS.ORDER_STATE[order["orderStatus"]]
                            order_update = OrderUpdate(
                                trading_pair=updatable_order.trading_pair,
                                update_timestamp=int(order["updatedTime"]) * 1e-3,
                                new_state=new_state,
                                client_order_id=client_order_id,
                                exchange_order_id=str(exchange_order_id),
                            )
                            self._order_tracker.process_order_update(order_update=order_update)
                elif channel == CONSTANTS.PRIVATE_WALLET_CHANNEL:
                    accounts = event_message["data"]
                    account_type = self._account_type
                    balances = []
                    for account in accounts:
                        if account["accountType"] == account_type:
                            balances = account["coin"]
                    for balance_entry in balances:
                        asset_name = balance_entry["coin"]
                        free_balance = Decimal(balance_entry["free"])
                        total_balance = Decimal(balance_entry["walletBalance"])
                        self._account_available_balances[asset_name] = free_balance
                        self._account_balances[asset_name] = total_balance
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []
        exchange_order_id = order.exchange_order_id
        client_order_id = order.client_order_id
        trading_pair = order.trading_pair
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        api_params = {
            "category": self._category,
            "symbol": trading_pair
        }
        if exchange_order_id:
            api_params["orderId"] = exchange_order_id
        else:
            api_params["orderLinkId"] = client_order_id

        all_fills_response = await self._api_get(
            path_url=CONSTANTS.TRADE_HISTORY_PATH_URL,
            params=api_params,
            is_auth_required=True,
            limit_id=CONSTANTS.TRADE_HISTORY_PATH_URL
        )
        result = all_fills_response.get("result", [])
        if result not in (None, {}):
            for trade in result["list"]:
                exchange_order_id = trade["orderId"]
                ptoken = trading_pair.split("-")[1]
                fee = TradeFeeBase.new_spot_fee(
                    fee_schema=self.trade_fee_schema(),
                    trade_type=order.trade_type,
                    percent_token=ptoken,
                    flat_fees=[
                        TokenAmount(
                            amount=Decimal(trade["execFee"]),
                            token=ptoken
                        )
                    ]
                )
                trade_update = TradeUpdate(
                    trade_id=str(trade["blockTradeId"]),
                    client_order_id=client_order_id,
                    exchange_order_id=exchange_order_id,
                    trading_pair=symbol,
                    fee=fee,
                    fill_base_amount=Decimal(trade["qty"]),
                    fill_quote_amount=Decimal(trade["price"]) * Decimal(trade["qty"]),
                    fill_price=Decimal(trade["price"]),
                    fill_timestamp=int(trade["updatedTime"]) * 1e-3,
                )
                trade_updates.append(trade_update)
        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        exchange_order_id = tracked_order.exchange_order_id
        client_order_id = tracked_order.client_order_id
        trading_pair = tracked_order.trading_pair
        api_params = {
            "category": self._category,
            "symbol": trading_pair
        }
        if exchange_order_id:
            api_params["orderId"] = exchange_order_id
        else:
            api_params["orderLinkId"] = client_order_id
        updated_order_data = await self._api_get(
            path_url=CONSTANTS.GET_ORDERS_PATH_URL,
            params=api_params,
            is_auth_required=True,
            limit_id=CONSTANTS.GET_ORDERS_PATH_URL
        )

        order_data = updated_order_data["result"]["list"][0]
        order_status = order_data["orderStatus"]

        new_state = CONSTANTS.ORDER_STATE[order_status]

        order_update = OrderUpdate(
            client_order_id=client_order_id,
            exchange_order_id=str(order_data["orderId"]),
            trading_pair=trading_pair,
            update_timestamp=int(order_data["updatedTime"]) * 1e-3,
            new_state=new_state,
        )
        return order_update

    async def _update_balances(self):
        # Update the first time it is called
        if self._account_type is None:
            await self._update_account_type()

        balances = await self._api_request(
            method=RESTMethod.GET,
            path_url=CONSTANTS.BALANCE_PATH_URL,
            params={
                'accountType': self._account_type
            },
            is_auth_required=True
        )
        if balances["retCode"] != 0:
            raise ValueError(f"{balances['retMsg']}")
        self._account_available_balances.clear()
        self._account_balances.clear()
        for coin in balances["result"]["list"][0]["coin"]:
            name = coin["coin"]
            free_balance = Decimal(coin["free"]) if self._account_type == "SPOT" \
                else Decimal(coin["availableToWithdraw"])
            balance = Decimal(coin["walletBalance"])
            self._account_available_balances[name] = free_balance
            self._account_balances[name] = Decimal(balance)

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        for symbol_data in exchange_info["result"]['list']:
            mapping[symbol_data["symbol"]] = combine_to_hb_trading_pair(
                base=symbol_data["baseCoin"],
                quote=symbol_data["quoteCoin"]
            )
        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        params = {
            "category": self._category,
            "symbol": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
        }
        resp_json = await self._api_request(
            method=RESTMethod.GET,
            path_url=CONSTANTS.LAST_TRADED_PRICE_PATH,
            params=params,
        )

        return float(resp_json["result"]["list"][0]["lastPrice"])

    async def _api_request(self,
                           path_url,
                           method: RESTMethod = RESTMethod.GET,
                           params: Optional[Dict[str, Any]] = None,
                           data: Optional[Dict[str, Any]] = None,
                           is_auth_required: bool = False,
                           return_err: bool = False,
                           limit_id: Optional[str] = None,
                           headers: Optional[Dict[str, Any]] = None,
                           **kwargs) -> Dict[str, Any]:
        last_exception = None
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        url = web_utils.rest_url(path_url, domain=self.domain)
        params = dict(sorted(params.items())) if isinstance(params, dict) else params
        data = dict(sorted(data.items())) if isinstance(data, dict) else data

        for _ in range(CONSTANTS.API_REQUEST_RETRY):
            try:
                request_result = await rest_assistant.execute_request(
                    url=url,
                    params=params,
                    data=data,
                    method=method,
                    is_auth_required=is_auth_required,
                    return_err=return_err,
                    headers=headers,
                    throttler_limit_id=limit_id if limit_id else path_url,
                )
                return request_result
            except IOError as request_exception:
                last_exception = request_exception
                if self._is_request_exception_related_to_time_synchronizer(request_exception=request_exception):
                    self._time_synchronizer.clear_time_offset_ms_samples()
                    await self._update_time_synchronizer()
                else:
                    raise
        # Failed even after the last retry
        raise last_exception

    async def _make_trading_rules_request(self) -> Any:
        exchange_info = await self._api_get(
            path_url=self.trading_rules_request_path,
            params={
                'category': self._category
            }
        )
        return exchange_info

    async def _make_trading_pairs_request(self) -> Any:
        exchange_info = await self._api_get(
            path_url=self.trading_pairs_request_path,
            params={
                'category': self._category
            }
        )
        return exchange_info
