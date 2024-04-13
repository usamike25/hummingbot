import asyncio
import logging
import time
from collections import deque
from decimal import Decimal
from enum import Enum
from typing import Any, List

import numpy as np
import pandas as pd
from async_timeout import timeout

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.common import PositionAction, PositionSide
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.core.data_type.order_candidate import OrderCandidate, PerpetualOrderCandidate
from hummingbot.core.event.event_forwarder import SourceInfoEventForwarder
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderBookEvent,
    OrderBookTradeEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    OrderType,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
    TradeType,
)
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.order_book_asset_price_delegate import OrderBookAssetPriceDelegate
from hummingbot.strategy.strategy_py_base import StrategyPyBase
from hummingbot.strategy.xemm.xemm_utils import ActiveOrder, VolatilityIndicator2

# import sys
# from pmm_scripts.path import path_to_pmm_scripts

# sys.path.append(path_to_pmm_scripts)
# import volatility_indicator  # must have same python version


# from .order_id_market_pair_tracker import OrderIDMarketPairTracker
# from .xemm_reporter import xemm_reporter

NaN = float("nan")
s_decimal_zero = Decimal(0)
s_decimal_nan = Decimal("NaN")
s_float_nan = float("nan")
s_decimal_1 = Decimal("1")
s_decimal_0 = Decimal("0")
s_logger = None


class LogOption(Enum):
    NULL_ORDER_SIZE = 0
    REMOVING_ORDER = 1
    ADJUST_ORDER = 2
    CREATE_ORDER = 3
    MAKER_ORDER_FILLED = 4
    STATUS_REPORT = 5
    MAKER_ORDER_HEDGED = 6


class XEMMStrategy(StrategyPyBase):
    """
    this is a version of XEMM, that makes markets on multiple spot/futures markets and hedges on another spot/futures  market.

    version: 1.3
    """

    # def __init__(self, *args, **kwargs):
    #     super().__init__(args, kwargs)
    #     self._all_markets_ready = None

    @classmethod
    def logger(cls):
        global s_logger
        if s_logger is None:
            s_logger = logging.getLogger(__name__)
        return s_logger

    def init_params(self,
                    exchange_stats: dict,
                    connectors: dict,
                    max_order_size_quote: Decimal,
                    volatility_to_spread_multiplier: Decimal,
                    idle_base_amount: Decimal,
                    idle_quote_amount: Decimal,
                    mode=str,
                    market_making_settings=dict,
                    profit_settings=dict
                    ):
        self.exchange_stats = exchange_stats
        self.connectors = connectors
        self.mode = mode
        self.market_making_settings = market_making_settings
        self.profit_settings = profit_settings
        self.markets = {ex: {stats["pair"]} for ex, stats in self.exchange_stats.items()}
        self.max_order_size_quote = max_order_size_quote  # amount in USD for each order
        self.volatility_to_spread_multiplier = volatility_to_spread_multiplier
        self.idle_quote_amount = idle_quote_amount
        self.idle_base_amount = idle_base_amount
        self.min_profit_dict = {}
        self.max_order_age_seconds = 180
        self.min_notional_size_dict = {}  # "kucoin": 1 # custom min_notational. leave blank if not required

        self.active_maker_orders_ids = ActiveOrder()
        self.active_taker_orders_ids = ActiveOrder()

        # convert floats to Decimal
        for ex, ex_dict in self.exchange_stats.items():
            for key, value in ex_dict.items():
                if isinstance(value, float):
                    ex_dict[key] = Decimal(str(value))
        for ex, ex_dict in self.profit_settings.items():
            self.min_profit_dict[ex] = Decimal(ex_dict["min_profit"])

        # xemm multi variables
        self.status_ready = False
        self._all_markets_ready = False
        self.exchange_info = {}
        self.hedge_trades = {}  # {"exchange_trade_id": {"is_buy": True, "amount": Decimal(100),"maker_exchange": maker_exchange, "status": "in_process", "event": event}}
        self.hedge_order_id_to_filled_maker_exchange_trade_id = {}  # {"hedge_order_id": "filled_maker_order_id"}
        self.maker_order_id_to_hedge_exchange = {}
        self.ids_to_cancel = set()
        self.min_price_step = {}
        self.time_out_dict = {}
        self.optimal_quotes = {}
        self.is_perp = {}
        self.min_os_size_dict = {}
        self.volatility_indicator = {}
        self.order_id_creation_timestamp = []
        self.latency_roundtrip = {}
        self.order_creation_events = {}  # this is used to await pending create orders in order to properly cancel them

        self._base_asset_amount = None
        self._base_asset_amount_last_checked = 0

        self.add_markets([self.connectors[ex] for ex in self.markets.keys()])

    @property
    def active_maker_limit_orders(self):
        return [(ex, order, order.client_order_id) for ex, order in self._sb_order_tracker.active_limit_orders if self.active_maker_orders_ids.is_active_order(order.client_order_id)]

    @property
    def active_limit_orders(self):
        return [(ex, order, order.client_order_id) for ex, order in self._sb_order_tracker.active_limit_orders]

    def set_variables(self):
        self.subscribe_to_orderbook_trade_event()
        for exchange, token in self.markets.items():
            self.stop_tracking_all_orders(exchange)
            pair = list(token)[0]
            base, quote = pair.split("-")
            self.time_out_dict[exchange] = False
            self.is_perp[exchange] = self.is_perpetual_exchange(exchange)
            if exchange not in self.min_notional_size_dict.keys():
                self.min_notional_size_dict[exchange] = self.connectors[exchange].trading_rules[pair].min_notional_size
            self.min_os_size_dict[exchange] = self.connectors[exchange].trading_rules[pair].min_order_size
            self.min_price_step[exchange] = self.connectors[exchange].trading_rules[pair].min_price_increment
            self.latency_roundtrip[exchange] = deque(maxlen=10)  # store last X roundtrip times
            self.volatility_indicator[exchange] = {}
            self.volatility_indicator[exchange][pair] = VolatilityIndicator2(OrderBookAssetPriceDelegate(self.connectors[exchange], pair))

            # self._base_asset_amount = self.connectors[exchange].get_available_balance(base)

        self.status_ready = True

    def tick(self, timestamp: float):

        if not self._all_markets_ready:
            self._all_markets_ready = all([market.ready for market in self.active_markets])
            if not self._all_markets_ready:
                # Markets not ready yet. Don't do anything.
                self.logger().warning("Markets are not ready. No market making trades are permitted.")
                return
            else:
                # Markets are ready, ok to proceed.
                self.logger().info("Markets are ready.")

        if not self.status_ready:
            self.set_variables()

        start_time = time.perf_counter()

        # check quotes and calculate balances
        self.check_balances_and_quotes()

        # calculate optimal quotes
        self.calculate_optimal_quotes()

        # place orders
        self.place_orders()

        # check base asset drift
        if self.current_timestamp > self._base_asset_amount_last_checked + 10:
            self._base_asset_amount_last_checked = self.current_timestamp
            base_amount = 0
            for exchange, token in self.markets.items():
                pair = list(token)[0]
                base, quote = pair.split("-")
                base_amount += self.connectors[exchange].get_balance(base)
            if not self._base_asset_amount:
                self._base_asset_amount = base_amount
            if 0.5 < (abs(base_amount - self._base_asset_amount) / self._base_asset_amount) * 100:
                msg = f"Base asset drift detected: {self._base_asset_amount} -> {base_amount}"
                self.logger().info(msg)
                self.notify_hb_app_with_timestamp(msg)
            self._base_asset_amount = base_amount

        self.on_tick_runtime = ((time.perf_counter() - start_time) * 1000)

    def buy(self,
            connector_name: str,
            trading_pair: str,
            amount: Decimal,
            order_type: OrderType,
            price=s_decimal_nan,
            position_action=PositionAction.OPEN) -> str:

        self.logger().info(f"Creating {trading_pair} buy order: price: {price} amount: {amount}.")
        market = MarketTradingPairTuple(self.connectors[connector_name], trading_pair, trading_pair.split("-")[0], trading_pair.split("-")[1])
        return self.buy_with_specific_market(market, amount, order_type, price, position_action=position_action)

    def sell(self,
             connector_name: str,
             trading_pair: str,
             amount: Decimal,
             order_type: OrderType,
             price=s_decimal_nan,
             position_action=PositionAction.OPEN) -> str:

        self.logger().info(f"Creating {trading_pair} sell order: price: {price} amount: {amount}.")
        market_trading_pair_tuple = MarketTradingPairTuple(self.connectors[connector_name], trading_pair, trading_pair.split("-")[0], trading_pair.split("-")[1])
        return self.sell_with_specific_market(market_trading_pair_tuple, amount, order_type, price, position_action=position_action)

    def cancel(self,
               connector_name: str,
               trading_pair: str,
               order_id: str):
        market_trading_pair_tuple = MarketTradingPairTuple(self.connectors[connector_name], trading_pair, trading_pair.split("-")[0], trading_pair.split("-")[1])
        self.cancel_order(market_trading_pair_tuple=market_trading_pair_tuple, order_id=order_id)

    def is_valid_id(self, id):
        for (exchange, order, order_id) in self.active_limit_orders:
            if order.client_order_id == id:
                return True
        return False

    def stop_tracking_all_orders(self, exchange):
        all_order_ids = []
        for inflight_order in self.connectors[exchange].in_flight_orders.values():
            self.logger().info(f"stop_tracking_all_orders: delete: {inflight_order.client_order_id}")
            all_order_ids.append((inflight_order.trading_pair, inflight_order.client_order_id))

        for pair, id_ in all_order_ids:
            self.connectors[exchange].stop_tracking_order(id_)

    def place_orders(self):
        # todo do not place orders when rate limited
        if len(self.ids_to_cancel) != 0:
            return

        for exchange, token in self.markets.items():
            if self.time_out_dict[exchange]:
                continue

            pair = list(token)[0]

            for order_dict in self.optimal_quotes[exchange]["buy_orders"]:
                hedge_exchange = order_dict["hedge_exchange"]
                if order_dict["order_id"] or self.time_out_dict[hedge_exchange]:
                    continue

                amount = order_dict["amount"]
                maker_bid = order_dict["price"]

                if self.is_perp[exchange]:
                    buy_order = PerpetualOrderCandidate(
                        trading_pair=pair,
                        is_maker=True,
                        order_type=OrderType.LIMIT,
                        order_side=TradeType.BUY,
                        amount=Decimal(amount),
                        price=maker_bid,
                    )
                else:
                    buy_order = OrderCandidate(trading_pair=pair,
                                               is_maker=True,
                                               order_type=OrderType.LIMIT,
                                               order_side=TradeType.BUY,
                                               amount=Decimal(amount),
                                               price=maker_bid)

                buy_order_adjusted = self.connectors[exchange].budget_checker.adjust_candidate(buy_order, all_or_none=False)
                is_above_min_os = self.is_above_min_order_size(exchange, buy_order_adjusted.amount, buy_order_adjusted.price)

                # place order
                if is_above_min_os and not buy_order_adjusted.amount == Decimal(0):
                    order_id = self.buy(exchange, pair, buy_order_adjusted.amount, buy_order_adjusted.order_type, buy_order_adjusted.price)
                    self.order_id_creation_timestamp.append((exchange, order_id, time.perf_counter()))
                    if order_id is not None:
                        order_dict["order_id"] = order_id
                        self.active_maker_orders_ids.add(order_id, exchange)
                        self.maker_order_id_to_hedge_exchange[order_id] = hedge_exchange

            for order_dict in self.optimal_quotes[exchange]["sell_orders"]:
                hedge_exchange = order_dict["hedge_exchange"]
                if order_dict["order_id"] or self.time_out_dict[hedge_exchange]:
                    continue

                amount = order_dict["amount"]
                maker_ask = order_dict["price"]

                if self.is_perp[exchange]:
                    sell_order = PerpetualOrderCandidate(
                        trading_pair=pair,
                        is_maker=True,
                        order_type=OrderType.LIMIT,
                        order_side=TradeType.SELL,
                        amount=Decimal(amount),
                        price=maker_ask,
                    )
                else:
                    sell_order = OrderCandidate(trading_pair=pair,
                                                is_maker=True,
                                                order_type=OrderType.LIMIT,
                                                order_side=TradeType.SELL,
                                                amount=Decimal(amount),
                                                price=maker_ask, )

                sell_order_adjusted = self.connectors[exchange].budget_checker.adjust_candidate(sell_order, all_or_none=False)
                is_above_min_os = self.is_above_min_order_size(exchange, sell_order_adjusted.amount, sell_order_adjusted.price)

                # place order
                if is_above_min_os and not sell_order_adjusted.amount == Decimal(0):
                    order_id = self.sell(exchange, pair, sell_order_adjusted.amount, sell_order_adjusted.order_type, sell_order_adjusted.price)
                    self.order_id_creation_timestamp.append((exchange, order_id, time.perf_counter()))
                    if order_id is not None:
                        order_dict["order_id"] = order_id
                        self.active_maker_orders_ids.add(order_id, exchange)
                        self.maker_order_id_to_hedge_exchange[order_id] = hedge_exchange

    def has_pending_cancel_orders(self, exchange):
        for order in self.connectors[exchange].in_flight_orders.values():
            current_state = order.current_state
            if current_state == OrderState.PENDING_CANCEL:
                return True
        return False

    def optimize_order_placement(self, price, is_bid, exchange, pair, optimize_order=True):

        def amount_sub_my_orders_and_ignore_small_orders(ob_entry):
            amount = ob_entry.amount
            if ob_entry.price in my_limit_orders:
                amount = ob_entry.amount - float(my_limit_orders[ob_entry.price])

            # todo: calculate the max amount to ignore
            return amount if amount > (20 / ob_entry.price) else 0

        my_limit_orders = {}
        min_price_step = self.min_price_step[exchange]
        for (ex, order, order_id) in self.active_limit_orders:
            if ex.display_name == exchange:
                my_limit_orders[float(round(order.price / min_price_step) * min_price_step)] = order.quantity

        # place order just in front of another, don't quote higher than best bid ask
        if is_bid:
            order_book_iterator = self.connectors[exchange].get_order_book(pair).bid_entries()
            first_ob_entry = next(order_book_iterator, None)
            if price >= first_ob_entry.price and amount_sub_my_orders_and_ignore_small_orders(first_ob_entry) != 0:
                return Decimal(first_ob_entry.price)

            if optimize_order:

                for ob_entry in order_book_iterator:
                    if price == ob_entry.price and amount_sub_my_orders_and_ignore_small_orders(ob_entry) != 0:
                        return Decimal(ob_entry.price)
                    if price > ob_entry.price and amount_sub_my_orders_and_ignore_small_orders(ob_entry) != 0:
                        return Decimal(ob_entry.price) + min_price_step

        else:
            order_book_iterator = self.connectors[exchange].get_order_book(pair).ask_entries()
            first_ob_entry = next(order_book_iterator, None)
            if price <= first_ob_entry.price and amount_sub_my_orders_and_ignore_small_orders(first_ob_entry) != 0:
                return Decimal(first_ob_entry.price)

            if optimize_order:
                for ob_entry in order_book_iterator:
                    if price == ob_entry.price and amount_sub_my_orders_and_ignore_small_orders(ob_entry) != 0:
                        return Decimal(ob_entry.price)
                    if price < ob_entry.price and amount_sub_my_orders_and_ignore_small_orders(ob_entry) != 0:
                        return Decimal(ob_entry.price) - min_price_step

        return price

    def is_perpetual_exchange(self, exchange):
        return True if exchange.endswith("perpetual_testnet") or exchange.endswith("perpetual") else False

    def get_optimal_orders_lists_profit(self, exchange, pair, base_amount, quote_amount, mid):

        # check best hedge price
        buy_orders = []
        sell_orders = []
        possible_prices_list_bid = []
        possible_prices_list_ask = []

        vol = self.volatility_indicator[exchange][pair].current_value  # vol in decimal
        adjusted_vol = self.adjusted_vol(vol)
        min_profit = self.min_profit_dict[exchange]
        maker_fee = self.exchange_stats[exchange]["maker_fee"]
        optimize_order = self.profit_settings[exchange]["optimize_order"]

        for ex, token in self.markets.items():
            if ex != exchange:
                pair_ex = list(token)[0]

                available_quote = self.exchange_info[ex]["quote"]
                available_base = self.exchange_info[ex]["base"]

                # this is the price for a sell order
                possible_price_bid = self.connectors[ex].get_price_for_volume(pair_ex, False, min(available_base, (quote_amount / mid))).result_price

                # this is the price for a buy order
                possible_price_ask = self.connectors[ex].get_price_for_volume(pair_ex, True, min((available_quote / mid), base_amount)).result_price

                if not possible_price_ask.is_nan():
                    possible_prices_list_ask.append((ex, possible_price_ask))
                if not possible_price_bid.is_nan():
                    possible_prices_list_bid.append((ex, possible_price_bid))

        possible_prices_list_bid = sorted(possible_prices_list_bid, key=lambda item: item[1], reverse=True)
        possible_prices_list_ask = sorted(possible_prices_list_ask, key=lambda item: item[1], reverse=False)

        # check if there is enough capital to hedge on the exchange
        for i in range(len(possible_prices_list_ask)):
            hedge_exchange_ask = possible_prices_list_ask[i][0]
            hedge_price_ask = possible_prices_list_ask[i][1]
            taker_fee = self.exchange_stats[hedge_exchange_ask]["taker_fee"]
            available_quote = self.exchange_info[hedge_exchange_ask]["quote"]
            maker_ask = self.connectors[exchange].quantize_order_price(pair, (hedge_price_ask * (Decimal(1) + adjusted_vol + maker_fee + taker_fee + min_profit)))

            if base_amount <= available_quote / hedge_price_ask:
                if self.is_above_min_order_size(exchange, base_amount, maker_ask) and self.is_above_min_order_size(hedge_exchange_ask, base_amount, hedge_price_ask):
                    maker_ask = self.optimize_order_placement(maker_ask, False, exchange, pair, optimize_order)
                    sell_orders.append({"amount": base_amount,
                                        "price": maker_ask,
                                        "hedge_exchange": hedge_exchange_ask,
                                        "order_id": None})
                    break
            else:
                if self.is_above_min_order_size(exchange, available_quote / mid, maker_ask) and self.is_above_min_order_size(hedge_exchange_ask, base_amount, hedge_price_ask):
                    maker_ask = self.optimize_order_placement(maker_ask, False, exchange, pair, optimize_order)
                    sell_orders.append({"amount": available_quote / hedge_price_ask,
                                        "price": maker_ask,
                                        "hedge_exchange": hedge_exchange_ask,
                                        "order_id": None})
                    base_amount -= available_quote / hedge_price_ask

        for i in range(len(possible_prices_list_bid)):
            hedge_exchange_bid = possible_prices_list_bid[i][0]
            hedge_price_bid = possible_prices_list_bid[i][1]
            taker_fee = self.exchange_stats[hedge_exchange_bid]["taker_fee"]
            available_base = self.exchange_info[hedge_exchange_bid]["base"]
            maker_bid = self.connectors[exchange].quantize_order_price(pair, (hedge_price_bid * (Decimal(1) - adjusted_vol - maker_fee - taker_fee - min_profit)))

            if quote_amount / mid <= available_base:
                if self.is_above_min_order_size(exchange, quote_amount / mid, maker_bid) and self.is_above_min_order_size(hedge_exchange_bid, quote_amount / mid, maker_bid):
                    maker_bid = self.optimize_order_placement(maker_bid, True, exchange, pair, optimize_order)
                    buy_orders.append({"amount": quote_amount / mid,
                                       "price": maker_bid,
                                       "hedge_exchange": hedge_exchange_bid,
                                       "order_id": None})
                    break
            else:
                if self.is_above_min_order_size(exchange, available_base, maker_bid) and self.is_above_min_order_size(hedge_exchange_bid, quote_amount / mid, maker_bid):
                    maker_bid = self.optimize_order_placement(maker_bid, True, exchange, pair, optimize_order)
                    buy_orders.append({"amount": available_base,
                                       "price": maker_bid,
                                       "hedge_exchange": hedge_exchange_bid,
                                       "order_id": None})
                    quote_amount -= available_base * mid

        return buy_orders, sell_orders

    def get_optimal_orders_lists_market_making(self, exchange, pair, base_amount, quote_amount, mid):
        buy_orders = []
        sell_orders = []

        possible_prices_list_bid_dict = {}
        possible_prices_list_ask_dict = {}
        for i in range(self.market_making_settings["number_of_orders"]):
            possible_prices_list_ask_dict[i] = []
            possible_prices_list_bid_dict[i] = []

        vol = self.volatility_indicator[exchange][pair].current_value  # vol in decimal
        adjusted_vol = self.adjusted_vol(vol)
        maker_fee = self.exchange_stats[exchange]["maker_fee"]

        for ex, token in self.markets.items():
            if ex != exchange:
                pair_ex = list(token)[0]
                additional_amount_base = s_decimal_zero
                for i in range(self.market_making_settings["number_of_orders"]):
                    order_amount_base = self.market_making_settings["order_amounts"][i]
                    order_amount_quote = order_amount_base * mid

                    additional_amount_quote = additional_amount_base * mid

                    quote_amount_buy_order = min((self.exchange_info[ex]["quote"] / mid), order_amount_base) + additional_amount_quote
                    base_amount_sell_order = min(self.exchange_info[ex]["base"], (order_amount_quote / mid)) + additional_amount_base

                    # this is the price for a sell order
                    possible_price_bid = self.connectors[ex].get_price_for_volume(pair_ex, False, base_amount_sell_order).result_price

                    # this is the price for a buy order
                    possible_price_ask = self.connectors[ex].get_price_for_volume(pair_ex, True, quote_amount_buy_order).result_price

                    if not possible_price_ask.is_nan():
                        possible_prices_list_ask_dict[i].append((ex, possible_price_ask))
                    if not possible_price_bid.is_nan():
                        possible_prices_list_bid_dict[i].append((ex, possible_price_bid))

                    additional_amount_base += order_amount_base

        for i in range(self.market_making_settings["number_of_orders"]):
            possible_prices_list_bid_dict[i] = sorted(possible_prices_list_bid_dict[i], key=lambda item: item[1], reverse=True)
            possible_prices_list_ask_dict[i] = sorted(possible_prices_list_ask_dict[i], key=lambda item: item[1], reverse=False)

        for i in range(self.market_making_settings["number_of_orders"]):
            min_profit = Decimal(self.market_making_settings["min_profitability"][i])
            optimize_order = self.market_making_settings["optimize_order"][i]

            # sell orders
            for j in range(len(possible_prices_list_ask_dict[i])):
                hedge_exchange_ask = possible_prices_list_ask_dict[i][j][0]
                hedge_price_ask = possible_prices_list_ask_dict[i][j][1]
                taker_fee = self.exchange_stats[hedge_exchange_ask]["taker_fee"]
                available_quote = self.exchange_info[hedge_exchange_ask]["quote"]
                maker_ask = self.connectors[exchange].quantize_order_price(pair, (hedge_price_ask * (Decimal(1) + adjusted_vol + maker_fee + taker_fee + min_profit)))

                order_amount_base = min(self.market_making_settings["order_amounts"][i], base_amount)

                if order_amount_base <= available_quote / hedge_price_ask:
                    if self.is_above_min_order_size(exchange, order_amount_base, maker_ask) and self.is_above_min_order_size(hedge_exchange_ask, order_amount_base, hedge_price_ask):
                        maker_ask = self.optimize_order_placement(maker_ask, False, exchange, pair, optimize_order)
                        sell_orders.append({"amount": order_amount_base,
                                            "price": maker_ask,
                                            "hedge_exchange": hedge_exchange_ask,
                                            "order_id": None})
                        base_amount -= order_amount_base
                        break
                elif j == len(possible_prices_list_ask_dict[i]) - 1:  # deploy what ever is left to the best price exchange
                    hedge_exchange_ask = possible_prices_list_ask_dict[i][0][0]
                    hedge_price_ask = possible_prices_list_ask_dict[i][0][1]
                    available_quote = self.exchange_info[hedge_exchange_ask]["quote"]
                    maker_ask = self.connectors[exchange].quantize_order_price(pair, (hedge_price_ask * (Decimal(1) + adjusted_vol + maker_fee + taker_fee + min_profit)))
                    order_amount_base = available_quote / hedge_price_ask
                    if self.is_above_min_order_size(exchange, order_amount_base, maker_ask) and self.is_above_min_order_size(hedge_exchange_ask, order_amount_base, hedge_price_ask):
                        maker_ask = self.optimize_order_placement(maker_ask, False, exchange, pair, optimize_order)
                        sell_orders.append({"amount": order_amount_base,
                                            "price": maker_ask,
                                            "hedge_exchange": hedge_exchange_ask,
                                            "order_id": None})
                        base_amount -= order_amount_base
                        break

            # buy orders
            for j in range(len(possible_prices_list_bid_dict[i])):
                hedge_exchange_bid = possible_prices_list_bid_dict[i][j][0]
                hedge_price_bid = possible_prices_list_bid_dict[i][j][1]
                taker_fee = self.exchange_stats[hedge_exchange_bid]["taker_fee"]
                available_base = self.exchange_info[hedge_exchange_bid]["base"]
                maker_bid = self.connectors[exchange].quantize_order_price(pair, (hedge_price_bid * (Decimal(1) - adjusted_vol - maker_fee - taker_fee - min_profit)))

                order_amount_base = min(self.market_making_settings["order_amounts"][i], quote_amount / mid)

                if order_amount_base <= available_base:
                    if self.is_above_min_order_size(exchange, order_amount_base, maker_bid) and self.is_above_min_order_size(hedge_exchange_bid, order_amount_base, hedge_price_bid):
                        maker_bid = self.optimize_order_placement(maker_bid, True, exchange, pair, optimize_order)
                        buy_orders.append({"amount": order_amount_base,
                                           "price": maker_bid,
                                           "hedge_exchange": hedge_exchange_bid,
                                           "order_id": None})
                        quote_amount -= order_amount_base * mid
                        break
                elif j == len(possible_prices_list_bid_dict[i]) - 1:  # deploy what ever is left to the best price exchange
                    hedge_exchange_bid = possible_prices_list_bid_dict[i][0][0]
                    hedge_price_bid = possible_prices_list_bid_dict[i][0][1]
                    available_base = self.exchange_info[hedge_exchange_bid]["base"]
                    maker_bid = self.connectors[exchange].quantize_order_price(pair, (hedge_price_bid * (Decimal(1) - adjusted_vol - maker_fee - taker_fee - min_profit)))
                    order_amount_base = available_base
                    if self.is_above_min_order_size(exchange, order_amount_base, maker_bid) and self.is_above_min_order_size(hedge_exchange_bid, order_amount_base, hedge_price_bid):
                        maker_bid = self.optimize_order_placement(maker_bid, True, exchange, pair, optimize_order)
                        buy_orders.append({"amount": order_amount_base,
                                           "price": maker_bid,
                                           "hedge_exchange": hedge_exchange_bid,
                                           "order_id": None})
                        quote_amount -= order_amount_base * mid
                        break

        return buy_orders, sell_orders

    def get_optimal_orders_lists(self, exchange, pair, base_amount, quote_amount, mid):
        if self.mode == "profit":
            return self.get_optimal_orders_lists_profit(exchange, pair, base_amount, quote_amount, mid)
        elif self.mode == "market_making":
            return self.get_optimal_orders_lists_market_making(exchange, pair, base_amount, quote_amount, mid)
        else:
            raise Exception("mode not supported")

    def calculate_optimal_quotes(self):
        """calculate optimal quotes, based on the available hedge options. the output will be stored in a dict called optimal_quotes, with the format:
        optimal_quotes = {
        "Exchange": {
            "buy_orders": [
                {"amount": Decimal,
                 "price": Decimal,
                 "hedge_exchange": str,
                 "order_id": str}
            ],
            "sell_orders": [
                {"amount": Decimal,
                 "price": Decimal
                 "hedge_exchanges": str,
                 "order_id": str}
            ]
        }"""
        place_cancel_dict = {}
        new_optimal_quotes = {}
        for exchange, token in self.markets.items():
            pair = list(token)[0]
            base, quote = pair.split("-")
            base_amount = self.exchange_info[exchange]["base"]  # AVAX, BTC. ETH,....
            quote_amount = self.exchange_info[exchange]["quote"]  # USDT, USD,.....
            mid_price = self.connectors[exchange].get_mid_price(pair)

            # apply max size restriction
            if quote_amount >= self.max_order_size_quote:
                quote_amount = self.max_order_size_quote
            if base_amount >= self.max_order_size_quote / mid_price:
                base_amount = self.max_order_size_quote / mid_price

            if self.is_perp[exchange]:
                base_amount_capital_in_quote = self.exchange_info[exchange]["balance_base_from_positions"]
                quote_amount_capital_in_base = self.exchange_info[exchange]["balance_quote_from_positions"]
                buy_orders_capital_in_quote, sell_orders_capital_in_quote = self.get_optimal_orders_lists(exchange,
                                                                                                          pair,
                                                                                                          base_amount_capital_in_quote,
                                                                                                          quote_amount,
                                                                                                          mid_price)
                buy_orders_capital_in_base, sell_orders_capital_in_base = self.get_optimal_orders_lists(exchange, pair,
                                                                                                        base_amount,
                                                                                                        quote_amount_capital_in_base,
                                                                                                        mid_price)

                # we use the orders wherever the spread is smallest
                buy_orders, sell_orders = self.get_best_buy_sell_orders_for_perps(mid_price,
                                                                                  buy_orders_capital_in_quote,
                                                                                  sell_orders_capital_in_quote,
                                                                                  buy_orders_capital_in_base,
                                                                                  sell_orders_capital_in_base)
            else:
                buy_orders, sell_orders = self.get_optimal_orders_lists(exchange, pair, base_amount, quote_amount, mid_price)

            new_optimal_quotes[exchange] = {
                "buy_orders": buy_orders,
                "sell_orders": sell_orders
            }

        # compare old optimal quotes with new one and delete
        if self.optimal_quotes:
            for order_id, exchange in self.active_maker_orders_ids.get_active_orders_dict().items():
                if order_id not in self.ids_to_cancel:
                    old_order = self.connectors[exchange].in_flight_orders[order_id]
                    is_buy = True if old_order.trade_type == TradeType.BUY else False
                    price = old_order.price
                    creation_timestamp = old_order.creation_timestamp  # in flight orders have a resolution of 1S, Limit orders of 0.0000001S
                    hedge_exchange = self.maker_order_id_to_hedge_exchange[order_id]
                    order_to_old = True if creation_timestamp + self.max_order_age_seconds < self.current_timestamp else False

                    # check if to old
                    if order_to_old:
                        place_cancel_dict[order_id] = exchange
                        break

                    # check if it exists in the new quotes
                    order_side = "buy_orders" if is_buy else "sell_orders"
                    keep_order = False

                    for index, new_order in enumerate(new_optimal_quotes[exchange][order_side]):
                        if order_side == "buy_orders":
                            threshold_max = new_order["price"] * (Decimal(1) - Decimal(0.0008))
                        else:
                            threshold_max = new_order["price"] * (Decimal(1) + Decimal(0.0008))

                        # check if this order matches +-

                        if hedge_exchange == new_order["hedge_exchange"] and (
                                (order_side == "buy_orders" and new_order["price"] >= price > threshold_max) or (
                                order_side == "sell_orders" and new_order["price"] <= price < threshold_max)):
                            # keep order
                            new_optimal_quotes[exchange][order_side][index] = {"amount": new_order["amount"],
                                                                               "price": new_order["price"],
                                                                               "hedge_exchange": new_order["hedge_exchange"],
                                                                               "order_id": order_id}
                            keep_order = True
                            break

                    # cancel order if no equivalent is found
                    if not keep_order:
                        place_cancel_dict[order_id] = exchange

        # cancel orders below min profit or after max order age
        for id, ex in place_cancel_dict.items():
            # todo: KeyError,  if order got manually stop_tracking_order
            current_state = self.connectors[ex].in_flight_orders[id].current_state
            if current_state != OrderState.PENDING_CANCEL:
                trading_pair = list(self.markets[ex])[0]
                self.logger().info(f"calculate_optimal_quotes: cancel order {ex}: {id}, current_state: {current_state}")
                self.ids_to_cancel.add(id)
                safe_ensure_future(self.async_cancel(ex, trading_pair, id))

        # re assign optimal quotes
        self.optimal_quotes = new_optimal_quotes

    def get_best_buy_sell_orders_for_perps(self, mid_price, buy_orders_capital_in_quote, sell_orders_capital_in_quote, buy_orders_capital_in_base, sell_orders_capital_in_base):

        def get_total_amount_and_mean(list, mid_price):
            amount_sum = 0
            total_amount = 0
            for order in list:
                spread = abs(mid_price - order["price"])
                amount = order["amount"]
                amount_sum += spread * amount
                total_amount += amount
            if total_amount == 0:
                mean_spread = None
            else:
                mean_spread = amount_sum / total_amount
            return total_amount / mid_price, mean_spread

        total_amount_usd_capital_in_quote, mean_spread_capital_in_quote = get_total_amount_and_mean(
            buy_orders_capital_in_quote + sell_orders_capital_in_quote, mid_price)
        total_amount_usd_capital_in_base, mean_spread_capital_in_base = get_total_amount_and_mean(
            buy_orders_capital_in_base + sell_orders_capital_in_base, mid_price)

        if abs(total_amount_usd_capital_in_quote - total_amount_usd_capital_in_base) <= 5:  # 5 usd tolerance
            if mean_spread_capital_in_base and mean_spread_capital_in_quote:
                if mean_spread_capital_in_base <= mean_spread_capital_in_quote:
                    buy_orders = buy_orders_capital_in_base
                    sell_orders = sell_orders_capital_in_base
                else:
                    buy_orders = buy_orders_capital_in_quote
                    sell_orders = sell_orders_capital_in_quote
            elif not mean_spread_capital_in_base:
                buy_orders = buy_orders_capital_in_quote
                sell_orders = sell_orders_capital_in_quote
            else:
                buy_orders = buy_orders_capital_in_base
                sell_orders = sell_orders_capital_in_base
        elif total_amount_usd_capital_in_quote > total_amount_usd_capital_in_base:
            buy_orders = buy_orders_capital_in_quote
            sell_orders = sell_orders_capital_in_quote
        else:
            buy_orders = buy_orders_capital_in_base
            sell_orders = sell_orders_capital_in_base

        return buy_orders, sell_orders

    def adjusted_vol(self, vol):
        """vol needs to be in decimal. not percent"""
        if vol <= Decimal(0.0005):
            return Decimal(0)
        else:
            adjusted_vol = (vol - Decimal(0.0005))  # ** 2
            return adjusted_vol * self.volatility_to_spread_multiplier

    def is_above_min_order_size(self, exchange, amount, price):
        return True if self.min_notional_size_dict[exchange] * Decimal(1.1) < (amount * price) and self.min_os_size_dict[exchange] < amount else False

    def check_balances_and_quotes_perp(self, exchange, pair):
        base, quote = pair.split("-")
        mid_price = self.connectors[exchange].get_mid_price(pair)
        balance_base_from_positions = Decimal(0)
        balance_quote_from_positions = Decimal(0)
        balance_quote = self.connectors[exchange].get_available_balance(quote)
        # balance_base = self.connectors[exchange].get_available_balance(base)
        # self.logger().info(f"balance_quote: {balance_quote} balance_base:{balance_base} ")
        # balance_base = self.connectors[exchange].get_available_balance(base)
        balance_base = balance_quote / mid_price
        # self.logger().info(f"balance_quote: {balance_quote} balance_base:{balance_base} {balance_quote_from_positions} {balance_base_from_positions} ")

        # check open orders
        for order_ in self.connectors[exchange].in_flight_orders.values():
            amount = order_.amount
            executed_amount = order_.executed_amount_base
            trade_type = order_.trade_type
            is_buy = True if trade_type == TradeType.BUY else False
            price = order_.price
            current_state = order_.current_state

            # skip if order is pending create
            if current_state == OrderState.PENDING_CREATE and not exchange == "gate_io_perpetual":
                continue

            if not executed_amount.is_nan():
                open_amount = amount - executed_amount
            else:
                open_amount = amount

            if is_buy:
                balance_base += open_amount
                balance_quote += (open_amount * price)

            elif not is_buy and not exchange == "gate_io_perpetual":
                balance_base += open_amount
                balance_quote += (open_amount * price)

        # check open positions in futures exchanges
        if pair in self.connectors[exchange].account_positions.keys():
            position = self.connectors[exchange].account_positions[pair]
            if position.position_side == PositionSide.LONG:
                balance_base_from_positions += position.amount
                balance_base += position.amount
            else:
                balance_quote_from_positions += (position.amount * mid_price)
                balance_quote += (position.amount * mid_price)

        # check open hedge amount. this is not exchange specific
        for hedge_id, info in self.hedge_trades.items():
            if info["status"] in ["failed", "in_process"]:
                hedge_is_buy = info["is_buy"]
                hedge_amount = info["amount"]
                if hedge_is_buy:
                    balance_quote -= (hedge_amount * mid_price)
                else:
                    balance_base -= hedge_amount

        self.exchange_info[exchange] = {"base": balance_base,
                                        "quote": balance_quote,
                                        "balance_base_from_positions": balance_base_from_positions,
                                        "balance_quote_from_positions": balance_quote_from_positions}

    def check_balances_and_quotes(self):
        for exchange, token in self.markets.items():
            pair = list(token)[0]
            base, quote = pair.split("-")
            mid_price = self.connectors[exchange].get_mid_price(pair)
            balance_base_from_positions = Decimal(0)
            balance_quote_from_positions = Decimal(0)
            is_perp = self.is_perp[exchange]
            if is_perp:
                self.check_balances_and_quotes_perp(exchange, pair)
                continue

            # check free balances
            balance_quote = self.connectors[exchange].get_available_balance(quote)
            balance_base = self.connectors[exchange].get_available_balance(base)
            # check open orders balances
            for order_ in self.connectors[exchange].in_flight_orders.values():
                amount = order_.amount
                executed_amount = order_.executed_amount_base
                trade_type = order_.trade_type
                is_buy = True if trade_type == TradeType.BUY else False
                price = order_.price
                current_state = order_.current_state

                # skip if order is pending create
                if current_state == OrderState.PENDING_CREATE:
                    continue

                if not executed_amount.is_nan():
                    open_amount = amount - executed_amount
                else:
                    open_amount = amount

                if is_buy:
                    # add to balance to quote
                    balance_quote += (open_amount * price)
                elif not is_buy:
                    # add to balance to base
                    balance_base += open_amount

            # check open hedge amount. this is not exchange specific
            for hedge_id, info in self.hedge_trades.items():
                if info["status"] in ["failed", "in_process"]:
                    hedge_is_buy = info["is_buy"]
                    hedge_amount = info["amount"]
                    if hedge_is_buy:
                        balance_quote -= (hedge_amount * mid_price)
                    else:
                        balance_base -= hedge_amount

            # apply idle amounts
            if balance_base <= self.idle_base_amount:
                balance_base = Decimal(0)
            else:
                balance_base -= self.idle_base_amount

            if balance_quote <= self.idle_quote_amount:
                balance_quote = Decimal(0)
            else:
                balance_quote -= self.idle_quote_amount

            self.exchange_info[exchange] = {"base": balance_base,
                                            "quote": balance_quote,
                                            "balance_base_from_positions": balance_base_from_positions,
                                            "balance_quote_from_positions": balance_quote_from_positions}

    def subscribe_to_orderbook_trade_event(self):
        self.order_book_trade_event_0 = SourceInfoEventForwarder(self._process_public_trade_0)
        self.order_book_trade_event_1 = SourceInfoEventForwarder(self._process_public_trade_1)
        index = 0
        for market in self.connectors.values():
            for order_book in market.order_books.values():
                if index == 0:
                    order_book.add_listener(OrderBookEvent.TradeEvent, self.order_book_trade_event_0)
                elif index == 1:
                    order_book.add_listener(OrderBookEvent.TradeEvent, self.order_book_trade_event_1)
            index += 1

    def get_exchange_from_order_id(self, order_id):
        for exchange, token in self.markets.items():
            for inflight_order in self.connectors[exchange].in_flight_orders.values():
                if order_id == inflight_order.client_order_id:
                    return exchange
        return "exchange not found"

    async def async_cancel(self, exchange, trading_pair, order_id, cancel_try_count=0, await_pending_create=False):
        try:
            order = self.connectors[exchange].in_flight_orders[order_id]
            current_state = order.current_state
            exchange_order_id = order.exchange_order_id
        except KeyError:
            self.logger().info(f"async_cancel: Abort cancellation since order could not be found: {order_id}:")
            self.ids_to_cancel.discard(order_id)
            return

        if cancel_try_count > 5:
            self.logger().info(f"async_cancel: cancel trys reached limit for {order_id}:")
            self.ids_to_cancel.discard(order_id)
            return

        if current_state == OrderState.PENDING_CANCEL or current_state == OrderState.FILLED:
            self.logger().info(f"async_cancel: Abort cancellation since current_state is {current_state} for {order_id}:")
            return

        # handle exchanges that don't handle cancel pending create:
        special_treatment_exchanges = ["ascend_ex", "mexc"]
        if await_pending_create or (current_state == OrderState.PENDING_CREATE and (exchange in special_treatment_exchanges or exchange_order_id is None)):

            self.logger().info(f"async_cancel: Waiting for order creation event for {order_id}:")

            if order_id not in self.order_creation_events:
                self.order_creation_events[order_id] = asyncio.Event()
            await self.order_creation_events[order_id].wait()

            if order_id in self.order_creation_events:
                del self.order_creation_events[order_id]

            self.logger().info(f"async_cancel: Order created, proceeding to cancel {order_id}")

        def check_exchange_id(exchange, order_id):
            """
                Parameters:
                - exchange (str): The name of the exchange to check the order in.
                - id (str): The ID of the order to check.

                Returns:
                - int: Returns 1 if the order is found and has no valid exchange order ID, hence stopped tracking.
                       Returns 0 if the order does not meet the criteria for stopping tracking or is not found.
                """
            if order_id in self.connectors[exchange].in_flight_orders.keys():
                order = self.connectors[exchange].in_flight_orders[order_id]
                current_state = order.current_state
                exchange_order_id = order.exchange_order_id

                self.logger().info(f"async_cancel: order: {order_id}, exchange_order_id: {exchange_order_id}, current_state: {current_state}")
                if (exchange_order_id is None or exchange_order_id == "None") and current_state == OrderState.OPEN:
                    self.logger().info(f"async_cancel: order: {order_id} has no exchange id: {exchange_order_id} will stop tracking the order!")
                    self.connectors[exchange].stop_tracking_order(order_id)

                    # perform clean up
                    self.active_maker_orders_ids.remove(order_id)
                    self.maker_order_id_to_hedge_exchange.pop(exchange, None)
                    if self.is_perp[exchange]:
                        safe_ensure_future(self.connectors[exchange]._update_balances())
                    self.ids_to_cancel.discard(order_id)
                    return 1
            return 0

        # check exchange order id
        if check_exchange_id(exchange, order_id) == 1:
            return

        try:
            async with timeout(10.0):
                try:
                    result = await safe_gather(self.connectors[exchange]._execute_cancel(trading_pair, order_id), return_exceptions=True)

                except AttributeError:
                    result = await safe_gather(self.connectors[exchange].execute_cancel(trading_pair, order_id), return_exceptions=True)

                if result[0] is None:
                    if check_exchange_id(exchange, order_id) == 0:
                        # cancel again!
                        await asyncio.sleep(0.1)
                        await safe_gather(self.async_cancel(exchange, trading_pair, order_id, cancel_try_count + 1))

        except Exception:
            self.logger().network(
                "async_cancel: Unexpected error canceling orders (update orders).",
                exc_info=True,
                app_warning_msg=f"Failed to cancel order with {exchange} . Check API key and network connection.")

            check_exchange_id(exchange, order_id)
        self.ids_to_cancel.discard(order_id)

    def cancel_all_maker(self):
        for (order_exchange, order, order_id) in self.active_maker_limit_orders:
            self.cancel(order_exchange.display_name, order.trading_pair, order_id)

    async def execute_hedge(self, amount, hedge_order_is_buy, hedge_exchange, exchange_trade_id):

        self.logger().info(f"execute_hedge: place market order {'buy' if hedge_order_is_buy else 'sell'} as hedge at {hedge_exchange}")

        # check if above min order-size
        hedge_pair = list(self.markets[hedge_exchange])[0]
        base, quote = hedge_pair.split("-")
        mid_price = self.connectors[hedge_exchange].get_mid_price(hedge_pair)
        quantized_amount = self.connectors[hedge_exchange].quantize_order_amount(hedge_pair, amount)
        is_above_min_os = self.is_above_min_order_size(hedge_exchange, quantized_amount, mid_price)
        is_perp = self.is_perp[hedge_exchange]

        if not is_above_min_os:
            self.logger().info(f"execute_hedge: can not hedge trade, order size of {quantized_amount} below min order size")
            self.hedge_trades[exchange_trade_id]["status"] = "failed"
            return

        # cancel maker order on other exchange
        async_cancellation_tasks = []
        for ex, order, order_id in self.active_maker_limit_orders:
            if ex.display_name == hedge_exchange:
                maker_order_on_hedge_exchange = self.connectors[ex.display_name].in_flight_orders[order_id]
                maker_order_on_hedge_exchange_pair = maker_order_on_hedge_exchange.trading_pair
                trade_type = maker_order_on_hedge_exchange.trade_type
                maker_order_is_buy = True if trade_type == TradeType.BUY else False

                if is_perp or (not is_perp and maker_order_is_buy == hedge_order_is_buy):
                    self.logger().info(f"execute_hedge: Cancel maker order to free assets: {order_id}")
                    self.time_out_dict[hedge_exchange] = True
                    self.active_maker_orders_ids.remove(order_id)
                    if hedge_exchange in ["ascend_ex", "mexc"]:
                        self.logger().info(f"execute_hedge: Await cancellation for {hedge_exchange}: {order_id}")
                        async_cancellation_tasks.append(self.async_cancel(hedge_exchange, maker_order_on_hedge_exchange_pair, order_id))
                    else:
                        self.cancel(hedge_exchange, maker_order_on_hedge_exchange_pair, order_id)

        if async_cancellation_tasks:
            self.logger().info("execute_hedge: run safe_gather")
            await safe_gather(*async_cancellation_tasks)

        if hedge_order_is_buy:
            order_id_1 = self.buy(hedge_exchange, hedge_pair, quantized_amount, OrderType.MARKET, mid_price)
        else:
            order_id_1 = self.sell(hedge_exchange, hedge_pair, quantized_amount, OrderType.MARKET, mid_price)

        self.logger().info(
            f"execute_hedge: placed hedge {'buy' if hedge_order_is_buy else 'sell'} "
            f"{hedge_exchange}, amount: {quantized_amount}, budget quote: "
            f"{self.connectors[hedge_exchange].get_available_balance(quote)}, budget base: "
            f"{self.connectors[hedge_exchange].get_available_balance(base)}"
        )
        # update variables
        self.hedge_order_id_to_filled_maker_exchange_trade_id[order_id_1] = exchange_trade_id
        self.active_taker_orders_ids.add(order_id_1, hedge_exchange)

    def handle_failed_hedge(self, exchange_trade_id):

        self.logger().info("handle_failed_hedge:")
        is_buy = self.hedge_trades[exchange_trade_id]["is_buy"]
        amount = self.hedge_trades[exchange_trade_id]["amount"]
        event = self.hedge_trades[exchange_trade_id]["event"]

        # check where to hedge, we include all exchanges, as the prices might have diverged
        best_available_price = float('inf') if is_buy else float('-inf')
        best_available_exchange = None

        for exchange, token in self.markets.items():
            pair = list(token)[0]
            base, quote = pair.split("-")
            available_base_amount = self.exchange_info[exchange]["base"]
            available_quote_amount = self.exchange_info[exchange]["quote"]
            mid_price = self.connectors[exchange].get_mid_price(pair)
            if (available_quote_amount / mid_price >= amount) if is_buy else (available_base_amount >= amount):

                available_price = self.connectors[exchange].get_price_for_volume(pair, is_buy, amount).result_price
                if (is_buy and available_price < best_available_price) or (not is_buy and available_price > best_available_price):
                    best_available_price = available_price
                    best_available_exchange = exchange

        if best_available_exchange:
            self.logger().info(f"handle_failed_hedge: re submit hedge for: {event}"
                               f"amount {amount}, is_buy: {is_buy}, best_available_exchange: {best_available_exchange} exchange_trade_id: {exchange_trade_id}: ")
            safe_ensure_future(self.execute_hedge(amount, is_buy, best_available_exchange, exchange_trade_id))
        else:
            self.logger().info(f"handle_failed_hedge: no hedge possible for: {event}")
            msg = f"handle_failed_hedge: no hedge possible for: {event}"
            self.notify_hb_app_with_timestamp(msg)

    async def place_hedge(self, event, hedge_exchange):
        self.logger().info(f"place_hedge: place hedge for: {event}")
        amount = event.amount
        is_buy = True if event.trade_type == TradeType.BUY else False
        maker_exchange = self.active_maker_orders_ids.get_exchange(event.order_id)  # self.get_exchange_from_order_id(event.order_id)

        # check for failed orders
        for hedge_id, info in self.hedge_trades.items():
            if info["status"] == "failed":
                hedge_is_buy = info["is_buy"]
                hedge_amount = info["amount"]
                # add to amount
                if hedge_is_buy and not is_buy or not hedge_is_buy and is_buy:
                    amount += hedge_amount
                elif hedge_is_buy and is_buy or not hedge_is_buy and not is_buy:
                    amount -= hedge_amount
                # mark as processed
                info["status"] = "processed"
                self.logger().info(f"place_hedge: add failed order: {self.hedge_trades[hedge_id]}")

        # if we have a negative value of amount, the order needs to be switched from buy to sell etc.
        if amount < s_decimal_zero:
            is_buy = False if is_buy else True
            amount = abs(amount)

        # create entry in hedge_trades
        self.hedge_trades[event.exchange_trade_id] = {"is_buy": False if is_buy else True,
                                                      "amount": amount,
                                                      "status": "in_process",
                                                      "maker_exchange": maker_exchange,
                                                      "event": event}

        # place hedge
        exchange_trade_id = event.exchange_trade_id
        amount = self.hedge_trades[exchange_trade_id]["amount"]
        maker_order_id = self.hedge_trades[exchange_trade_id]["event"].order_id
        maker_exchange = self.hedge_trades[exchange_trade_id]["maker_exchange"]

        self.logger().info(
            f"place_hedge: place hedge\n exchange_trade_id: {exchange_trade_id}\n amount {amount},\n maker_order_id: {maker_order_id}\n maker_exchange: {maker_exchange}")

        hedge_order_is_buy = True if not is_buy else False

        # execute
        safe_ensure_future(self.execute_hedge(amount, hedge_order_is_buy, hedge_exchange, exchange_trade_id))

    # format status
    def format_status(self) -> str:
        """
        Returns status of the current strategy on user balances and current active orders. This function is called
        when status command is issued. Override this function to create custom status display output.
        """
        if not self._all_markets_ready:
            return "Market connectors are not ready."
        lines = []

        exchange_stats_df = self.get_exchange_stats_df()
        lines.extend(["", "  Exchange stats:"] + ["    " + line for line in
                                                  exchange_stats_df.to_string(index=False).split("\n")])

        lines.extend(["", f" on_tick_runtime: {self.on_tick_runtime:.4f} ms"])

        balance_df = self.get_balance_df()
        lines.extend(["", "  Balances:"] + ["    " + line for line in balance_df.to_string(index=False).split("\n")])

        try:
            df = self.get_active_maker_trades_df()
            lines.extend(["", "  Maker Orders:"] + ["    " + line for line in df.to_string(index=False).split("\n")])
        except ValueError:
            lines.extend(["", "  No active maker orders."])

        try:
            df = self.get_hedge_trades_df()
            lines.extend(
                ["", "  hedge_trades_dict:"] + ["    " + line for line in df.to_string(index=False).split("\n")])
        except ValueError:
            lines.extend(["", "  No hedge_trades."])

        return "\n".join(lines)

    def get_balance_df(self) -> pd.DataFrame:
        """
        Returns a data frame for all asset balances for displaying purpose.
        """
        columns: List[str] = ["Exchange", "Asset", "Total Balance", "Available Balance"]
        data: List[Any] = []
        for exchange, token in self.markets.items():
            pair = list(token)[0]
            for asset in pair.split("-"):
                data.append([exchange,
                             asset,
                             float(self.connectors[exchange].get_balance(asset)),
                             float(self.connectors[exchange].get_available_balance(asset))])
        df = pd.DataFrame(data=data, columns=columns).replace(np.nan, '', regex=True)
        df.sort_values(by=["Exchange", "Asset"], inplace=True)
        return df

    def get_hedge_trades_df(self) -> pd.DataFrame:
        """
        Return a data frame of all active orders for displaying purpose.
        """
        columns = ["maker_ex_trade_id", "Side", "Amount", "Status"]
        data = []

        for ex_trade_id, trade_info in self.hedge_trades.items():
            hedge_is_buy = trade_info["is_buy"]
            hedge_amount = trade_info["amount"]
            hedge_status = trade_info["status"]

            data.append([
                ex_trade_id,
                "buy" if hedge_is_buy else "sell",
                float(hedge_amount),
                hedge_status
            ])

        if not data:
            raise ValueError("No data available.")

        df = pd.DataFrame(data=data, columns=columns)
        df.sort_values(by=["maker_ex_trade_id", "Side"], inplace=True)
        return df

    def get_exchange_stats_df(self):
        """
        Returns a data frame for all volatility for displaying purpose.
        """
        columns: List[str] = ["Ex", "vol", "spread", "mid", "vol_add_spread", "rt_latency"]
        data: List[Any] = []
        for connector_name, connector in self.connectors.items():
            pair = list(self.markets[connector_name])[0]
            vol_decimal = self.volatility_indicator[connector_name][pair].current_value
            vol_pct = self.volatility_indicator[connector_name][pair].current_value_pct
            vol_adjusted = self.adjusted_vol(vol_decimal)
            vol_additional_spread = vol_adjusted * Decimal(100)
            roundtrip_latency = sum(self.latency_roundtrip[connector_name]) / len(self.latency_roundtrip[connector_name]) if self.latency_roundtrip[connector_name] else 0
            spread = ((self.connectors[connector_name].get_price(pair, True) - self.connectors[connector_name].get_price(pair, False)) / self.connectors[connector_name].get_price(pair, True)) * 100

            mid_price = self.connectors[connector_name].get_mid_price(pair)
            data.append([connector_name,
                         f"{round(vol_pct, 2)}%",
                         f"{round(spread, 2)}%",
                         f"{mid_price}",
                         f"{round(vol_additional_spread, 2)}%",
                         f"{round(roundtrip_latency, 2)} ms"
                         ])

        df = pd.DataFrame(data=data, columns=columns).replace(np.nan, '', regex=True)
        return df

    def get_active_orders_df(self):
        columns = ["Ex", "Market", "Side", "Price", "Amount", "Age", "hedge_ex", "spread"]
        data = []
        for ex, order, order_id in self.active_limit_orders:
            age_txt = "n/a" if order.age() <= 0. else pd.Timestamp(order.age(), unit='s').strftime('%H:%M:%S')
            hedge_exchange = self.maker_order_id_to_hedge_exchange[order_id]
            mid_price = self.connectors[ex.display_name].get_mid_price(order.trading_pair)
            spread = round(((abs(mid_price - order.price) / mid_price) * 100), 2)
            data.append([
                ex.display_name,
                order.trading_pair,
                "buy" if order.is_buy else "sell",
                float(order.price),
                float(order.quantity),
                age_txt,
                hedge_exchange,
                f"{spread}%"
            ])
        if not data:
            raise ValueError
        df = pd.DataFrame(data=data, columns=columns)
        df.sort_values(by=["Exchange", "Market", "Side"], inplace=True)
        return df

    def get_active_maker_trades_df(self) -> pd.DataFrame:
        """
        Return a data frame of all active orders for displaying purpose.
        """
        columns = ["Exchange", "Market", "Side", "Price", "Amount", "Age", "hedge_exchange", "spread"]
        data = []
        for ex, order, order_id in self.active_maker_limit_orders:
            age_txt = "n/a" if order.age() <= 0. else pd.Timestamp(order.age(), unit='s').strftime('%H:%M:%S')
            hedge_exchange = self.maker_order_id_to_hedge_exchange[order_id]
            mid_price = self.connectors[ex.display_name].get_mid_price(order.trading_pair)
            spread = round(((abs(mid_price - order.price) / mid_price) * 100), 2)
            data.append([
                ex.display_name,
                order.trading_pair,
                "buy" if order.is_buy else "sell",
                float(order.price),
                float(order.quantity),
                age_txt,
                hedge_exchange,
                f"{spread}%",
            ])
        if not data:
            raise ValueError
        df = pd.DataFrame(data=data, columns=columns)
        df.sort_values(by=["Exchange", "Market", "Side"], inplace=True)
        return df

    # event handlers
    def did_create_order(self, event):

        if event.order_id in self.order_creation_events:
            self.order_creation_events[event.order_id].set()

        # todo: delete this if the HB foundation fixes the issue with kucoin

        if self.active_taker_orders_ids.is_active_order(event.order_id):
            # check if the exchange order id exists:
            if event.exchange_order_id is None:
                self.logger().info(f"did_create_order: exchange_order_id is None for {event.order_id} will remove order from tracking and re submit hedge")
                exchange = self.active_taker_orders_ids.get_exchange(event.order_id)
                self.connectors[exchange].stop_tracking_order(event.order_id)

                # perform clean up
                self.active_taker_orders_ids.remove(event.order_id)

                # uptade variables
                exchange_trade_id = self.hedge_order_id_to_filled_maker_exchange_trade_id[event.order_id]
                del self.hedge_order_id_to_filled_maker_exchange_trade_id[event.order_id]
                self.hedge_trades[exchange_trade_id]["status"] = "failed"
                self.time_out_dict[exchange] = False
                base, quote = list(self.markets[exchange])[0].split("-")
                safe_ensure_future(self.connectors[exchange]._update_balances())
                self.logger().info(f"did_create_order: {exchange}, quote: {self.connectors[exchange].get_available_balance(quote)}, base: {self.connectors[exchange].get_available_balance(base)}")
                self.handle_failed_hedge(exchange_trade_id)

    def did_create_buy_order(self, event: BuyOrderCreatedEvent):
        """
        Method called when the connector notifies an order has been created
        """
        self.did_create_order(event)
        self.calculate_latency(event)
        return

    def did_create_sell_order(self, event: SellOrderCreatedEvent):
        """
        Method called when the connector notifies an order has been created
        """
        self.did_create_order(event)
        self.calculate_latency(event)
        return

    def calculate_latency(self, event):
        for exchange, id, timestamp in self.order_id_creation_timestamp:
            if id == event.order_id:
                self.latency_roundtrip[exchange].append((time.perf_counter() - timestamp) * 1000)
                self.order_id_creation_timestamp.remove((exchange, id, timestamp))

    def did_fail_order(self, event: MarketOrderFailureEvent):
        """
        Method called when the connector notifies an order has failed
        """
        self.calculate_latency(event)

        # handle if maker order
        if self.active_maker_orders_ids.is_or_was_active_order(event.order_id):
            exchange = self.active_maker_orders_ids.get_exchange(event.order_id)
            self.active_maker_orders_ids.remove((exchange, event.order_id))
            self.time_out_dict[exchange] = False
            self.ids_to_cancel.discard(event.order_id)
            safe_ensure_future(self.connectors[exchange]._update_balances())
            base, quote = list(self.markets[exchange])[0].split("-")
            self.logger().info(f"did_fail_order: {exchange}, quote: {self.connectors[exchange].get_available_balance(quote)}, base: {self.connectors[exchange].get_available_balance(base)}")
            self.logger().info(f"did_fail_order: try to cancel Failed order {event.order_id}")
            self.cancel(exchange, list(self.markets[exchange])[0], event.order_id)

        # handle if taker order
        if self.active_taker_orders_ids.is_or_was_active_order(event.order_id):
            exchange = self.active_taker_orders_ids.get_exchange(event.order_id)

            # uptade variables
            exchange_trade_id = self.hedge_order_id_to_filled_maker_exchange_trade_id[event.order_id]
            del self.hedge_order_id_to_filled_maker_exchange_trade_id[event.order_id]
            self.hedge_trades[exchange_trade_id]["status"] = "failed"
            self.time_out_dict[exchange] = False
            base, quote = list(self.markets[exchange])[0].split("-")
            safe_ensure_future(self.connectors[exchange]._update_balances())
            self.logger().info(f"did_fail_order: {exchange}, quote: {self.connectors[exchange].get_available_balance(quote)}, base: {self.connectors[exchange].get_available_balance(base)}")
            self.handle_failed_hedge(exchange_trade_id)

            # todo: delete this if the HB foundation fixes the issue with kucoin
            if exchange == "kucoin":
                self.logger().info("did_fail_order: remove order from tracking")
                self.connectors[exchange].stop_tracking_order(event.order_id)

        self.log_inflight_orders()

    def log_inflight_orders(self):
        # log all orders to debug
        self.logger().info("in_flight_orders:")

        for exchange, token in self.markets.items():
            for order in self.connectors[exchange].in_flight_orders.values():
                order_id = order.client_order_id
                amount = order.amount
                side = "BUY" if order.trade_type == TradeType.BUY else "SELL"
                current_state = order.current_state
                self.logger().info(f"{exchange}, side: {side}, order_id: {order_id}, amount: {amount}, current_state: {current_state}")

    def did_fill_order(self, event: OrderFilledEvent):
        # handle if maker order
        if self.active_maker_orders_ids.is_or_was_active_order(event.order_id):
            exchange = self.active_maker_orders_ids.get_exchange(event.order_id)
            self.logger().info(f"Filled maker {event.trade_type} order for {event.amount} with price: {event.price} exchange: {exchange}")

            # hedge trade
            hedge_exchange = self.maker_order_id_to_hedge_exchange[event.order_id]
            safe_ensure_future(self.place_hedge(event, hedge_exchange))

    def handle_order_complete_event(self, event):
        # handle if maker order
        if self.active_maker_orders_ids.is_or_was_active_order(event.order_id):
            self.active_maker_orders_ids.remove(event.order_id)

        # handle if taker order
        if self.active_taker_orders_ids.is_or_was_active_order(event.order_id):
            self.active_taker_orders_ids.remove(event.order_id)
            exchange = self.active_taker_orders_ids.get_exchange(event.order_id)
            self.logger().info(f"handle_order_complete_event: hedge_trades: {self.hedge_trades}")
            exchange_trade_id = self.hedge_order_id_to_filled_maker_exchange_trade_id[event.order_id]
            del self.hedge_order_id_to_filled_maker_exchange_trade_id[event.order_id]
            del self.hedge_trades[exchange_trade_id]
            self.delete_all_processed_trades_from_hedge_trades()
            self.time_out_dict[exchange] = False

        self.ids_to_cancel.discard(event.order_id)
        self.log_inflight_orders()

    def did_complete_sell_order(self, event: SellOrderCompletedEvent):
        """
        Method called when the connector notifies a sell order has been completed (fully filled)
        """
        self.handle_order_complete_event(event)

    def did_complete_buy_order(self, event: BuyOrderCompletedEvent):
        """
        Method called when the connector notifies a buy order has been completed (fully filled)
        """
        self.handle_order_complete_event(event)

    def did_cancel_order(self, event: OrderCancelledEvent):
        """
        Method called when the connector notifies an order has been cancelled
        """
        # handle if maker order
        if self.active_maker_orders_ids.is_or_was_active_order(event.order_id):
            exchange = self.active_maker_orders_ids.get_exchange(event.order_id)
            self.active_maker_orders_ids.remove(event.order_id)
            self.maker_order_id_to_hedge_exchange.pop(exchange, None)
            if self.is_perp[exchange]:
                safe_ensure_future(self.connectors[exchange]._update_balances())

        # handle if taker order
        if self.active_taker_orders_ids.is_or_was_active_order(event.order_id):
            self.logger().info(f"did_cancel_order: taker order {event.order_id} has been cancelled! ")

            for exchange, token in self.markets.items():
                try:
                    # Attempt to access the in-flight order using the order ID
                    order = self.connectors[exchange].in_flight_orders[event.order_id]
                    amount_left = order.amount - order.executed_amount_base
                    pair = order.trading_pair
                    is_buy = True if order.trade_type == TradeType.BUY else False
                    exchange_trade_id = self.hedge_order_id_to_filled_maker_exchange_trade_id[event.order_id]
                    self.logger().info(f"did_cancel_order: place left over order: exchange: {exchange} part: {pair} amount_left: {amount_left} ")

                    # place left over
                    safe_ensure_future(self.execute_hedge(amount_left, is_buy, exchange, exchange_trade_id))
                    break

                except KeyError:
                    continue

    def _process_public_trade_0(self, event_tag: int, market: ConnectorBase, event: OrderBookTradeEvent):
        # self.logger().info(f"Trade on exchange: {market.display_name}")
        # self.logger().info(f"event_tag: {event_tag}")
        # self.logger().info(f"market: {market}")
        # self.logger().info(f"event gate io: {event}")
        pass

    def _process_public_trade_1(self, event_tag: int, market: ConnectorBase, event: OrderBookTradeEvent):
        pass

    def delete_all_processed_trades_from_hedge_trades(self):
        keys_to_delete = []
        for id, info in self.hedge_trades.items():
            if info["status"] == "processed":
                keys_to_delete.append(id)
        for key in keys_to_delete:
            del self.hedge_trades[key]

    def stop(self, clock: Clock):
        """
        Without this functionality, the network iterator will continue running forever after stopping the strategy
        """
        super().stop(clock)

        for exchange, token in self.markets.items():
            # stop indicator threads
            for pair in token:
                try:
                    self.volatility_indicator[exchange][pair].stop()
                except KeyError:
                    pass
