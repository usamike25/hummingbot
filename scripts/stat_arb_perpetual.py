import logging
import time
from decimal import Decimal
from statistics import mean
from typing import List

from async_timeout import timeout
from hummingbot.core.network_base import NetworkBase, NetworkStatus
from hummingbot.core.rate_oracle.rate_oracle import NetworkStatus
from hummingbot.connector.client_order_tracker import ClientOrderTracker
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.data_type.common import PositionAction
from hummingbot.strategy.strategy_py_base import StrategyPyBase
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.perpetual_trading import PerpetualTrading
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.data_type.common import PositionSide
from hummingbot.core.data_type.in_flight_order import OrderState, PerpetualDerivativeInFlightOrder, InFlightOrder
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
    OrderType,
    TradeType,
    LimitOrderStatus,
)
from hummingbot.core.data_type.order_book_query_result import ClientOrderBookQueryResult, OrderBookQueryResult
from hummingbot.strategy import hanging_orders_tracker

from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.user.user_balances import UserBalances

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import threading
import asyncio
import requests

# ML
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn import metrics
from kneed import KneeLocator

# statistics
import statsmodels.api as sm
from statsmodels.tsa.stattools import coint
from numpy import cumsum, log, polyfit, sqrt, std, subtract
from statsmodels.tsa.stattools import adfuller


class StatArb(ScriptStrategyBase):
    """
    """

    # general

    status_ready = False
    connector_name: str = "binance_perpetual_testnet"
    markets = {connector_name: {'BTC-USDT', 'ETH-USDT',
                    'BCH-USDT', 'XRP-USDT',
                    'EOS-USDT', 'LTC-USDT',}}  # "MANA-USDT", "RARE-USDT", "TOMO-USDT", "STX-USDT"

    buy_usd_amount: Decimal = Decimal("100")
    max_pairs = 20
    leverage = 2
    historical_ohlc = {}
    active_orders_entry_ids = []
    active_orders_exit_ids = []
    active_positions = {}
    active_positions_pairs = {}
    selected_pairs_df = pd.DataFrame()
    cancel_order_fills = {}
    orders_ids_about_to_cancel = []
    is_updating_orders = False
    last_order_update = 0
    active_pairs = {'BTC-USDT': 'ETH-USDT',
                    'BCH-USDT': 'XRP-USDT',
                    'EOS-USDT': 'LTC-USDT',}  # "MANA-USDT": "RARE-USDT", "TOMO-USDT": "STX-USDT"
    timeframe = "1h"
    z_score_window = 21

    # execution
    split_order = True  # if true order will be split, else average liq will be used (just leave it true) #not implemented
    split_order_quantity = 1  # split order up #not implemented
    stop_loss_fail_safe = 0.20  # stop loss at market order in case of drastic event (0.15 = 15%) #not implemented
    dynamic_signal_trigger_thresh = False  # ture -> 2.0 - 3.0 (based on volatility) #not implemented
    signal_trigger_thresh = 0.01  # z-score threshold which determines trade (must be above zero)(absoulute z-score)
    perform_beta_loading = True  # adjust position size to the beta between tickers #not implemented
    black_list = ["PAXGUSDT", "USDCUSDT", "BUSDUDST"]
    order_update_time = 3  # seconds #not implemented
    max_trade_duration_seconds = 200  # seconds
    cooldown_period_seconds = 5
    last_closed_trade = {}
    next_ohlc_update_timestamp = []
    is_updateing_ohlc = False

    # backtester
    backtest_time = "00:00"
    min_24h_trade_vol = 200000
    min_zero_crossings = 5  # min zero crossings on the spread
    backtest_signal_thresh = 2.5 #not implemented

    def on_tick(self):
        """
        Every tick this strategy will calculate the spread between two tickers and the standard deviation. depending on
        the signal_trigger_thresh variable it will enter a long/short position.
        this strategy works with perpetual markets.
        """

        #test test
        try:
            active_positions_df = self.active_positions_df()
            for i, row in active_positions_df.iterrows():
                amount = row["Amount"]
                Market = row["Market"]
                #self.logger().info(f"amount {amount * self.connectors[self.connector_name].get_mid_price(Market)}, pair {Market}")

                if amount * self.connectors[self.connector_name].get_mid_price(row["Market"]) > 65:
                    pass

        except ValueError:
            pass


        # set variables
        if self.status_ready == False:
            self.set_leverage_all()
            self.get_historical_closes()
            self.status_ready = True

        # make updates
        if self.next_ohlc_update_timestamp < self.current_timestamp and not self.is_updateing_ohlc:

            # every h update h candles
            thread_ohlc_update = threading.Thread(target=self.get_historical_closes, daemon=True)
            thread_ohlc_update.start()

        # check z-score
        for pairs in self.active_pairs.items():  # itterate trough active_pairs dic
            ticker_1 = pairs[0]
            ticker_2 = pairs[1]

            spread, zscore = self.calculate_pair_mertics(ticker_1, ticker_2, self.historical_ohlc[ticker_1],
                                                         self.historical_ohlc[ticker_2])
            #self.logger().info(f"tickers: {ticker_1} {ticker_2}  spread: {round(spread[-1],2)}  zscore: {round(zscore[-1],2)}")


            # check if we have an open position or open order
            if f"{ticker_1}/{ticker_2}" in self.active_positions_pairs.keys():
                # check signal sign
                signal_sign_positive = self.active_positions_pairs[f"{ticker_1}/{ticker_2}"]["signal_sign_positive"]
                entry_time = self.active_positions_pairs[f"{ticker_1}/{ticker_2}"]["trade_entry_timestamp"]
                status = self.active_positions_pairs[f"{ticker_1}/{ticker_2}"]["status"]
                is_time_to_close = True if (self.current_timestamp - entry_time) > self.max_trade_duration_seconds else False

                positions = self.connectors[self.connector_name].account_positions
                if not ticker_1.replace("-","") in positions.keys() and not ticker_2.replace("-","") in positions.keys() and status == "exit":
                    # update active positions
                    self.last_closed_trade[f"{ticker_1}/{ticker_2}"] = self.current_timestamp
                    # delete active pair if we have no position open in both tickers
                    self.active_positions_pairs.pop(f"{ticker_1}/{ticker_2}")

                # close trade
                if signal_sign_positive and zscore[-1] < 0 or not signal_sign_positive and zscore[-1] >= 0 or is_time_to_close:
                    if not status == "exit":
                        self.active_positions_pairs[f"{ticker_1}/{ticker_2}"]["status"] = "exit"
                        self.logger().info(f"close trade on {ticker_1, ticker_2}")
                        safe_ensure_future(self.create_orders_exit(ticker_1, ticker_2))
            else:
                # enter trade
                if abs(zscore[-1]) >= self.signal_trigger_thresh:
                    last_trade = self.last_closed_trade.get(f"{ticker_1}/{ticker_2}")

                    if isinstance(last_trade, type(None)) or last_trade + self.cooldown_period_seconds < self.current_timestamp:
                        self.logger().info(f"enter trade on {ticker_1, ticker_2}")
                        self.create_orders_entry(ticker_1, ticker_2, zscore[-1])

        if not self.is_updating_orders and self.last_order_update + self.order_update_time < self.current_timestamp:
            self.is_updating_orders = True
            safe_ensure_future(self.update_order())

    def set_leverage_all(self):
        for ticker in self.markets[self.connector_name]:
            self.connectors[self.connector_name].set_leverage(ticker, self.leverage)

    def _get_h_close_list(self, trading_pair: str) -> List[Decimal]:
        """
        Fetches binance candle stick data and returns a list daily close
        This is the API response data structure:
        [
          [
            1499040000000,      // Open time
            "0.01634790",       // Open
            "0.80000000",       // High
            "0.01575800",       // Low
            "0.01577100",       // Close
            "148976.11427815",  // Volume
            1499644799999,      // Close time
            "2434.19055334",    // Quote asset volume
            308,                // Number of trades
            "1756.87402397",    // Taker buy base asset volume
            "28.46694368",      // Taker buy quote asset volume
            "17928899.62484339" // Ignore.
          ]
        ]

        :param trading_pair: A market trading pair to

        :return: A list of daily close
        """

        url = "https://fapi.binance.com/fapi/v1/klines"  # perp endpoint
        params = {"symbol": trading_pair.replace("-", ""),
                  "interval": "1h"}
        records = requests.get(url=url, params=params).json()
        return [Decimal(str(record[4])) for record in records]

    def get_historical_closes(self):
        """
        Updates historical closes
        """
        self.is_updateing_ohlc = True
        historical_prices_dict = {}
        self.logger().info("save historical prices...")
        for pairs in self.active_pairs.items():  # itterate trough active_pairs dic
            ticker_1 = pairs[0]
            ticker_2 = pairs[1]
            historical_closes1 = self._get_h_close_list(ticker_1)
            historical_closes2 = self._get_h_close_list(ticker_2)
            historical_prices_dict[ticker_1] = historical_closes1
            historical_prices_dict[ticker_2] = historical_closes2

        # get next update timestamp
        next_ohlc_update_timestamp = (
                    datetime.now().replace(microsecond=0, second=0, minute=0) + timedelta(hours=1)).timestamp()
        self.next_ohlc_update_timestamp = next_ohlc_update_timestamp
        self.historical_ohlc = historical_prices_dict
        self.is_updateing_ohlc = False
        self.logger().info(f"now: {self.current_timestamp} next ohlc update: {next_ohlc_update_timestamp}")
        self.logger().info("Historical prices saved!")

    def calculate_pair_mertics(self, trading_pair_1, trading_pair_2, series_1, series_2):
        """
        Calculates the Spread and the corresponding Z-Score of two tickers
        """
        # cut close prices
        series_1 = series_1[-500:]
        series_2 = series_2[-500:]

        # replace last price with live mid_price
        series_1 = series_1[:-1]
        series_2 = series_2[:-1]
        series_1.append(self.connectors[self.connector_name].get_mid_price(trading_pair_1))
        series_2.append(self.connectors[self.connector_name].get_mid_price(trading_pair_2))

        # get hedge ratio
        hedge_ratio = sm.OLS(np.float_(series_1), np.float_(series_2)).fit().params[0]  # simple linear regression

        # calculate spread
        spread = pd.Series(series_1) - (pd.Series(series_2) * Decimal(hedge_ratio))

        # calculate standard deviation
        df = pd.DataFrame(np.float_(spread))  # put spread in to a pandas dataframe
        mean = df.rolling(center=False, window=self.z_score_window).mean()
        std = df.rolling(center=False, window=self.z_score_window).std()
        x = df.rolling(center=False, window=1).mean()  # take latest info
        df["ZSCORE"] = (x - mean) / std

        return np.float_(spread), df['ZSCORE'].values  # return series

    async def create_orders_exit(self, ticker_1, ticker_2):
        """
        Creates exit orders and cancels open orders.
        """
        # cancel entry orders
        order_to_cancel = []
        active_orders = self.connectors[self.connector_name]._client_order_tracker.active_orders.values()
        for order in active_orders:
            if order.trading_pair in [ticker_1, ticker_2]:
                is_done = order.is_done
                if not is_done and order.client_order_id in self.active_orders_entry_ids:
                    order_to_cancel.append(order)

        # cancel all orders in incomplete_orders
        for order in order_to_cancel:
            order_id_ = order.client_order_id
            self.orders_ids_about_to_cancel.append(order_id_)

        # create tasks
        tasks = [self.connectors[self.connector_name]._execute_cancel(order.trading_pair, order.client_order_id)
                         for order in order_to_cancel]
        try:
            async with timeout(10.0):
                cancellation_results = await safe_gather(*tasks, return_exceptions=True)
        except Exception:
            self.logger().network(
                "Unexpected error canceling orders.",
                exc_info=True,
                app_warning_msg="Failed to cancel order with Binance Perpetual. Check API key and network connection."
            )
            active_orders_test = self.connectors[self.connector_name]._client_order_tracker.active_orders #test
            account_positions_test = self.connectors[self.connector_name].account_positions #test
            pass

        for order in order_to_cancel:
            current_state = order.current_state

            #check if CANCELED
            if not current_state in [OrderState.CANCELED, OrderState.FILLED]:
                self.logger().info(f"order:{order.client_order_id} did not cancle nor fill state: {current_state}")
                await asyncio.sleep(0.1)
                await self.create_orders_exit(ticker_1, ticker_2)
                return


        # get amount to exit
        account_positions = self.connectors[self.connector_name].account_positions
        if ticker_1.replace("-","") in self.connectors[self.connector_name].account_positions.keys():
            ticker_1_is_position = True
            position_1 = self.connectors[self.connector_name].account_positions[ticker_1.replace("-","")]
            ticker_1_amount = abs(position_1.amount)
            ticker_1_side = "BUY" if position_1.amount > Decimal(0) else "SELL"
        else:
            ticker_1_amount = Decimal(0)
            ticker_1_is_position = False
            self.logger().info(f"no position in {ticker_1}")

        if ticker_2.replace("-","") in self.connectors[self.connector_name].account_positions.keys():
            ticker_2_is_position = True
            position_2 = self.connectors[self.connector_name].account_positions[ticker_2.replace("-","")]
            ticker_2_amount = abs(position_2.amount)
            ticker_2_side = "BUY" if position_2.amount > Decimal(0) else "SELL"
        else:
            ticker_2_amount = Decimal(0)
            ticker_2_is_position = False
            self.logger().info(f"no position in {ticker_2}")

        if ticker_1_is_position:
            if ticker_1_side == "SELL":
                price = self.get_best_price(ticker_1, is_buy=True, is_update=False)
                order_id_1 = self.buy(self.connector_name, ticker_1, ticker_1_amount, OrderType.LIMIT, price,
                                      PositionAction.CLOSE)
                self.active_orders_exit_ids.append(order_id_1)
            else:
                price = self.get_best_price(ticker_1, is_buy=False, is_update=False)
                order_id_1 = self.sell(self.connector_name, ticker_1, ticker_1_amount, OrderType.LIMIT, price,
                                       PositionAction.CLOSE)
                self.active_orders_exit_ids.append(order_id_1)

        if ticker_2_is_position:
            if ticker_2_side == "SELL":
                price = self.get_best_price(ticker_2, is_buy=True, is_update=False)
                order_id_2 = self.buy(self.connector_name, ticker_2, ticker_2_amount, OrderType.LIMIT, price,
                                      PositionAction.CLOSE)
                self.active_orders_exit_ids.append(order_id_2)
            else:
                price = self.get_best_price(ticker_2, is_buy=False, is_update=False)
                order_id_2 = self.sell(self.connector_name, ticker_2, ticker_2_amount, OrderType.LIMIT, price,
                                       PositionAction.CLOSE)
                self.active_orders_exit_ids.append(order_id_2)

        return

    def test_create_orders_entry(self, ticker_1, ticker_2, zscore):
        """
        Creates entry orders and checks if there is enough balance
        """
        # save pair and signal_sign_positive
        signal_sign_positive = True if zscore >= 0 else False
        timestamp = self.current_timestamp
        self.active_positions_pairs.update({f"{ticker_1}/{ticker_2}": {"signal_sign_positive": signal_sign_positive,
                                                                       "trade_entry_timestamp": timestamp,
                                                                       "status": "entry"}})
        if zscore > 0:
            long_ticker = ticker_1
            short_ticker = ticker_2
        else:
            short_ticker = ticker_1
            long_ticker = ticker_2


        proposal = []
        # long proposal
        order_price_long = self.get_best_price(long_ticker, is_buy=True, is_update=False)
        # usd_conversion_rate_long = RateOracle.get_instance().rate(long_ticker)
        amount_long = ((self.buy_usd_amount / 2) / order_price_long)
        amount_long_quantized = self.connectors[self.connector_name].quantize_order_amount(long_ticker, amount_long,
                                                                                           order_price_long)
        proposal.append(
            OrderCandidate(long_ticker, False, OrderType.LIMIT, TradeType.BUY, amount_long_quantized, order_price_long))



        for order_candidate in proposal:
            if order_candidate.amount > Decimal("0"):
                if order_candidate.order_side == TradeType.BUY:
                    buy_order_id = self.buy(self.connector_name,
                                            order_candidate.trading_pair,
                                            order_candidate.amount,
                                            order_candidate.order_type,
                                            order_candidate.price,
                                            PositionAction.OPEN)
                    self.active_orders_entry_ids.append(buy_order_id)

                if order_candidate.order_side == TradeType.SELL:
                    sell_order_id = self.sell(self.connector_name,
                                              order_candidate.trading_pair,
                                              order_candidate.amount,
                                              order_candidate.order_type,
                                              order_candidate.price,
                                              PositionAction.OPEN)
                    self.active_orders_entry_ids.append(sell_order_id)

    def create_orders_entry(self, ticker_1, ticker_2, zscore):
        """
        Creates entry orders and checks if there is enough balance
        """
        # save pair and signal_sign_positive
        signal_sign_positive = True if zscore >= 0 else False
        timestamp = self.current_timestamp
        self.active_positions_pairs.update({f"{ticker_1}/{ticker_2}": {"signal_sign_positive": signal_sign_positive,
                                                                       "trade_entry_timestamp": timestamp,
                                                                       "status": "entry"}})

        # check if there is enough balance & create proposals
        if zscore > 0:
            long_ticker = ticker_1
            short_ticker = ticker_2
        else:
            short_ticker = ticker_1
            long_ticker = ticker_2

        proposal = []

        # long proposal
        order_price_long = self.get_best_price(long_ticker, is_buy=True, is_update=False) #- 100  ############### test
        # usd_conversion_rate_long = RateOracle.get_instance().rate(long_ticker)
        amount_long = ((self.buy_usd_amount / 2) / order_price_long)
        amount_long_quantized = self.connectors[self.connector_name].quantize_order_amount(long_ticker, amount_long,
                                                                                           order_price_long)
        proposal.append(
            OrderCandidate(long_ticker, False, OrderType.LIMIT, TradeType.BUY, amount_long_quantized, order_price_long))

        # short proposal
        order_price_short = self.get_best_price(short_ticker, is_buy=False, is_update=False) #+ 100  ############## test
        # usd_conversion_rate_short = RateOracle.get_instance().rate(short_ticker)
        amount_short = ((self.buy_usd_amount / 2) / order_price_short)
        amount_short_quantized = self.connectors[self.connector_name].quantize_order_amount(short_ticker, amount_short,
                                                                                            order_price_short)
        proposal.append(OrderCandidate(short_ticker, False, OrderType.LIMIT, TradeType.SELL, amount_short_quantized,
                                       order_price_short))

        # check if there is enough capital
        # proposal = self.connectors[self.connector_name].budget_checker.adjust_candidates(proposal, all_or_none=False)
        ## -> to do: check if both orders can be filled, instead of only one at a time
        # self.logger().info(proposal)

        for order_candidate in proposal:
            if order_candidate.amount > Decimal("0"):
                if order_candidate.order_side == TradeType.BUY:
                    buy_order_id = self.buy(self.connector_name,
                                            order_candidate.trading_pair,
                                            order_candidate.amount,
                                            order_candidate.order_type,
                                            order_candidate.price,
                                            PositionAction.OPEN)
                    self.active_orders_entry_ids.append(buy_order_id)

                if order_candidate.order_side == TradeType.SELL:
                    sell_order_id = self.sell(self.connector_name,
                                              order_candidate.trading_pair,
                                              order_candidate.amount,
                                              order_candidate.order_type,
                                              order_candidate.price,
                                              PositionAction.OPEN)
                    self.active_orders_entry_ids.append(sell_order_id)

    def format_status(self) -> str:
        """
         Returns status of the current strategy on user balances and current active orders. This function is called
         when status command is issued. Override this function to create custom status display output.
         """
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []
        warning_lines = []
        warning_lines.extend(self.network_warning(self.get_market_trading_pair_tuples()))
        if len(warning_lines) > 0:
            lines.extend(["", "*** WARNINGS ***"] + warning_lines)

        balance_df = self.get_balance_df()
        lines.extend(["", "  Balances:"] + ["    " + line for line in balance_df.to_string(index=False).split("\n")])

        try:
            df = self.active_orders_df()
            lines.extend(["", "  Orders:"] + ["    " + line for line in df.to_string(index=False).split("\n")])
        except ValueError:
            lines.extend(["", "  No active maker orders."])

        try:
            df1 = self.active_positions_df()
            lines.extend(["", "  Positions:"] + ["    " + line for line in df1.to_string(index=False).split("\n")])
        except ValueError:
            lines.extend(["", "  No active Positions."])


        return "\n".join(lines)

    async def update_order(self):
        """
        Cancels the order and places a new one if the order is not at the top of the orderbook
        """
        # check orders and if the have to be replaced
        incomplete_orders = []
        all_active_orders_ids = self.active_orders_entry_ids + self.active_orders_exit_ids
        active_orders = self.connectors[self.connector_name]._client_order_tracker.active_orders.values()
        for order in active_orders:
            order_id = order.client_order_id
            ticker = order.trading_pair
            amount = order.amount
            executed_amount = order.executed_amount_base
            trade_type = order.order_type
            is_buy = True if trade_type == TradeType.BUY else False
            price = order.price
            is_done = order.is_done
            current_state = order.current_state
            is_pending = True if current_state in [OrderState.PENDING_CREATE,
                                                   OrderState.PENDING_CANCEL,
                                                   OrderState.PENDING_APPROVAL] else False
            order_in_active_order = True if order_id in all_active_orders_ids else False ###### delete
            #todo: calculate best price
            new_best_price = self.connectors[self.connector_name].get_price(ticker, False) if is_buy else self.connectors[self.connector_name].get_price(ticker, True)
            is_updateable = True if not price == new_best_price and not is_done and not is_pending and order_in_active_order and not current_state == OrderState.CANCELED else False
            # append order to incomple orders if it is updateable
            if is_updateable:
                incomplete_orders.append(order)

        # cancel all orders in incomplete_orders
        for order in incomplete_orders:
            incomplete_order_id = order.client_order_id
            self.orders_ids_about_to_cancel.append(incomplete_order_id)

        # create tasks
        tasks = [self.connectors[self.connector_name]._execute_cancel(order.trading_pair, order.client_order_id) for order in incomplete_orders]
        try:
            async with timeout(10.0):
                cancellation_results = await safe_gather(*tasks, return_exceptions=True)
        except Exception:
            self.logger().network(
                "Unexpected error canceling orders.",
                exc_info=True,
                app_warning_msg="Failed to cancel order with Binance Perpetual. Check API key and network connection."
            )
            #active_orders_test = self.connectors[self.connector_name]._client_order_tracker.active_orders  # test
            #account_positions_test = self.connectors[self.connector_name].account_positions  # test
            #pass

        # place new orders <----- only if i am sure the order got cancelled
        for order in incomplete_orders:
            order_id = order.client_order_id
            ticker = order.trading_pair
            amount = order.amount
            executed_amount = order.executed_amount_base
            is_buy = True if order.trade_type == TradeType.BUY else False
            current_state = order.current_state
            position = order.position
            is_entry = True if position == PositionAction.OPEN else False
            is_pending = True if current_state in [OrderState.PENDING_CREATE,
                                                   OrderState.PENDING_CANCEL,
                                                   OrderState.PENDING_APPROVAL] else False

            #check if place new order
            if current_state == OrderState.CANCELED:
                place_new_order = True
                # check if there is a patiall fill
                if not executed_amount == Decimal(0):
                    amount = amount - executed_amount
                    #todo: check if we can place the remaining amount (min order size)

            elif current_state == OrderState.FILLED:
                place_new_order = False
            else:
                self.logger().info(f"incomplete_order: {order_id} is not filled nor cancelled. state: {current_state}")
                place_new_order = False

            ticker_is_entry = {}
            for pair in self.active_positions_pairs.keys():
                status = self.active_positions_pairs[pair]["status"]
                ticker1, ticker2 = pair.split("/")
                ticker_is_entry[ticker1] = status
                ticker_is_entry[ticker2] = status

            if ticker_is_entry[ticker] == "exit":
                if is_entry:
                    place_new_order = False

            if place_new_order:
                new_best_price = self.connectors[self.connector_name].get_price(ticker, False) if is_buy else \
                    self.connectors[self.connector_name].get_price(ticker, True)

                # place order
                if is_buy:
                    new_order_id = self.buy(self.connector_name,
                                            ticker,
                                            abs(amount),
                                            OrderType.LIMIT,
                                            new_best_price,
                                            PositionAction.OPEN if is_entry else PositionAction.CLOSE)
                else:  # is sell
                    new_order_id = self.sell(self.connector_name,
                                             ticker,
                                             abs(amount),
                                             OrderType.LIMIT,
                                             new_best_price,
                                            PositionAction.OPEN if is_entry else PositionAction.CLOSE)

                # update order id
                if is_entry:
                    self.active_orders_entry_ids.append(new_order_id)
                else:  # is exit
                    self.active_orders_exit_ids.append(new_order_id)

        self.is_updating_orders = False
        self.orders_ids_about_to_cancel = []
        self.cancel_order_fills = {}
        self.last_order_update = self.current_timestamp
        return

    def get_best_price(self, ticker, is_buy, is_update=False, order_id=""):

        # get price step
        price_step = self.connectors[self.connector_name].get_order_price_quantum(ticker, self.connectors[
            self.connector_name].get_mid_price(ticker))

        if is_update:
            df_active_orders = self.active_orders_df()
            for i, row in df_active_orders.iterrows():
                if order_id == row["order_id"]:
                    ticker = row["Market"]
                    old_price = row["Price"]
                    not_filled_quantity = row["not_filled_quantity"]

            if is_buy:
                # check if my order is best ask/bid
                best_ask = self.connectors[self.connector_name].get_price(ticker, True)
                best_bid = self.connectors[self.connector_name].get_price(ticker, False)  # buyers
                orderbook_query = self.connectors[self.connector_name].get_volume_for_price(ticker, False, old_price)  # bid

                best_bid_volume = orderbook_query.result_volume

                if best_bid == old_price:
                    # to do: check if we are infront of the other volume, else outbid if possible.
                    if not_filled_quantity == best_bid_volume:  # ckeck if we are alone in that price
                        best_price = old_price
                    else:
                        # outbid the orderbook if possible
                        if best_bid + price_step >= best_ask:
                            best_price = best_bid
                        else:
                            best_price = best_bid + price_step
                else:
                    # outbid the orderbook if possible
                    if best_bid + price_step >= best_ask:
                        best_price = best_bid
                    else:
                        best_price = best_bid + price_step

            else:
                # check if my order is best ask/bid
                best_ask = self.connectors[self.connector_name].get_price(ticker, True)  # sellers
                best_bid = self.connectors[self.connector_name].get_price(ticker, False)
                orderbook_query = self.connectors[self.connector_name].get_volume_for_price(ticker, True, old_price)  # offer

                best_ask_volume = orderbook_query.result_volume

                if best_ask == old_price:
                    # to do: check if we are infront of the other volume, else outbid if possible.
                    if not_filled_quantity == best_ask_volume:  # ckeck if we are alone in that price
                        best_price = old_price
                    else:
                        # outbid the orderbook if possible
                        if best_ask - price_step <= best_bid:
                            best_price = best_ask
                        else:
                            best_price = best_ask + price_step
                else:
                    # outbid the orderbook if possible
                    if best_ask - price_step <= best_bid:
                        best_price = best_ask
                    else:
                        best_price = best_ask + price_step

        else:
            if is_buy:
                best_ask = self.connectors[self.connector_name].get_price(ticker, True)
                best_bid = self.connectors[self.connector_name].get_price(ticker, False)  # buyers

                # outbid the orderbook if possible
                if best_bid + price_step >= best_ask:
                    best_price = best_bid
                else:
                    best_price = best_bid + price_step
            else:
                best_ask = self.connectors[self.connector_name].get_price(ticker, True)  # sellers
                best_bid = self.connectors[self.connector_name].get_price(ticker, False)

                # outbid the orderbook if possible
                if best_ask - price_step <= best_bid:
                    best_price = best_ask
                else:
                    best_price = best_ask + price_step

        return best_price

    def get_active_positions(self):
        return self.connectors[self.connector_name].account_positions

    def active_positions_df(self) -> pd.DataFrame:
        """
        Return a data frame of all active orders for displaying purpose.
        """
        columns = ["Exchange", "Market", "Side", "Amount", "Unrealized PNL"]
        data = []
        for key, val in self.get_active_positions().items():
            data.append([
                self.connector_name,
                val.trading_pair,
                "BUY" if val.amount > Decimal(0) else "SELL",
                val.amount,
                val.unrealized_pnl,
            ])
        if not data:
            raise ValueError
        df = pd.DataFrame(data=data, columns=columns)
        df.sort_values(by=["Exchange", "Market", "Side"], inplace=True)
        return df

    def active_orders_df(self) -> pd.DataFrame:
        """
        Return a data frame of all active orders for displaying purpose.
        """
        columns = ["Exchange", "Market", "Side", "Price", "Amount", "Age", "order_id", "filled_quantity",
                   "not_filled_quantity"]
        data = []
        for connector_name, connector in self.connectors.items():
            for order in self.get_active_orders(connector_name):
                age_txt = "n/a" if order.age() <= 0. else pd.Timestamp(order.age(), unit='s').strftime('%H:%M:%S')
                data.append([
                    connector_name,
                    order.trading_pair,
                    "buy" if order.is_buy else "sell",
                    order.price,
                    order.quantity,
                    age_txt,
                    order.client_order_id,
                    order.filled_quantity,
                    order.quantity - order.filled_quantity
                ])
        if not data:
            raise ValueError
        df = pd.DataFrame(data=data, columns=columns)
        df.sort_values(by=["Exchange", "Market", "Side"], inplace=True)
        return df

    # Pairs selection

    def get_selected_pairs(self):
        """
        Selects possible trading pairs, based on K-means clustering, cointegration and other factors.
        """

        # STEP 1 - Get list of symbols
        self.logger().info("get tickers")
        ticker_list = self.get_binance_perp_tickers()

        # STEP 2 - Construct and save price history
        self.logger().info("get ohlc")
        ticker_ohlc_df = pd.DataFrame()
        for ticker in ticker_list:
            ohlc = self._get_h_close_list(ticker)
            if len(np.float_(ohlc)) == 500:
                ticker_ohlc_df[ticker] = np.float_(ohlc)

        # step3 calculate cointegration
        self.logger().info("get coint tickers")
        df_coint = self.get_cointegrated_pairs(ticker_ohlc_df)

        # step4 filter coint list
        self.selected_pairs_df = self.sort_cointegrated_list(df_coint)

    def get_binance_perp_tickers(self):
        """
        Gets all possible perpetual tickers on binance
        """
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        records = requests.get(url=url).json()
        ticker_list = []
        for i in records["symbols"]:
            sym = i["symbol"]
            base = sym.replace("USDT", "")
            ticker = base + "-USDT"
            ticker_list.append(ticker)

        return ticker_list

    def calculate_beta_to_BTC(self, sym_1, sym_2, ticker_ohlc_df):
        """
        Calculates the beta of a two tickes to BTCUSDT
        """
        # get dataframe
        price_df = ticker_ohlc_df

        # calculate pct change
        returns = price_df.pct_change()
        returns.dropna(inplace=True)

        # perform linear regression
        z1 = np.polyfit(returns["BTCUSDT"], returns[sym_1], 1)  # 1 for linear, 2 for quadratic
        z2 = np.polyfit(returns["BTCUSDT"], returns[sym_2], 1)  # 1 for linear, 2 for quadratic
        p1 = np.poly1d(z1)  # function of the line of z
        p2 = np.poly1d(z2)  # function of the line of z
        beta1 = round(p1[1], 2)
        beta2 = round(p2[1], 2)

        return beta1, beta2

    def calculate_cointegration(self, series_1, series_2):
        """
        Calculates cointegration and other metrics.
        """
        coint_flag = 0  # 0 not coint./ 1 = coint.
        coint_res = coint(series_1, series_2)  # calculates coint. for two series of prices
        coint_t = coint_res[0]  # first item coint calculation returns
        p_value = coint_res[1]
        critical_value = coint_res[2][1]
        model = sm.OLS(series_1, series_2).fit()  # calculate hedge ratio
        hedge_ratio = model.params[0]
        spread = pd.Series(series_1) - (pd.Series(series_2) * hedge_ratio)
        zero_crossings = len(np.where(np.diff(np.sign(spread)))[0])
        if p_value < 0.05 and coint_t < critical_value:
            coint_flag = 1

        # hurst exponent
        hurst_res = round(self.hurst(spread.values), 2)

        # ad fuller test ( pvalue should be below 0.05 amd ttest should be true for stationarity)
        dftest = adfuller(spread)
        p_value_adfuller = round(dftest[1], 2)
        t_test_adfuller = dftest[0] < dftest[4]["1%"]

        if t_test_adfuller:
            t_test_adfuller = 1
        else:
            t_test_adfuller = 0

        return (coint_flag, round(p_value, 2), round(coint_t, 2), round(critical_value, 2), round(hedge_ratio, 2),
                zero_crossings, hurst_res, p_value_adfuller, t_test_adfuller)

    def hurst(self, ts, min_lag=1, max_lag=100):
        """
        Calculates the hurst exponent.
        """
        lags = range(min_lag, max_lag)
        tau = [sqrt(std(subtract(ts[lag:], ts[:-lag]))) for lag in lags]
        poly = polyfit(log(lags), log(tau), 1)
        return poly[0] * 2.0

    def get_cointegrated_pairs(self, ticker_ohlc_df):
        """
        Gets a df of cointegrated pairs
        """
        cluster_series = self.get_k_means_cluster(ticker_ohlc_df)

        tested_pairs = []
        cointegrated_pairs = []

        for base_asset in cluster_series.index:
            base_label = cluster_series[base_asset]

            for compare_asset in cluster_series.index:
                compare_label = cluster_series[compare_asset]

                test_pair = base_asset + compare_asset
                test_pair = ''.join(sorted(test_pair))
                is_tested = test_pair in tested_pairs
                tested_pairs.append(test_pair)

                if compare_asset != base_asset and base_label == compare_label and not is_tested:

                    # Get close prices
                    for i, column in enumerate(ticker_ohlc_df):
                        if column == base_asset:
                            series_1 = ticker_ohlc_df[f"{base_asset}"].values
                        if column == compare_asset:
                            series_2 = ticker_ohlc_df[f"{compare_asset}"].values

                    # Check for cointegration and add cointegrated pair
                    coint_flag, p_value, t_value, c_value, hedge_ratio, zero_crossings, hurst_res, p_value_adfuller, t_test_adfuller = self.calculate_cointegration(
                        series_1, series_2)
                    if coint_flag == 1:
                        ## backtest
                        # try:
                        #    backtest_pnl = round(backtest_CCXT(base_asset, compare_asset), 3)
                        # except BadSymbol:
                        #    backtest_pnl = []

                        # calculate beta
                        beta_sym_1, beta_sym_2 = self.calculate_beta_to_BTC(base_asset, compare_asset, ticker_ohlc_df)

                        cointegrated_pairs.append({"base_asset": base_asset,
                                                   "compare_asset": compare_asset,
                                                   "label": base_label,
                                                   "p_value": p_value,
                                                   "t_value": t_value,
                                                   "c_value": c_value,
                                                   "hedge_ratio": hedge_ratio,
                                                   "zero_crossings": zero_crossings,
                                                   "backtest": 0,
                                                   "beta_sym_1": beta_sym_1,
                                                   "beta_sym_2": beta_sym_2,
                                                   "hurst_res": hurst_res,
                                                   "p_value_adfuller": p_value_adfuller,
                                                   "t_test_adfuller": t_test_adfuller,
                                                   })

        # Output results
        df_coint = pd.DataFrame(cointegrated_pairs)
        df_coint.rename(columns={"base_asset": "sym_1", "compare_asset": "sym_2"}, inplace=True)
        df_coint = df_coint.sort_values("zero_crossings", ascending=False)  # sort most crosses at the top

        return df_coint

    def get_k_means_cluster(self, ticker_ohlc_df):
        """
        Returns a Series of Pairs with corresponding cluster.
        """

        price_df = ticker_ohlc_df

        # Create DataFrame with Returns and Volatility information
        df_returns = pd.DataFrame(price_df.pct_change().mean() * 365, columns=["Returns"])  # 365 for crypto
        df_returns["Volatility"] = price_df.pct_change().std() * np.sqrt(365)  # 365 for crypto

        # Scale Features
        scaler = StandardScaler()
        scaler = scaler.fit_transform(df_returns)
        scaled_data = pd.DataFrame(scaler, columns=df_returns.columns, index=df_returns.index)
        df_scaled = scaled_data

        # Find the optimum number of clusters
        X = df_scaled.copy()
        K = range(1, 15)  # from 1 to 15
        distortions = []
        for k in K:
            kmeans = KMeans(n_clusters=k)
            kmeans.fit(X)
            distortions.append(kmeans.inertia_)

        kl = KneeLocator(K, distortions, curve="convex", direction="decreasing")
        c = kl.elbow

        # Fit K-Means Model
        k_means = KMeans(n_clusters=c)
        k_means.fit(X)
        prediction = k_means.predict(df_scaled)

        # Return the series
        clustered_series = pd.Series(index=X.index, data=k_means.labels_.flatten())
        clustered_series_all = pd.Series(index=X.index, data=k_means.labels_.flatten())
        clustered_series = clustered_series[clustered_series != -1]

        # clean df
        df = pd.DataFrame(clustered_series[:])
        df.rename(columns={0: "cluster"}, inplace=True)
        df['sym'] = df.index
        df.reset_index(drop=True, inplace=True)
        columns_titles = ["sym", "cluster"]
        df = df.reindex(columns=columns_titles)

        return clustered_series

    def get_24h_volume(self, ticker):
        """
        gets the volume traded the day before.
        """
        url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": ticker, "interval": "1d", "limit": 2}

        records = requests.get(url=url, params=params).json()
        return records[0][7]

    def sort_cointegrated_list(self, df_coint):
        """
        sorts the df of cointegrated pairs based on custom metrics.
        """
        # filters
        min_daily_vol = self.min_24h_trade_vol  # usdt

        df = df_coint

        # get all pairs listed
        ticker_list_all = []

        for symbol in df["sym_1"]:
            if not symbol in ticker_list_all:
                ticker_list_all.append(symbol)
        for symbol in df["sym_2"]:
            if not symbol in ticker_list_all:
                ticker_list_all.append(symbol)

        # get trade liq.
        filtered_ticker_list = []
        ticker_and_vol = []

        # print("filter tickers...")

        for symbol in ticker_list_all:
            time.sleep(0.1)  # protect api
            ticker = symbol
            daily_vol = self.get_24h_volume(symbol)

            # add to list if filter matches
            if float(daily_vol) > float(min_daily_vol):
                ticker_and_vol.append({"ticker": ticker,
                                       "vol": daily_vol})
                filtered_ticker_list.append(ticker)

        df_filtered_tickers = pd.DataFrame(ticker_and_vol)

        # update coint pairs list
        df1 = df_coint

        # add zscores to backtest and zero crossings
        # df1['zc_zscore'] = stats.zscore(df1['zero_crossings'])
        # df1['bt_zscore'] = stats.zscore(df1['backtest'])
        # df1['total_zscore'] = (df1['zc_zscore'] + df1['bt_zscore']) / 2

        ##
        # iterate over rows and if symbol is not in filtered list delete row
        for i, row in df1.iterrows():
            if row["sym_1"] not in filtered_ticker_list or row["sym_2"] not in filtered_ticker_list:
                df1.drop(i, inplace=True)

        # drop all rows that do not have enough 0-crossings
        for i, row in df1.iterrows():
            if row["zero_crossings"] < self.min_zero_crossings:
                df1.drop(i, inplace=True)

        ## drop rows with negative backtest
        # for i, row in df1.iterrows():
        #    if row["backtest"] < 0:
        #        df1.drop(i, inplace=True)

        # drop rows with  blocked pairs
        for i, row in df1.iterrows():
            if row["sym_1"] in self.black_list or row["sym_2"] in self.black_list:
                df1.drop(i, inplace=True)

        # drop rows with  p_value_adfuller more than 0.05 and t_test_adfuller = 0
        for i, row in df1.iterrows():
            if row["p_value_adfuller"] > 0.05 or row["t_test_adfuller"] == 0:
                df1.drop(i, inplace=True)

        # sort list by total zscore
        # df1 = df1.sort_values("total_zscore", ascending=False)
        df1 = df1.sort_values("zero_crossings", ascending=False)

        # delete if a ticker is allready used. every ticker is only used once
        used_tickers = []
        for i, row in df1.iterrows():
            if row["sym_1"] not in used_tickers and row["sym_2"] not in used_tickers:
                used_tickers.append(row["sym_1"])
                used_tickers.append(row["sym_2"])
            else:
                df1.drop(i, inplace=True)

        df1.reset_index(inplace=True)

        return df1

    # events

    def did_fail_order(self, event: MarketOrderFailureEvent):
        """
        Method called when the connector notifies an order has failed
        """

        active_orders = self.connectors[self.connector_name]._client_order_tracker.active_orders.values()
        for order in active_orders:
            if order.client_order_id == event.order_id:
                order_id = order.client_order_id
                ticker = order.trading_pair
                amount = order.amount
                trade_type = order.order_type
                position = order.position
                is_buy = True if trade_type == TradeType.BUY else False
                is_entry = True if position == PositionAction.OPEN else False

                # re submit order
                new_best_price = self.get_best_price(ticker, is_buy)
                # place order
                if is_buy:
                    new_order_id = self.buy(self.connector_name,
                                            ticker,
                                            abs(amount),
                                            OrderType.LIMIT,
                                            new_best_price,
                                            PositionAction.OPEN if is_entry else PositionAction.CLOSE)
                else:  # is sell
                    new_order_id = self.sell(self.connector_name,
                                             ticker,
                                             abs(amount),
                                             OrderType.LIMIT,
                                             new_best_price,
                                             PositionAction.OPEN if is_entry else PositionAction.CLOSE)

                # update order id
                if is_entry:
                    self.active_orders_entry_ids.append(new_order_id)
                else:  # is exit
                    self.active_orders_exit_ids.append(new_order_id)
        self.logger().info(logging.INFO, f"The order {event.order_id} failed")

    def did_cancel_order(self, event: OrderCancelledEvent):
        """
        Method called when the connector notifies an order has been cancelled
        """
        if event.order_id in self.orders_ids_about_to_cancel:
            self.orders_ids_about_to_cancel.remove(event.order_id)

        if event.order_id in self.active_orders_entry_ids:
            self.active_orders_entry_ids.remove(event.order_id)

        if event.order_id in self.active_orders_exit_ids:
            self.active_orders_exit_ids.remove(event.order_id)

        self.logger().info(f"The order {event.order_id} has been cancelled")

    def did_complete_buy_order(self, event: BuyOrderCompletedEvent):
        """
        Method called when the connector notifies a buy order has been completed (fully filled)
        """
        ticker = f"{event.base_asset}-{event.quote_asset}"
        amount = event.base_asset_amount
        order_id = event.order_id

        if order_id in self.active_orders_entry_ids:
            self.active_orders_entry_ids.remove(order_id)

        if order_id in self.active_orders_exit_ids:
            self.active_orders_exit_ids.remove(order_id)

        self.logger().info(f"The buy order {event.order_id} has been completed")

    def did_complete_sell_order(self, event: SellOrderCompletedEvent):
        """
        Method called when the connector notifies a sell order has been completed (fully filled)
        """
        ticker = f"{event.base_asset}-{event.quote_asset}"
        amount = event.base_asset_amount
        order_id = event.order_id

        if order_id in self.active_orders_entry_ids:
            self.active_orders_entry_ids.remove(order_id)

        if order_id in self.active_orders_exit_ids:
            self.active_orders_exit_ids.remove(order_id)

        self.logger().info(f"The sell order {event.order_id} has been completed")

    def did_fill_order(self, event: OrderFilledEvent):
        """
         Listens to fill order event to log it and notify the Hummingbot application.
         If you set up Telegram bot, you will get notification there as well.
         """
        if event.order_id in self.orders_ids_about_to_cancel:
            self.logger().info(f"the order: {event.order_id} has been filled anyway")
            self.cancel_order_fills[event.order_id] = event.amount
