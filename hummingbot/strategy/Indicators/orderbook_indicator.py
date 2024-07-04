from decimal import Decimal

import numpy as np

from hummingbot.strategy.Indicators.base_indicator import BaseIndicator
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple


class OrderbookIndicator(BaseIndicator):
    def __init__(self, market: MarketTradingPairTuple, main_loop_update_interval_s):
        super().__init__(market, main_loop_update_interval_s)

    def main_function(self):
        pass

    @staticmethod
    def scale_to_0_1(arr):
        return np.interp(arr, (arr.min(), arr.max()), (0, 1))

    def get_slippage_for_volume(self, is_buy: bool, amount: Decimal):
        mid_price = self.market.get_mid_price()
        execution_price = self.market.get_price_for_volume(is_buy, amount).result_price
        if execution_price.is_nan():
            return execution_price  # returns nan

        if is_buy:
            slippage = ((execution_price - mid_price) / mid_price * Decimal("100"))
            slippage = Decimal("0") if slippage < Decimal("0") else slippage  # slippage cant be negative!
        else:
            slippage = ((mid_price - execution_price) / mid_price * Decimal("100"))
            slippage = Decimal("0") if slippage < Decimal("0") else slippage  # slippage cant be negative!

        return slippage

    def notional_depth(self, is_bid: bool, depth_pct: int = 2):
        mid_price_float = float(self.market.get_mid_price())
        max_fraction = depth_pct / 100
        if is_bid:
            bid_iterator = self.market.order_book_bid_entries()
            bid_stop_price = mid_price_float * (1 - max_fraction)
            y = [float(ob_entry.amount) for ob_entry in bid_iterator if float(ob_entry.price) >= bid_stop_price]
        else:
            ask_iterator = self.market.order_book_ask_entries()
            ask_stop_price = mid_price_float * (1 + max_fraction)
            y = [float(ob_entry.amount) for ob_entry in ask_iterator if float(ob_entry.price) <= ask_stop_price]

        notional_depth = sum(y) * mid_price_float
        return Decimal(notional_depth)

    def calculate_slope_of_bid_ask(self, depth: int = 25):
        mid_price_float = float(self.market.get_mid_price())
        bid_iterator = self.market.order_book_bid_entries()
        ask_iterator = self.market.order_book_ask_entries()
        y_bid = [float(ob_entry.amount) for ob_entry in bid_iterator]
        y_ask = [float(ob_entry.amount) for ob_entry in ask_iterator]

        y_bid = y_bid[:depth]
        y_ask = y_ask[:depth]
        x_values = np.arange(depth)

        # Convert each amount to USD value
        y_bid = [amount * mid_price_float for amount in y_bid]
        y_ask = [amount * mid_price_float for amount in y_ask]

        # Convert to numpy arrays
        bid_prices = np.array(x_values)
        bid_quantities = np.array(y_bid)
        ask_prices = np.array(x_values)
        ask_quantities = np.array(y_ask)

        # Scale bid and ask quantities to 0-1
        bid_quantities = self.scale_to_0_1(bid_quantities)
        ask_quantities = self.scale_to_0_1(ask_quantities)

        # Compute cumulative quantities
        bid_cumulative_quantities = np.cumsum(bid_quantities)
        ask_cumulative_quantities = np.cumsum(ask_quantities)

        def linear_regression(x, y):
            slope, intercept = np.polyfit(x, y, 1)
            return slope, intercept

        def linear_regression_with_intercept_zero(x, y):
            slope = np.sum(x * y) / np.sum(x ** 2)
            intercept = 0
            return slope, intercept

        bid_slope_zero, _ = linear_regression_with_intercept_zero(bid_prices[:len(bid_cumulative_quantities)], bid_cumulative_quantities)
        ask_slope_zero, _ = linear_regression_with_intercept_zero(ask_prices[:len(ask_cumulative_quantities)], ask_cumulative_quantities)

        return bid_slope_zero, ask_slope_zero
