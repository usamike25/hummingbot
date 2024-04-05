import threading
import time
from decimal import Decimal

import numpy as np

from hummingbot.core.data_type.common import PriceType


class ActiveOrder:
    def __init__(self):
        self.buffer_size = 50
        self.data = {}  # for active lookup {order_id: exchange}
        self.deleted_data = {}  # for secondary lookup

    def add(self, order_id, exchange):
        self.data[order_id] = exchange
        self.deleted_data[order_id] = exchange

        # Ensure the dictionary does not exceed the maximum size
        if len(self.deleted_data) > self.buffer_size:
            oldest_key = next(iter(self.deleted_data))
            del self.deleted_data[oldest_key]

    def remove(self, order_id):
        self.data.pop(order_id, None)

    def get_exchange(self, order_id):
        if order_id in self.data:
            return self.data[order_id]
        elif order_id in self.deleted_data:
            return self.deleted_data[order_id]
        else:
            raise ValueError(f"Order ID {order_id} not found in active orders")

    def get_active_orders_dict(self):
        return self.data

    def is_active_order(self, order_id):
        return True if order_id in self.data or order_id in self.deleted_data else False


class UtilsFunctions:

    @staticmethod
    def is_perpetual_exchange(name):
        return True if name.endswith("perpetual_testnet") or name.endswith("perpetual") else False

    def active_limit_orders(self, child_self):
        return [(ex, order, order.client_order_id) for ex, order in child_self._sb_order_tracker.active_limit_orders]

    def optimize_order_placement(self, price, is_bid, exchange, pair, child_self):

        def amount_sub_my_orders_and_ignore_small_orders(ob_entry):
            amount = ob_entry.amount
            if ob_entry.price in my_limit_orders:
                amount = ob_entry.amount - float(my_limit_orders[ob_entry.price])

            # todo: calculate the max amount to ignore
            return amount if amount > (20 / ob_entry.price) else 0

        my_limit_orders = {}
        min_price_step = child_self.connectors[exchange].trading_rules[pair].min_price_increment

        for (ex, order, order_id) in self.active_limit_orders(child_self):
            if ex.display_name == exchange:
                my_limit_orders[float(round(order.price / min_price_step) * min_price_step)] = order.quantity

        # place order just if front of another, don't quote higher than best bid ask
        if is_bid:
            order_book_iterator = child_self.connectors[exchange].get_order_book(pair).bid_entries()
            first_ob_entry = next(order_book_iterator, None)
            if price >= first_ob_entry.price and amount_sub_my_orders_and_ignore_small_orders(first_ob_entry) != 0:
                return Decimal(first_ob_entry.price)

            for ob_entry in order_book_iterator:
                if price == ob_entry.price and amount_sub_my_orders_and_ignore_small_orders(ob_entry) != 0:
                    return Decimal(ob_entry.price)
                if price > ob_entry.price and amount_sub_my_orders_and_ignore_small_orders(ob_entry) != 0:
                    return Decimal(ob_entry.price) + min_price_step

        else:
            order_book_iterator = child_self.connectors[exchange].get_order_book(pair).ask_entries()
            first_ob_entry = next(order_book_iterator, None)
            if price <= first_ob_entry.price and amount_sub_my_orders_and_ignore_small_orders(first_ob_entry) != 0:
                return Decimal(first_ob_entry.price)

            for ob_entry in order_book_iterator:
                if price == ob_entry.price and amount_sub_my_orders_and_ignore_small_orders(ob_entry) != 0:
                    return Decimal(ob_entry.price)
                if price < ob_entry.price and amount_sub_my_orders_and_ignore_small_orders(ob_entry) != 0:
                    return Decimal(ob_entry.price) - min_price_step

        return price


class VolatilityIndicator2:
    """feed indicator every second to calcuulate vol"""

    def __init__(self, price_delegate, price_feed_update_interval_s=1, vol_update_s=5, window=300):
        self.vol = 0
        self._price_delegate = price_delegate
        self.prices = []
        self.window = window
        self.vol_update_s = vol_update_s  # recalculates vol every x seconds
        self.price_feed_update_interval_s = price_feed_update_interval_s  # updates the prices every x seconds
        self.update_every_x_prices = vol_update_s * (1 / price_feed_update_interval_s)
        self.counter = 0
        self.running = True  # Flag to control the execution of the thread
        self.thread = threading.Thread(target=self.run_main_loop)
        self.thread.daemon = True
        self.thread.start()

    def run_main_loop(self):
        while self.running:
            self.append_price(self._price_delegate.get_price_by_type(PriceType.MidPrice))
            time.sleep(self.price_feed_update_interval_s)  # sleep for the update interval, not every x prices

    def stop(self):
        """Signal the main loop to stop"""
        self.running = False
        self.thread.join()

    @property
    def current_value(self):
        if not np.isnan(self.vol):
            return Decimal(self.vol)
        else:
            return Decimal(0)

    @property
    def current_value_pct(self):
        if not np.isnan(self.vol):
            return (Decimal(self.vol) / self._price_delegate.get_price_by_type(PriceType.MidPrice)) * Decimal(100.0)
        else:
            return Decimal(0)

    def append_price(self, price):
        if self.update_every_x_prices < self.counter:
            self.calculate()
            self.counter = 0
        else:
            self.counter += 1

        self.prices.append(float(price))
        self.prices = self.prices[-self.window:]

    def calculate(self):
        arr = np.array(self.prices)
        volatility = np.nanstd(np.diff(arr)) * (np.sqrt(1 / self.price_feed_update_interval_s))  # adjust to get vol in ticks per quare root

        self.vol = volatility
        if not np.isnan(self.vol):
            return self.vol
        else:
            return 0
