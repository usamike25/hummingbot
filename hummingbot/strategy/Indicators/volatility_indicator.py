from decimal import Decimal

import numpy as np

from hummingbot.strategy.Indicators.base_indicator import BaseIndicator
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple


class VolatilityIndicator(BaseIndicator):
    def __init__(self, market: MarketTradingPairTuple, main_loop_update_interval_s, vol_update_s=5, window=300):
        """
        Initialize the VolatilityIndicator with the given market, price delegate, update interval, and window size.

        Args:
            market (MarketTradingPairTuple): The market, trading pair, base and quote.
            main_loop_update_interval_s (int): Interval in seconds for updating the price feed.
            vol_update_s (int): Interval in seconds for recalculating volatility.
            window (int): The number of price points to consider for volatility calculation.
        """
        super().__init__(market, main_loop_update_interval_s)
        self.vol = 0
        self.prices = []
        self.window = window
        self.vol_update_s = vol_update_s  # Recalculates volatility every vol_update_s seconds
        self._update_every_x_prices = vol_update_s * (1 / main_loop_update_interval_s)
        self.counter = 0

    def main_function(self):
        self.append_price(self.market.get_mid_price())

    @property
    def current_value(self):
        if not np.isnan(self.vol):
            return Decimal(self.vol)
        else:
            return Decimal(0)

    @property
    def current_value_pct(self):
        if not np.isnan(self.vol):
            return (Decimal(self.vol) / self.market.get_mid_price()) * Decimal("100.0")
        else:
            return Decimal(0)

    def append_price(self, price):
        if self._update_every_x_prices < self.counter:
            self.calculate()
            self.counter = 0
        else:
            self.counter += 1

        self.prices.append(float(price))
        self.prices = self.prices[-self.window:]

    def calculate(self):
        arr = np.array(self.prices)
        mid_price = float(self.market.get_mid_price())
        volatility = np.nanstd(np.diff(arr) / mid_price) * (np.sqrt(1 / self.main_loop_update_interval_s))  # adjust to get vol in ticks per quare root

        self.vol = volatility
        if not np.isnan(self.vol):
            return self.vol
        else:
            return 0
