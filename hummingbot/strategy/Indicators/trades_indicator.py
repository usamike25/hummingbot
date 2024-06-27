from collections import deque

import numpy as np

from hummingbot.connector.connector_base import ConnectorBase, Decimal, TradeType
from hummingbot.core.event.events import OrderBookTradeEvent
from hummingbot.strategy.Indicators.base_indicator import BaseIndicator
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple


class TradesIndicator(BaseIndicator):
    def __init__(self, market: MarketTradingPairTuple, main_loop_update_interval_s, window=300):
        super().__init__(market, main_loop_update_interval_s)
        self.trades = deque(maxlen=window)
        self._buy_slippage = deque(maxlen=window)
        self._sell_slippage = deque(maxlen=window)
        self.last_mid_price = None

    def main_function(self):
        self.last_mid_price = self.market.get_mid_price()

    def process_public_trade(self, event_tag: int, market: ConnectorBase, event: OrderBookTradeEvent):
        self.trades.append(event)
        self.calculate_slippage(event)

    def get_arrival_rate(self):
        return Decimal(self.calculate_arrival_rate())

    def get_buy_slippage(self):
        return Decimal(np.mean(self._buy_slippage)) if self._buy_slippage else Decimal("0")

    def get_sell_slippage(self):
        return Decimal(np.mean(self._sell_slippage)) if self._sell_slippage else Decimal("0")

    def get_overall_slippage(self):
        return Decimal(np.mean(self._buy_slippage + self._sell_slippage)) if self._buy_slippage and self._sell_slippage else Decimal("0")

    def calculate_arrival_rate(self):
        # this calculates the arrival rate per second
        timestamps = [trade.timestamp for trade in self.trades]
        inter_arrival_times = [timestamps[i] - timestamps[i - 1] for i in range(1, len(timestamps))]
        mean_inter_arrival_time = np.mean(inter_arrival_times)
        arrival_rate = 1 / mean_inter_arrival_time

        return arrival_rate

    def calculate_slippage(self, trade: OrderBookTradeEvent):
        """
            Calculates the slippage in pct
        """
        if not self.last_mid_price:
            return
        if trade.type == TradeType.BUY:
            slippage = Decimal(((trade.price - float(self.last_mid_price)) / float(self.last_mid_price) * 100))
            slippage = Decimal("0") if slippage < Decimal("0") else slippage  # slippage cant be negative!
            self._buy_slippage.append(slippage)
        elif trade.type == TradeType.SELL:
            slippage = Decimal(((float(self.last_mid_price) - trade.price) / float(self.last_mid_price) * 100))
            slippage = Decimal("0") if slippage < Decimal("0") else slippage  # slippage cant be negative!
            self._sell_slippage.append(slippage)
