import logging
import threading
import time

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.event.event_forwarder import SourceInfoEventForwarder
from hummingbot.core.event.events import OrderBookEvent, OrderBookTradeEvent
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple

s_logger = None


class BaseIndicator:
    """Base Indicator class that all other indicators should inherit from"""

    @classmethod
    def logger(cls):
        global s_logger
        if s_logger is None:
            s_logger = logging.getLogger(__name__)
        return s_logger

    def __init__(self, market: MarketTradingPairTuple, main_loop_update_interval_s=1):
        """
        Initialize the BaseIndicator with the given market and price feed update interval.

        Args:
            market (MarketTradingPairTuple): The market trading pair base and quote.
            main_loop_update_interval_s (int): Interval in seconds for updating the price feed.
        """
        self.market = market
        self.trading_pair = market.trading_pair
        self.base_asset = market.base_asset
        self.quote_asset = market.quote_asset
        self.main_loop_update_interval_s = main_loop_update_interval_s
        self.order_book_trade_event = SourceInfoEventForwarder(self.process_public_trade)
        self.order_book = market.order_book
        self.order_book.add_listener(OrderBookEvent.TradeEvent, self.order_book_trade_event)
        self.running = True
        self.thread = threading.Thread(target=self.run_main_loop)
        self.thread.daemon = True  # Daemonize the thread to ensure it exits when the main program does
        self.thread.start()

    def process_public_trade(self, event_tag: int, market: ConnectorBase, event: OrderBookTradeEvent):
        """
        Process public trade events.

        Args:
            event_tag (int): The event tag.
            market (ConnectorBase): The market where the event occurred.
            event (OrderBookTradeEvent): The trade event details.
        """
        pass

    def run_main_loop(self):
        """
        Main loop
        """
        time.sleep(1)

        while self.running:
            self.main_function()
            time.sleep(self.main_loop_update_interval_s)

    def main_function(self):
        """
        Placeholder for the main function of the indicator.
        """
        pass

    def stop(self):
        """
        Signal the main loop to stop and wait for the thread to finish.
        """
        self.running = False
        self.thread.join()
        self.on_stop()

    def on_stop(self):
        """
        Placeholder for the on stop function of the indicator.
        """
        pass
