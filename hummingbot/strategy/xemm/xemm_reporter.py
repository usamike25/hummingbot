import logging
from decimal import Decimal

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy.strategy_reporter import StrategyReporter

# from influxdb_client import Point, WritePrecision

logger = logging.getLogger(__name__)


class TradeRecord:
    def __init__(self, entry_trade_id, entry_exchange, entry_price, entry_amount, entry_side, entry_timestamp, entry_order_id, entry_latency):
        self.entry_exchange = entry_exchange
        self.entry_price = Decimal(entry_price)
        self.entry_amount = Decimal(entry_amount)
        self.entry_side = entry_side
        self.entry_timestamp = entry_timestamp
        self.entry_trade_id = entry_trade_id
        self.entry_order_id = entry_order_id
        self.entry_latency = entry_latency

        self.exit_exchange = None
        self.exit_in_flight_order = None
        self.exit_price = None
        self.exit_amount = None
        self.exit_side = None
        self.exit_timestamp = None
        self.exit_trade_ids = []
        self.exit_order_id = None
        self.exit_latency = None
        self.exit_fills = []
        self.exit_last_reported_mid_price = None

        self.pnl_quote = Decimal("0")
        self.pnl_base = Decimal("0")
        self.pnl_pct = Decimal("0")
        self.trade_duration = Decimal("0")
        self.slippage = Decimal("0")

    def set_exit_trade(self, exit_exchange, exit_order_id, exit_latency, exit_last_reported_mid_price, exit_in_flight_order):
        self.exit_exchange = exit_exchange
        self.exit_order_id = exit_order_id
        self.exit_latency = exit_latency
        self.exit_last_reported_mid_price = exit_last_reported_mid_price
        self.exit_in_flight_order = exit_in_flight_order

        if exit_in_flight_order is None:
            return
        self.set_exit_amount()
        self.set_exit_price()
        self.set_exit_side()
        self.set_exit_timestamp()

        self.set_trade_duration()
        self.set_pnl_quote()
        self.set_pnl_base()
        self.set_pnl_pct()
        self.set_slippage_from_mid_price(exit_last_reported_mid_price)

    def set_exit_amount(self):
        """
        Calculates the exit amount of the trade as the sum of the exit fills.

        :return: Exit amount of the trade
        """
        self.exit_amount = self.exit_in_flight_order.executed_amount_base

    def set_exit_price(self):
        """
        Calculates the exit price of the trade as the weighted average of the exit fills.

        :return: Exit price of the trade
        """
        total_amount = self.exit_amount
        exit_price = sum([fill.fill_price * fill.fill_base_amount for fill in self.exit_in_flight_order.order_fills]) / total_amount
        self.exit_price = exit_price

    def set_exit_side(self):
        """
        Calculates the exit side of the trade as the side of the exit fills.

        :return: Exit side of the trade
        """
        self.exit_side = "buy" if self.exit_in_flight_order.trade_type == TradeType.BUY else "sell"

    def set_exit_timestamp(self):
        """
        Sets the exit timestamp of the trade as the timestamp of the last exit fill.

        """
        self.exit_timestamp = self.exit_in_flight_order.creation_timestamp

    def set_exit_trade_ids(self):
        """
        Sets the exit trade ids of the trade as the trade ids of the exit fills.
        """
        self.exit_trade_ids = [fill.trade_id for fill in self.exit_in_flight_order.order_fills]

    # def add_exit_fill_event(self, event: OrderFilledEvent):
    #     self.exit_fills.append(event)

    def set_pnl_pct(self):
        """
        Calculates the profit/loss of the trade as a percentage.

        :return: Profit/loss of the trade as a percentage
        """
        if self.entry_price == 0:
            self.pnl_pct = Decimal("0")  # To avoid division by zero

        side_multiplier = 1 if self.entry_side == "buy" else -1
        pnl_pct = side_multiplier * ((self.exit_price - self.entry_price) / self.entry_price) * 100
        self.pnl_pct = Decimal(pnl_pct)

    def set_pnl_quote(self):
        """
        Calculates the profit/loss of the trade in quote currency.

        :return: Profit/loss of the trade in quote currency
        """
        pnl_pct = self.get_pnl_pct() / Decimal("100")
        trade_amount = min(self.entry_amount, self.exit_amount)  # min, because one hedge can relate to many entry ids
        pnl_quote = pnl_pct * self.entry_price * trade_amount
        self.pnl_quote = Decimal(pnl_quote)

    def set_pnl_base(self):
        """
        Calculates the profit/loss of the trade in base currency.

        :return: Profit/loss of the trade in base currency
        """
        pnl_pct = self.get_pnl_pct() / Decimal("100")
        trade_amount = min(self.entry_amount, self.exit_amount)  # min, because one hedge can relate to many entry ids
        pnl_base = pnl_pct * trade_amount
        self.pnl_base = Decimal(pnl_base)

    def set_trade_duration(self):
        self.trade_duration = self.exit_timestamp - self.entry_timestamp

    def set_slippage_from_mid_price(self, last_reported_mid_price: Decimal):
        slippage = Decimal(abs(((self.exit_price - last_reported_mid_price) / last_reported_mid_price) * 100)) if last_reported_mid_price is not None else Decimal(0)
        self.slippage = slippage

    def get_pnl_quote(self):
        return self.pnl_quote

    def get_pnl_base(self):
        return self.pnl_base

    def get_pnl_pct(self):
        return self.pnl_pct

    def __str__(self):
        return (f"Trade(Entry: [ID: {self.entry_trade_id}, Exchange: {self.entry_exchange}, Price: {self.entry_price}, "
                f"Amount: {self.entry_amount}, Side: {self.entry_side}, Timestamp: {self.entry_timestamp}, Order ID: {self.entry_order_id}, Latency: {self.entry_latency}], "
                f"Exit: [IDs: {self.exit_trade_ids}, Exchange: {self.exit_exchange}, Price: {self.exit_price}, "
                f"Amount: {self.exit_amount}, Side: {self.exit_side}, Timestamp: {self.exit_timestamp}, Order ID: {self.exit_order_id}, Latency: {self.exit_latency}], "
                f"Trade Duration: {self.trade_duration}, "
                f"PnL Quote: {self.get_pnl_quote()}, "
                f"PnL Base: {self.get_pnl_base()}, "
                f"PnL Pct: {self.get_pnl_pct()}, "
                f"Exit Slippage: {self.slippage})"
                )


class XEMMReporter(StrategyReporter):
    #     """
    #     def __init__(self,
    #                  sb_order_tracker,
    #                  market_pairs: list,
    #                  bot_identifier: int,
    #                  monitor_market_data: bool = True,
    #                  monitor_balance_data: bool = True,
    #                  monitor_open_order_data: bool = True,
    #                  asset_price_delegate=None,
    #                  bucket="market_making_monitoring",
    #                  interval=int(10),
    #                  ):
    #
    #         super().__init__(bucket=bucket, interval=interval)
    #
    #     async def send_trade_record_data(self, market_info: MarketTradingPairTuple, trade_record: TradeRecord):
    #         time = datetime.utcnow()
    #         entry_exchange = trade_record.entry_exchange
    #         entry_price = trade_record.entry_price
    #         entry_amount = trade_record.entry_amount
    #         entry_side = trade_record.entry_side
    #         entry_timestamp = trade_record.entry_timestamp
    #         entry_trade_id = trade_record.entry_trade_id
    #         entry_order_id = trade_record.entry_order_id
    #         entry_latency = trade_record.entry_latency
    #
    #         exit_exchange = trade_record.exit_exchange
    #         exit_price = trade_record.exit_price
    #         exit_amount = trade_record.exit_amount
    #         exit_side = trade_record.exit_side
    #         exit_timestamp = trade_record.exit_timestamp
    #         exit_trade_ids = trade_record.exit_trade_ids
    #         exit_order_id = trade_record.exit_order_id
    #         exit_latency = trade_record.exit_latency
    #
    #         pnl_quote = trade_record.pnl_quote
    #         pnl_base = trade_record.pnl_base
    #         pnl_pct = trade_record.pnl_pct
    #         trade_duration = trade_record.trade_duration
    #         slippage = trade_record.slippage
    #
    #         try:
    #             point = Point("trade_records") \
    #                 .tag("trading_pair", str(market_info.trading_pair)) \
    #                 .tag("entry_exchange", entry_exchange) \
    #                 .field("entry_price", entry_price) \
    #                 .field("entry_amount", entry_amount) \
    #                 .field("entry_side", entry_side) \
    #                 .field("entry_timestamp", entry_timestamp) \
    #                 .field("entry_trade_id", entry_trade_id) \
    #                 .field("entry_order_id", entry_order_id) \
    #                 .field("entry_latency", entry_latency) \
    #                 .field("exit_exchange", exit_exchange) \
    #                 .field("exit_price", exit_price) \
    #                 .field("exit_amount", exit_amount) \
    #                 .field("exit_side", exit_side) \
    #                 .field("exit_timestamp", exit_timestamp) \
    #                 .field("exit_trade_ids", exit_trade_ids) \
    #                 .field("exit_order_id", exit_order_id) \
    #                 .field("exit_latency", exit_latency) \
    #                 .field("pnl_quote", pnl_quote) \
    #                 .field("pnl_base", pnl_base) \
    #                 .field("pnl_pct", pnl_pct) \
    #                 .field("trade_duration", trade_duration) \
    #                 .tag("bot_identifier"), int(self.bot_identifier) \
    #                 .field("slippage_pct", slippage) \
    #                 .time(time, WritePrecision.NS)
    #
    #             safe_ensure_future(self.write_point(point))
    #
    #         except Exception as e:
    #             self.log_with_clock(
    #                 logging.INFO,
    #                 f"{trade_record} -  is unable to write trade_records to influx db. Error: {e}")
    pass
