# import logging
# from datetime import datetime
# from decimal import Decimal
# from typing import Dict, List
#
# from influxdb_client import Point, WritePrecision
#
# from hummingbot.core.data_type.common import PriceType, TradeType
# from hummingbot.core.data_type.limit_order import LimitOrder
# from hummingbot.core.event.events import OrderFilledEvent
# from hummingbot.core.utils.async_utils import safe_ensure_future
# from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
# from hummingbot.strategy.multi_market_asset_price_delegate import MultiMarketAssetPriceDelegate
# from hummingbot.strategy.pure_market_making.reporting_functions import calculate_bid_asks_within_depth, get_market_info
#
# from .reporter_base import ReporterBase
#
# logger = logging.getLogger(__name__)

class StrategyReporter:
    pass

# class StrategyReporter(ReporterBase):
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
#         self._market_pairs = market_pairs
#         self._sb_order_tracker = sb_order_tracker
#         self._asset_price_delegate = asset_price_delegate
#         self.monitor_market_data = monitor_market_data
#         self.monitor_open_order_data = monitor_open_order_data
#         self.monitor_balance_data = monitor_balance_data
#         self.bot_identifier = bot_identifier
#         self.started = False
#         super().__init__(bucket=bucket, interval=interval)
#
#     @property
#     def market_info_to_active_orders(self) -> Dict[MarketTradingPairTuple, List[LimitOrder]]:
#         return self._sb_order_tracker.market_pair_to_active_orders
#
#     def active_orders(self, market_pair: MarketTradingPairTuple) -> List[LimitOrder]:
#         if market_pair not in self.market_info_to_active_orders:
#             return []
#         return self.market_info_to_active_orders[market_pair]
#
#     async def calculate_and_send_metrics_to_influx(self):
#         for market_info in self._market_pairs:
#             orders_metrics, market_metrics, balance_metrics = await self.collect_metrics(market_info)
#             self.send_metrics_to_influx(market_info, orders_metrics, market_metrics, balance_metrics)
#
#     async def collect_metrics(self, market_info: MarketTradingPairTuple):
#         """ Collects metrics and send to influx DB
#
#         :return: None
#         """
#         market_metrics = {}
#         balance_metrics = {}
#         orders_metrics = {}
#
#         status_dict = market_info.market.status_dict
#
#         if self.monitor_market_data:
#             if status_dict.get('order_books_initialized'):
#                 market_metrics = get_market_info(market_info, self._asset_price_delegate)
#             else:
#                 logger.info(
#                     f"Orderbook not initialized, data not collected {status_dict}")
#
#         if self.monitor_balance_data:
#             if status_dict.get('account_balance') and status_dict.get('user_stream_initialized'):
#                 balance_metrics = await self.get_balance_info(market_info)
#             else:
#                 logger.info(
#                     f"Balances not initialized, data not collected {status_dict}")
#
#         if self.monitor_open_order_data:
#             active_orders = self.active_orders(market_info)
#
#             orders_metrics = calculate_bid_asks_within_depth(active_orders, market_info, self._asset_price_delegate)
#
#             if not status_dict.get('user_stream_initialized'):
#                 logger.info(
#                     f"User Stream not connected, balance info and orders info might be inaccurate {status_dict}")
#
#         return orders_metrics, market_metrics, balance_metrics
#
#     async def get_balance_info(self, market_info) -> dict:
#         """ Collects balance info to send to influx DB
#
#         :return: dictionary with the values to send to influx DB
#         """
#
#         balance_info = {}
#
#         if isinstance(self._asset_price_delegate, MultiMarketAssetPriceDelegate) and ('USD' not in market_info.trading_pair):
#
#             quote_rate = self._asset_price_delegate.get_conversion_price(market_info.trading_pair)
#         else:
#             quote_rate = Decimal(1)
#
#         balance_info["available_base_balance"] = market_info.market.get_available_balance(
#             market_info.base_asset)
#         balance_info["available_quote_balance"] = market_info.market.get_available_balance(
#             market_info.quote_asset)
#
#         balance_info["base_balance"] = market_info.market.get_balance(
#             market_info.base_asset)
#         balance_info["quote_balance"] = market_info.market.get_balance(
#             market_info.quote_asset)
#
#         mid_price = market_info.get_mid_price()
#
#         base_balance_in_quote = balance_info["base_balance"] * mid_price
#
#         balance_info["base_balance_in_quote"] = base_balance_in_quote
#
#         balance_info["quote_balance_usdt"] = balance_info["quote_balance"] * quote_rate
#         balance_info["base_balance_in_usdt"] = balance_info["base_balance_in_quote"] * quote_rate
#
#         balance_info["inventory_skew"] = base_balance_in_quote / (base_balance_in_quote + balance_info["quote_balance"])
#
#         return balance_info
#
#     async def get_open_order_metrics(self, market_info: MarketTradingPairTuple):
#         active_orders = self.active_orders(market_info)
#         return calculate_bid_asks_within_depth(active_orders, market_info, self._asset_price_delegate)
#
#     async def get_market_metrics(self, market_info: MarketTradingPairTuple):
#         return get_market_info(market_info, self.asset_price_delegate)
#
#     async def get_balance_metrics(self, market_info: MarketTradingPairTuple) -> dict:
#         """ Collects balance info to send to influx DB
#
#         :return: dictionary with the values to send to influx DB
#         """
#
#         balance_info = {}
#
#         if isinstance(self._asset_price_delegate, MultiMarketAssetPriceDelegate):
#
#             quote_rate = self._asset_price_delegate.get_conversion_price(market_info.trading_pair)
#         else:
#             quote_rate = Decimal(1)
#
#         balance_info["available_base_balance"] = market_info.market.get_available_balance(
#             market_info.base_asset)
#         balance_info["available_quote_balance"] = market_info.market.get_available_balance(
#             market_info.quote_asset)
#
#         balance_info["base_balance"] = market_info.market.get_balance(
#             market_info.base_asset)
#         balance_info["quote_balance"] = market_info.market.get_balance(
#             market_info.quote_asset)
#
#         mid_price = market_info.get_mid_price()
#
#         base_balance_in_quote = balance_info["base_balance"] * mid_price * quote_rate
#
#         balance_info["base_balance_in_quote"] = base_balance_in_quote
#
#         balance_info["quote_balance_usdt"] = balance_info["quote_balance"] * quote_rate
#         balance_info["base_balance_in_usdt"] = balance_info["base_balance_in_quote"] * quote_rate
#
#         balance_info["inventory_skew"] = base_balance_in_quote / (base_balance_in_quote + balance_info["quote_balance"])
#
#         return balance_info
#
#     def send_metrics_to_influx(self, market_pair: MarketTradingPairTuple, orders_info: dict, market_info: dict, balance_info: dict):
#         """ Sends the orders info, market info and balance info dictionaries to influxDB.
#
#         Also sends requirements data,
#         Vwap data
#         DeltaPnL data
#         Coingecko Terminal Price
#
#         If these featuers are toggled on in the settings
#
#         :return: None
#         """
#
#         time_utc = datetime.utcnow()
#
#         if market_info != {}:
#             try:
#
#                 market_info_point = Point("market_info") \
#                     .tag("exchange", market_pair.market.name) \
#                     .tag("trading_pair", market_pair.trading_pair) \
#                     .field("mid_price", market_info["mid_price"]) \
#                     .field("top_bid", market_info["top_bid"]) \
#                     .field("top_ask", market_info["top_ask"]) \
#                     .field("spread_pct", market_info["spread"]) \
#                     .field("half_pct_bid_depth", market_info["0.5_pct_bid_depth"]) \
#                     .field("half_bid_order_level_count", market_info['0.5_pct_bid_order_count']) \
#                     .field("half_pct_ask_depth", market_info["0.5_pct_ask_depth"]) \
#                     .field("half_ask_order_level_count", market_info['0.5_pct_ask_order_count']) \
#                     .field("one_pct_bid_depth", market_info["1_pct_bid_depth"]) \
#                     .field("one_bid_order_level_count", market_info['1_pct_bid_order_count']) \
#                     .field("one_pct_ask_depth", market_info["1_pct_ask_depth"]) \
#                     .field("one_ask_order_level_count", market_info['1_pct_ask_order_count']) \
#                     .field("two_pct_bid_depth", market_info["2_pct_bid_depth"]) \
#                     .field("two_bid_order_level_count", market_info['2_pct_bid_order_count']) \
#                     .field("two_pct_ask_depth", market_info["2_pct_ask_depth"]) \
#                     .field("two_ask_order_level_count", market_info['2_pct_ask_order_count']) \
#                     .field("five_pct_bid_depth", market_info["5_pct_bid_depth"]) \
#                     .field("five_bid_order_level_count", market_info['5_pct_bid_order_count']) \
#                     .field("five_pct_ask_depth", market_info["5_pct_ask_depth"]) \
#                     .field("five_ask_order_level_count", market_info['5_pct_ask_order_count']) \
#                     .field("ten_pct_bid_depth", market_info["10_pct_bid_depth"]) \
#                     .field("ten_bid_order_level_count", market_info['10_pct_bid_order_count']) \
#                     .field("ten_pct_ask_depth", market_info["10_pct_ask_depth"]) \
#                     .field("ten_ask_order_level_count", market_info['10_pct_ask_order_count']) \
#                     .time(time_utc, WritePrecision.NS)
#
#                 safe_ensure_future(self.write_point(market_info_point))
#
#             except Exception as e:
#                 logger.info(
#                     f"Unable to write the market_info_point to the influx db. Error: {e}")
#
#         if orders_info != {}:
#             try:
#                 orders_info_point = Point("orders_info") \
#                     .tag("exchange", market_pair.market.name) \
#                     .tag("trading_pair", market_pair.trading_pair) \
#                     .tag("bot_identifier", int(self.bot_identifier)) \
#                     .field("half_bid_depth_order_count", orders_info.get('bid').get(0.5).get('count')) \
#                     .field("half_ask_depth_order_count", orders_info.get('ask').get(0.5).get('count')) \
#                     .field("one_bid_depth_order_count", orders_info.get('bid').get(1).get('count')) \
#                     .field("one_ask_depth_order_count", orders_info.get('ask').get(1).get('count')) \
#                     .field("two_bid_depth_order_count", orders_info.get('bid').get(2).get('count')) \
#                     .field("two_ask_depth_order_count", orders_info.get('ask').get(2).get('count')) \
#                     .field("five_bid_depth_order_count", orders_info.get('bid').get(5).get('count')) \
#                     .field("five_ask_depth_order_count", orders_info.get('ask').get(5).get('count')) \
#                     .field("ten_bid_depth_order_count", orders_info.get('bid').get(10).get('count')) \
#                     .field("ten_ask_depth_order_count", orders_info.get('ask').get(10).get('count')) \
#                     .field("half_bid_depth_order_quote_quantity", orders_info.get('bid').get(0.5).get('quote_quantity')) \
#                     .field("half_ask_depth_order_quote_quantity", orders_info.get('ask').get(0.5).get('quote_quantity')) \
#                     .field("one_bid_depth_order_quote_quantity", orders_info.get('bid').get(1).get('quote_quantity')) \
#                     .field("one_ask_depth_order_quote_quantity", orders_info.get('ask').get(1).get('quote_quantity')) \
#                     .field("two_bid_depth_order_quote_quantity", orders_info.get('bid').get(2).get('quote_quantity')) \
#                     .field("two_ask_depth_order_quote_quantity", orders_info.get('ask').get(2).get('quote_quantity')) \
#                     .field("five_bid_depth_order_quote_quantity", orders_info.get('bid').get(5).get('quote_quantity')) \
#                     .field("five_ask_depth_order_quote_quantity", orders_info.get('ask').get(5).get('quote_quantity')) \
#                     .field("ten_bid_depth_order_quote_quantity", orders_info.get('bid').get(10).get('quote_quantity')) \
#                     .field("ten_ask_depth_order_quote_quantity", orders_info.get('ask').get(10).get('quote_quantity')) \
#                     .field("inside_spread", orders_info.get('inside_spread')) \
#                     .field("best_bid", orders_info.get('best_bid')) \
#                     .field("best_ask", orders_info.get('best_ask')) \
#                     .time(time_utc, WritePrecision.NS)
#
#                 safe_ensure_future(self.write_point(orders_info_point))
#
#             except Exception as e:
#                 logger.info(
#                     f"Unable to write the orders_info_point to the influx db. Error: {e}")
#
#         if balance_info != {}:
#             try:
#                 balance_info_point = Point("balance_info") \
#                     .tag("exchange", market_pair.market.name) \
#                     .tag("trading_pair", market_pair.trading_pair) \
#                     .tag("bot_identifier", int(self.bot_identifier)) \
#                     .field("base_balance", balance_info.get("base_balance")) \
#                     .field("quote_balance", balance_info.get("quote_balance")) \
#                     .field("quote_balance_usdt", balance_info.get("quote_balance_usdt")) \
#                     .field("base_balance_in_usdt", balance_info.get("base_balance_in_usdt")) \
#                     .field("available_quote_balance", balance_info.get("available_quote_balance")) \
#                     .field("available_base_balance", balance_info.get("available_base_balance")) \
#                     .field("base_balance_in_quote", balance_info.get("base_balance_in_quote")) \
#                     .field("inventory_skew", balance_info.get("inventory_skew")) \
#                     .time(time_utc, WritePrecision.NS)
#
#                 safe_ensure_future(self.write_point(balance_info_point))
#
#             except Exception as e:
#                 logger.info(
#                     f"Unable to write the balance_info_point to the influx db for the maker market. Error: {e}")
#
#         if self._asset_price_delegate and self.monitor_market_data:
#             price = self._asset_price_delegate.get_price_by_type(PriceType.MidPrice)
#             if price != {}:
#                 try:
#                     asset_price_delegate_price_point = Point("asset_price_delegate_price_point") \
#                         .tag("exchange", market_pair.market.name) \
#                         .tag("trading_pair", market_pair.trading_pair) \
#                         .field("external_api_data_feed_price", Decimal(price)) \
#                         .time(time_utc, WritePrecision.NS)
#
#                     safe_ensure_future(self.write_point(asset_price_delegate_price_point))
#
#                 except Exception as e:
#                     logger.info(
#                         f"Unable to write the asset_price_delegate_price_point to the influx db. Error: {e}")
#
#     async def send_order_fill_data(self, market_info: MarketTradingPairTuple, order_filled_event: OrderFilledEvent, last_recorded_mid_price: Decimal):
#         time = datetime.utcnow()
#         side = str('buy') if order_filled_event.trade_type is TradeType.BUY else str('sell')
#         exchange = str(market_info.market.name)
#         slippage = Decimal(abs(((Decimal(order_filled_event.price) - last_recorded_mid_price) / last_recorded_mid_price) * 100)) if last_recorded_mid_price is not None else Decimal(0)
#         try:
#             point = Point("private_trades") \
#                 .tag("exchange", exchange) \
#                 .tag("trading_pair", str(order_filled_event.trading_pair)) \
#                 .tag("trading_side", side) \
#                 .tag("bot_identifier"), int(self.bot_identifier) \
#                 .field("price", order_filled_event.price) \
#                 .field("amount", order_filled_event.amount) \
#                 .field("slippage_pct", slippage) \
#                 .time(time, WritePrecision.NS)
#
#             safe_ensure_future(self.write_point(point))
#
#         except Exception as e:
#             self.log_with_clock(
#                 logging.INFO,
#                 f"{exchange} - {str(order_filled_event.trading_pair)} is unable to write trades to influx db. Error: {e}")
#
#         try:
#             base, quote = order_filled_event.trading_pair.split("-")
#             if len(order_filled_event.trade_fee.flat_fees) > 0:
#                 for fee in order_filled_event.trade_fee.flat_fees:
#                     if fee.token == base:
#                         amount_in_quote = fee.amount * market_info.get_mid_price()
#                     else:
#                         amount_in_quote = fee.amount
#
#                     fee_percentage = (amount_in_quote / (order_filled_event.amount * order_filled_event.price))
#                     point = Point("trading_fee") \
#                         .tag("exchange", exchange) \
#                         .tag("trading_pair", str(order_filled_event.trading_pair)) \
#                         .tag("fee_token", fee.token) \
#                         .tag("bot_identifier"), int(self.bot_identifier) \
#                         .field("fee_percentage", fee_percentage) \
#                         .field("fee_amount", fee.amount) \
#                         .field("fee_amount_in_quote", amount_in_quote) \
#                         .time(time, WritePrecision.NS)
#
#                     safe_ensure_future(self.write_point(point))
#             else:
#                 fee_percentage = order_filled_event.trade_fee.percent
#                 base, quote = order_filled_event.trading_pair.split("-")
#                 fee_amount = (order_filled_event.amount * order_filled_event.price) * fee_percentage
#                 point = Point("trading_fee") \
#                     .tag("exchange", exchange) \
#                     .tag("trading_pair", str(order_filled_event.trading_pair)) \
#                     .tag("fee_token", quote) \
#                     .field("fee_percentage", fee_percentage) \
#                     .field("fee_amount", fee_amount) \
#                     .field("fee_amount_in_quote", fee_amount) \
#                     .time(time, WritePrecision.NS)
#
#                 safe_ensure_future(self.write_point(point))
#
#         except Exception as e:
#             self.log_with_clock(
#                 logging.INFO,
#                 f"{exchange} - {str(order_filled_event.trading_pair)} fee is unable to write trades to influx db. Error: {e}")
#
#     async def write_point(self, point: Point):
#         self.write_api.write(bucket=self.bucket, record=point)
