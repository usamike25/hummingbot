# import logging
#
# from hummingbot.strategy.pure_market_making.reporting_functions import calculate_bid_asks_within_depth, get_market_info
#
# import hummingbot.core.utils.async_utils as async_utils
# from hummingbot.core.utils.async_utils import safe_ensure_future
#
# from hummingbot.core.data_type.common import PriceType
# from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
# from decimal import Decimal
# import asyncio
# from typing import Dict, List, Optional
#
# from datetime import datetime
#
# import influxdb_client
# import urllib3
# from influxdb_client import Point, WritePrecision
# from influxdb_client.client.write_api import SYNCHRONOUS
# from datetime import datetime
# from hummingbot.core.event.events import OrderFilledEvent
# from abc import ABC, abstractmethod
# from hummingbot.core.data_type.common import OrderType, TradeType
# from hummingbot.strategy.multi_market_asset_price_delegate import MultiMarketAssetPriceDelegate
#
# from hummingbot.core.data_type.limit_order import LimitOrder
#
# logger = logging.getLogger(__name__)
#
# class ReporterBase():
#     def __init__(self,
#                  bucket,
#                  interval = 10,
#                  ):
#         self.calculate_and_send_metrics: Optional[asyncio.Task] = None
#         self.interval = interval
#         self.bucket = bucket
#         self.write_api = self.initialize_write_client()
#         self.started = False
#
#     @abstractmethod
#     def calculate_and_send_metrics_to_influx(self):
#          raise NotImplementedError
#
#     def initialize_write_client(self):
#         token = str("Jx8d3U7m1NHgXslT2wBFJBgLm6gQJ0U0LRxeX4RkpJ9MIoCEJoLM4FUcyt1iUVJ8Yba0S1OIMOwj_l4psMtOVw==")
#         org = "Hummingbot"
#         url = "https://eu-central-1-1.aws.cloud2.influxdata.com"
#         client = influxdb_client.InfluxDBClient(url=url, token=token, org=org, verify_ssl=False)
#         return client.write_api(write_options=SYNCHRONOUS)
#
#     async def calculate_and_send_metrics_loop(self):
#         while True:
#             try:
#                 await self.calculate_and_send_metrics_to_influx()
#                 await asyncio.sleep(self.interval)
#             except Exception:
#                 logger.error("Error in log metrics loop.", exc_info=True)
#             await asyncio.sleep(1)
#
#     def start(self):
#         if self.started:
#             return
#         self.calculate_and_send_metrics = async_utils.safe_ensure_future(
#             self.calculate_and_send_metrics_loop())
#         self.started = True
#         logger.info(f"Started Bot monitoring and send data to InfluxDB")
#
#     def stop(self):
#         logger.info(f"Stopped Bot monitoring and send data to InfluxDB")
#         print((self.calculate_and_send_metrics == None))
#
#         if self.calculate_and_send_metrics is not None:
#             self.calculate_and_send_metrics.cancel()
#             self.calculate_and_send_metrics = None
#
#         self.started = False
#
#     def write_point(self, point: Point):
#         safe_ensure_future(self.write_api.write(bucket=self.bucket, record=point))
#
#
