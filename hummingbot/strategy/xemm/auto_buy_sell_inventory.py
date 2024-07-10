import asyncio
import logging
from decimal import Decimal

from hummingbot.core.data_type.common import OrderType
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger
from hummingbot.smart_components.executors.trade_executor.data_types import TradeExecutorConfig, TradeExecutorStatus
from hummingbot.smart_components.executors.trade_executor.trade_executor import TradeExecutor
from hummingbot.strategy.strategy_py_base import StrategyPyBase
from hummingbot.strategy_v2.executors.data_types import ConnectorPair


class AutoBuySellInventory:
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, strategy: StrategyPyBase, market_info_list: list, amount: Decimal, asset: str):
        self.strategy = strategy
        self.market_info_list = market_info_list
        self.amount = amount
        self.asset = asset
        self.amount_bought = Decimal("0")
        self._buy_trade_executor = None
        self._sell_trade_executors = []
        self._buy_check_loop_task = None
        self._sell_check_loop_task = None

    @property
    def is_finished(self):
        return self._buy_trade_executor is None and all(executor.trade_status == TradeExecutorStatus.COMPLETED for executor in self._sell_trade_executors)

    async def check_buy_trade_executor(self):
        while self._buy_trade_executor.trade_status in [TradeExecutorStatus.OPENING, TradeExecutorStatus.NOT_STARTED]:
            await asyncio.sleep(10)
        self.amount_bought = self._buy_trade_executor._open_executed_amount
        self._buy_trade_executor.stop_trade()
        self._buy_trade_executor = None

    async def check_sell_trade_executor(self):
        while any(executor.trade_status in [TradeExecutorStatus.OPENING, TradeExecutorStatus.NOT_STARTED] for executor in self._sell_trade_executors):
            await asyncio.sleep(10)
        for executor in self._sell_trade_executors:
            executor.stop_trade()

    def buy_inventory(self):

        if self._buy_trade_executor is not None:
            raise Exception("TradeExecutor is already running")

        asset_inventory = Decimal("0")
        for market_info in self.market_info_list:
            base_balance = market_info.market.get_balance(self.asset)
            asset_inventory += base_balance

        if asset_inventory >= self.amount:
            self.logger().info(f"Asset inventory is sufficient {self.asset} balance: {asset_inventory}")
            return
        else:
            self.amount -= asset_inventory

        exchanges_mid_price = {}
        exchanges_quote_balance = {}
        for market_info in self.market_info_list:
            mid_price = market_info.get_mid_price()
            exchanges_mid_price[market_info] = mid_price
            quote_balance = market_info.market.get_balance(market_info.quote_asset)
            exchanges_quote_balance[market_info] = quote_balance

        exchanges_mid_price = dict(sorted(exchanges_mid_price.items(), key=lambda item: item[1], reverse=False))  # ascending 0,1,2,3

        for market_info, _ in exchanges_mid_price.items():
            if exchanges_quote_balance[market_info] > self.amount * exchanges_mid_price[market_info]:
                trade_config = TradeExecutorConfig(
                    market=ConnectorPair(connector_name=market_info.market.name, trading_pair=market_info.trading_pair),
                    order_amount=self.amount,
                    open_order_type=OrderType.LIMIT,
                    close_order_type=OrderType.LIMIT,
                    is_buy=True,
                    timestamp=self.strategy.current_timestamp
                )
                self.logger().info(f"Buying {self.amount} {market_info.trading_pair} on {market_info.market.name}")
                self._buy_trade_executor = TradeExecutor(strategy=self.strategy, config=trade_config)
                self._buy_check_loop_task = safe_ensure_future(self.check_buy_trade_executor())
                return

    def sell_inventory(self):
        if self._buy_trade_executor is not None and self._buy_trade_executor.trade_status != TradeExecutorStatus.COMPLETED:
            self._buy_trade_executor.stop_trade()

        for market_info in self.market_info_list:
            base_balance = market_info.market.get_balance(self.asset)
            if base_balance >= self.amount_bought:

                trade_config = TradeExecutorConfig(
                    market=ConnectorPair(connector_name=market_info.market.name, trading_pair=market_info.trading_pair),
                    order_amount=self.amount_bought,
                    open_order_type=OrderType.LIMIT,
                    close_order_type=OrderType.LIMIT,
                    is_buy=False,
                    timestamp=self.strategy.current_timestamp
                )
                self._sell_trade_executors.append(TradeExecutor(strategy=self.strategy, config=trade_config))
                break
            else:
                trade_config = TradeExecutorConfig(
                    market=ConnectorPair(connector_name=market_info.market.name, trading_pair=market_info.trading_pair),
                    order_amount=base_balance,
                    open_order_type=OrderType.LIMIT,
                    close_order_type=OrderType.LIMIT,
                    is_buy=False,
                    timestamp=self.strategy.current_timestamp
                )
                self._sell_trade_executors.append(TradeExecutor(strategy=self.strategy, config=trade_config))
                self.amount_bought -= base_balance
        self._sell_check_loop_task = safe_ensure_future(self.check_sell_trade_executor())
