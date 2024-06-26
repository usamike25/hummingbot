import logging
from decimal import Decimal
from typing import Union

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.core.event.events import (
    BuyOrderCreatedEvent,
    FundingPaymentCompletedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCreatedEvent,
)
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.logger import HummingbotLogger
from hummingbot.smart_components.executors.executor_base import ExecutorBase
from hummingbot.smart_components.executors.trade_executor.data_types import TradeExecutorConfig, TradeExecutorStatus
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class TradeExecutor(ExecutorBase):
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @property
    def is_closed(self):
        return self.trade_status in [TradeExecutorStatus.COMPLETED, TradeExecutorStatus.FAILED]

    def __init__(self, strategy: ScriptStrategyBase, config: TradeExecutorConfig, update_interval: float = 1.0):
        self.logger().info(" TradeExecutor innit")

        super().__init__(strategy=strategy, connectors=[config.market.connector_name], config=config, update_interval=update_interval)

        self.market = config.market
        self.trading_pair = config.market.trading_pair
        self.order_amount = config.order_amount

        self.trade_status = TradeExecutorStatus.NOT_STARTED
        self.is_buy = config.is_buy
        self.is_perpetual = self.is_perpetual_connector(self.market.connector_name)

        # Order tracking
        self._active_open_order_id: str = None
        self._active_close_order_id: str = None
        self._open_order_fills = []
        self._close_order_fills = []
        self._funding_payment_events = []
        self._open_executed_amount = Decimal("0")
        self._close_executed_amount = Decimal("0")
        self._position_amount = Decimal("0")
        # self.max_retries = 10

        self._cumulative_failures = 0
        self.start()

    @property
    def active_open_order(self) -> str:
        return self._active_open_order_id

    @property
    def active_close_order(self) -> str:
        return self._active_close_order_id

    @property
    def open_order_type(self) -> OrderType:
        return self.config.open_order_type

    @property
    def close_order_type(self) -> OrderType:
        return self.config.close_order_type

    @property
    def current_position_average_price(self) -> Decimal:
        pass

    @property
    def trade_pnl_pct(self):
        """
        This method is responsible for calculating the trade pnl (Pure pnl without fees)
        """
        pass

    @property
    def trade_pnl_quote(self) -> Decimal:
        """
        This method is responsible for calculating the trade pnl in quote asset
        """
        return self.trade_pnl_pct * self.open_filled_amount_quote

    def get_net_pnl_pct(self) -> Decimal:
        """
        This method is responsible for calculating the net pnl percentage
        """
        return self.net_pnl_quote / self.open_filled_amount_quote if self.open_filled_amount_quote > Decimal("0") else Decimal("0")

    def get_cum_fees_quote(self) -> Decimal:
        """
        This method is responsible for calculating the cumulative fees in quote asset
        """
        all_orders = self._open_orders + self._close_orders
        return sum([order.cum_fees_quote for order in all_orders])

    def validate_sufficient_balance(self):
        # TODO: Implement this method checking balances in the two exchanges
        pass

    @property
    def active_limit_orders(self):
        return [(ex, order, order.client_order_id) for ex, order in self._strategy._sb_order_tracker.active_limit_orders]

    async def control_task(self):
        if self.trade_status == TradeExecutorStatus.NOT_STARTED:
            self.place_order_at_best_price(is_open=True, is_buy=self.is_buy)
            self.trade_status = TradeExecutorStatus.OPENING

        elif self.trade_status == TradeExecutorStatus.OPENING:
            if self._active_open_order_id:
                self.check_order_and_cancel(self._active_open_order_id)
            if not self._active_open_order_id:
                self.place_order_at_best_price(is_open=True, is_buy=self.is_buy)

        elif self.trade_status == TradeExecutorStatus.ACTIVE:
            pass

        elif self.trade_status == TradeExecutorStatus.CLOSING:
            if self._active_close_order_id:
                self.check_order_and_cancel(self._active_close_order_id)
            if not self._active_close_order_id:
                self.place_order_at_best_price(is_open=False, is_buy=not self.is_buy)

    def check_order_and_cancel(self, order_id):
        order = self.get_in_flight_order(self.market.connector_name, order_id)
        if order.price != self.get_best_price(self.market.connector_name, self.market.trading_pair, True if order.trade_type == TradeType.BUY else False, order.amount):
            self._strategy.cancel(connector_name=self.market.connector_name, trading_pair=self.market.trading_pair,
                                  order_id=order_id)

    def place_order_at_best_price(self, is_open: bool, is_buy: bool):
        amount = self.order_amount - self._open_executed_amount if is_open else self._position_amount - self._close_executed_amount
        best_price = self.get_best_price(self.market.connector_name, self.market.trading_pair, self.is_buy, amount)
        order_type = self.open_order_type if is_open else self.close_order_type
        position_action = PositionAction.OPEN if is_open else PositionAction.CLOSE
        side = TradeType.BUY if is_buy else TradeType.SELL
        order_id = self.place_order(connector_name=self.market.connector_name,
                                    trading_pair=self.market.trading_pair, order_type=order_type,
                                    side=side, amount=amount, price=best_price,
                                    position_action=position_action)
        if is_open:
            self._active_open_order_id = order_id
        else:
            self._active_close_order_id = order_id

    async def get_tx_cost_in_asset(self, exchange: str, trading_pair: str, is_buy: bool, order_amount: Decimal, asset: str):
        connector = self.connectors[exchange]
        price = await self.get_resulting_price_for_amount(exchange, trading_pair, is_buy, order_amount)
        if self.is_amm_connector(exchange=exchange):
            gas_cost = connector.network_transaction_fee
            conversion_price = RateOracle.get_instance().get_pair_rate(f"{asset}-{gas_cost.token}")
            return gas_cost.amount / conversion_price
        else:
            fee = connector.get_fee(
                base_currency=asset,
                quote_currency=asset,
                order_type=OrderType.MARKET,
                order_side=TradeType.BUY if is_buy else TradeType.SELL,
                amount=order_amount,
                price=price,
                is_maker=False
            )
            return fee.fee_amount_in_token(
                trading_pair=trading_pair,
                price=price,
                order_amount=order_amount,
                token=asset,
                exchange=connector,
            )

    def process_order_created_event(self, _, market, event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        pass

    def process_order_filled_event(self, _, market, event: OrderFilledEvent):
        if event.order_id == self._active_open_order_id:
            self._open_executed_amount += event.amount
            self._open_order_fills.append(event)
        elif event.order_id == self._active_close_order_id:
            self._close_executed_amount += event.amount
            self._open_order_fills.append(event)
            if self._close_executed_amount >= self._position_amount:
                self.trade_status = TradeExecutorStatus.COMPLETED
                self.stop()

    def process_order_failed_event(self, _, market, event: MarketOrderFailureEvent):
        if event.order_id == self._active_open_order_id:
            self._active_open_order_id = None
        elif event.order_id == self._active_close_order_id.order.order_id:
            self._active_close_order_id = None

    def process_order_canceled_event(self, event_tag: int, market: ConnectorBase, event: OrderCancelledEvent):
        if event.order_id == self._active_open_order_id:
            self._active_open_order_id = None
        elif event.order_id == self._active_close_order_id.order.order_id:
            self._active_close_order_id = None

    def process_funding_payment_completed_event(self, event_tag: int, market: ConnectorBase, event: FundingPaymentCompletedEvent):
        self._funding_payment_events.append(event)

    def get_pnl(self):
        # Calculate total entry value and amount
        total_entry_value = sum(order_filled_event.price * order_filled_event.amount for order_filled_event in self._open_order_fills)
        total_entry_amount = sum(order_filled_event.amount for order_filled_event in self._open_order_fills)
        entry_weighted_average_price = (total_entry_value / total_entry_amount) if total_entry_amount != 0 else Decimal("0")

        # Calculate total exit value and amount
        total_exit_value = sum(order_filled_event.price * order_filled_event.amount for order_filled_event in self._close_order_fills)
        total_exit_amount = sum(order_filled_event.amount for order_filled_event in self._close_order_fills)
        exit_weighted_average_price = (total_exit_value / total_exit_amount) if total_exit_amount != 0 else Decimal("0")

        side_multiplier = 1 if self.is_buy else -1
        pnl_quote = side_multiplier * (exit_weighted_average_price - entry_weighted_average_price) * total_exit_amount

        # Add funding fees to PnL in quote currency
        for funding_payment_event in self._funding_payment_events:
            pnl_quote += funding_payment_event.amount

        # Calculate PnL in base currency
        pnl_base = pnl_quote / exit_weighted_average_price if exit_weighted_average_price != Decimal("0") else Decimal("0")

        return pnl_quote, pnl_base

    async def exit_trade(self):
        self.trade_status = TradeExecutorStatus.CLOSING
        if self._active_open_order_id:
            self._strategy.cancel(self.market.connector_name, self.market.trading_pair, self._active_open_order_id)
        pnl = 0
        return pnl

    def stop_trade(self):
        self.trade_status = TradeExecutorStatus.COMPLETED
        if self._active_open_order_id:
            self._strategy.cancel(self.market.connector_name, self.market.trading_pair, self._active_open_order_id)
        self.stop()

    def to_format_status(self):
        pass

    def get_best_price(self, connector_name, trading_pair, is_buy, amount):
        if is_buy:
            price = best_bid = self.connectors[connector_name].get_price(trading_pair, False)
            price = self.optimize_order_placement(best_bid, is_buy, connector_name, trading_pair, optimize_order=True)
        else:
            price = best_ask = self.connectors[connector_name].get_price(trading_pair, True)
            price = self.optimize_order_placement(best_ask, is_buy, connector_name, trading_pair, optimize_order=True)

        return price

    def optimize_order_placement(self, price, is_bid, exchange, pair, optimize_order=True):

        def amount_sub_my_orders_and_ignore_small_orders(ob_entry, amount_to_ignore_quote=20):
            amount = ob_entry.amount
            if ob_entry.price in my_limit_orders:
                amount = ob_entry.amount - float(my_limit_orders[ob_entry.price])
            return amount if amount > (amount_to_ignore_quote / ob_entry.price) else 0

        my_limit_orders = {}
        min_price_step = self.connectors[exchange].trading_rules[pair].min_price_increment
        for (ex, order, order_id) in self.active_limit_orders:
            if ex.display_name == exchange:
                my_limit_orders[float(round(order.price / min_price_step) * min_price_step)] = order.quantity

        # place order just if front of another, don't quote higher than best bid ask
        if is_bid:
            order_book_iterator = self.connectors[exchange].get_order_book(pair).bid_entries()
            first_ob_entry = next(order_book_iterator, None)
            if price >= first_ob_entry.price and amount_sub_my_orders_and_ignore_small_orders(first_ob_entry) != 0:
                return self.connectors[exchange].quantize_order_price(pair, Decimal(first_ob_entry.price))

            if optimize_order:
                for ob_entry in order_book_iterator:
                    if price == ob_entry.price and amount_sub_my_orders_and_ignore_small_orders(ob_entry) != 0:
                        return Decimal(ob_entry.price)
                    if price > ob_entry.price and amount_sub_my_orders_and_ignore_small_orders(ob_entry) != 0:
                        return self.connectors[exchange].quantize_order_price(pair, Decimal(ob_entry.price) + min_price_step)

        else:
            order_book_iterator = self.connectors[exchange].get_order_book(pair).ask_entries()
            first_ob_entry = next(order_book_iterator, None)
            if price <= first_ob_entry.price and amount_sub_my_orders_and_ignore_small_orders(first_ob_entry) != 0:
                return self.connectors[exchange].quantize_order_price(pair, Decimal(first_ob_entry.price))

            if optimize_order:
                for ob_entry in order_book_iterator:
                    if price == ob_entry.price and amount_sub_my_orders_and_ignore_small_orders(ob_entry) != 0:
                        return Decimal(ob_entry.price)
                    if price < ob_entry.price and amount_sub_my_orders_and_ignore_small_orders(ob_entry) != 0:
                        return self.connectors[exchange].quantize_order_price(pair, Decimal(ob_entry.price) - min_price_step)

        return self.connectors[exchange].quantize_order_price(pair, price)
