from decimal import Decimal
from enum import Enum

from hummingbot.core.data_type.common import OrderType
from hummingbot.smart_components.executors.data_types import ConnectorPair, ExecutorConfigBase


class TradeExecutorConfig(ExecutorConfigBase):
    type = "trade_executor"
    market: ConnectorPair
    order_amount: Decimal
    open_order_type: OrderType
    close_order_type: OrderType
    is_buy: bool


class TradeExecutorStatus(Enum):
    NOT_STARTED = 1
    OPENING = 2
    ACTIVE = 3
    CLOSING = 4
    COMPLETED = 5
    FAILED = 6
