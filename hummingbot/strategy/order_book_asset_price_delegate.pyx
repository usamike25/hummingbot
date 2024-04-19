from hummingbot.core.data_type.common import PriceType
from hummingbot.connector.exchange_base import ExchangeBase
from decimal import Decimal
from .asset_price_delegate cimport AssetPriceDelegate
from hummingbot.connector.utils import combine_to_hb_trading_pair, split_hb_trading_pair

cdef class OrderBookAssetPriceDelegate(AssetPriceDelegate):
    def __init__(self, market: ExchangeBase, trading_pair: str):
        super().__init__()
        self._market = market
        self._trading_pair = trading_pair

    cdef object c_get_mid_price(self):
        return (self._market.c_get_price(self._trading_pair, True) +
                self._market.c_get_price(self._trading_pair, False))/Decimal('2')

    @property
    def ready(self) -> bool:
        return self._market.ready

    def get_price_by_type(self, price_type: PriceType) -> Decimal:
        return self._market.get_price_by_type(self._trading_pair, price_type)

    @property
    def market(self) -> ExchangeBase:
        return self._market

    @property
    def trading_pair(self) -> str:
        return self._trading_pair

    def get_pair_rate(self, pair) -> Decimal:
        return self.get_price_by_type(PriceType.MidPrice)

cdef class OrderBookInverseAssetPriceDelegate(AssetPriceDelegate):
    def __init__(self, direct_delegate: OrderBookAssetPriceDelegate):
        super().__init__()
        self._direct_delegate = direct_delegate

    @classmethod
    def create_for(cls, market: ExchangeBase, trading_pair: str) -> OrderBookInverseAssetPriceDelegate:
        direct_delegate = OrderBookAssetPriceDelegate(market=market, trading_pair=trading_pair)
        indirect_delegate = cls(direct_delegate=direct_delegate)
        return indirect_delegate

    @property
    def direct_delegate(self):
        return self._direct_delegate

    cdef object c_get_mid_price(self):
        cdef:
            object direct_price

        direct_price = self._direct_delegate.c_get_mid_price()

        if direct_price == Decimal("0"):
            self._raise_direct_delegate_price_zero_exception()

        return Decimal("1") / direct_price

    @property
    def ready(self) -> bool:
        return self._direct_delegate.ready

    def get_price_by_type(self, price_type: PriceType) -> Decimal:
        direct_price = self._direct_delegate.get_price_by_type(price_type=price_type)
        if direct_price == Decimal("0"):
            self._raise_direct_delegate_price_zero_exception()
        return Decimal("1") / direct_price

    @property
    def market(self) -> ExchangeBase:
        return self._direct_delegate.market

    @property
    def all_markets(self) -> List[ExchangeBase]:
        return self._direct_delegate.all_markets

    @property
    def all_markets_with_trading_pairs(self) -> List[Tuple[ExchangeBase, str]]:
        return self._direct_delegate.all_markets_with_trading_pairs

    @property
    def trading_pair(self) -> str:
        base, quote = split_hb_trading_pair(self._direct_delegate.trading_pair)
        return combine_to_hb_trading_pair(base=quote, quote=base)

    def _raise_direct_delegate_price_zero_exception(self):
        raise ValueError(f"Can't calculate the price of {self.trading_pair} from "
                         f"{self._direct_delegate.trading_pair} because its current price is zero")