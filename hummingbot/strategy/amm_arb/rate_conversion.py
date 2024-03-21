from decimal import Decimal

from hummingbot.connector.exchange.paper_trade import create_paper_trade_market
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.core.utils.trading_pair_fetcher import TradingPairFetcher
from hummingbot.strategy.order_book_asset_price_delegate import OrderBookAssetPriceDelegate

asset_equality_map = {"USDT": ("USDT", "USDC", "USDC.E"),
                      "BTC": ("BTC", "WBTC"),
                      "ETH": ("ETH", "WETH"),
                      "BNB": ("BNB", "WBNB"),
                      "NXM": ("NXM", "WNXM"),
                      "MATIC": ("MATIC", "WMATIC"),
                      "HT": ("HT", "WHT"),
                      "TELOS": ("TELOS", "WTELOS"),
                      "AVAX": ("AVAX", "WAVAX"),
                      "ONE": ("ONE", "WONE")}


def assets_equality(asset1, asset2):
    if asset1 == asset2:
        return True
    for _, eq_set in asset_equality_map.items():
        if asset1 in eq_set and asset2 in eq_set:
            return True
    return False


def get_basis_asset(asset):
    if asset in asset_equality_map.keys():
        return asset
    for basis_asset, eq_set in asset_equality_map.items():
        if asset in eq_set:
            return basis_asset
    return asset


class RateConversionOracle:
    """
    A class to convert rates between different asset pairs using market data.
    """

    def __init__(self, asset_set, client_config_map, paper_trade_market="binance"):
        self._paper_trade_market = paper_trade_market
        self._asset_set = asset_set
        self._rates = {}
        self._trading_pair_fetcher = TradingPairFetcher.get_instance()
        self._markets = {}
        self._client_config_map = client_config_map
        self.init_rates()

    @property
    def markets(self):
        return self._markets

    def init_rates(self):
        """Initializes rates for the asset set."""
        for asset in self._asset_set:
            self.add_asset_price_delegate(asset)

    def add_asset_price_delegate(self, asset):
        """Add an OrderBookAssetPriceDelegate to self._rates"""
        asset = get_basis_asset(asset)  # convert WBTC to BTC for the exchange
        self._rates[f"{asset}-USDT"] = self.get_asset_price_delegate(asset)

    def get_asset_price_delegate(self, asset):
        """Fetches the price delegate for a given asset."""
        conversion_pair = f"{asset}-USDT"
        base, quote = conversion_pair.split("-")
        if base == quote:
            return

        if self._trading_pair_fetcher.ready:
            trading_pairs = self._trading_pair_fetcher.trading_pairs.get(self._paper_trade_market, [])
            if len(trading_pairs) == 0 or conversion_pair not in trading_pairs:
                raise ValueError(f"Trading pair '{conversion_pair}' not found for exchange '{self._paper_trade_market}'.")

        ext_market = create_paper_trade_market(self._paper_trade_market, self._client_config_map, [conversion_pair])
        self._markets[conversion_pair]: ExchangeBase = ext_market
        conversion_asset_price_delegate = OrderBookAssetPriceDelegate(ext_market, conversion_pair)
        return conversion_asset_price_delegate

    def get_pair_rate(self, pair):
        return self.get_mid_price(pair)

    def get_mid_price(self, pair):
        cross_pair_price = self.get_cross_pair_price(pair)
        if not isinstance(cross_pair_price, Decimal):
            raise ValueError(f"could not fetch mid price for {pair}. value {cross_pair_price} is not a Decimal")
        return cross_pair_price

    def get_rate_price_delegate(self, pair):
        if pair in self._rates:
            return self._rates[pair].get_mid_price()

        # create inverse and check if exist
        base, quote = pair.split("-")
        base, quote = quote, base
        pair = base + "-" + quote
        if pair in self._rates:
            return 1/self._rates[pair].get_mid_price()
        elif assets_equality(base, quote):
            return Decimal(1)
        else:
            raise ValueError(f"no OrderBookAssetPriceDelegate exists for pair '{pair}'.")
    def get_cross_pair_price(self, pair):
        """Calculates the cross pair price for two assets."""

        # convert to basis asset -> WBTC to BTC
        base, quote = pair.split("-")
        base, quote = get_basis_asset(base), get_basis_asset(quote)
        pair = base + "-" + quote
        reverse_pair = quote + "-" + base

        if assets_equality(base, quote):
            return Decimal(1)
        elif pair in self._rates.keys():
            return self._rates[pair].get_mid_price()
        elif reverse_pair in self._rates.keys():
            return 1/self._rates[reverse_pair].get_mid_price()

        base_rate_token = f"{base}-USDT"
        quote_rate_token = f"{quote}-USDT"

        if base_rate_token in self._rates and quote_rate_token in self._rates:
            base_price_in_usdt = self.get_rate_price_delegate(base_rate_token)
            quote_price_in_usdt = self.get_rate_price_delegate(quote_rate_token)
            return base_price_in_usdt / quote_price_in_usdt
        else:
            raise ValueError(f"One or both of the required pairs are not available. base: {base}, quote: {quote} in {self._rates}")
