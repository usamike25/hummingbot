from decimal import Decimal
from typing import cast

from hummingbot.connector.exchange.paper_trade import create_paper_trade_market
from hummingbot.connector.gateway.amm.gateway_evm_amm import GatewayEVMAMM
from hummingbot.connector.gateway.common_types import Chain
from hummingbot.connector.gateway.gateway_price_shim import GatewayPriceShim
from hummingbot.core.utils.trading_pair_fetcher import TradingPairFetcher
from hummingbot.strategy.amm_arb.amm_arb import AmmArbStrategy
from hummingbot.strategy.amm_arb.amm_arb_config_map import amm_arb_config_map
from hummingbot.strategy.amm_arb.utils import assets_equality, get_basis_asset
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.order_book_asset_price_delegate import (
    OrderBookAssetPriceDelegate,
    OrderBookInverseAssetPriceDelegate,
)


def start(self):
    connector_1 = amm_arb_config_map.get("connector_1").value.lower()
    market_1 = amm_arb_config_map.get("market_1").value
    connector_2 = amm_arb_config_map.get("connector_2").value.lower()
    market_2 = amm_arb_config_map.get("market_2").value
    order_amount = amm_arb_config_map.get("order_amount").value
    min_profitability = amm_arb_config_map.get("min_profitability").value / Decimal("100")
    market_1_slippage_buffer = amm_arb_config_map.get("market_1_slippage_buffer").value / Decimal("100")
    market_2_slippage_buffer = amm_arb_config_map.get("market_2_slippage_buffer").value / Decimal("100")
    concurrent_orders_submission = amm_arb_config_map.get("concurrent_orders_submission").value
    debug_price_shim = amm_arb_config_map.get("debug_price_shim").value
    gateway_transaction_cancel_interval = amm_arb_config_map.get("gateway_transaction_cancel_interval").value
    arb_asset = amm_arb_config_map.get("arb_asset").value

    dex_orders_only = amm_arb_config_map.get("dex_orders_only").value
    min_required_quote_balance = amm_arb_config_map.get("min_required_quote_balance").value

    base_1, quote_1 = market_1.split("-")
    base_2, quote_2 = market_2.split("-")

    # move arb_asset to the base
    if not assets_equality(arb_asset, base_1):
        base_1, quote_1 = quote_1, base_1
        market_1 = f"{base_1}-{quote_1}"

    if not assets_equality(arb_asset, base_2):
        base_2, quote_2 = quote_2, base_2
        market_2 = f"{base_2}-{quote_2}"

    self._initialize_markets([(connector_1, [market_1]), (connector_2, [market_2])])
    base_1, quote_1 = market_1.split("-")
    base_2, quote_2 = market_2.split("-")

    market_info_1 = MarketTradingPairTuple(self.markets[connector_1], market_1, base_1, quote_1)
    market_info_2 = MarketTradingPairTuple(self.markets[connector_2], market_2, base_2, quote_2)
    self.market_trading_pair_tuples = [market_info_1, market_info_2]

    # create conversion_asset_price_delegate
    conversion_asset_price_delegate = None
    conversion_pair_base = get_basis_asset(quote_1)
    conversion_pair_quote = get_basis_asset(quote_2)
    conversion_pair: str = f"{conversion_pair_base}-{conversion_pair_quote}"
    print(f"conversion_pair: {conversion_pair}")

    trading_pair_fetcher: TradingPairFetcher = TradingPairFetcher.get_instance()
    if trading_pair_fetcher.ready:
        trading_pairs = trading_pair_fetcher.trading_pairs.get(exchange, [])
        if len(trading_pairs) == 0 or conversion_pair not in trading_pairs:
            # todo if the pair is for example ETH-XXX and it does not exist create a synthetic market with ETH-USDT and XXX-USDT
            raise ValueError(f"Trading pair '{conversion_pair}' not found for exchange '{exchange}'.")

    ext_market = create_paper_trade_market('binance', self.client_config_map, [conversion_pair])
    self.markets['binance']: ExchangeBase = ext_market
    conversion_asset_price_delegate = OrderBookAssetPriceDelegate(ext_market, conversion_pair)


    if debug_price_shim:
        amm_market_info: MarketTradingPairTuple = market_info_1
        other_market_info: MarketTradingPairTuple = market_info_2
        other_market_name: str = connector_2
        if AmmArbStrategy.is_gateway_market(other_market_info):
            amm_market_info = market_info_2
            other_market_info = market_info_1
            other_market_name = connector_1
        if Chain.ETHEREUM.chain == amm_market_info.market.chain:
            amm_connector: GatewayEVMAMM = cast(GatewayEVMAMM, amm_market_info.market)
        else:
            raise ValueError(f"Unsupported chain: {amm_market_info.market.chain}")
        GatewayPriceShim.get_instance().patch_prices(
            other_market_name,
            other_market_info.trading_pair,
            amm_connector.connector_name,
            amm_connector.chain,
            amm_connector.network,
            amm_market_info.trading_pair
        )

    self.strategy = AmmArbStrategy()
    self.strategy.init_params(market_info_1=market_info_1,
                              market_info_2=market_info_2,
                              min_profitability=min_profitability,
                              order_amount=order_amount,
                              market_1_slippage_buffer=market_1_slippage_buffer,
                              market_2_slippage_buffer=market_2_slippage_buffer,
                              concurrent_orders_submission=concurrent_orders_submission,
                              gateway_transaction_cancel_interval=gateway_transaction_cancel_interval,
                              conversion_asset_price_delegate=conversion_asset_price_delegate,
                              dex_orders_only=dex_orders_only,
                              min_required_quote_balance=min_required_quote_balance
                              )
