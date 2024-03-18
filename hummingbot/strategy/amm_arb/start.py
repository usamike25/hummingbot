from decimal import Decimal
from typing import cast

from hummingbot.connector.exchange.paper_trade import create_paper_trade_market
from hummingbot.connector.gateway.amm.gateway_evm_amm import GatewayEVMAMM
from hummingbot.connector.gateway.common_types import Chain
from hummingbot.connector.gateway.gateway_price_shim import GatewayPriceShim
from hummingbot.strategy.amm_arb.amm_arb import AmmArbStrategy
from hummingbot.strategy.amm_arb.amm_arb_config_map import amm_arb_config_map
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.order_book_asset_price_delegate import (
    OrderBookAssetPriceDelegate,
    OrderBookInverseAssetPriceDelegate,
)

wrapped_tokens = ['WBNB', 'WETH', 'WBTC', 'WNXM', 'WMATIC', 'WHT', 'WCRO']


def normalize_wrapped_token(token):
    if token in wrapped_tokens:
        return token[1:]
    return token

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

    dex_orders_only = amm_arb_config_map.get("dex_orders_only").value
    min_required_quote_balance = amm_arb_config_map.get("min_required_quote_balance").value

    base_1, quote_1 = market_1.split("-")
    base_2, quote_2 = market_2.split("-")
    normalized_base_1 = normalize_wrapped_token(base_1)
    normalized_base_2 = normalize_wrapped_token(base_2)

    # Check if the normalized base tokens are the same
    if normalized_base_1 != normalized_base_2:
        base_1, quote_1 = quote_1, base_1
        market_1 = f"{base_1}-{quote_1}"

    self._initialize_markets([(connector_1, [market_1]), (connector_2, [market_2])])
    base_1, quote_1 = market_1.split("-")
    base_2, quote_2 = market_2.split("-")

    market_info_1 = MarketTradingPairTuple(self.markets[connector_1], market_1, base_1, quote_1)
    market_info_2 = MarketTradingPairTuple(self.markets[connector_2], market_2, base_2, quote_2)
    self.market_trading_pair_tuples = [market_info_1, market_info_2]

    conversion_asset_price_delegate = None

    # normalize for wrapped tokens
    print(f"base_1: {base_1}, quote_1: {quote_1}")
    print(f"base_2: {base_2}, quote_1: {quote_2}")

    conversion_pair: str = f"{quote_1}-{quote_2}"
    print(f"conversion_pair: {quote_1}-{quote_2}")

    # create conversion_asset_price_delegate
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
