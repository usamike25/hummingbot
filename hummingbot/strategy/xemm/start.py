# import ast
# from decimal import Decimal
# from typing import List, Tuple, cast
#
# from hummingbot.client.settings import AllConnectorSettings
# from hummingbot.connector.gateway.amm.gateway_evm_amm import GatewayEVMAMM
# from hummingbot.connector.gateway.gateway_price_shim import GatewayPriceShim
# from hummingbot.strategy.cross_exchange_market_making.cross_exchange_market_making import (
#     CrossExchangeMarketMakingStrategy,
#     LogOption,
# )
# from hummingbot.strategy.cross_exchange_market_making.order_level import OrderLevel
# from hummingbot.strategy.maker_taker_market_pair import MakerTakerMarketPair
# from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
#
from hummingbot.strategy.xemm.xemm import XEMMStrategy
from hummingbot.strategy.xemm.xemm_config_map import xemm_config_map


def start(self):
    exchange_stats = xemm_config_map.get("exchange_stats").value
    max_order_size_quote = xemm_config_map.get("max_order_size_quote").value
    volatility_to_spread_multiplier = xemm_config_map.get("volatility_to_spread_multiplier").value
    idle_base_amount = xemm_config_map.get("idle_base_amount").value
    idle_quote_amount = xemm_config_map.get("idle_quote_amount").value

    self._initialize_markets([(connector, [connector_dict["pair"]]) for connector, connector_dict in exchange_stats.items()])

    connectors = {connector: self.markets[connector] for connector in exchange_stats.keys()}

    self.strategy = XEMMStrategy()
    self.strategy.init_params(exchange_stats=exchange_stats,
                              connectors=connectors,
                              max_order_size_quote=max_order_size_quote,
                              volatility_to_spread_multiplier=volatility_to_spread_multiplier,
                              idle_base_amount=idle_base_amount,
                              idle_quote_amount=idle_quote_amount)
