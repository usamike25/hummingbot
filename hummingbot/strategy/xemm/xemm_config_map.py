from decimal import Decimal

from hummingbot.client.config.config_validators import validate_decimal, validate_market_trading_pair
from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.settings import AllConnectorSettings, required_exchanges, requried_connector_trading_pairs


def exchange_on_validated(value: str) -> None:
    required_exchanges.add(value)


def market_1_validator(value: str) -> None:
    exchange = xemm_config_map["connector_1"].value
    return validate_market_trading_pair(exchange, value)


def market_1_on_validated(value: str) -> None:
    requried_connector_trading_pairs[xemm_config_map["connector_1"].value] = [value]


def market_1_prompt() -> str:
    connector = xemm_config_map.get("connector_1").value
    example = AllConnectorSettings.get_example_pairs().get(connector)
    return "Enter the token trading pair you would like to trade on %s%s >>> " \
        % (connector, f" (e.g. {example})" if example else "")


xemm_config_map = {
    "strategy": ConfigVar(
        key="strategy",
        prompt="",
        default="amm_arb"),

    # todo this could be donne differently
    "exchange_stats": ConfigVar(
        key="exchange_stats",
        prompt="",
        prompt_on_new=False,
        type_str="decimal"),
    "max_order_size_quote": ConfigVar(
        key="max_order_size_quote",
        prompt="max_order_size_quote ? >>> ",
        prompt_on_new=True,
        default=Decimal("0"),
        validator=lambda v: validate_decimal(v),
        type_str="decimal"),
    "volatility_to_spread_multiplier": ConfigVar(
        key="volatility_to_spread_multiplier",
        prompt="volatility_to_spread_multiplier ? >>> ",
        prompt_on_new=True,
        default=Decimal("1"),
        validator=lambda v: validate_decimal(v),
        type_str="decimal"),
    "idle_base_amount": ConfigVar(
        key="idle_base_amount",
        prompt="idle_base_amount ? >>>",
        prompt_on_new=True,
        default=Decimal("0"),
        validator=lambda v: validate_decimal(v),
        type_str="decimal"),
    "idle_quote_amount": ConfigVar(
        key="idle_quote_amount",
        prompt="idle_quote_amount ? >>>",
        prompt_on_new=True,
        default=Decimal("0"),
        validator=lambda v: validate_decimal(v),
        type_str="decimal"),
}
