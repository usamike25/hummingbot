from decimal import Decimal

from hummingbot.client.config.config_validators import validate_bool, validate_decimal, validate_market_trading_pair
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
    "mode": ConfigVar(
        key="mode",
        prompt="mode: profit or market_making ? >>> ",
        prompt_on_new=True,
        default="profit",
        type_str="str"),
    # todo this could be donne differently
    "exchange_stats": ConfigVar(
        key="exchange_stats",
        prompt="",
        prompt_on_new=False,
        type_str="decimal"),
    "profit_settings": ConfigVar(
        key="profit_settings",
        prompt="",
        prompt_on_new=False,
        type_str="decimal"),
    "market_making_settings": ConfigVar(
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
    "idle_amount_in_quote": ConfigVar(
        key="idle_amount_in_quote",
        prompt="idle_amount_in_quote ? >>>",
        prompt_on_new=True,
        default=Decimal("0"),
        validator=lambda v: validate_decimal(v),
        type_str="decimal"),
    "report_to_dbs": ConfigVar(
        key="report_to_dbs",
        prompt="report_to_dbs ? >>>",
        prompt_on_new=True,
        default=False,
        validator=lambda v: validate_bool(v),
        type_str="bool"),
    "hedge_order_slippage_tolerance": ConfigVar(
        key="hedge_order_slippage_tolerance",
        prompt="hedge_order_slippage_tolerance ? >>>",
        prompt_on_new=True,
        default=Decimal("0.01"),
        validator=lambda v: validate_decimal(v),
        type_str="decimal"),
    "bucket": ConfigVar(
        key="bucket",
        prompt="bucket ? >>>",
        prompt_on_new=True,
        default="new",
        # validator=lambda v: validate_bool(v),
        type_str="str"),
    "interval": ConfigVar(
        key="interval",
        prompt="interval ? >>>",
        prompt_on_new=True,
        default=Decimal("10"),
        validator=lambda v: validate_decimal(v),
        type_str="decimal"),
    "bot_identifier": ConfigVar(
        key="bot_identifier",
        prompt="bot_identifier ? >>>",
        prompt_on_new=True,
        default=Decimal("0"),
        validator=lambda v: validate_decimal(v),
        type_str="decimal"),
    "monitor_open_order_data": ConfigVar(
        key="monitor_open_order_data",
        prompt="monitor_open_order_data ? >>>",
        prompt_on_new=True,
        default=True,
        validator=lambda v: validate_bool(v),
        type_str="bool"),
    "monitor_balance_data": ConfigVar(
        key="monitor_balance_data",
        prompt="monitor_balance_data ? >>>",
        prompt_on_new=True,
        default=True,
        validator=lambda v: validate_bool(v),
        type_str="bool"),
    "monitor_market_data": ConfigVar(
        key="monitor_market_data",
        prompt="monitor_market_data ? >>>",
        prompt_on_new=True,
        default=True,
        validator=lambda v: validate_bool(v),
        type_str="bool"),

}
