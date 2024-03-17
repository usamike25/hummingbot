from decimal import Decimal
from enum import Enum
from typing import List

from hummingbot.connector.exchange_base import PriceType
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple

from .data_types import ArbProposal, ArbProposalSide, TokenAmount

s_decimal_nan = Decimal("NaN")


class TradeDirection(Enum):
    BUY = 1
    SELL = 0


def convert_rate(from_rate, intermediary_rate):
    """
    Converts the rate of from_rate (A-B) to the target rate (A-C) using intermediary_rate (B-C).

    :param from_rate: The rate for the initial currency pair (A-B).
    :param intermediary_rate: The rate for the intermediary currency pair (B-C).
    :return: The converted rate for the target currency pair (A-C).
    """
    target_rate = from_rate * intermediary_rate
    return target_rate


async def create_arb_proposals(
        market_info_1: MarketTradingPairTuple,
        market_info_2: MarketTradingPairTuple,
        market_1_extra_flat_fees: List[TokenAmount],
        market_2_extra_flat_fees: List[TokenAmount],
        order_amount: Decimal,
        market_info_base_token_rate_source: MarketTradingPairTuple,
        base_asset,
        quote_asset
) -> List[ArbProposal]:
    order_amount = Decimal(str(order_amount))
    results = []

    tasks = []
    for trade_direction in TradeDirection:
        is_buy = trade_direction == TradeDirection.BUY
        tasks.append([
            market_info_1.market.get_quote_price(market_info_1.trading_pair, is_buy, order_amount),
            market_info_1.market.get_order_price(market_info_1.trading_pair, is_buy, order_amount),
            market_info_2.market.get_quote_price(market_info_2.trading_pair, not is_buy, order_amount),
            market_info_2.market.get_order_price(market_info_2.trading_pair, not is_buy, order_amount)
        ])

    results_raw = await safe_gather(*[safe_gather(*task_group) for task_group in tasks])

    for trade_direction, task_group_result in zip(TradeDirection, results_raw):
        is_buy = trade_direction == TradeDirection.BUY
        m_1_q_price, m_1_o_price, m_2_q_price, m_2_o_price = task_group_result

        # convert
        if market_info_1.base_asset != base_asset:
            intermediary_rate = market_info_base_token_rate_source.market.get_price_by_type(market_info_base_token_rate_source.trading_pair, PriceType.MidPrice)
            m_1_q_price = convert_rate(m_1_q_price, intermediary_rate)
            m_1_o_price = convert_rate(m_1_o_price, intermediary_rate)

        if market_info_2.base_asset != base_asset:
            intermediary_rate = market_info_base_token_rate_source.market.get_price_by_type(market_info_base_token_rate_source.trading_pair, PriceType.MidPrice)
            m_2_q_price = convert_rate(m_2_q_price, intermediary_rate)
            m_2_o_price = convert_rate(m_2_o_price, intermediary_rate)

        if any(p is None for p in (m_1_o_price, m_1_q_price, m_2_o_price, m_2_q_price)):
            continue

        first_side = ArbProposalSide(
            market_info=market_info_1,
            is_buy=is_buy,
            quote_price=m_1_q_price,
            order_price=m_1_o_price,
            amount=order_amount,
            extra_flat_fees=market_1_extra_flat_fees,
        )
        second_side = ArbProposalSide(
            market_info=market_info_2,
            is_buy=not is_buy,
            quote_price=m_2_q_price,
            order_price=m_2_o_price,
            amount=order_amount,
            extra_flat_fees=market_2_extra_flat_fees
        )

        results.append(ArbProposal(first_side, second_side))

    return results
