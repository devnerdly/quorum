"""Position sizing for DCA campaigns on Brent crude CFDs.

XTB Brent CFD specs:
  - Lot size: 100 barrels
  - Leverage: x10
  - 1 lot at price P needs margin = (100 * P) / 10 = 10*P USD
"""
from __future__ import annotations

LEVERAGE = 10
LOT_SIZE_BBL = 100  # 1 lot = 100 barrels

# DCA layer schedule in MARGIN USD. Sums to ~$99k = full $100k account.
DCA_LAYERS_MARGIN: list[float] = [3000.0, 6000.0, 10000.0, 20000.0, 30000.0, 30000.0]

# Drawdown threshold (in %) that triggers the next DCA layer relative to the
# campaign's current weighted-average entry price.
DCA_DRAWDOWN_TRIGGER_PCT = 5.0

# Hard stop: close the campaign when its unrealised PnL drops below this % of equity.
HARD_STOP_DRAWDOWN_PCT = 50.0


def lots_from_margin(margin_usd: float, price: float) -> float:
    """Convert a margin amount into a number of lots at the given price."""
    nominal = margin_usd * LEVERAGE
    return nominal / (price * LOT_SIZE_BBL)


def margin_for_lots(lots: float, price: float) -> float:
    """Margin required to hold *lots* at the given price."""
    nominal = lots * LOT_SIZE_BBL * price
    return nominal / LEVERAGE


def nominal_value(lots: float, price: float) -> float:
    return lots * LOT_SIZE_BBL * price


def next_layer_margin(layers_used: int) -> float | None:
    """Return the next DCA layer's margin amount, or None if layers exhausted."""
    if layers_used >= len(DCA_LAYERS_MARGIN):
        return None
    return DCA_LAYERS_MARGIN[layers_used]


def total_planned_margin() -> float:
    return sum(DCA_LAYERS_MARGIN)
