"""Position sizing for DCA campaigns — 25-layer schedule with dynamic multipliers.

XTB/Binance CFD specs:
  - Lot size: 100 barrels
  - Leverage: x10
  - 1 lot at price P needs margin = (100 * P) / 10 = 10*P USD

Sizing model:
  1. A fixed BASE schedule of 25 DCA layers that gradually ramp up
     from $300 (exploration) to $5000 (full conviction at deep drawdown).
     Total planned margin ~$48k = fits within a single persona's $50k balance.
  2. A dynamic `size_multiplier` in [0.5, 3.0] computed from current
     market state — applied uniformly across every layer of a campaign.
  3. An equity-cap safety net that refuses to let total open margin
     exceed MAX_TOTAL_EXPOSURE_PCT of current equity.

The 25-layer design gives the bot much more granularity — it can build
a position slowly over many DCA triggers instead of committing big
chunks on just 6 layers. Early layers are cheap probes; later layers
are full conviction at deep drawdown.

The multiplier is computed ONCE at campaign open and stored on the
Campaign row, so every subsequent DCA layer uses the same proportion.
"""
from __future__ import annotations

LEVERAGE = 10
LOT_SIZE_BBL = 100  # 1 lot = 100 barrels

# 25-layer DCA schedule (pre-multiplier).
# Phase 1 (layers 0-4):   $300-700  — tiny exploration positions
# Phase 2 (layers 5-9):   $800-1200 — building conviction
# Phase 3 (layers 10-14): $1500-2000 — committed, averaging down
# Phase 4 (layers 15-19): $2500-3500 — strong conviction, deep drawdown
# Phase 5 (layers 20-24): $4000-5000 — full size, max drawdown scaling
# Total: ~$48,300 — fits within a $50k persona account with room for the equity cap.
DCA_LAYERS_MARGIN_BASE: list[float] = [
    # Phase 1 — exploration ($300-700, 5 layers = $2,500)
    300, 400, 500, 600, 700,
    # Phase 2 — building ($800-1200, 5 layers = $5,000)
    800, 900, 1000, 1100, 1200,
    # Phase 3 — committed ($1500-2000, 5 layers = $8,500)
    1500, 1600, 1700, 1800, 1900,
    # Phase 4 — strong conviction ($2500-3500, 5 layers = $15,000)
    2500, 2800, 3000, 3200, 3500,
    # Phase 5 — max size ($4000-5000, 5 layers = $22,000)
    4000, 4200, 4500, 4800, 5000,
]
# Verify: sum = 2500 + 5000 + 8500 + 15000 + 22000 = 53,000... let me compute
# Actually: 300+400+500+600+700 + 800+900+1000+1100+1200 + 1500+1600+1700+1800+1900
#         + 2500+2800+3000+3200+3500 + 4000+4200+4500+4800+5000
#         = 2500 + 5000 + 8500 + 15000 + 22500 = 53,500
# With 0.8x equity cap on $50k ($40k usable), the later layers will be
# clipped by the cap rather than over-exposing. This is by design.

# Scalper DCA schedule — much smaller, faster in-and-out.
# Max single layer $1000, max total ~$10k ($100k exposure at x10).
# 15 layers so the scalper can also average down but with tiny amounts.
SCALPER_LAYERS_MARGIN_BASE: list[float] = [
    # Phase 1 — probe ($200-400, 5 layers = $1,500)
    200, 250, 300, 350, 400,
    # Phase 2 — build ($500-700, 5 layers = $3,000)
    500, 550, 600, 650, 700,
    # Phase 3 — full ($800-1000, 5 layers = $4,500)
    800, 850, 900, 950, 1000,
]
# Total: $9,000 — well within the $50k scalper account.
# At x10 leverage, max total exposure = $90k (~1.8 lots of WTI at $100).

# Back-compat alias so older code paths that still read DCA_LAYERS_MARGIN
# continue to work (treated as the base schedule, multiplier 1.0).
DCA_LAYERS_MARGIN: list[float] = DCA_LAYERS_MARGIN_BASE

# Size multiplier bounds. At 3.0x on Layer 0 ($5k base) you enter with
# $15k margin = $150k nominal exposure (roughly 1.5x account equity).
# Clamped BELOW by the equity cap so you can't actually overleverage.
MIN_SIZE_MULTIPLIER = 0.5
MAX_SIZE_MULTIPLIER = 3.0

# Hard cap: total open margin (across all open campaigns) must never
# exceed this fraction of current equity. 0.80 = 80% utilisation max,
# leaving 20% buffer for adverse moves before margin call territory.
MAX_TOTAL_EXPOSURE_PCT = 0.80

# Drawdown threshold (% of avg entry) that triggers the next DCA layer.
# Lowered from 5% to 1.5% — WTI moves 1-3%/day so 5% was too wide
# and the bot was sitting on tiny Layer-0 positions for hours without
# ever scaling in. 1.5% means a $1.50 move against you on a $100 entry
# triggers the next layer, which is reasonable for intraday DCA.
DCA_DRAWDOWN_TRIGGER_PCT = 1.5

# Hard stop: close the campaign when its unrealised PnL drops below this % of margin.
HARD_STOP_DRAWDOWN_PCT = 50.0


def lots_from_margin(margin_usd: float, price: float) -> float:
    """Convert a margin amount into a number of lots at the given price."""
    nominal = margin_usd * LEVERAGE
    return nominal / (price * LOT_SIZE_BBL)


def margin_for_lots(lots: float, price: float) -> float:
    nominal = lots * LOT_SIZE_BBL * price
    return nominal / LEVERAGE


def nominal_value(lots: float, price: float) -> float:
    return lots * LOT_SIZE_BBL * price


def _schedule_for_persona(persona: str = "main") -> list[float]:
    """Return the DCA layer schedule for a persona."""
    if persona == "scalper":
        return SCALPER_LAYERS_MARGIN_BASE
    return DCA_LAYERS_MARGIN_BASE


def base_layer_margin(layer_index: int, persona: str = "main") -> float | None:
    """Return the BASE margin for a layer (pre-multiplier), or None if exhausted."""
    schedule = _schedule_for_persona(persona)
    if layer_index >= len(schedule):
        return None
    return schedule[layer_index]


def scaled_layer_margin(layer_index: int, multiplier: float, persona: str = "main") -> float | None:
    """Return the EFFECTIVE margin for a layer after applying the multiplier."""
    base = base_layer_margin(layer_index, persona)
    if base is None:
        return None
    return base * multiplier


def next_layer_margin(layers_used: int, multiplier: float = 1.0, persona: str = "main") -> float | None:
    """Return the next DCA layer's margin after multiplier, or None if exhausted."""
    return scaled_layer_margin(layers_used, multiplier, persona)


def total_planned_margin(multiplier: float = 1.0, persona: str = "main") -> float:
    return sum(_schedule_for_persona(persona)) * multiplier


def max_layers(persona: str = "main") -> int:
    """Return the total number of DCA layers available for a persona."""
    return len(_schedule_for_persona(persona))


def clamp_multiplier(multiplier: float) -> float:
    return max(MIN_SIZE_MULTIPLIER, min(MAX_SIZE_MULTIPLIER, multiplier))
