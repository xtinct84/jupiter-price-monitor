"""
execution_validator.py
──────────────────────
5-stage MEV-aware execution validation pipeline.
Called by arbitrage_detector.py on signals that have already
passed weighted scoring (≥ 40pts).

Pipeline:
  Stage 1 — Gross divergence gate        (> GROSS_DIVERGENCE_THRESHOLD)
  Stage 2 — Liquidity ratio pre-filter   (depth vs capital)
  Stage 3 — Slippage simulation per leg  (inferred from price impact)
  Stage 4 — Net profit calculation       (gross - slippage - fees)
  Stage 5 — Execute gate                 (net > NET_PROFIT_THRESHOLD)

Capital range: $10–$100 USD, read from TRADE_CAPITAL_USD in .env
All thresholds configurable at top of file.

Usage:
    from execution_validator import validate_signal
    result = validate_signal(signal, quote_data, price_data)
"""

import os
import logging
from dataclasses import dataclass, field
from typing import Optional
from decimal import Decimal, getcontext
from dotenv import load_dotenv

load_dotenv()
getcontext().prec = 28

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

# Capital amount read from .env — default $50 if not set
TRADE_CAPITAL_USD = float(os.getenv('TRADE_CAPITAL_USD', '50.0'))

# Clamp to valid range $10–$100
TRADE_CAPITAL_USD = max(10.0, min(100.0, TRADE_CAPITAL_USD))

# Stage 1 — Gross divergence gate
GROSS_DIVERGENCE_THRESHOLD = 1.5       # Minimum gross profit % to proceed

# Stage 2 — Liquidity ratio thresholds (depth / capital)
LIQUIDITY_RATIO_REJECT     = 5.0       # Below this → reject immediately
LIQUIDITY_RATIO_SIMULATE   = 10.0      # Below this → run exact simulation
                                        # Above this → safe assumption

# Stage 3 — Slippage simulation
LEGS_PER_TRIANGULAR        = 3         # Number of swap legs in triangular path
LEGS_PER_DIRECT            = 2         # Number of swap legs in direct trade

# Stage 4 — Fee assumptions
JUPITER_PLATFORM_FEE_PCT   = 0.2       # Jupiter platform fee per swap (%)
SOLANA_TX_FEE_USD          = 0.0004    # ~0.000005 SOL at $85 per SOL

# Stage 5 — Execute gate
# Strategy-specific net profit thresholds
NET_PROFIT_THRESHOLD_DIRECT      = 1.5   # Direct 2-leg trades (rate divergence, impact anomaly)
NET_PROFIT_THRESHOLD_TRIANGULAR  = 1.0   # Triangular 3-leg trades (higher fee burden, better MEV protection)
DYNAMIC_SLIPPAGE_BUFFER    = 0.8       # dynamic_slippage = net_profit * this

# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass
class ValidationResult:
    """
    Full result of the 5-stage validation pipeline.
    Attached to each signal as 'validation' key before Telegram alert.
    """
    passed:                 bool  = False
    stage_reached:          int   = 0       # Last stage completed (1-5)
    reject_reason:          str   = ""

    # Stage 1
    gross_profit_pct:       float = 0.0
    gross_gate_passed:      bool  = False

    # Stage 2
    capital_usd:            float = 0.0
    implied_depth_usd:      float = 0.0
    liquidity_ratio:        float = 0.0
    liquidity_tier:         str   = ""      # REJECT / SIMULATE / SAFE
    liquidity_gate_passed:  bool  = False

    # Stage 3
    simulated_slippage_pct: float = 0.0
    legs:                   int   = 0

    # Stage 4
    gross_pct:              float = 0.0
    fee_pct:                float = 0.0
    slippage_pct:           float = 0.0
    net_profit_pct:         float = 0.0

    # Stage 5
    execute_gate_passed:    bool  = False
    dynamic_slippage_bps:   int   = 0       # Recommended slippage in basis points
    recommendation:         str   = ""      # EXECUTE / ANALYSIS / REJECT

    def summary(self) -> str:
        """One-line summary for Telegram alert"""
        if not self.passed:
            return (f"VALIDATION FAILED at Stage {self.stage_reached}: "
                    f"{self.reject_reason}")
        return (
            f"VALIDATED | Net: {self.net_profit_pct:.4f}% | "
            f"Depth: {self.liquidity_ratio:.1f}x capital | "
            f"Dynamic slippage: {self.dynamic_slippage_bps}bps | "
            f"{self.recommendation}"
        )

    def detail_lines(self) -> list:
        """Full breakdown lines for Telegram"""
        lines = [
            f"Capital         : ${self.capital_usd:.2f}",
            f"Gross profit    : {self.gross_profit_pct:.4f}%",
            f"Implied depth   : ${self.implied_depth_usd:,.0f} "
            f"({self.liquidity_ratio:.1f}x capital) [{self.liquidity_tier}]",
            f"Sim. slippage   : {self.simulated_slippage_pct:.4f}% "
            f"({self.legs} legs)",
            f"Fees            : {self.fee_pct:.4f}%",
            f"Net profit      : {self.net_profit_pct:.4f}%",
            f"Dyn. slippage   : {self.dynamic_slippage_bps}bps "
            f"(= net * {DYNAMIC_SLIPPAGE_BUFFER})",
            f"Recommendation  : {self.recommendation}",
        ]
        if not self.passed:
            lines.append(f"Reject reason   : {self.reject_reason}")
        return lines


# =============================================================================
# DEPTH INFERENCE
# =============================================================================

def infer_depth_usd(price_impact_pct: float,
                    capital_usd: float,
                    price_usd: float = 1.0) -> float:
    """
    Infer implied order book depth from price impact percentage.

    Formula: implied_depth = capital / price_impact
    If price impact on a $50 trade is 0.5%, implied depth = $10,000.

    Jupiter's price_impact_pct already reflects the actual quote size
    stored in our quotes table, so this is the capital that generated
    that impact — not our trade capital. We normalize to our capital.

    Args:
        price_impact_pct: Price impact % from Jupiter quote (e.g. 0.5)
        capital_usd:      Our trade capital in USD
        price_usd:        Token price for normalization (unused for ratio)

    Returns:
        Implied depth in USD at our capital level
    """
    if price_impact_pct <= 0:
        # Zero impact — extremely deep market, return large safe value
        return capital_usd * 1000.0

    try:
        # Depth implied by the stored quote's impact
        implied = Decimal(str(capital_usd)) / Decimal(str(price_impact_pct / 100))
        return float(implied)
    except Exception:
        return 0.0


def get_liquidity_tier(ratio: float) -> str:
    """Classify liquidity ratio into actionable tier"""
    if ratio < LIQUIDITY_RATIO_REJECT:
        return "REJECT"
    elif ratio < LIQUIDITY_RATIO_SIMULATE:
        return "SIMULATE"
    else:
        return "SAFE"


# =============================================================================
# SLIPPAGE SIMULATION
# =============================================================================

def simulate_leg_slippage(price_impact_pct: float,
                           capital_usd: float,
                           quote_capital_usd: float = 50.0) -> float:
    """
    Estimate slippage for a single swap leg at our capital size.

    Jupiter stores price_impact_pct for a fixed quote amount.
    We scale it proportionally to our actual capital.

    Slippage scales roughly linearly with size for small trades
    relative to pool depth.

    Args:
        price_impact_pct:  Impact % from stored quote
        capital_usd:       Our actual trade capital
        quote_capital_usd: Capital size used to generate the stored quote

    Returns:
        Estimated slippage % for our capital size
    """
    if price_impact_pct <= 0:
        return 0.0

    try:
        scale = Decimal(str(capital_usd)) / Decimal(str(quote_capital_usd))
        scaled_impact = Decimal(str(price_impact_pct)) * scale
        return float(scaled_impact)
    except Exception:
        return float(price_impact_pct)


def simulate_total_slippage(legs_impact: list,
                             capital_usd: float,
                             quote_capital_usd: float = 50.0) -> float:
    """
    Sum simulated slippage across all swap legs.

    Args:
        legs_impact:       List of price_impact_pct values per leg
        capital_usd:       Our trade capital
        quote_capital_usd: Capital size used for stored quotes

    Returns:
        Total estimated slippage % across all legs
    """
    total = sum(
        simulate_leg_slippage(impact, capital_usd, quote_capital_usd)
        for impact in legs_impact
    )
    return total


# =============================================================================
# FEE CALCULATION
# =============================================================================

def calculate_fees_pct(capital_usd: float, num_legs: int) -> float:
    """
    Calculate total fee burden as % of capital.

    Components:
    - Jupiter platform fee: 0.2% per swap leg
    - Solana transaction fee: ~$0.0004 per tx (negligible at $50+)

    Args:
        capital_usd: Trade capital in USD
        num_legs:    Number of swap legs (2 for direct, 3 for triangular)

    Returns:
        Total fee as percentage of capital
    """
    try:
        platform_fees_pct = JUPITER_PLATFORM_FEE_PCT * num_legs
        solana_fee_pct = (SOLANA_TX_FEE_USD / capital_usd) * 100
        return platform_fees_pct + solana_fee_pct
    except Exception:
        return JUPITER_PLATFORM_FEE_PCT * num_legs


# =============================================================================
# 5-STAGE VALIDATION PIPELINE
# =============================================================================

def validate_signal(signal: dict,
                    quote_legs: list,
                    capital_usd: float = None) -> ValidationResult:
    """
    Run the full 5-stage MEV-aware validation pipeline on a signal.

    Args:
        signal:      Signal dict from arbitrage_detector (must have
                     estimated_profit_pct, signal_type, pair)
        quote_legs:  List of quote dicts, one per swap leg.
                     Each must have: price_impact_pct, in_amount,
                     out_amount, input_symbol, output_symbol
        capital_usd: Override capital (defaults to TRADE_CAPITAL_USD)

    Returns:
        ValidationResult with full breakdown
    """
    result = ValidationResult()
    result.capital_usd = capital_usd or TRADE_CAPITAL_USD

    signal_type = signal.get('signal_type', 'rate_divergence')
    gross_profit = signal.get('estimated_profit_pct', 0.0)
    result.gross_profit_pct = gross_profit

    num_legs = LEGS_PER_TRIANGULAR if 'triangular' in signal_type else LEGS_PER_DIRECT

    # ─────────────────────────────────────────────
    # STAGE 1 — Gross Divergence Gate
    # ─────────────────────────────────────────────
    result.stage_reached = 1
    if gross_profit < GROSS_DIVERGENCE_THRESHOLD:
        result.reject_reason = (
            f"Gross profit {gross_profit:.4f}% below "
            f"threshold {GROSS_DIVERGENCE_THRESHOLD}%"
        )
        result.gross_gate_passed = False
        result.recommendation = "ANALYSIS"
        logger.info(
            f"  Stage 1 FAIL: {signal.get('pair')} — "
            f"gross {gross_profit:.4f}% < {GROSS_DIVERGENCE_THRESHOLD}%"
        )
        return result

    result.gross_gate_passed = True
    logger.info(
        f"  Stage 1 PASS: gross profit {gross_profit:.4f}% "
        f">= {GROSS_DIVERGENCE_THRESHOLD}%"
    )

    # ─────────────────────────────────────────────
    # STAGE 2 — Liquidity Ratio Pre-filter
    # Use thinnest leg (highest price impact = lowest depth)
    # ─────────────────────────────────────────────
    result.stage_reached = 2

    if not quote_legs:
        result.reject_reason = "No quote leg data available for depth inference"
        result.recommendation = "ANALYSIS"
        return result

    # Find thinnest leg — highest price impact implies shallowest depth
    impacts = [abs(q.get('price_impact_pct', 0.0)) for q in quote_legs]
    thinnest_impact = max(impacts) if impacts else 0.0

    implied_depth = infer_depth_usd(thinnest_impact, result.capital_usd)
    liquidity_ratio = implied_depth / result.capital_usd if result.capital_usd > 0 else 0.0

    result.implied_depth_usd = implied_depth
    result.liquidity_ratio = liquidity_ratio
    result.liquidity_tier = get_liquidity_tier(liquidity_ratio)

    logger.info(
        f"  Stage 2: depth ${implied_depth:,.0f} | "
        f"ratio {liquidity_ratio:.1f}x | tier {result.liquidity_tier}"
    )

    if result.liquidity_tier == "REJECT":
        result.reject_reason = (
            f"Liquidity ratio {liquidity_ratio:.1f}x below minimum "
            f"{LIQUIDITY_RATIO_REJECT}x — slippage will exceed 1%"
        )
        result.liquidity_gate_passed = False
        result.recommendation = "REJECT"
        logger.info(f"  Stage 2 FAIL: {result.reject_reason}")
        return result

    result.liquidity_gate_passed = True

    # ─────────────────────────────────────────────
    # STAGE 3 — Slippage Simulation Per Leg
    # ─────────────────────────────────────────────
    result.stage_reached = 3
    result.legs = num_legs

    # Estimate quote capital from stored quote (approximate as $50)
    # This is the reference size our stored quotes were generated at
    quote_capital_estimate = 50.0

    if result.liquidity_tier == "SAFE":
        # Deep market — use standard assumption
        simulated_slippage = 0.5 * num_legs  # 0.5% per leg assumption
        logger.info(
            f"  Stage 3: SAFE tier — using standard "
            f"{0.5}% * {num_legs} legs = {simulated_slippage:.4f}%"
        )
    else:
        # SIMULATE tier — calculate from actual price impact data
        simulated_slippage = simulate_total_slippage(
            impacts, result.capital_usd, quote_capital_estimate
        )
        logger.info(
            f"  Stage 3: SIMULATE tier — "
            f"computed slippage {simulated_slippage:.4f}% across {num_legs} legs"
        )

    result.simulated_slippage_pct = simulated_slippage

    # ─────────────────────────────────────────────
    # STAGE 4 — Net Profit Calculation
    # ─────────────────────────────────────────────
    result.stage_reached = 4

    fee_pct = calculate_fees_pct(result.capital_usd, num_legs)
    net_profit = gross_profit - simulated_slippage - fee_pct

    result.gross_pct     = gross_profit
    result.fee_pct       = fee_pct
    result.slippage_pct  = simulated_slippage
    result.net_profit_pct = net_profit

    logger.info(
        f"  Stage 4: gross {gross_profit:.4f}% - "
        f"slippage {simulated_slippage:.4f}% - "
        f"fees {fee_pct:.4f}% = net {net_profit:.4f}%"
    )

    # ─────────────────────────────────────────────
    # STAGE 5 — Execute Gate
    # ─────────────────────────────────────────────
    result.stage_reached = 5

    if net_profit <= 0:
        result.reject_reason = (
            f"Net profit {net_profit:.4f}% is negative after "
            f"slippage and fees"
        )
        result.recommendation = "REJECT"
        logger.info(f"  Stage 5 FAIL: negative net profit")
        return result

    # Dynamic slippage = net_profit * buffer, expressed in basis points
    dynamic_slippage_pct = net_profit * DYNAMIC_SLIPPAGE_BUFFER
    dynamic_slippage_bps = int(dynamic_slippage_pct * 100)

    # Clamp to sensible range: 50bps minimum, 200bps maximum
    dynamic_slippage_bps = max(50, min(200, dynamic_slippage_bps))

    result.dynamic_slippage_bps = dynamic_slippage_bps

    # Select threshold based on strategy type
    net_threshold = (
        NET_PROFIT_THRESHOLD_TRIANGULAR
        if 'triangular' in signal_type
        else NET_PROFIT_THRESHOLD_DIRECT
    )

    if net_profit >= net_threshold:
        result.execute_gate_passed = True
        result.passed = True
        result.recommendation = "EXECUTE"
        logger.info(
            f"  Stage 5 PASS: net {net_profit:.4f}% >= "
            f"{net_threshold}% | "
            f"dynamic slippage {dynamic_slippage_bps}bps"
        )
    else:
        result.execute_gate_passed = False
        result.passed = False
        result.recommendation = "ANALYSIS"
        result.reject_reason = (
            f"Net profit {net_profit:.4f}% below execute "
            f"threshold {net_threshold}% ({signal_type})"
        )
        logger.info(
            f"  Stage 5: net {net_profit:.4f}% < "
            f"{net_threshold}% — flagged for ANALYSIS"
        )

    return result


# =============================================================================
# SIGNAL ENRICHMENT HELPER
# =============================================================================

def enrich_signal_with_validation(signal: dict,
                                   quote_legs: list,
                                   capital_usd: float = None) -> dict:
    """
    Run validation pipeline and attach result to signal dict.
    Updates execute_candidate flag based on validation outcome.

    Args:
        signal:      Signal dict (modified in place)
        quote_legs:  List of quote dicts per leg
        capital_usd: Optional capital override

    Returns:
        Enriched signal dict with 'validation' key added
    """
    logger.info(
        f"🔬 Validating: {signal.get('pair')} | "
        f"Type: {signal.get('signal_type')} | "
        f"Gross: {signal.get('estimated_profit_pct', 0):.4f}%"
    )

    result = validate_signal(signal, quote_legs, capital_usd)
    signal['validation'] = result

    # Override execute_candidate based on pipeline result
    if result.recommendation == "EXECUTE":
        signal['execute_candidate'] = True
        signal['dynamic_slippage_bps'] = result.dynamic_slippage_bps
        logger.info(
            f"  ✅ EXECUTE CANDIDATE: {signal.get('pair')} | "
            f"Net {result.net_profit_pct:.4f}% | "
            f"Slippage {result.dynamic_slippage_bps}bps"
        )
    elif result.recommendation == "REJECT":
        signal['execute_candidate'] = False
        logger.info(
            f"  ❌ REJECTED: {signal.get('pair')} — {result.reject_reason}"
        )
    else:
        signal['execute_candidate'] = False
        logger.info(
            f"  🟡 ANALYSIS: {signal.get('pair')} — {result.reject_reason}"
        )

    return signal


# =============================================================================
# STANDALONE TEST
# =============================================================================

if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║         EXECUTION VALIDATOR — 5-Stage Pipeline              ║
    ║         MEV-Aware Validation for Small Funds                ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    print(f"  Trade capital:          ${TRADE_CAPITAL_USD:.2f} USD")
    print(f"  Gross divergence gate:  > {GROSS_DIVERGENCE_THRESHOLD}%")
    print(f"  Liquidity reject:       < {LIQUIDITY_RATIO_REJECT}x capital")
    print(f"  Liquidity simulate:     {LIQUIDITY_RATIO_REJECT}x - {LIQUIDITY_RATIO_SIMULATE}x capital")
    print(f"  Liquidity safe:         > {LIQUIDITY_RATIO_SIMULATE}x capital")
    print(f"  Jupiter platform fee:   {JUPITER_PLATFORM_FEE_PCT}% per leg")
    print(f"  Solana tx fee:          ${SOLANA_TX_FEE_USD:.4f} per tx")
    print(f"  Net profit (direct):    > {NET_PROFIT_THRESHOLD_DIRECT}%  [2-leg trades]")
    print(f"  Net profit (triangular):> {NET_PROFIT_THRESHOLD_TRIANGULAR}%  [3-leg trades]")
    print(f"  Dynamic slippage:       net% * {DYNAMIC_SLIPPAGE_BUFFER}")
    print()

    # Test case 1 — should PASS execute gate
    test_signal_pass = {
        'pair': 'BONK->SOL->USDC->BONK',
        'signal_type': 'triangular_arbitrage',
        'estimated_profit_pct': 3.42,
        'weighted_score': 95
    }
    test_quotes_pass = [
        {'price_impact_pct': 0.007, 'in_amount': 100000,
         'out_amount': 6800, 'input_symbol': 'BONK', 'output_symbol': 'SOL'},
        {'price_impact_pct': 0.001, 'in_amount': 6800,
         'out_amount': 583000, 'input_symbol': 'SOL', 'output_symbol': 'USDC'},
        {'price_impact_pct': 0.009, 'in_amount': 583000,
         'out_amount': 99350000, 'input_symbol': 'USDC', 'output_symbol': 'BONK'},
    ]

    print("  TEST 1 — High profit triangular (should PASS):")
    result1 = validate_signal(test_signal_pass, test_quotes_pass)
    print(f"  Result: {result1.summary()}")
    print()

    # Test case 2 — should FAIL at stage 1 (gross too low)
    test_signal_fail = {
        'pair': 'SOL/USDC',
        'signal_type': 'rate_divergence',
        'estimated_profit_pct': 0.08,
        'weighted_score': 45
    }
    test_quotes_fail = [
        {'price_impact_pct': 0.001, 'in_amount': 1000000000,
         'out_amount': 85560000, 'input_symbol': 'SOL', 'output_symbol': 'USDC'},
    ]

    print("  TEST 2 — Low profit direct trade (should FAIL Stage 1):")
    result2 = validate_signal(test_signal_fail, test_quotes_fail)
    print(f"  Result: {result2.summary()}")
    print()

    # Test case 3 — should FAIL at stage 2 (thin liquidity)
    test_signal_thin = {
        'pair': 'WIF/SOL',
        'signal_type': 'rate_divergence',
        'estimated_profit_pct': 2.5,
        'weighted_score': 80
    }
    test_quotes_thin = [
        {'price_impact_pct': 25.0, 'in_amount': 1000000,
         'out_amount': 1500, 'input_symbol': 'WIF', 'output_symbol': 'SOL'},
    ]

    print("  TEST 3 — Thin liquidity (should FAIL Stage 2 REJECT):")
    result3 = validate_signal(test_signal_thin, test_quotes_thin)
    print(f"  Result: {result3.summary()}")
