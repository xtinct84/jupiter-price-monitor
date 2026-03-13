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
import time
import json
import urllib.request
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

# Token decimals for quote capital derivation
TOKEN_DECIMALS = {
    'SOL': 9, 'USDC': 6, 'USDT': 6, 'JUP': 6,
    'RAY': 6, 'BONK': 5, 'JTO': 9, 'PYTH': 6,
    'WIF': 6, 'POPCAT': 9, 'MOUTAI': 9, 'MYRO': 9, 'WEN': 5
}

# Stage 1 — Gross divergence gate
GROSS_DIVERGENCE_THRESHOLD = 1.2       # Minimum gross profit % to proceed (fee-justified floor at $10–$100 capital)

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

# ── Jito tip floor — live API + capital-based fallback ────────────────────────
#
# Primary:  fetch_jito_tip_floor() calls the Jito REST API once per minute.
#           Returns EMA-smoothed 50th-percentile and raw percentile lamport tips.
#           Results are cached for JITO_CACHE_TTL_S seconds to avoid hammering
#           the endpoint on every signal in the same detection cycle.
#
# Fallback: if the API is unreachable or returns unexpected data, PRIORITY_FEE_TIER
#           hardcoded USD values are used (same realistic lamport estimates as before).
#
# Tier → percentile mapping:
#   none      ($10–$19)  → $0.000   — no tip at all
#   low       ($20–$60)  → EMA p50  — calm baseline, smoothed to reduce spikes
#   medium    ($61–$100) → p75      — slightly above median for better inclusion
#   high      (manual)   → p95      — competitive arb / elevated congestion
#   desperate (manual)   → p99      — extreme events only
#
# .env PRIORITY_FEE_TIER= always wins if explicitly set (bypasses live lookup).

JITO_TIP_FLOOR_URL  = "https://bundles-api-rest.jito.wtf/api/v1/bundles/tip_floor"
JITO_CACHE_TTL_S    = 60        # Re-fetch at most once per minute
JITO_TIMEOUT_S      = 3         # Abort if API takes longer than 3s
LAMPORTS_PER_SOL    = 1_000_000_000

# Fallback USD values used when live API is unavailable
# Calibrated to realistic lamport ranges — not worst-case event pricing
PRIORITY_FEE_TIER = {
    'none':      0.000,  # No Jito tip — $10–$19 capital only
    'low':       0.002,  # ~10,000 lamports EMA p50 fallback
    'medium':    0.005,  # ~27,000 lamports p75 fallback
    'high':      0.010,  # ~54,000 lamports p95 fallback
    'desperate': 0.050,  # ~270,000 lamports p99 fallback
}

# Cache state — module-level, shared across all calls within a process
_jito_cache: dict = {}   # keys: 'data', 'fetched_at'


def fetch_jito_tip_floor(sol_price_usd: float = 90.0) -> dict:
    """
    Fetch the current Jito tip floor from the REST API with 60-second caching.

    Returns a dict of tier -> USD amount derived from live lamport percentiles:
        {
            'none':      0.0,
            'low':       <EMA p50 in USD>,
            'medium':    <p75 in USD>,
            'high':      <p95 in USD>,
            'desperate': <p99 in USD>,
            'source':    'live' | 'cache' | 'fallback',
            'fetched_at': <unix timestamp>,
        }

    Falls back to PRIORITY_FEE_TIER hardcoded values on any error.

    Args:
        sol_price_usd: Current SOL/USD price for lamport conversion.
                       Caller should pass live price from prices table when available.
    """
    global _jito_cache

    now = time.time()

    # Return cached result if still fresh
    if _jito_cache.get('fetched_at', 0) + JITO_CACHE_TTL_S > now:
        cached = dict(_jito_cache['data'])
        cached['source'] = 'cache'
        return cached

    try:
        req = urllib.request.Request(
            JITO_TIP_FLOOR_URL,
            headers={'Accept': 'application/json', 'User-Agent': 'jupiter-arb-bot/2.2'},
        )
        with urllib.request.urlopen(req, timeout=JITO_TIMEOUT_S) as resp:
            payload = json.loads(resp.read().decode('utf-8'))

        # API returns a list — take the most recent entry
        entry = payload[0] if isinstance(payload, list) else payload

        def lamps_to_usd(lamports: int) -> float:
            return round((int(lamports) / LAMPORTS_PER_SOL) * sol_price_usd, 6)

        result = {
            'none':      0.0,
            'low':       lamps_to_usd(entry['ema_landed_tips_50th_percentile']),
            'medium':    lamps_to_usd(entry['landed_tips_75th_percentile']),
            'high':      lamps_to_usd(entry['landed_tips_95th_percentile']),
            'desperate': lamps_to_usd(entry['landed_tips_99th_percentile']),
            'source':    'live',
            'fetched_at': now,
        }

        _jito_cache = {'data': result, 'fetched_at': now}
        logger.info(
            f"  Jito tip floor (live): low=${result['low']:.5f} "
            f"med=${result['medium']:.5f} "
            f"high=${result['high']:.5f} "
            f"(SOL=${sol_price_usd:.2f})"
        )
        return result

    except Exception as e:
        logger.warning(f"  Jito tip floor API unavailable ({e}) — using hardcoded fallback")
        fallback = dict(PRIORITY_FEE_TIER)
        fallback['source']     = 'fallback'
        fallback['fetched_at'] = now
        return fallback


def get_live_tip_usd(tier: str, sol_price_usd: float = 90.0) -> float:
    """
    Return the current Jito tip in USD for the given tier.
    Uses the live API with caching; falls back to hardcoded values if unavailable.

    Args:
        tier:          One of 'none' | 'low' | 'medium' | 'high' | 'desperate'
        sol_price_usd: Live SOL/USD price for lamport conversion

    Returns:
        Tip amount in USD
    """
    tips = fetch_jito_tip_floor(sol_price_usd)
    return tips.get(tier, PRIORITY_FEE_TIER.get(tier, 0.0))


# Capital-based tier formula — unchanged
# $10–$19  → none   (tip cost disproportionate; sandwich risk negligible at this size)
# $20–$60  → low    (EMA p50 — calm market baseline)
# $61–$100 → medium (p75 — slightly above median for better inclusion)
# .env PRIORITY_FEE_TIER= always wins if explicitly set
def get_capital_based_tier(capital: float) -> str:
    """
    Derive Jito tip tier from trade capital.
    Only used when PRIORITY_FEE_TIER is not set in .env.

    Args:
        capital: Trade capital in USD

    Returns:
        Tier string: 'none' | 'low' | 'medium'
    """
    if capital < 20.0:
        return 'none'
    elif capital <= 60.0:
        return 'low'
    else:
        return 'medium'


_env_tier = os.getenv('PRIORITY_FEE_TIER', '').strip().lower()
PRIORITY_FEE_DEFAULT = (
    _env_tier
    if _env_tier in PRIORITY_FEE_TIER
    else get_capital_based_tier(TRADE_CAPITAL_USD)
)
# Stage 5 — Execute gate
# Strategy-specific net profit thresholds
NET_PROFIT_THRESHOLD_DIRECT      = 1.2   # Direct 2-leg trades — fee-justified floor (fees ~0.404% + net 0.5% + slip 0.3%)
NET_PROFIT_THRESHOLD_TRIANGULAR  = 0.7   # Triangular 3-leg trades — lower due to multi-hop MEV protection reducing sandwich risk

# 3:1 ratio formula constants
DESIRED_NET_PROFIT_PCT     = 0.5       # Minimum acceptable net profit per trade
MIN_GROSS_SLIPPAGE_RATIO   = 3.0       # Gross must be >= 3x derived slippage tolerance
# Dynamic slippage multiplier tiers (based on depth / capital ratio)
# Replaces flat 0.8 buffer — adjusts execution aggressiveness to liquidity
SLIP_MULT_ULTRA   = 0.9    # depth > 50x capital  — ultra-liquid, push closer to limit
SLIP_MULT_NORMAL  = 0.8    # depth > 20x capital  — standard market
SLIP_MULT_THIN    = 0.6    # depth > 10x capital  — thin, extra conservative
SLIP_MULT_REJECT  = None   # depth <= 10x capital — reject, not worth the risk
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

    # Stage 0
    price_drift_pct:        float = 0.0     # Rate drift since signal detection

    # Stage 5
    execute_gate_passed:    bool  = False
    dynamic_slippage_bps:   int   = 0       # Recommended slippage in basis points
    multiplier_tier:        str   = ""      # Liquidity depth tier label
    recommendation:         str   = ""      # EXECUTE / ANALYSIS / REJECT / STALE

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
            f"Price drift     : {self.price_drift_pct:.4f}% since detection",
            f"Gross profit    : {self.gross_profit_pct:.4f}%",
            f"Implied depth   : ${self.implied_depth_usd:,.0f} "
            f"({self.liquidity_ratio:.1f}x capital) [{self.liquidity_tier}]",
            f"Sim. slippage   : {self.simulated_slippage_pct:.4f}% "
            f"({"accumulated" if self.legs == 3 else "worst-leg"})",
            f"Derived tol.    : (gross {self.gross_profit_pct:.4f}% - fees "
            f"{self.fee_pct:.4f}% - net {DESIRED_NET_PROFIT_PCT}%) / 2",
            f"Effective slip  : {self.slippage_pct:.4f}% (max of worst-leg vs derived)",
            f"Fees            : {self.fee_pct:.4f}% ({self.legs} legs x {JUPITER_PLATFORM_FEE_PCT}%)",
            f"Net profit      : {self.net_profit_pct:.4f}%",
            f"Dyn. slip. set  : {self.dynamic_slippage_bps}bps "
            f"[{self.multiplier_tier}]",
            f"Recommendation  : {self.recommendation}",
        ]
        if not self.passed:
            lines.append(f"Reject reason   : {self.reject_reason}")
        return lines


# =============================================================================
# DEPTH INFERENCE
# =============================================================================

def get_quote_capital_usd(quote: dict, token_prices: dict = None) -> float:
    """
    Derive the actual USD value of the quote from its stored in_amount.

    Replaces the hardcoded 50.0 assumption. Uses token_prices dict
    (symbol → price_usd) if provided, otherwise falls back to
    TRADE_CAPITAL_USD from .env so slippage scaling uses real reference size.

    Args:
        quote:        Quote dict with in_amount and input_symbol keys
        token_prices: Optional {symbol: price_usd} from price_history table

    Returns:
        Estimated USD capital represented by this quote
    """
    try:
        in_sym    = quote.get('input_symbol', '')
        in_amount = int(quote.get('in_amount', 0))
        if in_amount <= 0:
            return TRADE_CAPITAL_USD
        decimals      = TOKEN_DECIMALS.get(in_sym, 6)
        token_amount  = in_amount / (10 ** decimals)
        if token_prices and in_sym in token_prices:
            price_usd = float(token_prices[in_sym])
        else:
            # Fallback: assume in_amount represents roughly TRADE_CAPITAL_USD
            return TRADE_CAPITAL_USD
        return token_amount * price_usd
    except Exception:
        return TRADE_CAPITAL_USD


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

def derive_slippage_tolerance(gross_pct: float,
                               fees_pct: float,
                               desired_net_pct: float = DESIRED_NET_PROFIT_PCT) -> float:
    """
    Derive the maximum safe slippage tolerance using the formula:
        Slippage Tolerance = (Gross% - Fees% - Desired Net%) / 2

    A negative result means gross profit is insufficient to cover
    fees + desired net even with zero slippage — reject immediately.

    Args:
        gross_pct:       Gross profit percentage
        fees_pct:        Total fees across all legs
        desired_net_pct: Minimum acceptable net profit

    Returns:
        Maximum safe slippage % (negative = reject)
    """
    try:
        tolerance = (Decimal(str(gross_pct))
                     - Decimal(str(fees_pct))
                     - Decimal(str(desired_net_pct))) / Decimal('2')
        return float(tolerance)
    except Exception:
        return -1.0


def check_slippage_ratio(gross_pct: float,
                          slippage_tolerance: float) -> tuple:
    """
    Check whether gross profit satisfies the 3:1 ratio vs slippage.

    Returns:
        (ratio: float, passes: bool, warning: str)
    """
    if slippage_tolerance <= 0:
        return 0.0, False, 'Slippage tolerance negative'
    ratio = gross_pct / slippage_tolerance
    passes = ratio >= MIN_GROSS_SLIPPAGE_RATIO
    warning = (
        f'OK ({ratio:.1f}:1)' if passes
        else f'WARN — {ratio:.1f}:1 below {MIN_GROSS_SLIPPAGE_RATIO}:1 target'
    )
    return ratio, passes, warning


def get_dynamic_multiplier(liquidity_ratio: float) -> tuple:
    """
    Return dynamic slippage multiplier based on depth/capital ratio.

    Tiers:
      > 50x capital  → 0.9  (ultra-liquid, can push closer to limit)
      > 20x capital  → 0.8  (normal market conditions)
      > 10x capital  → 0.6  (thin market, extra conservative)
      <= 10x capital → None (reject — not worth the risk)

    Returns:
        (multiplier: float|None, tier_label: str)
    """
    if liquidity_ratio > 50:
        return SLIP_MULT_ULTRA,  'ULTRA-LIQUID (0.9x)'
    elif liquidity_ratio > 20:
        return SLIP_MULT_NORMAL, 'NORMAL (0.8x)'
    elif liquidity_ratio > 10:
        return SLIP_MULT_THIN,   'THIN (0.6x)'
    else:
        return SLIP_MULT_REJECT, 'REJECT (≤10x capital)'


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

def calculate_fees_pct(capital_usd: float,
                        num_legs: int,
                        priority_tier: str = None) -> float:
    """
    Calculate total fee burden as % of capital.

    Components:
      - Jupiter platform fee : 0.2% per swap leg
      - Solana base tx fee   : ~$0.0004 per tx
      - Jito priority fee    : capital-based tier (none/low/medium) or .env override
                               $10–$19 = none ($0.000), $20–$60 = low ($0.002),
                               $61–$100 = medium ($0.005). .env wins if set.

    At $50 capital, low tier adds ~0.004% — negligible vs 1.5% gross target.

    Args:
        capital_usd:   Trade capital in USD
        num_legs:      Number of swap legs (2 direct, 3 triangular)
        priority_tier: Override tier. Defaults to PRIORITY_FEE_DEFAULT from .env

    Returns:
        Total fee as percentage of capital
    """
    try:
        tier               = priority_tier or PRIORITY_FEE_DEFAULT
        platform_fees_pct  = JUPITER_PLATFORM_FEE_PCT * num_legs
        solana_base_pct    = (SOLANA_TX_FEE_USD / capital_usd) * 100
        priority_fee_usd   = PRIORITY_FEE_TIER.get(tier, PRIORITY_FEE_TIER['medium'])
        priority_fee_pct   = (priority_fee_usd / capital_usd) * 100
        return platform_fees_pct + solana_base_pct + priority_fee_pct
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
    # STAGE 0 — Price Staleness Check
    # Re-derive rate from latest quote and compare to rate at
    # detection time. Drift > 0.2% means opportunity may be stale.
    # ─────────────────────────────────────────────
    STALE_PRICE_TOLERANCE_PCT = 0.2

    detected_rate = signal.get('detected_rate', 0.0)
    if detected_rate and detected_rate > 0 and quote_legs:
        try:
            latest_q = quote_legs[0]
            in_sym_s0  = signal.get('input_symbol', '')
            out_sym_s0 = signal.get('output_symbol', '')
            in_dec_s0  = TOKEN_DECIMALS.get(in_sym_s0,  6)
            out_dec_s0 = TOKEN_DECIMALS.get(out_sym_s0, 6)
            in_amt_s0  = int(latest_q.get('in_amount',  0))
            out_amt_s0 = int(latest_q.get('out_amount', 0))
            if in_amt_s0 > 0 and out_amt_s0 > 0:
                current_rate_s0 = (
                    Decimal(out_amt_s0) / Decimal(10 ** out_dec_s0) /
                    (Decimal(in_amt_s0) / Decimal(10 ** in_dec_s0))
                )
                rate_drift = abs(
                    float(current_rate_s0) - detected_rate
                ) / detected_rate * 100
                result.price_drift_pct = round(rate_drift, 4)
                if rate_drift > STALE_PRICE_TOLERANCE_PCT:
                    result.reject_reason = (
                        f'Price drifted {rate_drift:.4f}% since detection '
                        f'(tolerance {STALE_PRICE_TOLERANCE_PCT}%)'
                    )
                    result.recommendation = 'STALE'
                    logger.warning(
                        f'  Stage 0 STALE: {signal.get("pair")} — '
                        f'rate drifted {rate_drift:.4f}% '
                        f'(detected={detected_rate:.6f} '
                        f'current={float(current_rate_s0):.6f})'
                    )
                    return result
                else:
                    logger.info(
                        f'  Stage 0 OK: rate drift {rate_drift:.4f}% '
                        f'within {STALE_PRICE_TOLERANCE_PCT}% tolerance'
                    )
        except Exception as e:
            logger.debug(f'  Stage 0: skipped — {e}')
    else:
        logger.debug('  Stage 0: no detected_rate in signal — skipped')

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
    # STAGE 3 — Worst-Leg Slippage Simulation
    # ─────────────────────────────────────────────
    result.stage_reached = 3
    result.legs = num_legs

    # Derive actual quote capital from stored in_amount + token prices
    # Falls back to TRADE_CAPITAL_USD if price data unavailable
    quote_capital_estimate = get_quote_capital_usd(
        signal, signal.get('_token_prices', {})
    )
    logger.debug(
        f"  Quote capital: ${quote_capital_estimate:.2f} "
        f"(TRADE_CAPITAL_USD=${TRADE_CAPITAL_USD:.2f})"
    )

    is_triangular = num_legs == LEGS_PER_TRIANGULAR

    if result.liquidity_tier == "SAFE":
        if is_triangular:
            # Triangular: accumulate 0.5% per leg — slippage compounds across all 3
            simulated_slippage = 0.5 * num_legs
            logger.info(
                f"  Stage 3: SAFE tier (triangular) — "
                f"0.5% x {num_legs} legs = {simulated_slippage:.4f}%"
            )
        else:
            # Direct 2-leg: worst-leg is sufficient, smaller compounding risk
            simulated_slippage = 0.5
            logger.info(
                f"  Stage 3: SAFE tier (direct) — worst-leg assumption {simulated_slippage}%"
            )
    else:
        if is_triangular:
            # Triangular SIMULATE tier: sum slippage across all legs using actual quote size
            per_leg_impacts = impacts if impacts else [thinnest_impact] * num_legs
            simulated_slippage = simulate_total_slippage(
                per_leg_impacts[:num_legs],
                result.capital_usd,
                quote_capital_estimate
            )
            logger.info(
                f"  Stage 3: SIMULATE tier (triangular) — "
                f"accumulated slippage {simulated_slippage:.4f}% across {num_legs} legs"
            )
        else:
            # Direct SIMULATE tier: worst leg only
            simulated_slippage = simulate_leg_slippage(
                thinnest_impact, result.capital_usd, quote_capital_estimate
            )
            logger.info(
                f"  Stage 3: SIMULATE tier (direct) — "
                f"worst-leg slippage {simulated_slippage:.4f}%"
            )

    result.simulated_slippage_pct = simulated_slippage

    # ─────────────────────────────────────────────
    # STAGE 4 — Net Profit + Dynamic Slippage Formula
    # Formula: Slippage Tolerance = (Gross - Fees - Desired Net) / 2
    # ─────────────────────────────────────────────
    result.stage_reached = 4

    fee_pct = calculate_fees_pct(result.capital_usd, num_legs)

    # Derive safe slippage tolerance from formula
    derived_tolerance = derive_slippage_tolerance(gross_profit, fee_pct)

    # Reject if formula yields negative tolerance
    # (gross can't cover fees + desired net even with zero slippage)
    if derived_tolerance <= 0:
        result.reject_reason = (
            f"Insufficient gross {gross_profit:.4f}%: cannot cover "
            f"fees {fee_pct:.4f}% + desired net {DESIRED_NET_PROFIT_PCT}% "
            f"(tolerance = {derived_tolerance:.4f}%)"
        )
        result.recommendation = "REJECT"
        result.gross_pct = gross_profit
        result.fee_pct = fee_pct
        logger.info(f"  Stage 4 FAIL: {result.reject_reason}")
        return result

    # Check 3:1 ratio (warning only — does not reject)
    ratio, ratio_ok, ratio_msg = check_slippage_ratio(gross_profit, derived_tolerance)

    # Net profit using worst-leg slippage against derived tolerance
    # Use the more conservative of: worst-leg sim vs derived tolerance
    effective_slippage = max(simulated_slippage, derived_tolerance)
    net_profit = gross_profit - effective_slippage - fee_pct

    result.gross_pct          = gross_profit
    result.fee_pct            = fee_pct
    result.slippage_pct       = effective_slippage
    result.net_profit_pct     = net_profit

    logger.info(
        f"  Stage 4: gross {gross_profit:.4f}% | "
        f"derived tolerance {derived_tolerance:.4f}% | "
        f"sim. slippage {simulated_slippage:.4f}% | "
        f"effective slippage {effective_slippage:.4f}% | "
        f"fees {fee_pct:.4f}% | net {net_profit:.4f}% | "
        f"ratio {ratio_msg}"
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

    # Dynamic multiplier based on liquidity depth tier
    multiplier, mult_tier = get_dynamic_multiplier(result.liquidity_ratio)

    # Reject if depth is insufficient regardless of profit
    if multiplier is None:
        result.reject_reason = (
            f"Liquidity ratio {result.liquidity_ratio:.1f}x <= 10x capital — "
            f"dynamic multiplier REJECT tier ({mult_tier})"
        )
        result.recommendation = 'REJECT'
        logger.info(f"  Stage 5 REJECT: {result.reject_reason}")
        return result

    # Dynamic slippage = derived_tolerance * depth-adjusted multiplier
    dynamic_slippage_pct = derive_slippage_tolerance(gross_profit, fee_pct) * multiplier
    dynamic_slippage_bps = int(dynamic_slippage_pct * 100)
    result.multiplier_tier = mult_tier

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
            f"dynamic slippage {dynamic_slippage_bps}bps | "
            f"multiplier {mult_tier}"
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
    _sol_price = float(os.getenv('SOL_PRICE_USD', '90.0'))
    _live_tips = fetch_jito_tip_floor(_sol_price)
    _live_usd  = _live_tips.get(PRIORITY_FEE_DEFAULT, PRIORITY_FEE_TIER.get(PRIORITY_FEE_DEFAULT, 0.0))
    _tip_src   = _live_tips.get('source', 'fallback')
    _tier_src  = 'from .env' if os.getenv('PRIORITY_FEE_TIER','').strip().lower() in PRIORITY_FEE_TIER else 'capital-based formula'
    print(f"  Priority fee tier:      {PRIORITY_FEE_DEFAULT} "
          f"(${_live_usd:.5f} USD) — {_tier_src} / tip source: {_tip_src}")
    print(f"  Tier mapping:           none=$0  low=EMA_p50  medium=p75  high=p95  desperate=p99")
    print(f"  Capital formula:        $10-19=none  $20-60=low  $61-100=medium")
    print(f"  Override via .env:      PRIORITY_FEE_TIER=none|low|medium|high|desperate")
    print(f"  Live tip floor API:     {JITO_TIP_FLOOR_URL}")
    print(f"  Cache TTL:              {JITO_CACHE_TTL_S}s  |  Timeout: {JITO_TIMEOUT_S}s")
    print(f"  Desired net profit:     {DESIRED_NET_PROFIT_PCT}%  (anchors slippage formula)")
    print(f"  Slippage formula:       (Gross - Fees - {DESIRED_NET_PROFIT_PCT}%) / 2")
    print(f"  Min gross/slip ratio:   {MIN_GROSS_SLIPPAGE_RATIO}:1  (3:1 rule of thumb)")
    print(f"  Slippage method:        worst-leg (most conservative)")
    print(f"  Dynamic slippage multiplier tiers:")
    print(f"    > 50x capital  → {SLIP_MULT_ULTRA}  (ULTRA-LIQUID)")
    print(f"    > 20x capital  → {SLIP_MULT_NORMAL}  (NORMAL)")
    print(f"    > 10x capital  → {SLIP_MULT_THIN}  (THIN)")
    print(f"    ≤ 10x capital  → REJECT")
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
