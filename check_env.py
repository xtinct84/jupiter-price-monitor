"""
check_env.py
════════════════════════════════════════════════════
Confirms what values the live scripts are actually
reading from your .env file at runtime.

Run from your project root (same folder as .env):
    python check_env.py
════════════════════════════════════════════════════
"""
import os
from dotenv import load_dotenv

load_dotenv()

from execution_validator import (
    TRADE_CAPITAL_USD,
    PRIORITY_FEE_DEFAULT,
    PRIORITY_FEE_TIER,
    get_capital_based_tier,
    NET_PROFIT_THRESHOLD_DIRECT,
    NET_PROFIT_THRESHOLD_TRIANGULAR,
    GROSS_DIVERGENCE_THRESHOLD,
    MIN_GROSS_SLIPPAGE_RATIO,
    DESIRED_NET_PROFIT_PCT,
)

# Sanity-check: alert if thresholds look like old values
_old_thresholds = {
    'GROSS_DIVERGENCE_THRESHOLD': (GROSS_DIVERGENCE_THRESHOLD, 1.5),
    'NET_PROFIT_THRESHOLD_DIRECT': (NET_PROFIT_THRESHOLD_DIRECT, 1.5),
    'NET_PROFIT_THRESHOLD_TRIANGULAR': (NET_PROFIT_THRESHOLD_TRIANGULAR, 1.0),
}

env_capital   = os.getenv('TRADE_CAPITAL_USD', 'NOT SET')
env_fee_tier  = os.getenv('PRIORITY_FEE_TIER', 'NOT SET')
env_telegram  = os.getenv('TELEGRAM_BOT_TOKEN', 'NOT SET')

formula_tier  = get_capital_based_tier(TRADE_CAPITAL_USD)
tip_usd       = PRIORITY_FEE_TIER.get(PRIORITY_FEE_DEFAULT, 0.0)
tip_pct       = (tip_usd / TRADE_CAPITAL_USD * 100) if TRADE_CAPITAL_USD > 0 else 0
tier_source   = (
    'from .env (manual override)'
    if env_fee_tier.lower() in PRIORITY_FEE_TIER
    else 'from capital formula (no .env override)'
)

print("""
╔══════════════════════════════════════════════════════╗
║           LIVE ENVIRONMENT CONFIRMATION              ║
╚══════════════════════════════════════════════════════╝
""")

print("── .env raw values ──────────────────────────────────")
print(f"  TRADE_CAPITAL_USD  = {env_capital}")
print(f"  PRIORITY_FEE_TIER  = {env_fee_tier}")
print(f"  TELEGRAM_BOT_TOKEN = {'SET' if env_telegram != 'NOT SET' else 'NOT SET'}")

print("\n── Resolved by execution_validator.py ──────────────")
print(f"  TRADE_CAPITAL_USD  → ${TRADE_CAPITAL_USD:.2f} USD")
print(f"  Capital tier check → '{formula_tier}' tier (formula result)")
print(f"  PRIORITY_FEE_DEFAULT → '{PRIORITY_FEE_DEFAULT}' ({tier_source})")
print(f"  Jito tip amount    → ${tip_usd:.4f} USD ({tip_pct:.4f}% of capital)")

print("\n── Fee burden at this capital level ─────────────────")
from execution_validator import calculate_fees_pct, JUPITER_PLATFORM_FEE_PCT, SOLANA_TX_FEE_USD
platform = JUPITER_PLATFORM_FEE_PCT * 2
base     = (SOLANA_TX_FEE_USD / TRADE_CAPITAL_USD) * 100
total    = calculate_fees_pct(TRADE_CAPITAL_USD, 2, PRIORITY_FEE_DEFAULT)
print(f"  Platform fees (2-leg) = {platform:.4f}%")
print(f"  Solana base fee       = {base:.4f}%")
print(f"  Jito tip              = {tip_pct:.4f}%")
print(f"  Total fee burden      = {total:.4f}%")

print("\n── Validator thresholds active at this capital ──────")
min_gross = total + DESIRED_NET_PROFIT_PCT + 0.3
print(f"  GROSS_DIVERGENCE_THRESHOLD  = {GROSS_DIVERGENCE_THRESHOLD:.2f}%")
print(f"  NET_PROFIT_THRESHOLD_DIRECT = {NET_PROFIT_THRESHOLD_DIRECT:.2f}%")
print(f"  NET_PROFIT_THRESHOLD_TRI    = {NET_PROFIT_THRESHOLD_TRIANGULAR:.2f}%")
print(f"  DESIRED_NET_PROFIT_PCT      = {DESIRED_NET_PROFIT_PCT:.2f}%")
print(f"  Estimated min viable gross  = {min_gross:.4f}% (fees + net + 0.3% slippage)")

# Warn if old pre-reduction values are still active
_stale = [(k, v, old) for k, (v, old) in _old_thresholds.items() if abs(v - old) < 0.01]
if _stale:
    print(f"\n  ⚠️  STALE THRESHOLDS — still at pre-reduction values:")
    for k, v, old in _stale:
        print(f"     {k} = {v:.2f}% (expected < {old:.2f}%)")
    print(f"     Confirm execution_validator.py is up to date (git pull)")

print("\n── Capital tier boundaries (for reference) ──────────")
print(f"  $10–$19  → none   (no Jito tip)")
print(f"  $20–$60  → low    ($0.002 tip)")
print(f"  $61–$100 → medium ($0.005 tip)")
print(f"  Your capital ${TRADE_CAPITAL_USD:.2f} → '{formula_tier}' tier")

if TRADE_CAPITAL_USD < 20:
    print(f"""
  ⚠️  NOTE: At $10 capital the Solana base fee alone is
     {base:.4f}% per trade. Fixed costs are disproportionate
     at this capital level — profitability requires larger
     divergence events than typical market conditions produce.
""")
print("═" * 56)
