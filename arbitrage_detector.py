"""
arbitrage_detector.py
─────────────────────
Queries jupiter_monitor.db for price and quote data,
evaluates each trading pair against a weighted scoring
system, classifies signals, and sends Telegram notifications.

Signal Classification:
  ≥ EXECUTE_THRESHOLD   → AUTO-EXECUTE candidate (Telegram alert, execute=True)
  ≥ ANALYSIS_THRESHOLD  → NEEDS ANALYSIS (Telegram alert, execute=False)
  < ANALYSIS_THRESHOLD  → DISCARD (no alert)

Run independently:
  python arbitrage_detector.py

Or import and call run_detection() from price_monitor.py
after each fetch cycle.
"""

import sqlite3
import asyncio
import os
import logging
import httpx
import numpy as np
from decimal import Decimal, getcontext
getcontext().prec = 28
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from execution_validator import enrich_signal_with_validation

load_dotenv()

# In-memory duplicate cache — primary gate within a session
# key = (pair, signal_type), value = datetime fired
_signal_cache: dict = {}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION — tune these without touching detection logic
# =============================================================================

DB_PATH = Path("price_history/jupiter_monitor.db")

# --- Execution gate thresholds ---
EXECUTE_THRESHOLD  = 75   # Score >= this → auto-execute candidate
ANALYSIS_THRESHOLD = 51   # Score > 50% of 100pts — strict quality gate
# Score < ANALYSIS_THRESHOLD → discard silently

# --- Condition weights (must sum to 100) ---
# Profit is the dominant condition — a signal with insufficient profit
# cannot realistically reach the analysis threshold on other conditions alone.
WEIGHTS = {
    'profit':         45,   # DOMINANT — estimated profit above minimum
    'price_impact':   20,   # Price impact within acceptable range
    'liquidity':      15,   # Sufficient liquidity exists
    'slippage':       10,   # Slippage within bounds
    'no_duplicate':    5,   # No recent duplicate signal
    'divergence':      5,   # Statistically significant rate divergence
}

# --- Condition pass thresholds ---
# Strategy-specific profit thresholds (MEV-adjusted)
MIN_PROFIT_RATE_DIVERGENCE  = 1.0   # Direct 2-step trades — highest MEV risk
MIN_PROFIT_TRIANGULAR       = 0.6   # Multi-hop paths — lower MEV exposure
MIN_PROFIT_IMPACT_ANOMALY   = 1.0   # Liquidity shift signals — same MEV risk as direct
MIN_PROFIT_MOMENTUM         = 1.5   # Momentum breakout — strictest, fast reversal risk
MAX_PRICE_IMPACT_PCT  = 1.0      # Maximum acceptable price impact %
MIN_LIQUIDITY_USD     = 500_000  # Minimum liquidity in USD
MAX_SLIPPAGE_PCT      = 1.5      # Balanced MEV sweet spot (1.0% too tight, 2.0% sandwich risk)
DUPLICATE_WINDOW_MIN  = 15       # Conservative window — stricter thresholds reduce noise naturally
DIVERGENCE_SIGMA      = 2.0      # Standard deviations from rolling mean
ROLLING_WINDOW        = 20       # Number of recent quotes for rolling stats

# --- Momentum breakout configuration ---
MOMENTUM_SIGMA        = 1.5      # sigma threshold for momentum divergence
MOMENTUM_WINDOW       = 5        # Consecutive quotes for trend confirmation (~2.5 min)
MOMENTUM_IMPACT_SIGMA = 1.0      # sigma above mean price_impact for volume spike

MOMENTUM_PAIRS = [
    ('SOL',  'USDC'),
    ('JUP',  'USDC'),
    ('JTO',  'USDC'),
    ('PYTH', 'USDC'),
]

# Pairs excluded from ALL detection strategies
# Thin depth, decimal precision artifacts, or zero liquidity
EXCLUDED_PAIRS = {
    'BONK/SOL',    # 5-decimal precision artifacts + thin depth
    'MOUTAI/USDC', # Zero liquidity
    'MYRO/USDC',   # Zero liquidity
    'WEN/USDC',    # Zero liquidity
}

# --- Telegram configuration ---
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID   = os.getenv('TELEGRAM_CHAT_ID', '')

# --- Triangular arbitrage paths to evaluate ---
# Each tuple: (A, B, C) evaluates A→B→C→A
TRIANGULAR_PATHS = [
    ('SOL',  'USDC', 'USDT'),
    ('SOL',  'USDC', 'JUP'),
    ('JUP',  'USDC', 'SOL'),
    ('BONK', 'SOL',  'USDC'),
    ('WIF',  'SOL',  'USDC'),
]

# Token decimals for amount conversion
TOKEN_DECIMALS = {
    'SOL':    9, 'USDC':   6, 'USDT':  6,
    'JUP':    6, 'RAY':    6, 'BONK':  5,
    'JTO':    9, 'PYTH':   6, 'WIF':   6,
    'POPCAT': 9, 'MOUTAI': 9, 'MYRO':  9, 'WEN': 5
}

# =============================================================================
# DATABASE HELPERS
# =============================================================================

def get_connection():
    """Open connection to jupiter_monitor.db"""
    if not DB_PATH.exists():
        logger.error(f"Database not found at {DB_PATH}. Is run_monitor.py running?")
        return None
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def ensure_signals_table(conn):
    """Create signals table if it doesn't exist"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp            TEXT NOT NULL,
            signal_type          TEXT NOT NULL,
            pair                 TEXT NOT NULL,
            description          TEXT,
            estimated_profit_pct REAL,
            weighted_score       INTEGER,
            execute_candidate    INTEGER DEFAULT 0,
            condition_breakdown  TEXT,
            resolved             INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_signals_timestamp
        ON signals (timestamp)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_signals_pair
        ON signals (pair)
    """)
    conn.commit()
    logger.info("✅ Signals table verified")


def get_recent_quotes(conn, pair: str, limit: int = ROLLING_WINDOW) -> pd.DataFrame:
    """Fetch the most recent N quote records for a trading pair"""
    query = """
        SELECT timestamp, in_amount, out_amount, price_impact_pct, slippage_bps
        FROM quotes
        WHERE pair = ?
        ORDER BY timestamp DESC
        LIMIT ?
    """
    try:
        df = pd.read_sql_query(query, conn, params=(pair, limit))
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    except Exception as e:
        logger.error(f"Error fetching quotes for {pair}: {e}")
        return pd.DataFrame()


def get_latest_quote(conn, pair: str) -> dict:
    """Fetch the single most recent quote for a trading pair"""
    query = """
        SELECT timestamp, pair, input_symbol, output_symbol,
               in_amount, out_amount, price_impact_pct, slippage_bps
        FROM quotes
        WHERE pair = ?
        ORDER BY timestamp DESC
        LIMIT 1
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, (pair,))
        row = cursor.fetchone()
        if row:
            cols = ['timestamp', 'pair', 'input_symbol', 'output_symbol',
                    'in_amount', 'out_amount', 'price_impact_pct', 'slippage_bps']
            return dict(zip(cols, row))
        return {}
    except Exception as e:
        logger.error(f"Error fetching latest quote for {pair}: {e}")
        return {}


def get_latest_price(conn, symbol: str) -> dict:
    """Fetch the most recent price record for a token"""
    query = """
        SELECT timestamp, symbol, price_usd, price_change_24h, liquidity
        FROM prices
        WHERE symbol = ?
        ORDER BY timestamp DESC
        LIMIT 1
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, (symbol,))
        row = cursor.fetchone()
        if row:
            cols = ['timestamp', 'symbol', 'price_usd', 'price_change_24h', 'liquidity']
            return dict(zip(cols, row))
        return {}
    except Exception as e:
        logger.error(f"Error fetching price for {symbol}: {e}")
        return {}


def check_duplicate_signal(conn, pair: str, signal_type: str) -> bool:
    """
    Return True if identical signal fired within duplicate window.
    Uses in-memory cache as primary gate (within-session)
    and DB as secondary gate (cross-session).
    """
    key = (pair, signal_type)
    now = datetime.now()
    window = timedelta(minutes=DUPLICATE_WINDOW_MIN)

    # Primary: in-memory cache
    if key in _signal_cache:
        age = (now - _signal_cache[key]).total_seconds()
        if now - _signal_cache[key] < window:
            logger.info(f"🔇 Suppressed duplicate: {pair} ({signal_type}) — {age:.0f}s ago")
            return True
        else:
            del _signal_cache[key]

    # Secondary: database for cross-session duplicates
    window_start = (now - window).strftime('%Y-%m-%d %H:%M:%S')
    query = """
        SELECT COUNT(*) FROM signals
        WHERE pair = ?
        AND signal_type = ?
        AND timestamp >= ?
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, (pair, signal_type, window_start))
        count = cursor.fetchone()[0]
        return count > 0
    except Exception as e:
        logger.error(f"Error checking duplicate signal: {e}")
        return False


def register_signal_in_cache(pair: str, signal_type: str):
    """Register a signal in cache immediately after detection."""
    _signal_cache[(pair, signal_type)] = datetime.now()


def log_signal(conn, signal: dict):
    """Insert a detected signal into the signals table and update cache"""
    register_signal_in_cache(signal['pair'], signal['signal_type'])
    try:
        conn.execute("""
            INSERT INTO signals (
                timestamp, signal_type, pair, description,
                estimated_profit_pct, weighted_score,
                execute_candidate, condition_breakdown, resolved
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
        """, (
            signal['timestamp'],
            signal['signal_type'],
            signal['pair'],
            signal['description'],
            signal['estimated_profit_pct'],
            signal['weighted_score'],
            1 if signal.get('execute_candidate', False) else 0,
            signal['condition_breakdown']
        ))
        conn.commit()
        logger.info(f"💾 Signal logged: {signal['pair']} | "
                    f"Score: {signal['weighted_score']} | "
                    f"Type: {signal['signal_type']}")
    except Exception as e:
        logger.error(f"Error logging signal: {e}")

# =============================================================================
# WEIGHTED SCORING ENGINE
# =============================================================================

def calculate_effective_rate(in_amount: int, out_amount: int,
                              in_symbol: str, out_symbol: str) -> float:
    """
    Convert raw amounts to effective swap rate using high-precision
    Decimal arithmetic to prevent float truncation on small-value
    tokens like BONK (5 decimals, very small per-unit value).
    """
    try:
        in_dec  = TOKEN_DECIMALS.get(in_symbol, 6)
        out_dec = TOKEN_DECIMALS.get(out_symbol, 6)
        in_amt  = Decimal(in_amount)  / Decimal(10 ** in_dec)
        out_amt = Decimal(out_amount) / Decimal(10 ** out_dec)
        if in_amt == 0:
            return 0.0
        return float(out_amt / in_amt)
    except Exception:
        return 0.0


def score_conditions(quote: dict, price_data: dict,
                     recent_quotes: pd.DataFrame,
                     conn, signal_type: str,
                     min_profit_pct: float = 1.0) -> dict:
    """
    Evaluate each condition and return weighted score breakdown.

    Returns:
        {
          'total_score': int,
          'breakdown': dict of condition -> (points_earned, max_points, passed),
          'estimated_profit_pct': float
        }
    """
    breakdown = {}
    total_score = 0
    estimated_profit_pct = 0.0

    in_sym  = quote.get('input_symbol', '')
    out_sym = quote.get('output_symbol', '')
    pair    = quote.get('pair', '')

    # --- 1. PROFIT CONDITION ---
    if not recent_quotes.empty and len(recent_quotes) >= 3:
        in_dec  = TOKEN_DECIMALS.get(in_sym, 6)
        out_dec = TOKEN_DECIMALS.get(out_sym, 6)
        recent_quotes['rate'] = recent_quotes.apply(
            lambda row: float(
                Decimal(int(row['out_amount'])) / Decimal(10 ** out_dec) /
                (Decimal(int(row['in_amount'])) / Decimal(10 ** in_dec))
            ) if row['in_amount'] > 0 else 0.0,
            axis=1
        )
        current_rate = calculate_effective_rate(
            quote['in_amount'], quote['out_amount'], in_sym, out_sym
        )
        mean_rate = recent_quotes['rate'].mean()
        if mean_rate > 0:
            estimated_profit_pct = ((current_rate - mean_rate) / mean_rate) * 100

    passed_profit = estimated_profit_pct >= min_profit_pct
    points = WEIGHTS['profit'] if passed_profit else 0
    total_score += points
    breakdown['profit'] = {
        'points': points,
        'max': WEIGHTS['profit'],
        'passed': passed_profit,
        'value': f"{estimated_profit_pct:.4f}%",
        'threshold': f">= {min_profit_pct}%"
    }

    # --- 2. PRICE IMPACT CONDITION ---
    price_impact = abs(quote.get('price_impact_pct', 999))
    passed_impact = price_impact <= MAX_PRICE_IMPACT_PCT
    points = WEIGHTS['price_impact'] if passed_impact else 0
    total_score += points
    breakdown['price_impact'] = {
        'points': points,
        'max': WEIGHTS['price_impact'],
        'passed': passed_impact,
        'value': f"{price_impact:.4f}%",
        'threshold': f"<= {MAX_PRICE_IMPACT_PCT}%"
    }

    # --- 3. LIQUIDITY CONDITION ---
    liquidity = price_data.get('liquidity', 0)
    passed_liquidity = liquidity >= MIN_LIQUIDITY_USD
    points = WEIGHTS['liquidity'] if passed_liquidity else 0
    total_score += points
    breakdown['liquidity'] = {
        'points': points,
        'max': WEIGHTS['liquidity'],
        'passed': passed_liquidity,
        'value': f"${liquidity:,.0f}",
        'threshold': f">= ${MIN_LIQUIDITY_USD:,}"
    }

    # --- 4. SLIPPAGE CONDITION ---
    slippage_pct = quote.get('slippage_bps', 9999) / 100
    passed_slippage = slippage_pct <= MAX_SLIPPAGE_PCT
    points = WEIGHTS['slippage'] if passed_slippage else 0
    total_score += points
    breakdown['slippage'] = {
        'points': points,
        'max': WEIGHTS['slippage'],
        'passed': passed_slippage,
        'value': f"{slippage_pct:.2f}%",
        'threshold': f"<= {MAX_SLIPPAGE_PCT}%"
    }

    # --- 5. NO DUPLICATE CONDITION ---
    is_duplicate = check_duplicate_signal(conn, pair, signal_type)
    passed_duplicate = not is_duplicate
    points = WEIGHTS['no_duplicate'] if passed_duplicate else 0
    total_score += points
    breakdown['no_duplicate'] = {
        'points': points,
        'max': WEIGHTS['no_duplicate'],
        'passed': passed_duplicate,
        'value': 'No duplicate' if passed_duplicate else 'Duplicate found',
        'threshold': f"No signal within {DUPLICATE_WINDOW_MIN} min"
    }

    # --- 6. DIVERGENCE CONDITION ---
    passed_divergence = False
    sigma_value = 0.0
    if not recent_quotes.empty and len(recent_quotes) >= 3:
        in_dec  = TOKEN_DECIMALS.get(in_sym, 6)
        out_dec = TOKEN_DECIMALS.get(out_sym, 6)
        if 'rate' not in recent_quotes.columns:
            recent_quotes['rate'] = (
                (recent_quotes['out_amount'] / (10 ** out_dec)) /
                (recent_quotes['in_amount']  / (10 ** in_dec))
            )
        std  = recent_quotes['rate'].std()
        mean = recent_quotes['rate'].mean()
        current_rate = calculate_effective_rate(
            quote['in_amount'], quote['out_amount'], in_sym, out_sym
        )
        if std > 0:
            sigma_value = abs((current_rate - mean) / std)
            passed_divergence = sigma_value >= DIVERGENCE_SIGMA

    points = WEIGHTS['divergence'] if passed_divergence else 0
    total_score += points
    breakdown['divergence'] = {
        'points': points,
        'max': WEIGHTS['divergence'],
        'passed': passed_divergence,
        'value': f"{sigma_value:.2f}σ",
        'threshold': f">= {DIVERGENCE_SIGMA}σ"
    }

    return {
        'total_score': total_score,
        'breakdown': breakdown,
        'estimated_profit_pct': estimated_profit_pct
    }


def format_condition_breakdown(breakdown: dict) -> str:
    """Format condition breakdown as a readable string for database storage"""
    lines = []
    for condition, data in breakdown.items():
        status = "✓" if data['passed'] else "✗"
        lines.append(
            f"{status} {condition}: {data['value']} "
            f"(threshold: {data['threshold']}) "
            f"[{data['points']}/{data['max']}pts]"
        )
    return " | ".join(lines)

# =============================================================================
# DETECTION STRATEGIES
# =============================================================================

def detect_rate_divergence(conn) -> list:
    """
    Strategy 1: Direct Rate Divergence
    Detects when a pair's current effective rate diverges
    significantly from its recent rolling average.
    """
    signals = []
    monitored_pairs = [
        ('SOL', 'USDC'), ('JUP', 'USDC'), ('RAY', 'USDC'),
        ('BONK', 'SOL'), ('JTO', 'USDC'), ('PYTH', 'USDC'), ('WIF', 'SOL')
    ]

    for in_sym, out_sym in monitored_pairs:
        pair = f"{in_sym}/{out_sym}"

        # Exclusion check — skip thin/problematic pairs
        if pair in EXCLUDED_PAIRS:
            continue

        # Pre-flight duplicate check — skip scoring entirely if suppressed
        if check_duplicate_signal(conn, pair, 'rate_divergence'):
            logger.info(f"🔇 Skipping {pair} rate_divergence — duplicate within {DUPLICATE_WINDOW_MIN} min")
            continue

        quote = get_latest_quote(conn, pair)
        if not quote:
            continue

        price_data = get_latest_price(conn, in_sym)
        recent_quotes = get_recent_quotes(conn, pair)

        if recent_quotes.empty or len(recent_quotes) < 3:
            continue

        score_result = score_conditions(
            quote, price_data, recent_quotes, conn, 'rate_divergence',
            min_profit_pct=MIN_PROFIT_RATE_DIVERGENCE
        )
        total_score = score_result['total_score']

        # Discard negative profit signals — market has moved against the opportunity
        if score_result['estimated_profit_pct'] <= 0:
            logger.debug(f"Discarding {pair} rate_divergence — negative profit {score_result['estimated_profit_pct']:.4f}%")
            continue

        if total_score >= ANALYSIS_THRESHOLD:
            breakdown_str = format_condition_breakdown(score_result['breakdown'])
            signal = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'signal_type': 'rate_divergence',
                'pair': pair,
                'description': (
                    f"Rate divergence detected on {pair}. "
                    f"Estimated profit: {score_result['estimated_profit_pct']:.4f}%. "
                    f"Weighted score: {total_score}/100."
                ),
                'estimated_profit_pct': score_result['estimated_profit_pct'],
                'weighted_score': total_score,
                'condition_breakdown': breakdown_str
            }
            signals.append(signal)
            logger.info(f"📡 Rate divergence signal: {pair} | Score: {total_score}")

    return signals


def detect_triangular_arbitrage(conn) -> list:
    """
    Strategy 2: Triangular Arbitrage
    Evaluates A→B→C→A paths. If combined rates
    yield > 1.0 after slippage, a profit opportunity exists.
    """
    signals = []

    for token_a, token_b, token_c in TRIANGULAR_PATHS:
        pair_ab = f"{token_a}/{token_b}"
        pair_bc = f"{token_b}/{token_c}"
        pair_ca = f"{token_c}/{token_a}"
        path_label = f"{token_a}->{token_b}->{token_c}->{token_a}"

        # Exclusion check — skip paths containing excluded tokens
        path_pairs = [pair_ab, pair_bc, pair_ca]
        if any(p in EXCLUDED_PAIRS for p in path_pairs):
            continue

        # Pre-flight duplicate check
        if check_duplicate_signal(conn, path_label, 'triangular_arbitrage'):
            logger.info(f"🔇 Skipping {path_label} triangular — duplicate within {DUPLICATE_WINDOW_MIN} min")
            continue

        quote_ab = get_latest_quote(conn, pair_ab)
        quote_bc = get_latest_quote(conn, pair_bc)

        if not quote_ab or not quote_bc:
            continue

        # Calculate A→B→C effective rate
        rate_ab = calculate_effective_rate(
            quote_ab['in_amount'], quote_ab['out_amount'], token_a, token_b
        )
        rate_bc = calculate_effective_rate(
            quote_bc['in_amount'], quote_bc['out_amount'], token_b, token_c
        )

        # Infer C→A from price data
        price_a = get_latest_price(conn, token_a)
        price_c = get_latest_price(conn, token_c)

        if not price_a or not price_c:
            continue
        if price_a.get('price_usd', 0) == 0:
            continue

        rate_ca = price_c['price_usd'] / price_a['price_usd']

        # Combined return: start with 1 unit of A
        combined_return = rate_ab * rate_bc * rate_ca
        profit_pct = (combined_return - 1.0) * 100

        if profit_pct <= 0:
            continue

        # Use quote_ab as the primary quote for scoring
        price_data = price_a
        recent_quotes = get_recent_quotes(conn, pair_ab)
        path_label = f"{token_a}→{token_b}→{token_c}→{token_a}"

        score_result = score_conditions(
            quote_ab, price_data, recent_quotes,
            conn, 'triangular_arbitrage',
            min_profit_pct=MIN_PROFIT_TRIANGULAR
        )

        # Override profit score with actual triangular calculation
        score_result['estimated_profit_pct'] = profit_pct
        total_score = score_result['total_score']

        if total_score >= ANALYSIS_THRESHOLD:
            breakdown_str = format_condition_breakdown(score_result['breakdown'])
            signal = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'signal_type': 'triangular_arbitrage',
                'pair': path_label,
                'description': (
                    f"Triangular arbitrage path: {path_label}. "
                    f"Combined return: {combined_return:.6f}. "
                    f"Estimated profit: {profit_pct:.4f}%. "
                    f"Weighted score: {total_score}/100."
                ),
                'estimated_profit_pct': profit_pct,
                'weighted_score': total_score,
                'condition_breakdown': breakdown_str
            }
            signals.append(signal)
            logger.info(f"📡 Triangular signal: {path_label} | "
                        f"Return: {combined_return:.6f} | Score: {total_score}")

    return signals


def detect_impact_anomaly(conn) -> list:
    """
    Strategy 3: Price Impact Anomaly
    Flags when price impact spikes significantly above
    its rolling average, indicating a liquidity shift.
    """
    signals = []
    monitored_pairs = [
        ('SOL', 'USDC'), ('JUP', 'USDC'), ('RAY', 'USDC'),
        ('BONK', 'SOL'), ('JTO', 'USDC'), ('PYTH', 'USDC'), ('WIF', 'SOL')
    ]

    for in_sym, out_sym in monitored_pairs:
        pair = f"{in_sym}/{out_sym}"

        # Exclusion check
        if pair in EXCLUDED_PAIRS:
            continue

        # Pre-flight duplicate check
        if check_duplicate_signal(conn, pair, 'impact_anomaly'):
            logger.info(f"🔇 Skipping {pair} impact_anomaly — duplicate within {DUPLICATE_WINDOW_MIN} min")
            continue

        recent_quotes = get_recent_quotes(conn, pair)

        if recent_quotes.empty or len(recent_quotes) < 3:
            continue

        mean_impact = recent_quotes['price_impact_pct'].mean()
        std_impact  = recent_quotes['price_impact_pct'].std()
        latest_impact = recent_quotes.iloc[0]['price_impact_pct']

        if std_impact == 0:
            continue

        sigma = abs((latest_impact - mean_impact) / std_impact)

        if sigma < DIVERGENCE_SIGMA:
            continue

        quote = get_latest_quote(conn, pair)
        price_data = get_latest_price(conn, in_sym)

        if not quote:
            continue

        score_result = score_conditions(
            quote, price_data, recent_quotes, conn, 'impact_anomaly',
            min_profit_pct=MIN_PROFIT_IMPACT_ANOMALY
        )
        total_score = score_result['total_score']

        # Discard negative profit signals
        if score_result['estimated_profit_pct'] <= 0:
            logger.debug(f"Discarding {pair} impact_anomaly — negative profit {score_result['estimated_profit_pct']:.4f}%")
            continue

        if total_score >= ANALYSIS_THRESHOLD:
            breakdown_str = format_condition_breakdown(score_result['breakdown'])
            signal = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'signal_type': 'impact_anomaly',
                'pair': pair,
                'description': (
                    f"Price impact anomaly on {pair}. "
                    f"Current impact: {latest_impact:.4f}% "
                    f"({sigma:.2f}σ above mean). "
                    f"Weighted score: {total_score}/100."
                ),
                'estimated_profit_pct': score_result['estimated_profit_pct'],
                'weighted_score': total_score,
                'condition_breakdown': breakdown_str
            }
            signals.append(signal)
            logger.info(f"📡 Impact anomaly: {pair} | {sigma:.2f}σ | Score: {total_score}")

    return signals

# =============================================================================
# TELEGRAM NOTIFICATIONS
# =============================================================================

async def send_telegram_alert(signal: dict):
    """
    Send a Telegram notification for a detected signal.
    Requires TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram not configured. Add TELEGRAM_BOT_TOKEN and "
                       "TELEGRAM_CHAT_ID to your .env file.")
        return

    score = signal['weighted_score']

    # Determine classification label
    if score >= EXECUTE_THRESHOLD:
        classification = "🟢 EXECUTE CANDIDATE"
    else:
        classification = "🟡 ANALYSIS REQUIRED"

    # Build message — plain text only, no parse_mode to avoid HTML conflicts
    divider  = "-" * 35
    message = (
        f"{classification}\n"
        f"{divider}\n"
        f"Signal Type : {signal['signal_type'].replace('_', ' ').title()}\n"
        f"Pair        : {signal['pair']}\n"
        f"Est. Profit : {signal['estimated_profit_pct']:.4f}%\n"
        f"Score       : {score}/100\n"
        f"Time        : {signal['timestamp']}\n"
        f"{divider}\n"
        f"{signal['description']}\n"
        f"{divider}\n"
        f"Conditions:\n"
    )

    # Append condition breakdown
    breakdown_items = signal['condition_breakdown'].split(' | ')
    for item in breakdown_items:
        message += f"  {item}\n"

    # Append validation result if present
    validation = signal.get('validation')
    if validation:
        message += f"\n{divider}\n"
        message += "EXECUTION VALIDATION:\n"
        for line in validation.detail_lines():
            message += f"  {line}\n"

    if score >= EXECUTE_THRESHOLD and signal.get('execute_candidate'):
        message += f"\n{divider}\n"
        message += "MANUAL REVIEW REQUIRED before execution.\n"
        message += "OpenClaw agent not yet connected.\n"

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message
            })
            if response.status_code == 200:
                logger.info(f"✅ Telegram alert sent for {signal['pair']}")
            else:
                logger.error(f"Telegram error: {response.status_code} - {response.text}")
    except Exception as e:
        logger.error(f"Failed to send Telegram alert: {e}")

# =============================================================================
# MAIN DETECTION RUNNER
# =============================================================================

def detect_momentum_breakout(conn) -> list:
    """
    Detect momentum breakout signals on liquid pairs.

    Three conditions ALL required:
      1. Rate divergence > 1.5σ from rolling mean
      2. Price trend direction stable across last 5 quotes
         (monotonically increasing or decreasing — no reversals)
      3. Volume/liquidity spike: price_impact > rolling mean + 1.0σ
         (proxy for large order flow since Jupiter omits volume)

    Pairs monitored: SOL/USDC, JUP/USDC, JTO/USDC, PYTH/USDC
    Min gross profit: 1.5% (momentum reverses fast)
    """
    signals = []

    for in_sym, out_sym in MOMENTUM_PAIRS:
        pair = f"{in_sym}/{out_sym}"

        # Pre-flight duplicate check
        if check_duplicate_signal(conn, pair, 'momentum_breakout'):
            logger.info(f"🔇 Skipping {pair} momentum_breakout — duplicate within {DUPLICATE_WINDOW_MIN} min")
            continue

        # Fetch last MOMENTUM_WINDOW + extra quotes for rolling stats
        recent_quotes = get_recent_quotes(conn, pair, limit=ROLLING_WINDOW)
        if recent_quotes.empty or len(recent_quotes) < MOMENTUM_WINDOW + 3:
            continue

        latest_quote = get_latest_quote(conn, pair)
        if not latest_quote:
            continue

        price_data = get_latest_price(conn, in_sym)

        try:
            # Compute rate column if not present (not stored in DB, derived on-the-fly)
            if 'rate' not in recent_quotes.columns:
                in_dec  = TOKEN_DECIMALS.get(in_sym, 6)
                out_dec = TOKEN_DECIMALS.get(out_sym, 6)
                recent_quotes = recent_quotes.copy()
                recent_quotes['rate'] = (
                    (recent_quotes['out_amount'].astype(float) / (10 ** out_dec)) /
                    (recent_quotes['in_amount'].astype(float)  / (10 ** in_dec))
                    .replace(0, float('nan'))
                )
                recent_quotes = recent_quotes.dropna(subset=['rate'])
                if len(recent_quotes) < MOMENTUM_WINDOW + 3:
                    continue

            rates = recent_quotes['rate'].astype(float)
            impacts = recent_quotes['price_impact_pct'].astype(float)

            # ── Condition 1: Rate divergence > MOMENTUM_SIGMA ──
            rate_mean = rates.mean()
            rate_std  = rates.std()
            if rate_std == 0:
                continue

            current_rate = float(rates.iloc[-1])
            z_score = (current_rate - rate_mean) / rate_std

            if abs(z_score) < MOMENTUM_SIGMA:
                continue

            # ── Condition 2: Trend direction stable (no reversals) ──
            window_rates = rates.iloc[-MOMENTUM_WINDOW:].values
            diffs = [window_rates[i+1] - window_rates[i]
                     for i in range(len(window_rates) - 1)]

            # All diffs same sign = monotonic trend
            all_up   = all(d > 0 for d in diffs)
            all_down = all(d < 0 for d in diffs)
            trend_stable = all_up or all_down

            if not trend_stable:
                continue

            trend_direction = 'UP' if all_up else 'DOWN'

            # ── Condition 3: Volume/liquidity spike ──
            impact_mean = impacts.mean()
            impact_std  = impacts.std()
            current_impact = abs(float(latest_quote.get('price_impact_pct', 0)))

            if impact_std > 0:
                impact_z = (current_impact - impact_mean) / impact_std
                volume_spike = impact_z >= MOMENTUM_IMPACT_SIGMA
            else:
                # Zero std means flat impact — no spike detectable
                volume_spike = False

            if not volume_spike:
                continue

            # ── All 3 conditions passed — score the signal ──
            score_result = score_conditions(
                latest_quote, price_data, recent_quotes,
                conn, 'momentum_breakout',
                min_profit_pct=MIN_PROFIT_MOMENTUM
            )
            total_score = score_result['total_score']

            # Discard negative profit
            if score_result['estimated_profit_pct'] <= 0:
                continue

            if total_score >= ANALYSIS_THRESHOLD:
                estimated_profit = score_result['estimated_profit_pct']
                description = (
                    f"{pair} momentum breakout | "
                    f"z={z_score:.2f}σ | trend={trend_direction} "
                    f"({MOMENTUM_WINDOW} quotes) | "
                    f"impact spike={current_impact:.4f}% "
                    f"({impact_z:.2f}σ above mean)"
                )
                signal = {
                    'timestamp':            datetime.utcnow().isoformat(),
                    'signal_type':          'momentum_breakout',
                    'pair':                 pair,
                    'description':          description,
                    'estimated_profit_pct': estimated_profit,
                    'weighted_score':       total_score,
                    'execute_candidate':    False,
                    'condition_breakdown':  score_result['breakdown_str'],
                    'z_score':              round(z_score, 4),
                    'trend_direction':      trend_direction,
                    'impact_z':             round(impact_z, 4),
                }
                logger.info(
                    f"📈 Momentum breakout: {pair} | "
                    f"z={z_score:.2f}σ | {trend_direction} "
                    f"| impact {impact_z:.2f}σ | Score: {total_score}"
                )
                signals.append(signal)

        except Exception as e:
            logger.error(f"Error in momentum detection for {pair}: {e}")
            continue

    return signals


def _get_quote_legs_for_signal(conn, signal: dict) -> list:
    """
    Fetch the most recent quote leg(s) for a signal's pair(s).
    For triangular signals, fetches each leg in the path.
    For direct signals, fetches the single pair.
    """
    signal_type = signal.get('signal_type', '')
    pair = signal.get('pair', '')
    legs = []

    try:
        if 'triangular' in signal_type:
            # Parse path: TOKEN_A->TOKEN_B->TOKEN_C->TOKEN_A
            tokens = pair.replace('->', '/').split('/')
            if len(tokens) >= 3:
                leg_pairs = [
                    f'{tokens[0]}/{tokens[1]}',
                    f'{tokens[1]}/{tokens[2]}',
                ]
                for leg_pair in leg_pairs:
                    q = get_latest_quote(conn, leg_pair)
                    if q:
                        legs.append(q)
        else:
            # Direct pair
            q = get_latest_quote(conn, pair)
            if q:
                legs.append(q)
    except Exception as e:
        logger.error(f'Error fetching quote legs for {pair}: {e}')

    return legs


async def run_detection():
    """
    Main entry point. Runs all three detection strategies,
    logs signals to database, and sends Telegram alerts.
    Called after each fetch cycle or run independently.
    """
    conn = get_connection()
    if conn is None:
        return

    ensure_signals_table(conn)

    logger.info("🔍 Running arbitrage detection...")

    all_signals = []

    # Run all three strategies
    all_signals.extend(detect_rate_divergence(conn))
    all_signals.extend(detect_triangular_arbitrage(conn))
    all_signals.extend(detect_impact_anomaly(conn))
    all_signals.extend(detect_momentum_breakout(conn))

    if not all_signals:
        logger.info("No signals detected this cycle.")
        conn.close()
        return

    # Validate, log and alert
    for signal in all_signals:
        # Build quote_legs list from DB for this signal
        quote_legs = _get_quote_legs_for_signal(conn, signal)

        # Run 5-stage validation pipeline
        signal = enrich_signal_with_validation(signal, quote_legs)

        log_signal(conn, signal)

        # Telegram only for execute candidates — score >= 75 AND validator EXECUTE
        if (signal.get('weighted_score', 0) >= EXECUTE_THRESHOLD
                and signal.get('execute_candidate', False)):
            await send_telegram_alert(signal)
        else:
            logger.info(
                f"📵 No alert: {signal['pair']} | "
                f"Score {signal['weighted_score']}pts | "
                f"Validation: {signal.get('validation').recommendation if signal.get('validation') else 'N/A'}"
            )

    conn.close()

    execute_count  = sum(1 for s in all_signals
                         if s.get('execute_candidate', False))
    analysis_count = sum(1 for s in all_signals
                         if ANALYSIS_THRESHOLD <= s['weighted_score'] < EXECUTE_THRESHOLD)

    logger.info(f"✅ Detection complete: {len(all_signals)} signals | "
                f"{execute_count} execute candidates | "
                f"{analysis_count} need analysis")


# =============================================================================
# STANDALONE ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║         JUPITER ARBITRAGE DETECTION ENGINE                  ║
    ║         Weighted Scoring System v1.0                        ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    print(f"  Execute threshold:       {EXECUTE_THRESHOLD}/100 pts")
    print(f"  Analysis threshold:      {ANALYSIS_THRESHOLD}/100 pts  (>50% gate)")
    print(f"  Min profit (divergence): {MIN_PROFIT_RATE_DIVERGENCE}%  [MEV buffer: direct 2-step]")
    print(f"  Min profit (triangular): {MIN_PROFIT_TRIANGULAR}%   [MEV buffer: multi-hop]")
    print(f"  Min profit (impact):     {MIN_PROFIT_IMPACT_ANOMALY}%  [MEV buffer: liquidity shift]")
    print(f"  Max slippage:            {MAX_SLIPPAGE_PCT}%  [MEV sweet spot]")
    print(f"  Max price impact:        {MAX_PRICE_IMPACT_PCT}%")
    print(f"  Min liquidity:           ${MIN_LIQUIDITY_USD:,}")
    print(f"  Duplicate window:        {DUPLICATE_WINDOW_MIN} minutes")
    print(f"  Divergence sigma:        {DIVERGENCE_SIGMA}σ")
    print(f"  Momentum sigma:          {MOMENTUM_SIGMA}σ")
    print(f"  Momentum window:         {MOMENTUM_WINDOW} consecutive quotes")
    print(f"  Momentum impact spike:   > {MOMENTUM_IMPACT_SIGMA}σ above mean")
    print(f"  Min profit (momentum):   {MIN_PROFIT_MOMENTUM}%  [strictest]")
    print(f"  Rolling window:          {ROLLING_WINDOW} quotes")
    print()

    telegram_status = "✅ Configured" if TELEGRAM_BOT_TOKEN else "⚠️  Not configured"
    print(f"  Telegram:           {telegram_status}")
    print()

    asyncio.run(run_detection())
