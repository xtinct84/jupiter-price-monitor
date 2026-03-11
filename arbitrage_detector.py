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
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

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
ANALYSIS_THRESHOLD = 40   # Score >= this → request LLM analysis
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
MIN_PROFIT_PCT        = 0.05     # Minimum estimated profit % (filters below 0.049%)
MAX_PRICE_IMPACT_PCT  = 1.0      # Maximum acceptable price impact %
MIN_LIQUIDITY_USD     = 500_000  # Minimum liquidity in USD
MAX_SLIPPAGE_PCT      = 1.0      # Maximum acceptable slippage %
DUPLICATE_WINDOW_MIN  = 5        # Minutes before same signal can re-fire
DIVERGENCE_SIGMA      = 2.0      # Standard deviations from rolling mean
ROLLING_WINDOW        = 20       # Number of recent quotes for rolling stats

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
    """Return True if an identical signal fired within the duplicate window"""
    window_start = (datetime.now() - timedelta(minutes=DUPLICATE_WINDOW_MIN)
                    ).strftime('%Y-%m-%d %H:%M:%S')
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


def log_signal(conn, signal: dict):
    """Insert a detected signal into the signals table"""
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
            1 if signal['weighted_score'] >= EXECUTE_THRESHOLD else 0,
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
    """Convert raw amounts to effective swap rate"""
    try:
        in_dec  = TOKEN_DECIMALS.get(in_symbol, 6)
        out_dec = TOKEN_DECIMALS.get(out_symbol, 6)
        in_amt  = in_amount  / (10 ** in_dec)
        out_amt = out_amount / (10 ** out_dec)
        return out_amt / in_amt if in_amt > 0 else 0.0
    except Exception:
        return 0.0


def score_conditions(quote: dict, price_data: dict,
                     recent_quotes: pd.DataFrame,
                     conn, signal_type: str) -> dict:
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
        recent_quotes['rate'] = (
            (recent_quotes['out_amount'] / (10 ** out_dec)) /
            (recent_quotes['in_amount']  / (10 ** in_dec))
        )
        current_rate = calculate_effective_rate(
            quote['in_amount'], quote['out_amount'], in_sym, out_sym
        )
        mean_rate = recent_quotes['rate'].mean()
        if mean_rate > 0:
            estimated_profit_pct = ((current_rate - mean_rate) / mean_rate) * 100

    passed_profit = estimated_profit_pct >= MIN_PROFIT_PCT
    points = WEIGHTS['profit'] if passed_profit else 0
    total_score += points
    breakdown['profit'] = {
        'points': points,
        'max': WEIGHTS['profit'],
        'passed': passed_profit,
        'value': f"{estimated_profit_pct:.4f}%",
        'threshold': f">= {MIN_PROFIT_PCT}%"
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
        quote = get_latest_quote(conn, pair)
        if not quote:
            continue

        price_data = get_latest_price(conn, in_sym)
        recent_quotes = get_recent_quotes(conn, pair)

        if recent_quotes.empty or len(recent_quotes) < 3:
            continue

        score_result = score_conditions(
            quote, price_data, recent_quotes, conn, 'rate_divergence'
        )
        total_score = score_result['total_score']

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
            conn, 'triangular_arbitrage'
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
            quote, price_data, recent_quotes, conn, 'impact_anomaly'
        )
        total_score = score_result['total_score']

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

    if score >= EXECUTE_THRESHOLD:
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

    if not all_signals:
        logger.info("No signals detected this cycle.")
        conn.close()
        return

    # Log and alert
    for signal in all_signals:
        log_signal(conn, signal)
        await send_telegram_alert(signal)

    conn.close()

    execute_count  = sum(1 for s in all_signals
                         if s['weighted_score'] >= EXECUTE_THRESHOLD)
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
    print(f"  Execute threshold:  {EXECUTE_THRESHOLD}/100 pts")
    print(f"  Analysis threshold: {ANALYSIS_THRESHOLD}/100 pts")
    print(f"  Min profit:         {MIN_PROFIT_PCT}%")
    print(f"  Max price impact:   {MAX_PRICE_IMPACT_PCT}%")
    print(f"  Min liquidity:      ${MIN_LIQUIDITY_USD:,}")
    print(f"  Duplicate window:   {DUPLICATE_WINDOW_MIN} minutes")
    print(f"  Divergence sigma:   {DIVERGENCE_SIGMA}σ")
    print(f"  Rolling window:     {ROLLING_WINDOW} quotes")
    print()

    telegram_status = "✅ Configured" if TELEGRAM_BOT_TOKEN else "⚠️  Not configured"
    print(f"  Telegram:           {telegram_status}")
    print()

    asyncio.run(run_detection())
