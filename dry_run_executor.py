"""
dry_run_executor.py
══════════════════════════════════════════════════════════════════════
Paper trading simulator — validates execution logic against live market
data without signing or submitting any transactions.

What it does:
  1. On a validated EXECUTE signal, calls Jupiter Quote API to get a
     live entry quote at execution time (not the stored detector quote)
  2. Computes exact fees using the live Jito tip floor
  3. Records simulated entry: price, route, expected output, fee burden
  4. Schedules an exit check N minutes later to capture the actual
     price outcome, computing realized vs. estimated P&L
  5. Persists all results to dry_run_trades table in the existing DB
  6. Compares validator's slippage estimate vs. actual Jupiter slippage

Key questions it answers:
  - Does the validator's slippage estimate match Jupiter's actual quote?
  - Does the estimated profit materialize as a real price move?
  - Would the trade have been profitable net of fees?
  - How long does the opportunity persist after detection?

Activation:
  Set DRY_RUN=true in .env to enable. When active, execute candidates
  route through dry_run_executor instead of the real tx builder.
  The existing execute_with_retry() scaffold in transaction_executor.py
  passes the dry_run_builder as its tx_builder callable.

Usage:
  from dry_run_executor import DryRunExecutor
  executor = DryRunExecutor()
  result = await executor.simulate(signal, validation_result)

DB table:  dry_run_trades  (auto-created alongside existing tables)
Log file:  price_history/dry_run_log.jsonl  (one JSON record per trade)
══════════════════════════════════════════════════════════════════════
"""

import asyncio
import json
import logging
import os
import sqlite3
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

DRY_RUN_ENABLED      = os.getenv('DRY_RUN', 'false').strip().lower() == 'true'
DB_PATH              = Path(os.getenv('DB_PATH', 'price_history/jupiter_monitor.db'))
DRY_RUN_LOG_PATH     = Path('price_history/dry_run_log.jsonl')
TRADE_CAPITAL_USD    = float(os.getenv('TRADE_CAPITAL_USD', '10.0'))

# Exit price check horizons (minutes after entry)
EXIT_HORIZONS_MIN    = [5, 15, 30]

# Token mint addresses (mirrors token_registry.py)
TOKEN_MINTS = {
    'SOL':  'So11111111111111111111111111111111111111112',
    'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
    'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
    'JUP':  'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN',
    'RAY':  '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R',
    'JTO':  'jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL',
    'PYTH': 'HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3',
    'WIF':  'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm',
}

TOKEN_DECIMALS = {
    'SOL': 9, 'USDC': 6, 'USDT': 6, 'JUP': 6,
    'RAY': 6, 'JTO': 9, 'PYTH': 6, 'WIF': 6,
}


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class DryRunResult:
    """
    Complete record of a simulated trade.

    Entry fields are populated immediately when the signal fires.
    Exit fields are populated at each horizon check (5m, 15m, 30m).
    Slippage deviation measures how accurate the validator's estimate was.
    """
    # Identity
    trade_id:               str   = ""
    signal_id:              int   = 0
    signal_type:            str   = ""
    pair:                   str   = ""
    timestamp_entry:        str   = ""

    # Signal context
    estimated_profit_pct:   float = 0.0
    validator_slippage_pct: float = 0.0   # what execution_validator estimated
    validator_net_pct:      float = 0.0   # validator's net profit estimate
    dynamic_slippage_bps:   int   = 0     # slippage bps set by validator

    # Live entry quote (from Jupiter at execution time)
    entry_price_usd:        float = 0.0   # base token price at entry
    entry_in_amount:        int   = 0     # lamports/units in
    entry_out_amount:       int   = 0     # lamports/units out
    entry_actual_rate:      float = 0.0   # effective rate from live quote
    entry_price_impact_pct: float = 0.0   # actual price impact from Jupiter
    entry_route_hops:       int   = 0     # number of route hops
    entry_quote_latency_ms: float = 0.0   # time to get live quote (ms)

    # Fee breakdown
    capital_usd:            float = 0.0
    platform_fee_pct:       float = 0.0
    solana_base_fee_usd:    float = 0.0
    jito_tip_usd:           float = 0.0
    jito_tip_source:        str   = ""    # 'live' | 'fallback' | 'cache'
    total_fee_pct:          float = 0.0
    priority_tier:          str   = ""

    # Slippage accuracy
    slippage_deviation_pct: float = 0.0   # actual - estimated (positive = worse than expected)

    # Exit outcomes (populated by schedule_exit_checks)
    exit_price_5m:          Optional[float] = None
    exit_price_15m:         Optional[float] = None
    exit_price_30m:         Optional[float] = None
    move_5m_pct:            Optional[float] = None
    move_15m_pct:           Optional[float] = None
    move_30m_pct:           Optional[float] = None
    realized_pnl_5m_pct:    Optional[float] = None   # move - fees
    realized_pnl_15m_pct:   Optional[float] = None
    realized_pnl_30m_pct:   Optional[float] = None
    would_profit_5m:        Optional[bool]  = None
    would_profit_15m:       Optional[bool]  = None
    would_profit_30m:       Optional[bool]  = None

    # Verdict
    status:                 str   = "pending"  # pending | complete | failed
    failure_reason:         str   = ""

    def summary(self) -> str:
        if self.status == "failed":
            return f"❌ DRY RUN FAILED [{self.pair}]: {self.failure_reason}"
        lines = [
            f"📋 DRY RUN [{self.pair}] | {self.signal_type}",
            f"   Entry:     ${self.entry_price_usd:.6f} | impact {self.entry_price_impact_pct:.4f}%"
            f" | {self.entry_route_hops} hops | quote {self.entry_quote_latency_ms:.0f}ms",
            f"   Fees:      {self.total_fee_pct:.4f}% total"
            f" (platform + base + Jito {self.jito_tip_source} ${self.jito_tip_usd:.5f})",
            f"   Slippage:  estimated {self.validator_slippage_pct:.4f}%"
            f" | actual {self.entry_price_impact_pct:.4f}%"
            f" | deviation {self.slippage_deviation_pct:+.4f}%",
        ]
        for horizon, move, pnl, profit in [
            (5,  self.move_5m_pct,  self.realized_pnl_5m_pct,  self.would_profit_5m),
            (15, self.move_15m_pct, self.realized_pnl_15m_pct, self.would_profit_15m),
            (30, self.move_30m_pct, self.realized_pnl_30m_pct, self.would_profit_30m),
        ]:
            if move is not None:
                icon = "✅" if profit else "❌"
                lines.append(
                    f"   @{horizon:2d}m:     move {move:+.4f}%"
                    f" | net P&L {pnl:+.4f}%  {icon}"
                )
        return "\n".join(lines)


# ── DB schema ─────────────────────────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS dry_run_trades (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id                TEXT NOT NULL,
    signal_id               INTEGER,
    signal_type             TEXT,
    pair                    TEXT,
    timestamp_entry         TEXT,
    estimated_profit_pct    REAL,
    validator_slippage_pct  REAL,
    validator_net_pct       REAL,
    dynamic_slippage_bps    INTEGER,
    entry_price_usd         REAL,
    entry_in_amount         INTEGER,
    entry_out_amount        INTEGER,
    entry_actual_rate       REAL,
    entry_price_impact_pct  REAL,
    entry_route_hops        INTEGER,
    entry_quote_latency_ms  REAL,
    capital_usd             REAL,
    platform_fee_pct        REAL,
    solana_base_fee_usd     REAL,
    jito_tip_usd            REAL,
    jito_tip_source         TEXT,
    total_fee_pct           REAL,
    priority_tier           TEXT,
    slippage_deviation_pct  REAL,
    exit_price_5m           REAL,
    exit_price_15m          REAL,
    exit_price_30m          REAL,
    move_5m_pct             REAL,
    move_15m_pct            REAL,
    move_30m_pct            REAL,
    realized_pnl_5m_pct     REAL,
    realized_pnl_15m_pct    REAL,
    realized_pnl_30m_pct    REAL,
    would_profit_5m         INTEGER,
    would_profit_15m        INTEGER,
    would_profit_30m        INTEGER,
    status                  TEXT DEFAULT 'pending',
    failure_reason          TEXT
)
"""

CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_dry_run_timestamp
ON dry_run_trades (timestamp_entry)
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_db_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute(CREATE_TABLE_SQL)
    conn.execute(CREATE_INDEX_SQL)
    conn.commit()
    return conn


def _upsert_trade(result: DryRunResult) -> None:
    """Write or update a dry run result in the DB."""
    conn = _get_db_conn()
    d = asdict(result)
    # Remove non-column keys
    d.pop('trade_id', None)
    cols   = ', '.join(d.keys())
    placeholders = ', '.join(['?'] * len(d))
    try:
        conn.execute(
            f"INSERT OR REPLACE INTO dry_run_trades (trade_id, {cols}) "
            f"VALUES (?, {placeholders})",
            [result.trade_id] + list(d.values())
        )
        conn.commit()
    finally:
        conn.close()


def _log_jsonl(result: DryRunResult) -> None:
    """Append a JSON record to the dry run log file."""
    DRY_RUN_LOG_PATH.parent.mkdir(exist_ok=True)
    with open(DRY_RUN_LOG_PATH, 'a') as f:
        f.write(json.dumps(asdict(result)) + '\n')


def _get_exit_price_from_db(symbol: str, target_dt: datetime) -> Optional[float]:
    """
    Look up the closest price record in the DB within ±2 minutes of target_dt.
    Falls back to None if no record found.
    """
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()
        window_start = (target_dt - timedelta(minutes=2)).strftime('%Y-%m-%d %H:%M:%S')
        window_end   = (target_dt + timedelta(minutes=2)).strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute("""
            SELECT price_usd FROM prices
            WHERE symbol = ?
              AND timestamp BETWEEN ? AND ?
            ORDER BY ABS(
                strftime('%s', timestamp) -
                strftime('%s', ?)
            ) ASC
            LIMIT 1
        """, (symbol, window_start, window_end,
              target_dt.strftime('%Y-%m-%d %H:%M:%S')))
        row = cursor.fetchone()
        conn.close()
        return float(row[0]) if row else None
    except Exception as e:
        logger.warning(f"DB exit price lookup failed for {symbol}: {e}")
        return None


def _extract_base_symbol(pair: str) -> str:
    """Extract base token symbol from pair string (handles / and → formats)."""
    return pair.split('/')[0].split('→')[0].strip()


def _build_in_amount(symbol: str, capital_usd: float, token_price_usd: float) -> int:
    """
    Convert USD capital to token units in smallest denomination (lamports/units).
    Uses live token price to determine how many units to request.
    """
    decimals    = TOKEN_DECIMALS.get(symbol, 6)
    token_units = capital_usd / token_price_usd
    return int(token_units * (10 ** decimals))


# ── Main executor class ───────────────────────────────────────────────────────

class DryRunExecutor:
    """
    Paper trading executor — simulates trade execution using live Jupiter
    quotes without signing or submitting transactions.

    Lifecycle per signal:
      1. simulate()       — fetch live entry quote, record entry state
      2. schedule_exit_checks() — wait N minutes, lookup DB price, compute P&L
      3. Results written to dry_run_trades table and dry_run_log.jsonl
    """

    def __init__(self):
        from jupiter_api import JupiterAPI
        from execution_validator import (
            PRIORITY_FEE_DEFAULT, TRADE_CAPITAL_USD as EV_CAPITAL,
            JUPITER_PLATFORM_FEE_PCT, SOLANA_TX_FEE_USD,
            fetch_jito_tip_floor, get_live_tip_usd,
        )
        self.api               = JupiterAPI()
        self.priority_tier     = PRIORITY_FEE_DEFAULT
        self.capital_usd       = TRADE_CAPITAL_USD
        self.platform_fee_pct  = JUPITER_PLATFORM_FEE_PCT
        self.solana_fee_usd    = SOLANA_TX_FEE_USD
        self._fetch_tips       = fetch_jito_tip_floor
        self._get_tip_usd      = get_live_tip_usd

        # Ensure DB table exists
        conn = _get_db_conn()
        conn.close()

        logger.info(
            f"✅ DryRunExecutor initialized | capital=${self.capital_usd:.2f} "
            f"| tier={self.priority_tier} | DB={DB_PATH}"
        )

    async def simulate(
        self,
        signal: dict,
        validation_result=None,
    ) -> DryRunResult:
        """
        Core simulation entry point. Called when an EXECUTE candidate fires.

        Args:
            signal:            Signal dict from arbitrage_detector
            validation_result: ValidationResult from execution_validator (optional)
                               Used to compare estimated vs. actual slippage/fees.

        Returns:
            DryRunResult with entry fields populated.
            Exit fields populated asynchronously via schedule_exit_checks().
        """
        import time as _time

        pair        = signal.get('pair', '')
        signal_type = signal.get('signal_type', '')
        base_sym    = _extract_base_symbol(pair)
        entry_ts    = datetime.utcnow()
        trade_id    = f"DR_{entry_ts.strftime('%Y%m%d_%H%M%S')}_{pair.replace('/', '_').replace('→', '_')}"

        result = DryRunResult(
            trade_id             = trade_id,
            signal_id            = signal.get('id', 0),
            signal_type          = signal_type,
            pair                 = pair,
            timestamp_entry      = entry_ts.strftime('%Y-%m-%d %H:%M:%S'),
            estimated_profit_pct = signal.get('estimated_profit_pct', 0.0),
            capital_usd          = self.capital_usd,
            priority_tier        = self.priority_tier,
        )

        # Pull validator estimates if provided
        if validation_result:
            result.validator_slippage_pct = getattr(validation_result, 'slippage_pct', 0.0)
            result.validator_net_pct      = getattr(validation_result, 'net_profit_pct', 0.0)
            result.dynamic_slippage_bps   = getattr(validation_result, 'dynamic_slippage_bps', 0)

        # ── Live entry quote ─────────────────────────────────────────────────
        input_mint  = TOKEN_MINTS.get(base_sym)
        # Output mint: use USDC for SOL-quoted pairs, else SOL
        output_sym  = 'USDC' if pair.endswith('/USDC') or 'USDC' in pair else 'SOL'
        output_mint = TOKEN_MINTS.get(output_sym)

        if not input_mint or not output_mint:
            result.status         = 'failed'
            result.failure_reason = f"Unknown token in pair '{pair}' — no mint address"
            logger.error(f"DRY RUN skipped: {result.failure_reason}")
            _upsert_trade(result)
            return result

        # Get live entry price to compute in_amount
        try:
            price_data   = await self.api.get_price(input_mint)
            entry_price  = float(price_data['price_usd']) if price_data else 0.0
        except Exception:
            entry_price  = 0.0

        if entry_price <= 0:
            result.status         = 'failed'
            result.failure_reason = f"Could not fetch live price for {base_sym}"
            logger.error(f"DRY RUN skipped: {result.failure_reason}")
            _upsert_trade(result)
            return result

        in_amount = _build_in_amount(base_sym, self.capital_usd, entry_price)

        # Fetch live Jupiter quote using dynamic_slippage_bps from validator
        slippage_bps = result.dynamic_slippage_bps if result.dynamic_slippage_bps > 0 else 50
        t0 = _time.perf_counter()
        try:
            quote = await self.api.get_quote(
                input_mint  = input_mint,
                output_mint = output_mint,
                amount      = in_amount,
                slippage_bps = slippage_bps,
            )
        except Exception as e:
            result.status         = 'failed'
            result.failure_reason = f"Jupiter quote failed: {e}"
            logger.error(f"DRY RUN failed for {pair}: {result.failure_reason}")
            _upsert_trade(result)
            return result

        latency_ms = (_time.perf_counter() - t0) * 1000

        if not quote:
            result.status         = 'failed'
            result.failure_reason = "Jupiter returned no quote"
            _upsert_trade(result)
            return result

        # Decode quote
        in_dec  = TOKEN_DECIMALS.get(base_sym, 6)
        out_dec = TOKEN_DECIMALS.get(output_sym, 6)
        actual_in  = quote['in_amount']  / (10 ** in_dec)
        actual_out = quote['out_amount'] / (10 ** out_dec)
        actual_rate = actual_out / actual_in if actual_in > 0 else 0.0
        route_hops  = len(quote.get('route_plan', [])) or 1

        result.entry_price_usd        = entry_price
        result.entry_in_amount        = quote['in_amount']
        result.entry_out_amount       = quote['out_amount']
        result.entry_actual_rate      = actual_rate
        result.entry_price_impact_pct = abs(quote.get('price_impact_pct', 0.0))
        result.entry_route_hops       = route_hops
        result.entry_quote_latency_ms = latency_ms

        # ── Live fee calculation ──────────────────────────────────────────────
        sol_price  = entry_price if base_sym == 'SOL' else float(os.getenv('SOL_PRICE_USD', '90.0'))
        tip_data   = self._fetch_tips(sol_price)
        jito_usd   = self._get_tip_usd(self.priority_tier, sol_price)
        num_legs   = 3 if '→' in pair else 2

        platform_pct = self.platform_fee_pct * num_legs
        base_fee_pct = (self.solana_fee_usd / self.capital_usd) * 100
        jito_pct     = (jito_usd / self.capital_usd) * 100 if self.capital_usd > 0 else 0

        result.platform_fee_pct   = platform_pct
        result.solana_base_fee_usd = self.solana_fee_usd
        result.jito_tip_usd       = jito_usd
        result.jito_tip_source    = tip_data.get('source', 'fallback')
        result.total_fee_pct      = platform_pct + base_fee_pct + jito_pct

        # ── Slippage deviation ────────────────────────────────────────────────
        # Positive = actual slippage was worse than estimated
        result.slippage_deviation_pct = (
            result.entry_price_impact_pct - result.validator_slippage_pct
        )

        result.status = 'pending'

        logger.info(
            f"📋 DRY RUN ENTRY [{pair}] | "
            f"price=${entry_price:.4f} | impact={result.entry_price_impact_pct:.4f}% | "
            f"fees={result.total_fee_pct:.4f}% | slip_dev={result.slippage_deviation_pct:+.4f}% | "
            f"quote={latency_ms:.0f}ms | tip_src={result.jito_tip_source}"
        )

        _upsert_trade(result)
        _log_jsonl(result)

        # Schedule exit checks in background — non-blocking
        asyncio.create_task(self._schedule_exit_checks(result))

        return result

    async def _schedule_exit_checks(self, result: DryRunResult) -> None:
        """
        Wait N minutes after entry, then look up exit price from the DB
        prices table and compute realized P&L for each horizon.

        Uses DB prices rather than another API call — the monitor is already
        writing prices every 30 seconds so the data will be there.
        """
        entry_dt   = datetime.strptime(result.timestamp_entry, '%Y-%m-%d %H:%M:%S')
        base_sym   = _extract_base_symbol(result.pair)
        prev_horizon = 0

        for horizon_min in EXIT_HORIZONS_MIN:
            # Wait the incremental gap (5m, then 10 more for 15m, then 15 more for 30m)
            wait_secs = (horizon_min - prev_horizon) * 60
            await asyncio.sleep(wait_secs)
            prev_horizon = horizon_min

            target_dt  = entry_dt + timedelta(minutes=horizon_min)
            exit_price = _get_exit_price_from_db(base_sym, target_dt)

            if exit_price is None:
                logger.debug(
                    f"DRY RUN [{result.pair}] @{horizon_min}m: "
                    f"no price found near {target_dt.strftime('%H:%M:%S')}"
                )
                continue

            move_pct = ((exit_price - result.entry_price_usd)
                        / result.entry_price_usd * 100
                        if result.entry_price_usd > 0 else None)
            pnl_pct  = (move_pct - result.total_fee_pct) if move_pct is not None else None
            profit   = pnl_pct > 0 if pnl_pct is not None else None

            if horizon_min == 5:
                result.exit_price_5m        = exit_price
                result.move_5m_pct          = round(move_pct, 4) if move_pct is not None else None
                result.realized_pnl_5m_pct  = round(pnl_pct,  4) if pnl_pct  is not None else None
                result.would_profit_5m      = profit
            elif horizon_min == 15:
                result.exit_price_15m       = exit_price
                result.move_15m_pct         = round(move_pct, 4) if move_pct is not None else None
                result.realized_pnl_15m_pct = round(pnl_pct,  4) if pnl_pct  is not None else None
                result.would_profit_15m     = profit
            elif horizon_min == 30:
                result.exit_price_30m       = exit_price
                result.move_30m_pct         = round(move_pct, 4) if move_pct is not None else None
                result.realized_pnl_30m_pct = round(pnl_pct,  4) if pnl_pct  is not None else None
                result.would_profit_30m     = profit

            icon = "✅" if profit else "❌"
            logger.info(
                f"📋 DRY RUN EXIT [{result.pair}] @{horizon_min}m {icon} | "
                f"exit=${exit_price:.4f} | move={move_pct:+.4f}% | "
                f"net P&L={pnl_pct:+.4f}%"
            )

        result.status = 'complete'
        _upsert_trade(result)
        _log_jsonl(result)
        logger.info(f"\n{result.summary()}\n")


# ── Integration shim for transaction_executor.py ──────────────────────────────

def make_dry_run_builder(
    signal: dict,
    validation_result=None,
    executor: Optional[DryRunExecutor] = None,
) -> callable:
    """
    Returns an async callable compatible with execute_with_retry()'s
    tx_builder interface. When DRY_RUN=true, pass this as tx_builder.

    Usage in arbitrage_detector or OpenClaw agent:
        if DRY_RUN_ENABLED:
            builder = make_dry_run_builder(signal, validation_result)
            result  = await execute_with_retry(builder, signal)
        else:
            builder = real_jupiter_swap_builder  # v2.3
            result  = await execute_with_retry(builder, signal)

    The fake signature returned lets TransactionResult.success = True
    so the retry loop exits cleanly, with the real data in DryRunResult.
    """
    _executor = executor or DryRunExecutor()

    async def dry_run_builder(priority_fee_sol: float) -> str:
        dry_result = await _executor.simulate(signal, validation_result)
        if dry_result.status == 'failed':
            raise Exception(f"Dry run failed: {dry_result.failure_reason}")
        # Return a clearly-fake signature so it's never confused with real txs
        return f"DRY_RUN_{dry_result.trade_id}"

    return dry_run_builder


# ── Standalone summary query ──────────────────────────────────────────────────

def print_dry_run_summary(db_path: str = None) -> None:
    """
    Print a summary of all dry run trades from the DB.
    Run standalone: python dry_run_executor.py
    """
    path = db_path or str(DB_PATH)
    try:
        if not Path(path).exists():
            print(f"""
╔══════════════════════════════════════════════════════╗
║              DRY RUN TRADE SUMMARY                   ║
╚══════════════════════════════════════════════════════╝
  Status: No database found at {path}
  The monitor has not run yet — start it with:
      python run_monitor.py
  With DRY_RUN=true in .env, trades will be recorded
  here once the first EXECUTE candidate fires.
═══════════════════════════════════════════════════════""")
            return

        conn   = sqlite3.connect(path)
        cursor = conn.cursor()

        # Create table if it doesn't exist yet (first run before any trades)
        cursor.execute(CREATE_TABLE_SQL)
        cursor.execute(CREATE_INDEX_SQL)
        conn.commit()

        cursor.execute("SELECT COUNT(*) FROM dry_run_trades")
        total = cursor.fetchone()[0]
        if total == 0:
            print(f"""
╔══════════════════════════════════════════════════════╗
║              DRY RUN TRADE SUMMARY                   ║
╚══════════════════════════════════════════════════════╝
  Status: No dry run trades recorded yet.

  DRY_RUN is {'ENABLED' if DRY_RUN_ENABLED else 'DISABLED'} in your .env.
  {'Trades will be recorded when the next EXECUTE candidate fires.' if DRY_RUN_ENABLED else 'Set DRY_RUN=true in .env to enable paper trading.'}

  DB path: {path}
═══════════════════════════════════════════════════════""")
            conn.close()
            return

        print(f"""
╔══════════════════════════════════════════════════════╗
║              DRY RUN TRADE SUMMARY                   ║
╚══════════════════════════════════════════════════════╝
  Total trades recorded : {total}
""")

        # Overall stats
        cursor.execute("""
            SELECT
                COUNT(*) as n,
                AVG(estimated_profit_pct)    as avg_est,
                AVG(total_fee_pct)           as avg_fees,
                AVG(slippage_deviation_pct)  as avg_slip_dev,
                AVG(entry_quote_latency_ms)  as avg_latency,
                AVG(entry_price_impact_pct)  as avg_impact,
                SUM(CASE WHEN would_profit_5m  = 1 THEN 1 ELSE 0 END) as prof_5m,
                SUM(CASE WHEN would_profit_15m = 1 THEN 1 ELSE 0 END) as prof_15m,
                SUM(CASE WHEN would_profit_30m = 1 THEN 1 ELSE 0 END) as prof_30m,
                COUNT(move_5m_pct)  as n_5m,
                COUNT(move_15m_pct) as n_15m,
                COUNT(move_30m_pct) as n_30m
            FROM dry_run_trades
            WHERE status != 'failed'
        """)
        r = cursor.fetchone()
        if r and r[0]:
            n, avg_est, avg_fees, avg_dev, avg_lat, avg_imp = r[:6]
            p5, p15, p30, n5, n15, n30 = r[6:]
            print(f"  Avg estimated profit : {avg_est:.4f}%")
            print(f"  Avg total fees       : {avg_fees:.4f}%")
            print(f"  Avg slip deviation   : {avg_dev:+.4f}% (+ = worse than estimated)")
            print(f"  Avg quote latency    : {avg_lat:.0f}ms")
            print(f"  Avg price impact     : {avg_imp:.4f}%")
            print(f"  Would profit @5m     : {p5}/{n5} = {p5/n5*100:.1f}%" if n5 else "  @5m : no data")
            print(f"  Would profit @15m    : {p15}/{n15} = {p15/n15*100:.1f}%" if n15 else "  @15m: no data")
            print(f"  Would profit @30m    : {p30}/{n30} = {p30/n30*100:.1f}%" if n30 else "  @30m: no data")

        # Per-pair breakdown
        cursor.execute("""
            SELECT pair, COUNT(*),
                   AVG(estimated_profit_pct),
                   AVG(slippage_deviation_pct),
                   SUM(CASE WHEN would_profit_5m=1 THEN 1 ELSE 0 END),
                   COUNT(would_profit_5m)
            FROM dry_run_trades
            WHERE status != 'failed'
            GROUP BY pair ORDER BY COUNT(*) DESC
        """)
        rows = cursor.fetchall()
        if rows:
            print("\n  Per-pair results:")
            print(f"  {'Pair':<28} {'n':>4}  {'avg_est':>8}  {'slip_dev':>9}  {'@5m':>6}")
            for row in rows:
                pair, n, est, dev, p5, n5 = row
                acc = f"{p5}/{n5}={p5/n5*100:.0f}%" if n5 else "—"
                print(f"  {pair:<28} {n:>4}  {est:>7.4f}%  {dev:>+8.4f}%  {acc:>6}")

        # Slippage accuracy check
        cursor.execute("""
            SELECT
                AVG(ABS(slippage_deviation_pct)) as mae,
                MAX(ABS(slippage_deviation_pct)) as max_dev
            FROM dry_run_trades WHERE status != 'failed'
        """)
        r2 = cursor.fetchone()
        if r2 and r2[0]:
            print(f"\n  Slippage model accuracy:")
            print(f"    Mean absolute error : {r2[0]:.4f}%")
            print(f"    Max deviation       : {r2[1]:.4f}%")
            if r2[0] > 0.3:
                print(f"    ⚠️  MAE > 0.3% — consider recalibrating slippage estimate")
            else:
                print(f"    ✅ Slippage model within acceptable range")

        conn.close()
        print("═" * 54)

    except Exception as e:
        print(f"Error reading dry run summary: {e}")


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    print_dry_run_summary()
