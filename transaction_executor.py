"""
transaction_executor.py
═══════════════════════════════════════════════════════════════════════
Retry logic with exponential backoff and escalating priority fees
for Solana transaction execution.

Architecture:
  - Standalone utility module — called by OpenClaw agent when ready
  - Does NOT execute transactions yet (devnet testing phase pending)
  - Provides the retry scaffolding with fee escalation logic
  - Priority fees double per retry, capped at 'desperate' tier

Dependency chain:
  execution_validator.py  →  transaction_executor.py  →  Jupiter Swap API
                                                       →  Phantom Wallet (v2.3)

Usage (future):
    from transaction_executor import execute_with_retry, TransactionResult
    result = await execute_with_retry(tx_builder, signal, capital_usd=50.0)
═══════════════════════════════════════════════════════════════════════
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# RETRY CONFIGURATION
# ─────────────────────────────────────────────

MAX_RETRIES          = 3       # Maximum transaction attempts before giving up
BASE_BACKOFF_MS      = 100     # Initial wait between retries (milliseconds)
BACKOFF_MULTIPLIER   = 2       # Exponential base: wait doubles each retry

# Priority fee escalation per retry (SOL amounts)
# Attempt 0 → base tier (from .env PRIORITY_FEE_TIER)
# Attempt 1 → one tier up
# Attempt 2 → two tiers up (capped at desperate)
PRIORITY_FEE_SOL = {
    'low':       0.0001,   # ~$0.01 at $85/SOL
    'medium':    0.0005,   # ~$0.05
    'high':      0.001,    # ~$0.15
    'desperate': 0.005,    # ~$0.75 — only on final retry
}

PRIORITY_TIER_ORDER = ['low', 'medium', 'high', 'desperate']


# ─────────────────────────────────────────────
# RESULT DATACLASS
# ─────────────────────────────────────────────

@dataclass
class TransactionResult:
    """
    Outcome of an execute_with_retry call.

    Fields:
        success:          True if transaction confirmed on-chain
        signature:        Solana transaction signature (if success)
        attempts:         Number of attempts made
        final_fee_tier:   Priority fee tier used on successful attempt
        final_fee_sol:    Actual SOL paid in priority fee
        failure_reason:   Error message if all attempts failed
        timestamps:       ISO timestamps for each attempt
    """
    success:          bool  = False
    signature:        str   = ""
    attempts:         int   = 0
    final_fee_tier:   str   = ""
    final_fee_sol:    float = 0.0
    failure_reason:   str   = ""
    timestamps:       list  = field(default_factory=list)

    def summary(self) -> str:
        if self.success:
            return (
                f"✅ TX confirmed | sig: {self.signature[:16]}... | "
                f"attempts: {self.attempts} | "
                f"fee tier: {self.final_fee_tier} "
                f"({self.final_fee_sol:.4f} SOL)"
            )
        return (
            f"❌ TX failed after {self.attempts} attempts | "
            f"reason: {self.failure_reason}"
        )


# ─────────────────────────────────────────────
# FEE ESCALATION
# ─────────────────────────────────────────────

def get_escalated_fee_tier(base_tier: str, attempt: int) -> tuple:
    """
    Return the priority fee tier and SOL amount for a given retry attempt.

    Escalates one tier per retry, capped at 'desperate'.

    Args:
        base_tier: Starting tier from .env PRIORITY_FEE_TIER
        attempt:   Zero-indexed attempt number (0 = first try)

    Returns:
        (tier_name: str, fee_sol: float)

    Example:
        base='medium', attempt=0 → ('medium', 0.0005)
        base='medium', attempt=1 → ('high',   0.001)
        base='medium', attempt=2 → ('desperate', 0.005)
    """
    try:
        base_idx = PRIORITY_TIER_ORDER.index(base_tier)
    except ValueError:
        base_idx = PRIORITY_TIER_ORDER.index('medium')

    escalated_idx = min(base_idx + attempt, len(PRIORITY_TIER_ORDER) - 1)
    tier = PRIORITY_TIER_ORDER[escalated_idx]
    return tier, PRIORITY_FEE_SOL[tier]


# ─────────────────────────────────────────────
# CORE RETRY FUNCTION
# ─────────────────────────────────────────────

async def execute_with_retry(
    tx_builder: Callable,
    signal: dict,
    capital_usd: float = 50.0,
    base_priority_tier: str = 'medium',
    max_retries: int = MAX_RETRIES,
) -> TransactionResult:
    """
    Execute a transaction with exponential backoff and escalating priority fees.

    On each failed attempt:
      - Priority fee escalates one tier (medium → high → desperate)
      - Wait time doubles: 100ms → 200ms → 400ms
      - Logs each attempt with fee tier and failure reason

    Args:
        tx_builder:         Async callable that builds and sends the transaction.
                            Called with (priority_fee_sol: float) → tx_signature: str
        signal:             Signal dict from arbitrage_detector (for logging context)
        capital_usd:        Trade size in USD (for fee context logging)
        base_priority_tier: Starting fee tier — escalates on retries
        max_retries:        Maximum attempts before giving up

    Returns:
        TransactionResult dataclass with outcome details

    Example (future OpenClaw integration):
        async def my_tx_builder(priority_fee_sol):
            return await jupiter_swap_api.send(
                signal=signal,
                priority_fee_sol=priority_fee_sol,
                slippage_bps=signal['dynamic_slippage_bps']
            )
        result = await execute_with_retry(my_tx_builder, signal)
    """
    outcome = TransactionResult()
    pair    = signal.get('pair', 'UNKNOWN')

    for attempt in range(max_retries):
        outcome.attempts = attempt + 1
        tier, fee_sol = get_escalated_fee_tier(base_priority_tier, attempt)
        outcome.timestamps.append(datetime.utcnow().isoformat())

        logger.info(
            f"  TX attempt {attempt + 1}/{max_retries}: {pair} | "
            f"priority tier={tier} ({fee_sol:.4f} SOL)"
        )

        try:
            signature = await tx_builder(priority_fee_sol=fee_sol)

            outcome.success        = True
            outcome.signature      = signature
            outcome.final_fee_tier = tier
            outcome.final_fee_sol  = fee_sol

            logger.info(
                f"  ✅ TX confirmed on attempt {attempt + 1}: "
                f"{signature[:16]}... | fee={fee_sol:.4f} SOL"
            )
            return outcome

        except Exception as e:
            failure_msg = str(e)
            logger.warning(
                f"  ⚠️  TX attempt {attempt + 1} failed: {failure_msg} | "
                f"fee={fee_sol:.4f} SOL"
            )

            if attempt == max_retries - 1:
                # Final attempt exhausted
                outcome.failure_reason = failure_msg
                outcome.final_fee_tier = tier
                outcome.final_fee_sol  = fee_sol
                logger.error(
                    f"  ❌ All {max_retries} attempts failed for {pair}. "
                    f"Last error: {failure_msg}"
                )
                return outcome

            # Exponential backoff before next attempt
            wait_ms = BASE_BACKOFF_MS * (BACKOFF_MULTIPLIER ** attempt)
            logger.info(f"  ⏳ Waiting {wait_ms}ms before retry...")
            await asyncio.sleep(wait_ms / 1000)

    return outcome


# ─────────────────────────────────────────────
# STANDALONE TEST
# ─────────────────────────────────────────────

if __name__ == '__main__':
    import os

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║         TRANSACTION EXECUTOR — Retry + Fee Escalation       ║
    ║         Scaffolding for OpenClaw / Jupiter Swap API          ║
    ╚══════════════════════════════════════════════════════════════╝
    """)

    print("  Fee escalation table:")
    for base in ['low', 'medium', 'high']:
        row = []
        for attempt in range(3):
            tier, sol = get_escalated_fee_tier(base, attempt)
            row.append(f"attempt {attempt}: {tier} ({sol:.4f} SOL)")
        print(f"    base={base:<10} | {' → '.join(row)}")

    print()
    print("  Backoff schedule:")
    for attempt in range(MAX_RETRIES):
        wait_ms = BASE_BACKOFF_MS * (BACKOFF_MULTIPLIER ** attempt)
        print(f"    After attempt {attempt + 1}: wait {wait_ms}ms")

    print()
    print("  TEST 1 — Simulated success on attempt 1:")

    async def mock_success(priority_fee_sol):
        return "5KtP9kq3mNJxxxxSIMULATEDSIGNATURE"

    test_signal = {
        'pair': 'SOL/USDC',
        'signal_type': 'rate_divergence',
        'estimated_profit_pct': 2.45,
        'dynamic_slippage_bps': 104,
    }

    result = asyncio.run(execute_with_retry(mock_success, test_signal))
    print(f"  Result: {result.summary()}")

    print()
    print("  TEST 2 — Simulated failure with 2 retries then success:")
    call_count = {'n': 0}

    async def mock_fail_twice(priority_fee_sol):
        call_count['n'] += 1
        if call_count['n'] < 3:
            raise Exception("Transaction simulation failed: blockhash expired")
        return "9xzP2kq3mNJxxxxFINALSUCCESSIGNATURE"

    result2 = asyncio.run(execute_with_retry(
        mock_fail_twice, test_signal, base_priority_tier='medium'
    ))
    print(f"  Result: {result2.summary()}")

    print()
    print("  TEST 3 — All retries exhausted:")

    async def mock_all_fail(priority_fee_sol):
        raise Exception("Network congestion: unable to land transaction")

    result3 = asyncio.run(execute_with_retry(mock_all_fail, test_signal))
    print(f"  Result: {result3.summary()}")

    print()
    print("  Status: Scaffolding ready. Awaiting OpenClaw + Phantom wallet integration (v2.3)")
