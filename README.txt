================================================================================
     SOLANA DEX PRICE MONITORING & ARBITRAGE DETECTION SYSTEM
                     Jupiter API v2/v3 — v2.0
================================================================================

OVERVIEW
--------
A fully automated real-time price monitoring, arbitrage detection, and alert
system for Solana-based tokens built on the Jupiter Aggregator API. The system
continuously fetches token prices and swap quotes at configurable intervals,
stores all data in a SQLite database, detects arbitrage opportunities using a
weighted scoring engine, delivers alerts via Telegram, and visualizes everything
through a live Streamlit dashboard.

Designed as the data infrastructure and signal detection layer for a planned
autonomous trading agent using OpenClaw + Phantom wallet integration.

--------------------------------------------------------------------------------

KEY FEATURES
------------
  Data Pipeline
  - Real-time token price monitoring via Jupiter Price API v3
  - Swap quote monitoring via Jupiter Swap API v1
  - Configurable polling interval (default: 30 seconds)
  - Batch multi-token price fetching in a single API call
  - Persistent SQLite database storage with rolling 24-hour history
  - Excel (.xlsx) export retained for backward compatibility
  - Secure API credential management via .env file

  Arbitrage Detection Engine
  - Three detection strategies: rate divergence, triangular arbitrage,
    price impact anomaly
  - Weighted scoring system (100 points total) across six conditions
  - Signal classification: Execute Candidate (>=75pts) / Needs Analysis (>=40pts)
  - Configurable thresholds and weights for tuning sensitivity
  - Duplicate signal suppression within configurable time window
  - Signals logged persistently to SQLite for backtesting

  Telegram Alerts
  - Instant push notifications for all qualifying signals
  - Execute candidates and analysis-required signals differentiated clearly
  - Full condition breakdown included in each alert
  - Configured via TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env

  Streamlit Dashboard (localhost:8501)
  - Section 1: Database status bar (record counts, last update, data range)
  - Section 2: Live token price table with 24hr change and liquidity
  - Section 3: Price history line chart with token selector and time window
  - Section 4: Live swap quotes table with effective rates and price impact
  - Section 5: Quote history line chart with pair selector
  - Section 6: Signals panel with filtering, detail inspector, score history

--------------------------------------------------------------------------------

PROJECT STRUCTURE
-----------------
  run_monitor.py          Entry point. Configures and launches the monitor.
  price_monitor.py        Core monitoring loop. Fetches, stores, and triggers
                          detection after every cycle.
  jupiter_api.py          Primary Jupiter API client (Price v3 + Quote v1).
  jupiter_client.py       Legacy Jupiter API client (retained for compatibility).
  token_registry.py       Token definitions, mint addresses, decimals, categories.
  data_exporter.py        SQLite write/read methods and Excel export logic.
  arbitrage_detector.py   Weighted scoring detection engine and Telegram alerts.
  dashboard.py            Streamlit dashboard - 6-section live visualization.
  requirements.txt        Python package dependencies.
  .env                    API credentials (NOT included in repository).
  .env.example            Template showing required environment variables.

--------------------------------------------------------------------------------

MONITORED TOKENS
----------------
  High Volume:
    SOL    - Solana
    USDC   - USD Coin
    USDT   - Tether USD
    JUP    - Jupiter

  Mid Volume:
    RAY    - Raydium
    BONK   - Bonk
    JTO    - Jito
    PYTH   - Pyth Network
    WIF    - dogwifhat
    POPCAT - Popcat
    MOUTAI - Moutai
    MYRO   - Myro
    WEN    - WEN

MONITORED TRADING PAIRS
-----------------------
    SOL/USDC  |  JUP/USDC  |  RAY/USDC
    BONK/SOL  |  JTO/USDC  |  PYTH/USDC  |  WIF/SOL

TRIANGULAR ARBITRAGE PATHS
--------------------------
    SOL  -> USDC -> USDT -> SOL
    SOL  -> USDC -> JUP  -> SOL
    JUP  -> USDC -> SOL  -> JUP
    BONK -> SOL  -> USDC -> BONK
    WIF  -> SOL  -> USDC -> WIF

--------------------------------------------------------------------------------

REQUIREMENTS
------------
  Python 3.8 or higher

  Python Packages (see requirements.txt):
    httpx>=0.25.0
    pandas>=2.0.0
    openpyxl>=3.1.0
    python-dotenv>=1.0.0
    streamlit>=1.55.0
    numpy>=1.24.0

  External Services:
    Jupiter API key      https://station.jup.ag/
    Telegram Bot Token   https://t.me/BotFather
    Telegram Chat ID     Retrieved via Telegram API getUpdates endpoint

--------------------------------------------------------------------------------

INSTALLATION
------------
  1. Clone the repository:

       git clone https://github.com/xtinct84/jupiter-price-monitor.git
       cd jupiter-price-monitor

  2. Create and activate a virtual environment:

       python -m venv venv
       .\venv\Scripts\Activate.ps1   (Windows)
       source venv/bin/activate       (Mac/Linux)

  3. Install dependencies:

       pip install -r requirements.txt

  4. Copy .env.example to .env and fill in your credentials:

       JUPITER_API_KEY=your_jupiter_api_key_here
       TELEGRAM_BOT_TOKEN=your_telegram_bot_token_here
       TELEGRAM_CHAT_ID=your_telegram_chat_id_here
       MONITOR_INTERVAL_SECONDS=30
       MONITOR_DURATION_MINUTES=60

     IMPORTANT: Never commit your .env file to version control.

--------------------------------------------------------------------------------

CONFIGURATION
-------------
  All runtime configuration is managed through environment variables in .env:

  JUPITER_API_KEY            (required) Jupiter API subscription key.
  TELEGRAM_BOT_TOKEN         (required) Telegram bot token from @BotFather.
  TELEGRAM_CHAT_ID           (required) Your Telegram chat ID.
  MONITOR_INTERVAL_SECONDS   (optional) Polling interval in seconds. Default: 30
  MONITOR_DURATION_MINUTES   (optional) Run duration in minutes. Default: 60
                             Set to 0 for indefinite monitoring.

  Detection thresholds are configurable at the top of arbitrage_detector.py:

  EXECUTE_THRESHOLD    Score >= this triggers execute candidate alert. Default: 75
  ANALYSIS_THRESHOLD   Score >= this triggers analysis alert. Default: 40
  MIN_PROFIT_PCT       Minimum estimated profit to score. Default: 0.05%
  MAX_PRICE_IMPACT_PCT Maximum acceptable price impact. Default: 1.0%
  MIN_LIQUIDITY_USD    Minimum token liquidity required. Default: $500,000
  MAX_SLIPPAGE_PCT     Maximum acceptable slippage. Default: 1.0%
  DUPLICATE_WINDOW_MIN Minutes before same signal can re-fire. Default: 5
  DIVERGENCE_SIGMA     Standard deviations for divergence condition. Default: 2.0
  ROLLING_WINDOW       Number of recent quotes for rolling stats. Default: 20

--------------------------------------------------------------------------------

USAGE
-----
  The system runs as two simultaneous processes in separate terminals.
  Always activate the same virtual environment in both terminals first:

       .\venv\Scripts\Activate.ps1

  Terminal 1 - Start the monitor bot:

       python run_monitor.py

  Terminal 2 - Start the Streamlit dashboard:

       python -m streamlit run dashboard.py

  The dashboard will open automatically at http://localhost:8501

  To run the arbitrage detector independently (for testing or backtesting):

       python arbitrage_detector.py

  The detector also runs automatically after every fetch cycle when the
  monitor bot is running - no separate process required during normal operation.

--------------------------------------------------------------------------------

DATABASE SCHEMA
---------------
  All data is stored in price_history/jupiter_monitor.db (SQLite).
  The database is created automatically on first run.

  prices table
    id, timestamp, symbol, token_id, price_usd, price_change_24h,
    liquidity, created_at, block_id, decimals

  quotes table
    id, timestamp, pair, input_symbol, output_symbol, input_mint,
    output_mint, in_amount, out_amount, price_impact_pct, slippage_bps

  signals table
    id, timestamp, signal_type, pair, description, estimated_profit_pct,
    weighted_score, execute_candidate, condition_breakdown, resolved

--------------------------------------------------------------------------------

DETECTION ENGINE - WEIGHTED SCORING SYSTEM
-------------------------------------------
  Each detected signal is scored across six conditions. Signals scoring
  >= EXECUTE_THRESHOLD are flagged as execute candidates. Signals scoring
  >= ANALYSIS_THRESHOLD are flagged for manual analysis. All others discarded.

  Condition          Weight   Pass Threshold
  ------------------------------------------
  Profit               45pts  >= 0.05% estimated profit
  Price Impact         20pts  <= 1.0% price impact
  Liquidity            15pts  >= $500,000 available liquidity
  Slippage             10pts  <= 1.0% slippage
  No Duplicate          5pts  No identical signal within 5 minutes
  Divergence            5pts  >= 2.0 standard deviations from rolling mean
  ------------------------------------------
  TOTAL               100pts
  Execute Threshold    75pts  -> Auto-execute candidate (manual review for now)
  Analysis Threshold   40pts  -> Request deeper analysis
  Discard             <40pts  -> Silently discarded

  Detection Strategies:
  1. Rate Divergence    - Current effective rate vs rolling average
  2. Triangular Arb     - A->B->C->A combined return calculation
  3. Impact Anomaly     - Price impact spike above rolling mean

--------------------------------------------------------------------------------

TELEGRAM SETUP
--------------
  1. Open Telegram and search for @BotFather
  2. Send /newbot and follow the prompts to create your bot
  3. Copy the bot token provided by BotFather
  4. Send any message to your new bot
  5. Open in browser (replace YOUR_TOKEN):
       https://api.telegram.org/botYOUR_TOKEN/getUpdates
  6. Find "chat": {"id": YOUR_CHAT_ID} in the response
  7. Add both values to your .env file

--------------------------------------------------------------------------------

DATA PIPELINE OVERVIEW
----------------------
  Jupiter API v3
        |
        v
  price_monitor.py  <--  token_registry.py
        |
        v
  data_exporter.py
        |
        v
  jupiter_monitor.db (SQLite)
        |                    |
        v                    v
  arbitrage_            dashboard.py
  detector.py           (Streamlit)
  weighted scoring      6-section live UI
  3 strategies          localhost:8501
        |
        v
  signals table
        |
        v
  Telegram alerts
  (execute candidate / needs analysis)
        |
        v
  [PLANNED] OpenClaw Agent
  [PLANNED] Phantom Wallet
  [PLANNED] Jupiter Swap Execution

--------------------------------------------------------------------------------

SECURITY NOTES
--------------
  - All API credentials loaded exclusively from .env via python-dotenv
  - No credentials hardcoded in any .py file
  - .env listed in .gitignore - never committed to version control
  - price_history/ directory excluded from git - no database or Excel files
    are pushed to the repository

  Recommended .gitignore entries:
    .env
    price_history/
    __pycache__/
    *.pyc

--------------------------------------------------------------------------------

KNOWN NOTES & LIMITATIONS
--------------------------
  - jupiter_client.py retained for backward compatibility. Active client is
    jupiter_api.py using the corrected 'usdPrice' field from v3 API schema.
  - Price history capped at 1,000 entries in memory per token per session.
    Exporter maintains up to 2,880 entries (24 hours at 30-second intervals).
  - 0.3 second delay applied between quote requests to respect rate limits.
  - Triangular arbitrage C->A rate is inferred from price table rather than
    a live quote - this is an approximation and will be improved in v3.
  - Detection scoring requires minimum 3 quote records per pair before
    rolling statistics can be calculated.
  - Wallet execution layer (Phantom + Jupiter Swap API) not yet implemented.
    Execute candidate signals require manual review before any trade action.

--------------------------------------------------------------------------------

PLANNED ROADMAP
---------------
  v2.1  Signals panel backtesting analysis tools
  v2.2  OpenClaw agent integration - signal reading and analysis
  v2.3  Phantom wallet connection - devnet testing
  v2.4  Jupiter Swap API execution layer - devnet validation
  v2.5  Live trading with weighted gate - mainnet
  v3.0  Polymarket monitoring bot (EVM / MetaMask - separate project)

--------------------------------------------------------------------------------

DEPENDENCIES & ATTRIBUTION
---------------------------
  Jupiter Aggregator API     https://station.jup.ag/
  httpx                      https://www.python-httpx.org/
  pandas                     https://pandas.pydata.org/
  openpyxl                   https://openpyxl.readthedocs.io/
  python-dotenv              https://pypi.org/project/python-dotenv/
  streamlit                  https://streamlit.io/
  numpy                      https://numpy.org/
  sqlite3                    Python standard library
  Telegram Bot API           https://core.telegram.org/bots/api

--------------------------------------------------------------------------------

AUTHOR
------
  Joe Edward Luevano
  El Paso, Texas
  joeluevano@gmail.com
  GitHub: https://github.com/xtinct84/jupiter-price-monitor

================================================================================
