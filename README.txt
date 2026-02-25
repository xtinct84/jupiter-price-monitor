================================================================================
        SOLANA DEX PRICE MONITORING BOT - Jupiter API v2/v3
================================================================================

OVERVIEW
--------
A real-time price and swap quote monitoring bot for Solana-based tokens, built
on the Jupiter Aggregator API. This bot continuously fetches token prices and
swap quotes at configurable intervals, aggregates the data into structured
Excel files, and maintains a rolling 24-hour history for downstream analysis.

The collected data is designed for use with AI-assisted trend detection and
arbitrage pair opportunity analysis via the Claude API (Anthropic).

--------------------------------------------------------------------------------

KEY FEATURES
------------
- Real-time token price monitoring via Jupiter Price API v3
- Swap quote monitoring via Jupiter Swap API v1
- Configurable polling interval (default: 30 seconds)
- Supports batch multi-token price fetching in a single API call
- Aggregates rolling 24-hour history (up to 2,880 data points per token)
- Exports structured Excel (.xlsx) output with one sheet per token/pair
- Generates daily summary reports and consolidated history files
- Human-readable price and volume formatting
- Graceful shutdown with automatic data export on Ctrl+C
- Secure API key management via .env file

--------------------------------------------------------------------------------

PROJECT STRUCTURE
-----------------
  run_monitor.py       - Entry point. Configures and launches the monitor.
  price_monitor.py     - Core monitoring loop. Orchestrates fetching and storage.
  jupiter_api.py       - Primary Jupiter API client (Price v3 + Quote v1).
  jupiter_client.py    - Legacy Jupiter API client (retained for compatibility).
  token_registry.py    - Token definitions, mint addresses, decimals, categories.
  data_exporter.py     - Excel export logic for prices, quotes, and summaries.
  requirements.txt     - Python package dependencies.
  .env                 - API credentials (NOT included in repository).

--------------------------------------------------------------------------------

MONITORED TOKENS
----------------
  High Volume:
    SOL   - Solana
    USDC  - USD Coin
    USDT  - Tether USD
    JUP   - Jupiter

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

--------------------------------------------------------------------------------

REQUIREMENTS
------------
  Python 3.8 or higher

  Python Packages (see requirements.txt):
    httpx>=0.25.0
    pandas>=2.0.0
    openpyxl>=3.1.0
    python-dotenv>=1.0.0

  API Access:
    A valid Jupiter API key from https://station.jup.ag/

--------------------------------------------------------------------------------

INSTALLATION
------------
  1. Clone or download the project files into a local directory.

  2. Install dependencies:

       pip install -r requirements.txt

  3. Create a .env file in the project root directory:

       JUPITER_API_KEY=your_actual_api_key_here
       MONITOR_INTERVAL_SECONDS=30
       MONITOR_DURATION_MINUTES=60

     IMPORTANT: Never commit your .env file to version control.
     Add .env to your .gitignore file.

--------------------------------------------------------------------------------

CONFIGURATION
-------------
  All runtime configuration is managed through environment variables in .env:

  JUPITER_API_KEY            (required) Your Jupiter API subscription key.
  MONITOR_INTERVAL_SECONDS   (optional) Polling interval in seconds. Default: 30
  MONITOR_DURATION_MINUTES   (optional) Total run duration in minutes. Default: 60
                             Set to 0 for indefinite monitoring.

--------------------------------------------------------------------------------

USAGE
-----
  Run the monitor from your terminal:

       python run_monitor.py

  The program will:
    1. Validate your API key configuration.
    2. Display the token and trading pair list.
    3. Prompt for confirmation before beginning.
    4. Begin continuous price and quote fetching at the configured interval.
    5. Print live updates to the terminal for each iteration.
    6. On completion or Ctrl+C, automatically export all collected data to Excel.

--------------------------------------------------------------------------------

OUTPUT FILES
------------
  All output files are saved to the price_history/ directory, created
  automatically on first run.

  PRICE_HISTORY_CONSOLIDATED_YYYYMMDD.xlsx
    - One sheet per token.
    - Contains full rolling 24-hour price history.
    - Columns include timestamp, human-readable price, USD price, and
      API metadata (confidence score, 24h change, provider, etc.).

  QUOTE_HISTORY_CONSOLIDATED_YYYYMMDD.xlsx
    - One sheet per trading pair.
    - Contains swap quote history including in/out amounts, price impact,
      slippage, and route information.

  DAILY_SUMMARY_YYYYMMDD.xlsx
    - Single sheet with the most recent price snapshot for all monitored tokens.
    - Useful for quick reference and end-of-session reporting.

--------------------------------------------------------------------------------

DATA PIPELINE OVERVIEW
----------------------
  Jupiter Price API v3
         |
         v
  price_monitor.py  <--  token_registry.py (mint addresses + decimals)
         |
         v
  data_exporter.py  (aggregation + rolling 24hr storage)
         |
         v
  .xlsx output files  (price_history/ directory)
         |
         v
  Claude API (Anthropic)  -- AI-assisted arbitrage trend detection

--------------------------------------------------------------------------------

SECURITY NOTES
--------------
  - API keys are loaded exclusively from the .env file using python-dotenv.
  - No credentials are hardcoded in any .py file.
  - Ensure .env is listed in your .gitignore before pushing to any repository.

  Recommended .gitignore entries:
    .env
    price_history/
    __pycache__/
    *.pyc

--------------------------------------------------------------------------------

KNOWN NOTES & LIMITATIONS
--------------------------
  - jupiter_client.py is retained for backward compatibility. The active client
    is jupiter_api.py, which uses the corrected 'usdPrice' field from the v3
    API response schema.
  - Price history is capped at 1,000 entries in memory per token during a
    session. The exporter maintains up to 2,880 entries (24 hours at 30-second
    intervals) for Excel output.
  - A 0.3 second delay is applied between individual quote requests to respect
    API rate limits.

--------------------------------------------------------------------------------

FUTURE ENHANCEMENTS (PLANNED)
------------------------------
  - Migrate consolidated storage from .xlsx to SQLite or PostgreSQL database
  - Real-time dashboard visualization using Streamlit
  - Backtesting module for historical arbitrage signal validation
  - Automated alerting for threshold-crossing arbitrage opportunities
  - Expanded token registry with dynamic discovery

--------------------------------------------------------------------------------

DEPENDENCIES & ATTRIBUTION
---------------------------
  Jupiter Aggregator API     https://station.jup.ag/
  httpx                      https://www.python-httpx.org/
  pandas                     https://pandas.pydata.org/
  openpyxl                   https://openpyxl.readthedocs.io/
  python-dotenv              https://pypi.org/project/python-dotenv/

--------------------------------------------------------------------------------

AUTHOR
------
  Joe Edward Luevano
  El Paso, Texas
  joeluevano@gmail.com

================================================================================
