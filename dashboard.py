import streamlit as st
import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

DB_PATH = Path("price_history/jupiter_monitor.db")

st.set_page_config(
    page_title="Jupiter DEX Monitor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─────────────────────────────────────────────
# DATABASE HELPERS
# ─────────────────────────────────────────────

def get_connection():
    """Open a read-only connection to the SQLite database"""
    if not DB_PATH.exists():
        return None
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def get_db_stats(conn) -> dict:
    """Return record counts and data range from the database"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM prices")
        price_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM quotes")
        quote_count = cursor.fetchone()[0]
        cursor.execute("SELECT MAX(timestamp) FROM prices")
        last_update = cursor.fetchone()[0]
        cursor.execute("SELECT MIN(timestamp) FROM prices")
        first_record = cursor.fetchone()[0]
        return {
            "price_records": price_count,
            "quote_records": quote_count,
            "last_update": last_update,
            "first_record": first_record
        }
    except Exception as e:
        return {}


def get_latest_prices(conn) -> pd.DataFrame:
    """Fetch the most recent price record for each token"""
    query = """
        SELECT p.symbol, p.price_usd, p.price_change_24h,
               p.liquidity, p.timestamp
        FROM prices p
        INNER JOIN (
            SELECT symbol, MAX(timestamp) as max_ts
            FROM prices
            GROUP BY symbol
        ) latest ON p.symbol = latest.symbol
                 AND p.timestamp = latest.max_ts
        ORDER BY p.symbol
    """
    try:
        return pd.read_sql_query(query, conn)
    except Exception:
        return pd.DataFrame()


def get_price_history(conn, symbol: str, hours: int = 24) -> pd.DataFrame:
    """Fetch price history for a token over the last N hours"""
    query = """
        SELECT timestamp, price_usd, price_change_24h, confidence
        FROM prices
        WHERE symbol = ?
        AND timestamp >= datetime('now', ? || ' hours')
        ORDER BY timestamp ASC
    """
    try:
        df = pd.read_sql_query(query, conn, params=(symbol, f"-{hours}"))
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    except Exception:
        return pd.DataFrame()


def get_latest_quotes(conn) -> pd.DataFrame:
    """Fetch the most recent quote for each trading pair"""
    query = """
        SELECT q.pair, q.input_symbol, q.output_symbol,
               q.in_amount, q.out_amount,
               q.price_impact_pct, q.slippage_bps, q.timestamp
        FROM quotes q
        INNER JOIN (
            SELECT pair, MAX(timestamp) as max_ts
            FROM quotes
            GROUP BY pair
        ) latest ON q.pair = latest.pair
                 AND q.timestamp = latest.max_ts
        ORDER BY q.pair
    """
    try:
        return pd.read_sql_query(query, conn)
    except Exception:
        return pd.DataFrame()


def get_quote_history(conn, pair: str, hours: int = 24) -> pd.DataFrame:
    """Fetch quote history for a trading pair over the last N hours"""
    query = """
        SELECT timestamp, in_amount, out_amount, price_impact_pct
        FROM quotes
        WHERE pair = ?
        AND timestamp >= datetime('now', ? || ' hours')
        ORDER BY timestamp ASC
    """
    try:
        df = pd.read_sql_query(query, conn, params=(pair, f"-{hours}"))
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    except Exception:
        return pd.DataFrame()


def get_all_symbols(conn) -> list:
    """Return list of all token symbols in the database"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT symbol FROM prices ORDER BY symbol")
        return [row[0] for row in cursor.fetchall()]
    except Exception:
        return []


def get_all_pairs(conn) -> list:
    """Return list of all trading pairs in the database"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT pair FROM quotes ORDER BY pair")
        return [row[0] for row in cursor.fetchall()]
    except Exception:
        return []


def get_recent_signals(conn, limit: int = 50) -> pd.DataFrame:
    """Fetch the most recent signals from the signals table"""
    query = """
        SELECT timestamp, signal_type, pair, estimated_profit_pct,
               weighted_score, execute_candidate, description,
               condition_breakdown, resolved
        FROM signals
        ORDER BY timestamp DESC
        LIMIT ?
    """
    try:
        df = pd.read_sql_query(query, conn, params=(limit,))
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    except Exception:
        return pd.DataFrame()


def get_signal_stats(conn) -> dict:
    """Return summary statistics from the signals table"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM signals")
        total = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM signals WHERE execute_candidate = 1")
        execute_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM signals WHERE execute_candidate = 0")
        analysis_count = cursor.fetchone()[0]
        cursor.execute("SELECT MAX(estimated_profit_pct) FROM signals")
        max_profit = cursor.fetchone()[0] or 0
        cursor.execute("SELECT MAX(timestamp) FROM signals")
        last_signal = cursor.fetchone()[0]
        return {
            'total': total,
            'execute_candidates': execute_count,
            'needs_analysis': analysis_count,
            'max_profit_pct': max_profit,
            'last_signal': last_signal
        }
    except Exception:
        return {}

# ─────────────────────────────────────────────
# FORMATTING HELPERS
# ─────────────────────────────────────────────

def humanize_price(price: float) -> str:
    if price >= 1000:
        return f"${price:,.2f}"
    elif price >= 1:
        return f"${price:.4f}"
    elif price >= 0.01:
        return f"${price:.6f}"
    else:
        return f"${price:.8f}"


def format_change(change: float) -> str:
    if change > 0:
        return f"▲ +{change:.2f}%"
    elif change < 0:
        return f"▼ {change:.2f}%"
    else:
        return f"— {change:.2f}%"


TOKEN_DECIMALS = {
    'SOL': 9, 'USDC': 6, 'USDT': 6, 'JUP': 6,
    'RAY': 6, 'BONK': 5, 'JTO': 9, 'PYTH': 6,
    'WIF': 6, 'POPCAT': 9, 'MOUTAI': 9, 'MYRO': 9, 'WEN': 5
}


def effective_rate(row) -> float:
    """Calculate effective swap rate from raw in/out amounts"""
    try:
        in_dec = TOKEN_DECIMALS.get(row['input_symbol'], 6)
        out_dec = TOKEN_DECIMALS.get(row['output_symbol'], 6)
        in_amt = row['in_amount'] / (10 ** in_dec)
        out_amt = row['out_amount'] / (10 ** out_dec)
        return out_amt / in_amt if in_amt > 0 else 0
    except Exception:
        return 0

# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────

st.sidebar.title("⚙️ Dashboard Controls")
st.sidebar.markdown("---")

auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=True)
if auto_refresh:
    st.sidebar.caption("Dashboard refreshes every 30 seconds")

hours_filter = st.sidebar.slider(
    "History window (hours)",
    min_value=1,
    max_value=24,
    value=6,
    step=1
)

st.sidebar.markdown("---")
st.sidebar.markdown("**Database**")
st.sidebar.caption(f"`{DB_PATH}`")
st.sidebar.markdown("---")
st.sidebar.markdown("**Jupiter DEX Monitor**")
st.sidebar.caption("Built with Python + Streamlit")
st.sidebar.caption("Data: Jupiter API v3")

# ─────────────────────────────────────────────
# MAIN LAYOUT
# ─────────────────────────────────────────────

st.title("📊 Jupiter DEX Price Monitor")
st.caption(f"Last rendered: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Open DB connection
conn = get_connection()

if conn is None:
    st.error(
        "⚠️ Database not found. Make sure `run_monitor.py` has been started "
        "and the `price_history/jupiter_monitor.db` file exists."
    )
    st.stop()

# ─────────────────────────────────────────────
# SECTION 1 — DATABASE STATUS BAR
# ─────────────────────────────────────────────

stats = get_db_stats(conn)

col1, col2, col3, col4 = st.columns(4)
col1.metric("💾 Price Records", f"{stats.get('price_records', 0):,}")
col2.metric("💱 Quote Records", f"{stats.get('quote_records', 0):,}")
col3.metric("🕐 Last Update", stats.get('last_update', 'N/A'))
col4.metric("📅 Data Since", stats.get('first_record', 'N/A'))

st.markdown("---")

# ─────────────────────────────────────────────
# SECTION 2 — LIVE PRICE TABLE
# ─────────────────────────────────────────────

st.subheader("🪙 Live Token Prices")

prices_df = get_latest_prices(conn)

if prices_df.empty:
    st.warning("No price data available yet. Waiting for first fetch cycle...")
else:
    display_df = prices_df.copy()
    display_df['Price (USD)'] = display_df['price_usd'].apply(humanize_price)
    display_df['24h Change'] = display_df['price_change_24h'].apply(format_change)
    display_df['Last Updated'] = display_df['timestamp']
    display_df['Liquidity'] = display_df['liquidity'].apply(
        lambda x: f"${x:,.0f}" if x > 0 else "N/A"
    )
    display_df = display_df[['symbol', 'Price (USD)', '24h Change', 'Liquidity', 'Last Updated']]
    display_df.columns = ['Token', 'Price', '24h Change', 'Liquidity', 'Last Updated']

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True
    )

st.markdown("---")

# ─────────────────────────────────────────────
# SECTION 3 — PRICE HISTORY CHART
# ─────────────────────────────────────────────

st.subheader("📈 Price History")

all_symbols = get_all_symbols(conn)

if not all_symbols:
    st.warning("No token symbols found in database yet.")
else:
    col_left, col_right = st.columns([1, 3])

    with col_left:
        selected_symbol = st.selectbox(
            "Select Token",
            options=all_symbols,
            index=all_symbols.index('SOL') if 'SOL' in all_symbols else 0
        )

    history_df = get_price_history(conn, selected_symbol, hours=hours_filter)

    with col_right:
        if history_df.empty:
            st.info(f"No history data for {selected_symbol} in the last {hours_filter} hours.")
        else:
            st.line_chart(
                history_df.set_index('timestamp')['price_usd'],
                use_container_width=True
            )
            st.caption(
                f"Showing {len(history_df)} data points for "
                f"{selected_symbol} over the last {hours_filter} hours"
            )

st.markdown("---")

# ─────────────────────────────────────────────
# SECTION 4 — QUOTE MONITOR
# ─────────────────────────────────────────────

st.subheader("💱 Live Swap Quotes")

quotes_df = get_latest_quotes(conn)

if quotes_df.empty:
    st.warning("No quote data available yet. Waiting for first fetch cycle...")
else:
    display_quotes = quotes_df.copy()
    display_quotes['Effective Rate'] = display_quotes.apply(
        lambda row: f"{effective_rate(row):.6f} {row['output_symbol']} per {row['input_symbol']}",
        axis=1
    )
    display_quotes['Price Impact'] = display_quotes['price_impact_pct'].apply(
        lambda x: f"{x:.4f}%"
    )
    display_quotes['Slippage'] = display_quotes['slippage_bps'].apply(
        lambda x: f"{x/100:.2f}%"
    )
    display_quotes['Last Updated'] = display_quotes['timestamp']

    display_quotes = display_quotes[[
        'pair', 'Effective Rate', 'Price Impact', 'Slippage', 'Last Updated'
    ]]
    display_quotes.columns = ['Pair', 'Effective Rate', 'Price Impact', 'Slippage', 'Last Updated']

    st.dataframe(
        display_quotes,
        use_container_width=True,
        hide_index=True
    )

st.markdown("---")

# ─────────────────────────────────────────────
# SECTION 5 — QUOTE HISTORY CHART
# ─────────────────────────────────────────────

st.subheader("📉 Quote History")

all_pairs = get_all_pairs(conn)

if not all_pairs:
    st.warning("No trading pairs found in database yet.")
else:
    col_left2, col_right2 = st.columns([1, 3])

    with col_left2:
        selected_pair = st.selectbox(
            "Select Trading Pair",
            options=all_pairs,
            index=all_pairs.index('SOL/USDC') if 'SOL/USDC' in all_pairs else 0
        )

    quote_hist_df = get_quote_history(conn, selected_pair, hours=hours_filter)

    with col_right2:
        if quote_hist_df.empty:
            st.info(f"No quote history for {selected_pair} in the last {hours_filter} hours.")
        else:
            # Calculate effective rate over time for the chart
            parts = selected_pair.split('/')
            if len(parts) == 2:
                in_sym, out_sym = parts
                in_dec = TOKEN_DECIMALS.get(in_sym, 6)
                out_dec = TOKEN_DECIMALS.get(out_sym, 6)
                quote_hist_df['effective_rate'] = (
                    (quote_hist_df['out_amount'] / (10 ** out_dec)) /
                    (quote_hist_df['in_amount'] / (10 ** in_dec))
                )
                st.line_chart(
                    quote_hist_df.set_index('timestamp')['effective_rate'],
                    use_container_width=True
                )
                st.caption(
                    f"Effective rate: 1 {in_sym} → {out_sym} | "
                    f"{len(quote_hist_df)} data points over last {hours_filter} hours"
                )

st.markdown("---")

# ─────────────────────────────────────────────
# SECTION 6 — SIGNALS PANEL
# ─────────────────────────────────────────────

st.subheader("📡 Arbitrage Signals")

# Check if signals table exists
try:
    signal_stats = get_signal_stats(conn)
    signals_available = True
except Exception:
    signals_available = False

if not signals_available:
    st.info("Signals table not found. Run arbitrage_detector.py to initialize.")
else:
    # Signal summary metrics
    s_col1, s_col2, s_col3, s_col4 = st.columns(4)
    s_col1.metric("📡 Total Signals",      f"{signal_stats.get('total', 0):,}")
    s_col2.metric("🟢 Execute Candidates", f"{signal_stats.get('execute_candidates', 0):,}")
    s_col3.metric("🟡 Needs Analysis",     f"{signal_stats.get('needs_analysis', 0):,}")
    s_col4.metric("📈 Best Profit",        f"{signal_stats.get('max_profit_pct', 0):.4f}%")

    st.markdown("")

    # Filters row
    filter_col1, filter_col2, filter_col3 = st.columns([1, 1, 2])

    with filter_col1:
        signal_filter = st.selectbox(
            "Filter by Classification",
            options=["All", "Execute Candidates", "Needs Analysis"],
            index=0
        )

    with filter_col2:
        type_filter = st.selectbox(
            "Filter by Type",
            options=["All", "rate_divergence", "triangular_arbitrage", "impact_anomaly"],
            index=0
        )

    with filter_col3:
        signal_limit = st.slider(
            "Number of signals to display",
            min_value=10,
            max_value=200,
            value=50,
            step=10
        )

    # Fetch signals
    signals_df = get_recent_signals(conn, limit=signal_limit)

    if signals_df.empty:
        st.warning("No signals detected yet. Run arbitrage_detector.py to begin detection.")
    else:
        # Apply filters
        if signal_filter == "Execute Candidates":
            signals_df = signals_df[signals_df['execute_candidate'] == 1]
        elif signal_filter == "Needs Analysis":
            signals_df = signals_df[signals_df['execute_candidate'] == 0]

        if type_filter != "All":
            signals_df = signals_df[signals_df['signal_type'] == type_filter]

        if signals_df.empty:
            st.info("No signals match the selected filters.")
        else:
            # Format for display
            display_signals = signals_df.copy()

            display_signals['Classification'] = display_signals['execute_candidate'].apply(
                lambda x: "🟢 Execute" if x == 1 else "🟡 Analysis"
            )
            display_signals['Signal Type'] = display_signals['signal_type'].apply(
                lambda x: x.replace('_', ' ').title()
            )
            display_signals['Est. Profit'] = display_signals['estimated_profit_pct'].apply(
                lambda x: f"{x:.4f}%"
            )
            display_signals['Score'] = display_signals['weighted_score'].apply(
                lambda x: f"{x}/100"
            )
            display_signals['Time'] = display_signals['timestamp'].apply(
                lambda x: x.strftime('%Y-%m-%d %H:%M:%S')
            )

            display_signals = display_signals[[
                'Time', 'Classification', 'Signal Type',
                'pair', 'Est. Profit', 'Score'
            ]]
            display_signals.columns = [
                'Time', 'Classification', 'Type',
                'Pair', 'Est. Profit', 'Score'
            ]

            st.dataframe(
                display_signals,
                use_container_width=True,
                hide_index=True
            )

            st.caption(f"Showing {len(display_signals)} signals")

            # Signal detail expander
            st.markdown("")
            with st.expander("🔍 View Signal Details"):
                if not signals_df.empty:
                    selected_idx = st.selectbox(
                        "Select signal to inspect",
                        options=range(len(signals_df)),
                        format_func=lambda i: (
                            f"{signals_df.iloc[i]['timestamp'].strftime('%H:%M:%S')} | "
                            f"{signals_df.iloc[i]['pair']} | "
                            f"Score: {signals_df.iloc[i]['weighted_score']}"
                        )
                    )
                    selected_signal = signals_df.iloc[selected_idx]

                    detail_col1, detail_col2 = st.columns(2)
                    with detail_col1:
                        st.markdown(f"**Pair:** {selected_signal['pair']}")
                        st.markdown(f"**Type:** {selected_signal['signal_type'].replace('_', ' ').title()}")
                        st.markdown(f"**Score:** {selected_signal['weighted_score']}/100")
                        st.markdown(f"**Est. Profit:** {selected_signal['estimated_profit_pct']:.4f}%")
                        st.markdown(f"**Time:** {selected_signal['timestamp']}")

                    with detail_col2:
                        st.markdown("**Description:**")
                        st.info(selected_signal['description'])

                    st.markdown("**Condition Breakdown:**")
                    breakdown_items = selected_signal['condition_breakdown'].split(' | ')
                    for item in breakdown_items:
                        if item.startswith('✓'):
                            st.success(item)
                        else:
                            st.error(item)

    # Signal score history chart
    st.markdown("")
    st.markdown("**Signal Score History**")
    all_signals_df = get_recent_signals(conn, limit=200)
    if not all_signals_df.empty:
        chart_df = all_signals_df[['timestamp', 'weighted_score', 'signal_type']].copy()
        chart_df = chart_df.sort_values('timestamp')
        st.line_chart(
            chart_df.set_index('timestamp')['weighted_score'],
            use_container_width=True
        )
        st.caption("Weighted score trend across all recent signals")

# ─────────────────────────────────────────────
# AUTO REFRESH
# ─────────────────────────────────────────────

if auto_refresh:
    import time
    time.sleep(30)
    st.rerun()
