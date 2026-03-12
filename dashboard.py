import streamlit as st
import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import time

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

TOKEN_DECIMALS = {
    'SOL': 9, 'USDC': 6, 'USDT': 6, 'JUP': 6,
    'RAY': 6, 'BONK': 5, 'JTO': 9, 'PYTH': 6,
    'WIF': 6, 'POPCAT': 9, 'MOUTAI': 9, 'MYRO': 9, 'WEN': 5
}

# ─────────────────────────────────────────────
# DATABASE HELPERS
# ─────────────────────────────────────────────

def get_connection():
    if not DB_PATH.exists():
        return None
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def get_db_stats(conn) -> dict:
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
    except Exception:
        return {}


def get_latest_prices(conn) -> pd.DataFrame:
    query = """
        SELECT p.symbol, p.price_usd, p.price_change_24h,
               p.liquidity, p.timestamp
        FROM prices p
        INNER JOIN (
            SELECT symbol, MAX(timestamp) as max_ts
            FROM prices GROUP BY symbol
        ) latest ON p.symbol = latest.symbol
               AND p.timestamp = latest.max_ts
        ORDER BY p.symbol
    """
    try:
        return pd.read_sql_query(query, conn)
    except Exception:
        return pd.DataFrame()


def get_price_history(conn, symbol: str, hours: int = 24) -> pd.DataFrame:
    # FIX: removed confidence column — not in schema
    query = """
        SELECT timestamp, price_usd, price_change_24h
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
    query = """
        SELECT q.pair, q.input_symbol, q.output_symbol,
               q.in_amount, q.out_amount,
               q.price_impact_pct, q.slippage_bps, q.timestamp
        FROM quotes q
        INNER JOIN (
            SELECT pair, MAX(timestamp) as max_ts
            FROM quotes GROUP BY pair
        ) latest ON q.pair = latest.pair
               AND q.timestamp = latest.max_ts
        ORDER BY q.pair
    """
    try:
        return pd.read_sql_query(query, conn)
    except Exception:
        return pd.DataFrame()


def get_quote_history(conn, pair: str, hours: int = 24) -> pd.DataFrame:
    query = """
        SELECT timestamp, in_amount, out_amount,
               price_impact_pct, input_symbol, output_symbol
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
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT symbol FROM prices ORDER BY symbol")
        return [row[0] for row in cursor.fetchall()]
    except Exception:
        return []


def get_all_pairs(conn) -> list:
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT pair FROM quotes ORDER BY pair")
        return [row[0] for row in cursor.fetchall()]
    except Exception:
        return []


def get_recent_signals(conn, limit: int = 50) -> pd.DataFrame:
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


def get_backtest_data(conn, signal_type_filter: str, hours_back: int) -> pd.DataFrame:
    """
    Join signals with price_history to evaluate signal quality.
    For each signal, fetch the price at signal time and 5/15/30 min after.
    Returns a DataFrame with outcome columns for backtesting analysis.
    """
    type_clause = "" if signal_type_filter == "All" else "AND s.signal_type = ?"
    params = [f"-{hours_back}"]
    if signal_type_filter != "All":
        params.insert(0, signal_type_filter)

    query = f"""
        SELECT s.timestamp, s.signal_type, s.pair,
               s.estimated_profit_pct, s.weighted_score,
               s.execute_candidate, s.description,
               -- Extract the base token symbol from the pair (e.g. SOL from SOL/USDC)
               SUBSTR(s.pair, 1, INSTR(s.pair, '/') - 1) as base_symbol
        FROM signals s
        WHERE s.timestamp >= datetime('now', ? || ' hours')
        {type_clause}
        ORDER BY s.timestamp DESC
    """
    try:
        df = pd.read_sql_query(query, conn, params=params)
        if df.empty:
            return df
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # For each signal, look up price at signal time and forward prices
        outcomes = []
        for _, row in df.iterrows():
            sig_ts  = row['timestamp']
            symbol  = row['base_symbol']

            # Price at signal time (nearest record within ±2 minutes)
            price_query = """
                SELECT price_usd, timestamp FROM prices
                WHERE symbol = ?
                AND timestamp BETWEEN datetime(?, '-2 minutes')
                              AND datetime(?, '+2 minutes')
                ORDER BY ABS(strftime('%s', timestamp) - strftime('%s', ?))
                LIMIT 1
            """
            ts_str = sig_ts.strftime('%Y-%m-%d %H:%M:%S')
            try:
                p = pd.read_sql_query(
                    price_query, conn,
                    params=(symbol, ts_str, ts_str, ts_str)
                )
                price_at_signal = float(p['price_usd'].iloc[0]) if not p.empty else None
            except Exception:
                price_at_signal = None

            # Forward prices at +5, +15, +30 min
            forward_prices = {}
            for mins in [5, 15, 30]:
                fwd_ts = (sig_ts + timedelta(minutes=mins)).strftime('%Y-%m-%d %H:%M:%S')
                fwd_q = """
                    SELECT price_usd FROM prices
                    WHERE symbol = ?
                    AND timestamp BETWEEN datetime(?, '-2 minutes')
                                  AND datetime(?, '+2 minutes')
                    ORDER BY ABS(strftime('%s', timestamp) - strftime('%s', ?))
                    LIMIT 1
                """
                try:
                    p_fwd = pd.read_sql_query(
                        fwd_q, conn,
                        params=(symbol, fwd_ts, fwd_ts, fwd_ts)
                    )
                    forward_prices[mins] = float(p_fwd['price_usd'].iloc[0]) if not p_fwd.empty else None
                except Exception:
                    forward_prices[mins] = None

            # Calculate % price move after signal
            def pct_move(p0, p1):
                if p0 and p1 and p0 > 0:
                    return round((p1 - p0) / p0 * 100, 4)
                return None

            outcomes.append({
                'price_at_signal': price_at_signal,
                'move_5m':  pct_move(price_at_signal, forward_prices.get(5)),
                'move_15m': pct_move(price_at_signal, forward_prices.get(15)),
                'move_30m': pct_move(price_at_signal, forward_prices.get(30)),
            })

        outcomes_df = pd.DataFrame(outcomes)
        return pd.concat([df.reset_index(drop=True), outcomes_df], axis=1)

    except Exception as e:
        return pd.DataFrame()

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


def effective_rate(row) -> float:
    try:
        in_dec  = TOKEN_DECIMALS.get(row['input_symbol'], 6)
        out_dec = TOKEN_DECIMALS.get(row['output_symbol'], 6)
        in_amt  = row['in_amount'] / (10 ** in_dec)
        out_amt = row['out_amount'] / (10 ** out_dec)
        return out_amt / in_amt if in_amt > 0 else 0   # FIX: zero-guard
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
    min_value=1, max_value=48, value=6, step=1
)

st.sidebar.markdown("---")
st.sidebar.markdown("**Database**")
st.sidebar.caption(f"`{DB_PATH}`")
st.sidebar.markdown("---")
st.sidebar.markdown("**Jupiter DEX Monitor v2.1**")
st.sidebar.caption("Built with Python + Streamlit")
st.sidebar.caption("Data: Jupiter API v3")

# ─────────────────────────────────────────────
# MAIN LAYOUT
# ─────────────────────────────────────────────

st.title("📊 Jupiter DEX Price Monitor")
st.caption(f"Last rendered: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

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
col3.metric("🕐 Last Update",   stats.get('last_update', 'N/A'))
col4.metric("📅 Data Since",    stats.get('first_record', 'N/A'))

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
    display_df['Price (USD)']  = display_df['price_usd'].apply(humanize_price)
    display_df['24h Change']   = display_df['price_change_24h'].apply(format_change)
    display_df['Last Updated'] = display_df['timestamp']
    display_df['Liquidity']    = display_df['liquidity'].apply(
        lambda x: f"${x:,.0f}" if x and x > 0 else "N/A"
    )
    display_df = display_df[['symbol', 'Price (USD)', '24h Change', 'Liquidity', 'Last Updated']]
    display_df.columns = ['Token', 'Price', '24h Change', 'Liquidity', 'Last Updated']
    st.dataframe(display_df, use_container_width=True, hide_index=True)

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
            st.info(
                f"No price history for {selected_symbol} in the last "
                f"{hours_filter}h. Widen the history window or wait for more data."
            )
        else:
            st.line_chart(
                history_df.set_index('timestamp')['price_usd'],
                use_container_width=True
            )
            st.caption(
                f"{len(history_df)} data points for {selected_symbol} "
                f"over the last {hours_filter} hours"
            )

st.markdown("---")

# ─────────────────────────────────────────────
# SECTION 4 — LIVE QUOTE TABLE
# ─────────────────────────────────────────────

st.subheader("💱 Live Swap Quotes")

quotes_df = get_latest_quotes(conn)

if quotes_df.empty:
    st.warning("No quote data available yet. Waiting for first fetch cycle...")
else:
    display_quotes = quotes_df.copy()
    display_quotes['Effective Rate'] = display_quotes.apply(
        lambda row: (
            f"{effective_rate(row):.6f} "
            f"{row['output_symbol']} per {row['input_symbol']}"
        ),
        axis=1
    )
    display_quotes['Price Impact'] = display_quotes['price_impact_pct'].apply(
        lambda x: f"{x:.4f}%"
    )
    display_quotes['Slippage']     = display_quotes['slippage_bps'].apply(
        lambda x: f"{x/100:.2f}%"
    )
    display_quotes['Last Updated'] = display_quotes['timestamp']
    display_quotes = display_quotes[
        ['pair', 'Effective Rate', 'Price Impact', 'Slippage', 'Last Updated']
    ]
    display_quotes.columns = ['Pair', 'Effective Rate', 'Price Impact', 'Slippage', 'Last Updated']
    st.dataframe(display_quotes, use_container_width=True, hide_index=True)

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
            st.info(
                f"No quote history for {selected_pair} in the last "
                f"{hours_filter}h. Widen the history window or wait for more data."
            )
        else:
            parts = selected_pair.split('/')
            if len(parts) == 2:
                in_sym, out_sym = parts
                in_dec  = TOKEN_DECIMALS.get(in_sym,  6)
                out_dec = TOKEN_DECIMALS.get(out_sym, 6)

                # FIX: zero-guard on in_amount before division
                mask = quote_hist_df['in_amount'] > 0
                quote_hist_df = quote_hist_df[mask].copy()
                quote_hist_df['effective_rate'] = (
                    (quote_hist_df['out_amount'] / (10 ** out_dec)) /
                    (quote_hist_df['in_amount']  / (10 ** in_dec))
                )

                if quote_hist_df.empty:
                    st.info("All quote records for this pair have zero input amount.")
                else:
                    st.line_chart(
                        quote_hist_df.set_index('timestamp')['effective_rate'],
                        use_container_width=True
                    )
                    st.caption(
                        f"Effective rate: 1 {in_sym} → {out_sym} | "
                        f"{len(quote_hist_df)} data points over last {hours_filter}h"
                    )

st.markdown("---")

# ─────────────────────────────────────────────
# SECTION 6 — SIGNALS PANEL
# ─────────────────────────────────────────────

st.subheader("📡 Arbitrage Signals")

try:
    signal_stats  = get_signal_stats(conn)
    signals_available = True
except Exception:
    signals_available = False

if not signals_available:
    st.info("Signals table not found. Run arbitrage_detector.py to initialize.")
else:
    s_col1, s_col2, s_col3, s_col4 = st.columns(4)
    s_col1.metric("📡 Total Signals",      f"{signal_stats.get('total', 0):,}")
    s_col2.metric("🟢 Execute Candidates", f"{signal_stats.get('execute_candidates', 0):,}")
    s_col3.metric("🟡 Needs Analysis",     f"{signal_stats.get('needs_analysis', 0):,}")
    s_col4.metric("📈 Best Profit",        f"{signal_stats.get('max_profit_pct', 0):.4f}%")

    st.markdown("")

    filter_col1, filter_col2, filter_col3 = st.columns([1, 1, 2])

    with filter_col1:
        signal_filter = st.selectbox(
            "Filter by Classification",
            options=["All", "Execute Candidates", "Needs Analysis"]
        )
    with filter_col2:
        # FIX: added momentum_breakout to type filter
        type_filter = st.selectbox(
            "Filter by Type",
            options=["All", "rate_divergence", "triangular_arbitrage",
                     "impact_anomaly", "momentum_breakout"]
        )
    with filter_col3:
        signal_limit = st.slider(
            "Signals to display",
            min_value=10, max_value=200, value=50, step=10
        )

    signals_df = get_recent_signals(conn, limit=signal_limit)

    if signals_df.empty:
        st.warning("No signals detected yet. Run arbitrage_detector.py to begin detection.")
    else:
        if signal_filter == "Execute Candidates":
            signals_df = signals_df[signals_df['execute_candidate'] == 1]
        elif signal_filter == "Needs Analysis":
            signals_df = signals_df[signals_df['execute_candidate'] == 0]
        if type_filter != "All":
            signals_df = signals_df[signals_df['signal_type'] == type_filter]

        if signals_df.empty:
            st.info("No signals match the selected filters.")
        else:
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
                'Time', 'Classification', 'Signal Type', 'pair', 'Est. Profit', 'Score'
            ]]
            display_signals.columns = [
                'Time', 'Classification', 'Type', 'Pair', 'Est. Profit', 'Score'
            ]
            st.dataframe(display_signals, use_container_width=True, hide_index=True)
            st.caption(f"Showing {len(display_signals)} signals")

            st.markdown("")
            with st.expander("🔍 View Signal Details"):
                selected_idx = st.selectbox(
                    "Select signal to inspect",
                    options=range(len(signals_df)),
                    format_func=lambda i: (
                        f"{signals_df.iloc[i]['timestamp'].strftime('%H:%M:%S')} | "
                        f"{signals_df.iloc[i]['pair']} | "
                        f"Score: {signals_df.iloc[i]['weighted_score']}"
                    )
                )
                sel = signals_df.iloc[selected_idx]
                d1, d2 = st.columns(2)
                with d1:
                    st.markdown(f"**Pair:** {sel['pair']}")
                    st.markdown(f"**Type:** {sel['signal_type'].replace('_', ' ').title()}")
                    st.markdown(f"**Score:** {sel['weighted_score']}/100")
                    st.markdown(f"**Est. Profit:** {sel['estimated_profit_pct']:.4f}%")
                    st.markdown(f"**Time:** {sel['timestamp']}")
                with d2:
                    st.markdown("**Description:**")
                    st.info(sel['description'])
                st.markdown("**Condition Breakdown:**")
                for item in sel['condition_breakdown'].split(' | '):
                    if item.startswith('✓'):
                        st.success(item)
                    else:
                        st.error(item)

    # Score history chart
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

st.markdown("---")

# ─────────────────────────────────────────────
# SECTION 7 — SIGNALS BACKTESTING PANEL
# ─────────────────────────────────────────────

st.subheader("🔬 Signal Backtesting")
st.caption(
    "Evaluates signal quality by comparing price at signal time to actual "
    "price movement 5, 15, and 30 minutes after each signal fired."
)

try:
    conn.execute("SELECT COUNT(*) FROM signals").fetchone()
    backtest_available = True
except Exception:
    backtest_available = False

if not backtest_available:
    st.info("Signals table not found. Run arbitrage_detector.py to initialize.")
else:
    bt_col1, bt_col2, bt_col3 = st.columns([1, 1, 2])

    with bt_col1:
        bt_type_filter = st.selectbox(
            "Signal Type",
            options=["All", "rate_divergence", "triangular_arbitrage",
                     "impact_anomaly", "momentum_breakout"],
            key="bt_type"
        )
    with bt_col2:
        bt_hours = st.selectbox(
            "Look-back window",
            options=[6, 12, 24, 48],
            index=2,
            key="bt_hours"
        )
    with bt_col3:
        bt_exec_only = st.checkbox(
            "Execute candidates only",
            value=False,
            key="bt_exec"
        )

    with st.spinner("Loading backtest data..."):
        bt_df = get_backtest_data(conn, bt_type_filter, bt_hours)

    if bt_df.empty:
        st.info(
            f"No signals found in the last {bt_hours} hours "
            f"{'for ' + bt_type_filter if bt_type_filter != 'All' else ''}. "
            "Run the monitor overnight to accumulate signal history."
        )
    else:
        if bt_exec_only:
            bt_df = bt_df[bt_df['execute_candidate'] == 1]

        if bt_df.empty:
            st.info("No execute-candidate signals found in this window.")
        else:
            # ── Summary metrics ──
            total_signals  = len(bt_df)
            has_5m  = bt_df['move_5m'].notna().sum()
            has_15m = bt_df['move_15m'].notna().sum()
            has_30m = bt_df['move_30m'].notna().sum()

            avg_5m  = bt_df['move_5m'].mean()
            avg_15m = bt_df['move_15m'].mean()
            avg_30m = bt_df['move_30m'].mean()

            # Directional accuracy: did price move ≥ +0.1% within 15 min?
            if has_15m > 0:
                correct_dir = (bt_df['move_15m'] >= 0.1).sum()
                dir_accuracy = correct_dir / has_15m * 100
            else:
                dir_accuracy = None

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Signals Evaluated", total_signals)
            m2.metric(
                "Avg Move +5min",
                f"{avg_5m:+.4f}%" if pd.notna(avg_5m) else "N/A"
            )
            m3.metric(
                "Avg Move +15min",
                f"{avg_15m:+.4f}%" if pd.notna(avg_15m) else "N/A"
            )
            m4.metric(
                "Directional Accuracy",
                f"{dir_accuracy:.1f}%" if dir_accuracy is not None else "N/A",
                help="% of signals followed by ≥+0.1% price move within 15 min"
            )

            st.markdown("")

            # ── Price movement distribution chart ──
            move_cols = {
                '+5min move%':  'move_5m',
                '+15min move%': 'move_15m',
                '+30min move%': 'move_30m',
            }
            available_moves = {
                k: v for k, v in move_cols.items()
                if bt_df[v].notna().any()
            }

            if available_moves:
                move_chart_df = pd.DataFrame({
                    k: bt_df[v] for k, v in available_moves.items()
                }).dropna(how='all')

                if not move_chart_df.empty:
                    st.markdown("**Price Movement After Signal (% change)**")
                    st.bar_chart(move_chart_df, use_container_width=True)
                    st.caption(
                        "Each bar = one signal. Positive = price moved in profitable "
                        "direction after signal fired."
                    )

            # ── Per-signal breakdown table ──
            st.markdown("")
            st.markdown("**Per-Signal Outcome Table**")

            bt_display = bt_df.copy()
            bt_display['Time'] = bt_display['timestamp'].apply(
                lambda x: x.strftime('%m-%d %H:%M')
            )
            bt_display['Type'] = bt_display['signal_type'].apply(
                lambda x: x.replace('_', ' ').title()
            )
            bt_display['Est. Profit'] = bt_display['estimated_profit_pct'].apply(
                lambda x: f"{x:.4f}%"
            )
            bt_display['Score'] = bt_display['weighted_score'].apply(
                lambda x: f"{x}/100"
            )
            bt_display['Execute'] = bt_display['execute_candidate'].apply(
                lambda x: "🟢" if x == 1 else "🟡"
            )
            bt_display['Price @ Signal'] = bt_display['price_at_signal'].apply(
                lambda x: humanize_price(x) if pd.notna(x) else "N/A"
            )
            bt_display['+5min'] = bt_display['move_5m'].apply(
                lambda x: f"{x:+.4f}%" if pd.notna(x) else "—"
            )
            bt_display['+15min'] = bt_display['move_15m'].apply(
                lambda x: f"{x:+.4f}%" if pd.notna(x) else "—"
            )
            bt_display['+30min'] = bt_display['move_30m'].apply(
                lambda x: f"{x:+.4f}%" if pd.notna(x) else "—"
            )

            bt_display = bt_display[[
                'Time', 'Execute', 'Type', 'pair',
                'Est. Profit', 'Score',
                'Price @ Signal', '+5min', '+15min', '+30min'
            ]]
            bt_display.columns = [
                'Time', '', 'Type', 'Pair',
                'Est. Profit', 'Score',
                'Price @ Signal', '+5min', '+15min', '+30min'
            ]

            st.dataframe(bt_display, use_container_width=True, hide_index=True)
            st.caption(
                "— = price record not yet available for that time window. "
                "Signals fired in the last 30 min will show partial data."
            )

            # ── Signal type performance comparison ──
            if bt_type_filter == "All" and len(bt_df['signal_type'].unique()) > 1:
                st.markdown("")
                st.markdown("**Performance by Signal Type**")
                type_perf = bt_df.groupby('signal_type').agg(
                    count=('pair', 'count'),
                    avg_est_profit=('estimated_profit_pct', 'mean'),
                    avg_score=('weighted_score', 'mean'),
                    avg_move_15m=('move_15m', 'mean'),
                ).reset_index()
                type_perf.columns = [
                    'Signal Type', 'Count',
                    'Avg Est. Profit%', 'Avg Score',
                    'Avg +15min Move%'
                ]
                type_perf['Signal Type'] = type_perf['Signal Type'].apply(
                    lambda x: x.replace('_', ' ').title()
                )
                for col in ['Avg Est. Profit%', 'Avg Score', 'Avg +15min Move%']:
                    type_perf[col] = type_perf[col].apply(
                        lambda x: f"{x:.4f}" if pd.notna(x) else "N/A"
                    )
                st.dataframe(type_perf, use_container_width=True, hide_index=True)

# ─────────────────────────────────────────────
# AUTO REFRESH — FIX: render first, sleep after
# Streamlit renders top-to-bottom synchronously.
# time.sleep placed at the very end means all charts
# render before the 30s pause triggers st.rerun().
# ─────────────────────────────────────────────

if auto_refresh:
    time.sleep(30)
    st.rerun()
