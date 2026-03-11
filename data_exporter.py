import pandas as pd
import sqlite3
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

class DataExporter:
    """Export price and quote history to SQLite database and Excel files"""
    
    def __init__(self, output_dir: str = "price_history", db_name: str = "jupiter_monitor.db"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # SQLite database path
        self.db_path = self.output_dir / db_name
        
        # File tracking for consolidation
        self.current_date_file = None
        self.consolidated_data = {}
        
        # Initialize the database and create tables
        self._init_database()
        
        logger.info(f"✅ Data exporter initialized. Output directory: {self.output_dir}")
        logger.info(f"✅ SQLite database: {self.db_path}")
    
    # -------------------------------------------------------------------------
    # DATABASE SETUP
    # -------------------------------------------------------------------------

    def _init_database(self):
        """Create the SQLite database and tables if they don't already exist"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Prices table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS prices (
                    id                INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp         TEXT NOT NULL,
                    symbol            TEXT NOT NULL,
                    token_id          TEXT,
                    price_usd         REAL NOT NULL,
                    price_change_24h  REAL,
                    liquidity         REAL,
                    created_at        TEXT,
                    block_id          TEXT,
                    decimals          INTEGER
                )
            """)
            
            # Quotes table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS quotes (
                    id                INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp         TEXT NOT NULL,
                    pair              TEXT NOT NULL,
                    input_symbol      TEXT,
                    output_symbol     TEXT,
                    input_mint        TEXT,
                    output_mint       TEXT,
                    in_amount         INTEGER,
                    out_amount        INTEGER,
                    price_impact_pct  REAL,
                    slippage_bps      INTEGER
                )
            """)
            
            # Create indexes for fast querying by symbol and timestamp
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_prices_symbol
                ON prices (symbol)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_prices_timestamp
                ON prices (timestamp)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_quotes_pair
                ON quotes (pair)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_quotes_timestamp
                ON quotes (timestamp)
            """)
            
            conn.commit()
            logger.info("✅ Database tables and indexes verified")

    # -------------------------------------------------------------------------
    # SQLITE WRITE METHODS
    # -------------------------------------------------------------------------

    def insert_price(self, price_data: Dict):
        """
        Insert a single price data point into the prices table.

        Args:
            price_data: Price dictionary from jupiter_api.get_multiple_prices()
        """
        extra = price_data.get('extra_info', {})
        timestamp = price_data.get('timestamp', datetime.now())
        
        # Convert datetime to ISO string for SQLite storage
        if isinstance(timestamp, datetime):
            timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
        
        row = (
            timestamp,
            price_data.get('symbol', ''),
            price_data.get('token_id', ''),
            price_data.get('price_usd', 0.0),
            extra.get('price_change_24h', 0.0),
            extra.get('liquidity', 0.0),
            extra.get('created_at', ''),
            str(extra.get('block_id', '')),
            extra.get('decimals')
        )
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO prices (
                        timestamp, symbol, token_id, price_usd,
                        price_change_24h, liquidity, created_at,
                        block_id, decimals
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, row)
                conn.commit()
        except Exception as e:
            logger.error(f"Error inserting price for {price_data.get('symbol')}: {e}")

    def insert_prices_batch(self, prices: Dict[str, Dict]):
        """
        Insert multiple price data points in a single database transaction.
        More efficient than calling insert_price() in a loop.

        Args:
            prices: Dictionary mapping token symbol to price data dict
        """
        rows = []
        for symbol, price_data in prices.items():
            extra = price_data.get('extra_info', {})
            timestamp = price_data.get('timestamp', datetime.now())
            if isinstance(timestamp, datetime):
                timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            
            rows.append((
                timestamp,
                price_data.get('symbol', symbol),
                price_data.get('token_id', ''),
                price_data.get('price_usd', 0.0),
                extra.get('price_change_24h', 0.0),
                extra.get('liquidity', 0.0),
                extra.get('created_at', ''),
                str(extra.get('block_id', '')),
                extra.get('decimals')
            ))
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.executemany("""
                    INSERT INTO prices (
                        timestamp, symbol, token_id, price_usd,
                        price_change_24h, liquidity, created_at,
                        block_id, decimals
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, rows)
                conn.commit()
            logger.info(f"💾 Inserted {len(rows)} price records into database")
        except Exception as e:
            logger.error(f"Error batch inserting prices: {e}")

    def insert_quote(self, quote_data: Dict):
        """
        Insert a single quote data point into the quotes table.

        Args:
            quote_data: Quote dictionary from jupiter_api.get_quote()
        """
        timestamp = quote_data.get('timestamp', datetime.now())
        if isinstance(timestamp, datetime):
            timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
        
        row = (
            timestamp,
            quote_data.get('pair', ''),
            quote_data.get('input_symbol', ''),
            quote_data.get('output_symbol', ''),
            quote_data.get('input_mint', ''),
            quote_data.get('output_mint', ''),
            quote_data.get('in_amount', 0),
            quote_data.get('out_amount', 0),
            quote_data.get('price_impact_pct', 0.0),
            quote_data.get('slippage_bps', 0)
        )
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO quotes (
                        timestamp, pair, input_symbol, output_symbol,
                        input_mint, output_mint, in_amount, out_amount,
                        price_impact_pct, slippage_bps
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, row)
                conn.commit()
        except Exception as e:
            logger.error(f"Error inserting quote for {quote_data.get('pair')}: {e}")

    def insert_quotes_batch(self, quote_history: Dict[str, List[Dict]]):
        """
        Insert all quotes from a session into the database in one transaction.

        Args:
            quote_history: Dictionary mapping pair_name to list of quote dicts
        """
        rows = []
        for pair_name, data_list in quote_history.items():
            for quote_data in data_list:
                timestamp = quote_data.get('timestamp', datetime.now())
                if isinstance(timestamp, datetime):
                    timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                
                rows.append((
                    timestamp,
                    quote_data.get('pair', pair_name),
                    quote_data.get('input_symbol', ''),
                    quote_data.get('output_symbol', ''),
                    quote_data.get('input_mint', ''),
                    quote_data.get('output_mint', ''),
                    quote_data.get('in_amount', 0),
                    quote_data.get('out_amount', 0),
                    quote_data.get('price_impact_pct', 0.0),
                    quote_data.get('slippage_bps', 0)
                ))
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.executemany("""
                    INSERT INTO quotes (
                        timestamp, pair, input_symbol, output_symbol,
                        input_mint, output_mint, in_amount, out_amount,
                        price_impact_pct, slippage_bps
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, rows)
                conn.commit()
            logger.info(f"💾 Inserted {len(rows)} quote records into database")
        except Exception as e:
            logger.error(f"Error batch inserting quotes: {e}")

    # -------------------------------------------------------------------------
    # SQLITE READ METHODS (used later by Streamlit dashboard)
    # -------------------------------------------------------------------------

    def get_latest_prices(self) -> pd.DataFrame:
        """
        Fetch the most recent price record for each token symbol.
        Used by Streamlit dashboard for live price display.
        """
        query = """
            SELECT p.*
            FROM prices p
            INNER JOIN (
                SELECT symbol, MAX(timestamp) as max_ts
                FROM prices
                GROUP BY symbol
            ) latest ON p.symbol = latest.symbol AND p.timestamp = latest.max_ts
            ORDER BY p.symbol
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                return pd.read_sql_query(query, conn)
        except Exception as e:
            logger.error(f"Error fetching latest prices: {e}")
            return pd.DataFrame()

    def get_price_history(self, symbol: str, hours: int = 24) -> pd.DataFrame:
        """
        Fetch price history for a specific token over the last N hours.
        Used by Streamlit for charting individual token trends.

        Args:
            symbol: Token symbol e.g. 'SOL'
            hours: How many hours of history to retrieve (default 24)
        """
        query = """
            SELECT timestamp, symbol, price_usd, price_change_24h, confidence
            FROM prices
            WHERE symbol = ?
            AND timestamp >= datetime('now', ? || ' hours')
            ORDER BY timestamp ASC
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                return pd.read_sql_query(query, conn, params=(symbol, f'-{hours}'))
        except Exception as e:
            logger.error(f"Error fetching price history for {symbol}: {e}")
            return pd.DataFrame()

    def get_quote_history(self, pair: str, hours: int = 24) -> pd.DataFrame:
        """
        Fetch quote history for a trading pair over the last N hours.

        Args:
            pair: Trading pair e.g. 'SOL/USDC'
            hours: How many hours of history to retrieve (default 24)
        """
        query = """
            SELECT timestamp, pair, in_amount, out_amount,
                   price_impact_pct, slippage_bps
            FROM quotes
            WHERE pair = ?
            AND timestamp >= datetime('now', ? || ' hours')
            ORDER BY timestamp ASC
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                return pd.read_sql_query(query, conn, params=(pair, f'-{hours}'))
        except Exception as e:
            logger.error(f"Error fetching quote history for {pair}: {e}")
            return pd.DataFrame()

    def get_database_stats(self) -> Dict:
        """
        Return a summary of records currently stored in the database.
        Useful for monitoring and dashboard status displays.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM prices")
                price_count = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM quotes")
                quote_count = cursor.fetchone()[0]
                cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM prices")
                price_range = cursor.fetchone()
                return {
                    'total_price_records': price_count,
                    'total_quote_records': quote_count,
                    'price_data_from': price_range[0],
                    'price_data_to': price_range[1]
                }
        except Exception as e:
            logger.error(f"Error fetching database stats: {e}")
            return {}

    # -------------------------------------------------------------------------
    # FORMATTING HELPERS
    # -------------------------------------------------------------------------

    def humanize_price(self, price: float) -> str:
        """Format price for human readability"""
        if price >= 1000:
            return f"${price:,.2f}"
        elif price >= 1:
            return f"${price:.4f}"
        elif price >= 0.01:
            return f"${price:.6f}"
        else:
            return f"${price:.8f}"
    
    def humanize_volume(self, volume: float) -> str:
        """Format volume for human readability"""
        if volume >= 1_000_000_000:
            return f"${volume/1_000_000_000:.2f}B"
        elif volume >= 1_000_000:
            return f"${volume/1_000_000:.2f}M"
        elif volume >= 1_000:
            return f"${volume/1_000:.2f}K"
        else:
            return f"${volume:.2f}"

    # -------------------------------------------------------------------------
    # CONSOLIDATED IN-MEMORY STORAGE (retained for Excel export compatibility)
    # -------------------------------------------------------------------------

    def append_to_consolidated_data(self, token_symbol: str, data_point: Dict):
        """Append data point to in-memory consolidated storage for Excel export"""
        if token_symbol not in self.consolidated_data:
            self.consolidated_data[token_symbol] = []
        self.consolidated_data[token_symbol].append(data_point)
        if len(self.consolidated_data[token_symbol]) > 2880:
            self.consolidated_data[token_symbol] = self.consolidated_data[token_symbol][-2880:]

    # -------------------------------------------------------------------------
    # EXCEL EXPORT METHODS (retained for backward compatibility)
    # -------------------------------------------------------------------------

    def export_consolidated_price_history(self):
        """Export consolidated price history for all tokens to single Excel file"""
        if not self.consolidated_data:
            logger.warning("No consolidated data to export")
            return
        
        date_key = datetime.now().strftime('%Y%m%d')
        filename = self.output_dir / f"PRICE_HISTORY_CONSOLIDATED_{date_key}.xlsx"
        
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                for token_symbol, data_list in self.consolidated_data.items():
                    if not data_list:
                        continue
                    df = pd.DataFrame(data_list)
                    if 'price_usd' in df.columns:
                        df['Price (Human)'] = df['price_usd'].apply(self.humanize_price)
                        df['Timestamp (Readable)'] = df['timestamp'].apply(
                            lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if isinstance(x, datetime) else str(x)
                        )
                        if 'extra_info' in df.columns:
                            try:
                                extra_info_df = pd.json_normalize(df['extra_info'])
                                df = pd.concat([df.drop('extra_info', axis=1), extra_info_df], axis=1)
                            except Exception as e:
                                logger.warning(f"Could not extract extra_info for {token_symbol}: {e}")
                        columns_order = ['Timestamp (Readable)', 'Price (Human)', 'price_usd', 'symbol']
                        for col in df.columns:
                            if col not in columns_order and col != 'timestamp' and col != 'token_id':
                                columns_order.append(col)
                        columns_order = [col for col in columns_order if col in df.columns]
                        df = df[columns_order]
                        sheet_name = token_symbol[:31]
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            logger.info(f"📊 Exported consolidated price history for {len(self.consolidated_data)} tokens to {filename}")
            self.consolidated_data.clear()
            
        except Exception as e:
            logger.error(f"Error exporting consolidated data: {e}")
    
    def export_consolidated_quotes(self, quote_history: Dict[str, List[Dict]]):
        """Export consolidated quote history for all pairs to single Excel file"""
        if not quote_history:
            logger.warning("No quote data to export")
            return
        
        date_key = datetime.now().strftime('%Y%m%d')
        filename = self.output_dir / f"QUOTE_HISTORY_CONSOLIDATED_{date_key}.xlsx"
        
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                for pair_name, data_list in quote_history.items():
                    if not data_list:
                        continue
                    df = pd.DataFrame(data_list)
                    df['Timestamp (Readable)'] = df['timestamp'].apply(
                        lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if isinstance(x, datetime) else str(x)
                    )
                    columns_order = ['Timestamp (Readable)', 'pair', 'in_amount', 'out_amount',
                                     'price_impact_pct', 'slippage_bps', 'timestamp']
                    for col in df.columns:
                        if col not in columns_order:
                            columns_order.append(col)
                    columns_order = [col for col in columns_order if col in df.columns]
                    df = df[columns_order]
                    clean_pair_name = pair_name.replace('/', '-')[:31]
                    df.to_excel(writer, sheet_name=clean_pair_name, index=False)
            
            logger.info(f"💱 Exported consolidated quote history for {len(quote_history)} pairs to {filename}")
            
        except Exception as e:
            logger.error(f"Error exporting consolidated quotes: {e}")

    def export_combined_report(self, price_history: Dict[str, List[Dict]]):
        """Export a combined report with latest prices for all tokens"""
        if not price_history:
            logger.warning("No price data to create combined report")
            return
        
        date_key = datetime.now().strftime('%Y%m%d')
        filename = self.output_dir / f"DAILY_SUMMARY_{date_key}.xlsx"
        
        try:
            combined_data = []
            for symbol, data_list in price_history.items():
                if data_list:
                    latest_data = data_list[-1].copy()
                    latest_data['symbol'] = symbol
                    combined_data.append(latest_data)
            
            if not combined_data:
                return
            
            df = pd.DataFrame(combined_data)
            df['Price (Human)'] = df['price_usd'].apply(self.humanize_price)
            
            if 'extra_info' in df.columns:
                extra_info_df = pd.json_normalize(df['extra_info'])
                df = pd.concat([df.drop('extra_info', axis=1), extra_info_df], axis=1)
            
            df = df.sort_values('symbol')
            columns_order = ['symbol', 'Price (Human)', 'price_usd', 'timestamp']
            for col in df.columns:
                if col not in columns_order:
                    columns_order.append(col)
            columns_order = [col for col in columns_order if col in df.columns]
            df = df[columns_order]
            df.to_excel(filename, index=False, engine='openpyxl')
            logger.info(f"📊 Exported daily summary with {len(df)} tokens to {filename}")
            
        except Exception as e:
            logger.error(f"Error exporting daily summary: {e}")

    def export_to_excel(self, token_symbol: str, history_data: List[Dict]):
        """Legacy method: retained for backward compatibility"""
        if not history_data:
            logger.warning(f"No data to export for {token_symbol}")
            return
        for data_point in history_data:
            self.append_to_consolidated_data(token_symbol, data_point)
        logger.info(f"📊 Added {len(history_data)} records for {token_symbol} to consolidated storage")

    def export_all_tokens(self, all_token_data: Dict[str, List[Dict]]):
        """Legacy method: retained for backward compatibility"""
        for token_symbol, history_data in all_token_data.items():
            for data_point in history_data:
                self.append_to_consolidated_data(token_symbol, data_point)
        logger.info(f"Added data for {len(all_token_data)} tokens to consolidated storage")
