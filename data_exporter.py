import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

class DataExporter:
    """Export price and quote history to Excel files with consolidation"""
    
    def __init__(self, output_dir: str = "price_history"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # File tracking for consolidation
        self.current_date_file = None
        self.consolidated_data = {}
        
        logger.info(f"✅ Data exporter initialized. Output directory: {self.output_dir}")
    
    def _get_date_key(self) -> str:
        """Get current date key for file naming (YYYYMMDD)"""
        return datetime.now().strftime('%Y%m%d')
    
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
    
    def append_to_consolidated_data(self, token_symbol: str, data_point: Dict):
        """
        Append data point to consolidated storage for the token
        
        Args:
            token_symbol: Token symbol
            data_point: Single data point dictionary
        """
        if token_symbol not in self.consolidated_data:
            self.consolidated_data[token_symbol] = []
        
        self.consolidated_data[token_symbol].append(data_point)
        
        # Limit to last 2880 data points (24 hours at 30-second intervals)
        if len(self.consolidated_data[token_symbol]) > 2880:
            self.consolidated_data[token_symbol] = self.consolidated_data[token_symbol][-2880:]
    
    def export_consolidated_price_history(self):
        """
        Export consolidated price history for all tokens to single Excel file
        One sheet per token with 24 hours of data
        """
        if not self.consolidated_data:
            logger.warning("No consolidated data to export")
            return
        
        date_key = self._get_date_key()
        filename = self.output_dir / f"PRICE_HISTORY_CONSOLIDATED_{date_key}.xlsx"
        
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                for token_symbol, data_list in self.consolidated_data.items():
                    if not data_list:
                        continue
                    
                    # Convert to DataFrame
                    df = pd.DataFrame(data_list)
                    
                    # Format for prices
                    if 'price_usd' in df.columns:
                        # Add human-readable price
                        df['Price (Human)'] = df['price_usd'].apply(self.humanize_price)
                        
                        # Format timestamp for readability
                        df['Timestamp (Readable)'] = df['timestamp'].apply(
                            lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if isinstance(x, datetime) else str(x)
                        )
                        
                        # Extract extra_info columns if they exist
                        if 'extra_info' in df.columns:
                            try:
                                extra_info_df = pd.json_normalize(df['extra_info'])
                                df = pd.concat([df.drop('extra_info', axis=1), extra_info_df], axis=1)
                            except Exception as e:
                                logger.warning(f"Could not extract extra_info for {token_symbol}: {e}")
                        
                        # Order columns
                        columns_order = ['Timestamp (Readable)', 'Price (Human)', 'price_usd', 'symbol']
                        for col in df.columns:
                            if col not in columns_order and col != 'timestamp' and col != 'token_id':
                                columns_order.append(col)
                        
                        # Filter to existing columns
                        columns_order = [col for col in columns_order if col in df.columns]
                        df = df[columns_order]
                        
                        # Write to Excel sheet (limit sheet name to 31 chars)
                        sheet_name = token_symbol[:31]
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        
                        # Auto-adjust column widths
                        worksheet = writer.sheets[sheet_name]
                        for column in df:
                            column_width = max(df[column].astype(str).map(len).max(), len(str(column))) + 2
                            column_letter = pd.io.excel._xlsxwriter.__dict__.get('get_column_letter', 
                                                                                 lambda x: chr(65 + x - 1))(list(df.columns).index(column) + 1)
                            worksheet.column_dimensions[column_letter].width = min(column_width, 50)
            
            logger.info(f"📊 Exported consolidated price history for {len(self.consolidated_data)} tokens to {filename}")
            
            # Clear consolidated data for next day
            self.consolidated_data.clear()
            
        except Exception as e:
            logger.error(f"Error exporting consolidated data: {e}")
    
    def export_consolidated_quotes(self, quote_history: Dict[str, List[Dict]]):
        """
        Export consolidated quote history for all pairs to single Excel file
        
        Args:
            quote_history: Dictionary mapping pair_name to quote data list
        """
        if not quote_history:
            logger.warning("No quote data to export")
            return
        
        date_key = self._get_date_key()
        filename = self.output_dir / f"QUOTE_HISTORY_CONSOLIDATED_{date_key}.xlsx"
        
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                for pair_name, data_list in quote_history.items():
                    if not data_list:
                        continue
                    
                    # Convert to DataFrame
                    df = pd.DataFrame(data_list)
                    
                    # Format timestamp for readability
                    df['Timestamp (Readable)'] = df['timestamp'].apply(
                        lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if isinstance(x, datetime) else str(x)
                    )
                    
                    # Reorder columns for quotes
                    columns_order = [
                        'Timestamp (Readable)',
                        'pair',
                        'in_amount',
                        'out_amount',
                        'price_impact_pct',
                        'slippage_bps',
                        'timestamp'
                    ]
                    
                    # Add any extra columns
                    for col in df.columns:
                        if col not in columns_order:
                            columns_order.append(col)
                    
                    # Filter to only existing columns
                    columns_order = [col for col in columns_order if col in df.columns]
                    df = df[columns_order]
                    
                    # Clean pair name for sheet name
                    clean_pair_name = pair_name.replace('/', '-')[:31]
                    df.to_excel(writer, sheet_name=clean_pair_name, index=False)
            
            logger.info(f"💱 Exported consolidated quote history for {len(quote_history)} pairs to {filename}")
            
        except Exception as e:
            logger.error(f"Error exporting consolidated quotes: {e}")
    
    def export_to_excel(self, token_symbol: str, history_data: List[Dict]):
        """
        Legacy method: Export individual data to Excel (for backward compatibility)
        
        Args:
            token_symbol: Token symbol or pair name
            history_data: List of data dictionaries
        """
        if not history_data:
            logger.warning(f"No data to export for {token_symbol}")
            return
        
        # Convert to DataFrame
        df = pd.DataFrame(history_data)
        
        # Store in consolidated data for later export
        for data_point in history_data:
            self.append_to_consolidated_data(token_symbol, data_point)
        
        logger.info(f"📊 Added {len(history_data)} records for {token_symbol} to consolidated storage")
    
    def export_all_tokens(self, all_token_data: Dict[str, List[Dict]]):
        """
        Legacy method: Export history for all tokens (now uses consolidated approach)
        
        Args:
            all_token_data: Dictionary mapping token_symbol to list of data
        """
        for token_symbol, history_data in all_token_data.items():
            for data_point in history_data:
                self.append_to_consolidated_data(token_symbol, data_point)
        
        logger.info(f"Added data for {len(all_token_data)} tokens to consolidated storage")
    
    def export_combined_report(self, price_history: Dict[str, List[Dict]]):
        """
        Export a combined report with latest prices for all tokens
        
        Args:
            price_history: Dictionary mapping token_symbol to price data list
        """
        if not price_history:
            logger.warning("No price data to create combined report")
            return
        
        date_key = self._get_date_key()
        filename = self.output_dir / f"DAILY_SUMMARY_{date_key}.xlsx"
        
        try:
            combined_data = []
            for symbol, data_list in price_history.items():
                if data_list:
                    latest_data = data_list[-1].copy()  # Get latest price data
                    latest_data['symbol'] = symbol
                    combined_data.append(latest_data)
            
            if not combined_data:
                return
            
            df = pd.DataFrame(combined_data)
            
            # Add human-readable price
            df['Price (Human)'] = df['price_usd'].apply(self.humanize_price)
            
            # Extract extra_info if present
            if 'extra_info' in df.columns:
                extra_info_df = pd.json_normalize(df['extra_info'])
                df = pd.concat([df.drop('extra_info', axis=1), extra_info_df], axis=1)
            
            # Sort by symbol
            df = df.sort_values('symbol')
            
            # Reorder columns
            columns_order = ['symbol', 'Price (Human)', 'price_usd', 'timestamp']
            for col in df.columns:
                if col not in columns_order:
                    columns_order.append(col)
            
            # Filter to existing columns
            columns_order = [col for col in columns_order if col in df.columns]
            df = df[columns_order]
            
            # Export
            df.to_excel(filename, index=False, engine='openpyxl')
            logger.info(f"📊 Exported daily summary with {len(df)} tokens to {filename}")
            
        except Exception as e:
            logger.error(f"Error exporting daily summary: {e}")