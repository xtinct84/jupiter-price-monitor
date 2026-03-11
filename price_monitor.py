import asyncio
from datetime import datetime
from typing import Dict, List
import logging
import os
from collections import defaultdict

from jupiter_api import JupiterAPI
from token_registry import TokenRegistry
from data_exporter import DataExporter
from arbitrage_detector import run_detection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PriceMonitor:
    """Monitor token prices and quotes from Jupiter API, export to SQLite and Excel"""
    
    def __init__(self, interval_seconds: int = 30):
        self.api = JupiterAPI()
        self.exporter = DataExporter()
        self.interval = interval_seconds
        
        # Store price history in memory: {token_symbol: [price_data_list]}
        self.price_history = defaultdict(list)
        
        # Store quote history in memory: {pair_name: [quote_data_list]}
        self.quote_history = defaultdict(list)
        
        # Get tokens to monitor
        self.tokens = TokenRegistry.get_high_and_mid_volume_mints()
        self.token_map = {t.mint: t.symbol for t in TokenRegistry.ALL_TOKENS.values()}
        self.token_info_map = {t.mint: t for t in TokenRegistry.ALL_TOKENS.values()}
        
        # Define trading pairs to monitor for quotes
        self.quote_pairs = [
            ('SOL', 'USDC'),
            ('JUP', 'USDC'),
            ('RAY', 'USDC'),
            ('BONK', 'SOL'),
            ('JTO', 'USDC'),
            ('PYTH', 'USDC'),
            ('WIF', 'SOL'),
        ]
        
        logger.info(f"✅ Jupiter Price Monitor initialized")
        logger.info(f"   Monitoring {len(self.tokens)} tokens for prices")
        logger.info(f"   Monitoring {len(self.quote_pairs)} pairs for quotes")
        logger.info(f"   Check interval: {self.interval} seconds")
    
    def display_price_update(self, prices: Dict[str, Dict]):
        """Display price updates in human-readable format"""
        print("\n" + "="*80)
        print(f"📊 JUPITER PRICE UPDATE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
        sorted_items = sorted(prices.items(), key=lambda x: self.token_map.get(x[0], x[0]))
        
        for mint, price_data in sorted_items:
            symbol = self.token_map.get(mint, mint[:8])
            price = price_data['price_usd']
            price_change = price_data.get('extra_info', {}).get('price_change_24h', 0)
            confidence = price_data.get('extra_info', {}).get('confidence', 0)
            
            if price >= 1000:
                price_str = f"${price:,.2f}"
            elif price >= 1:
                price_str = f"${price:.4f}"
            elif price >= 0.01:
                price_str = f"${price:.6f}"
            else:
                price_str = f"${price:.8f}"
            
            if price_change > 0:
                change_str = f"📈 +{price_change:.2f}%"
            elif price_change < 0:
                change_str = f"📉 {price_change:.2f}%"
            else:
                change_str = f"➡️  {price_change:.2f}%"
            
            print(f"  {symbol:8s} | {price_str:20s} | 24h: {change_str:15s} | Conf: {confidence:.2f}")
        
        print("="*80)
    
    def display_quote_update(self, quotes: Dict[str, Dict]):
        """Display quote updates in human-readable format"""
        print("\n" + "="*80)
        print(f"💱 JUPITER QUOTE UPDATE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
        for pair_name, quote_data in quotes.items():
            if not quote_data:
                continue
            
            parts = pair_name.split('/')
            if len(parts) == 2:
                input_symbol, output_symbol = parts
                input_token = TokenRegistry.get_token(input_symbol)
                output_token = TokenRegistry.get_token(output_symbol)
                
                if input_token and output_token:
                    in_amount = quote_data['in_amount'] / (10 ** input_token.decimals)
                    out_amount = quote_data['out_amount'] / (10 ** output_token.decimals)
                    effective_price = out_amount / in_amount if in_amount > 0 else 0
                    price_impact = quote_data['price_impact_pct']
                    slippage = quote_data['slippage_bps']
                    route_plan = quote_data.get('route_plan', [])
                    route_str = "Direct" if len(route_plan) == 1 else f"{len(route_plan)} hops"
                    
                    print(f"  {pair_name:12s} | 1 {input_symbol} = {effective_price:.6f} {output_symbol}")
                    print(f"                    | Impact: {price_impact:.3f}% | Slippage: {slippage/100:.2f}% | Route: {route_str}")
                    print(f"                    {'-'*50}")
        
        print("="*80)
    
    async def fetch_and_store_prices(self):
        """Fetch prices from Jupiter API, store in memory and SQLite"""
        logger.info("Fetching prices from Jupiter API...")
        
        prices = await self.api.get_multiple_prices(self.tokens)
        
        if not prices:
            logger.warning("No prices fetched this round")
            return
        
        # Add symbol to each price record and store in memory
        labeled_prices = {}
        for mint, price_data in prices.items():
            symbol = self.token_map.get(mint, mint[:8])
            price_data['symbol'] = symbol
            labeled_prices[symbol] = price_data
            
            # Append to in-memory history (limit to last 1000 entries)
            self.price_history[symbol].append(price_data)
            if len(self.price_history[symbol]) > 1000:
                self.price_history[symbol] = self.price_history[symbol][-1000:]
        
        # --- NEW: Write this iteration's prices to SQLite in one transaction ---
        self.exporter.insert_prices_batch(labeled_prices)
        
        # Display update to terminal
        self.display_price_update(prices)
        
        logger.info(f"✅ Fetched and stored prices for {len(prices)} tokens")
    
    async def fetch_and_store_quotes(self):
        """Fetch quotes from Jupiter API, store in memory and SQLite"""
        logger.info("Fetching quotes from Jupiter API...")
        
        quotes = {}
        
        for input_symbol, output_symbol in self.quote_pairs:
            input_token = TokenRegistry.get_token(input_symbol)
            output_token = TokenRegistry.get_token(output_symbol)
            
            if not input_token or not output_token:
                logger.warning(f"Token not found for pair: {input_symbol}/{output_symbol}")
                continue
            
            amount = 1 * (10 ** input_token.decimals)
            
            quote_data = await self.api.get_quote(
                input_token.mint,
                output_token.mint,
                amount,
                slippage_bps=50
            )
            
            if quote_data:
                pair_name = f"{input_symbol}/{output_symbol}"
                quote_data['pair'] = pair_name
                quote_data['input_symbol'] = input_symbol
                quote_data['output_symbol'] = output_symbol
                
                # Append to in-memory history
                self.quote_history[pair_name].append(quote_data)
                if len(self.quote_history[pair_name]) > 1000:
                    self.quote_history[pair_name] = self.quote_history[pair_name][-1000:]
                
                quotes[pair_name] = quote_data
                
                # --- NEW: Write each quote to SQLite immediately ---
                self.exporter.insert_quote(quote_data)
                
                logger.debug(f"Got quote for {pair_name}: {quote_data['out_amount'] / (10 ** output_token.decimals):.6f}")
            else:
                logger.warning(f"Failed to get quote for {input_symbol}/{output_symbol}")
            
            await asyncio.sleep(0.3)
        
        if quotes:
            self.display_quote_update(quotes)
            logger.info(f"✅ Fetched and stored quotes for {len(quotes)} pairs")
        else:
            logger.warning("No quotes fetched this round")
    
    async def run(self, duration_minutes: int = 60):
        """
        Run the price and quote monitor.

        Args:
            duration_minutes: How long to run in minutes. Use 0 for infinite.
        """
        logger.info(f"🚀 Starting Jupiter Price & Quote Monitor...")
        logger.info(f"   Prices and quotes will be written to SQLite in real time")
        
        if duration_minutes > 0:
            logger.info(f"   Will run for {duration_minutes} minutes")
            total_iterations = (duration_minutes * 60) // self.interval
        else:
            logger.info(f"   Running indefinitely (Ctrl+C to stop)")
            total_iterations = float('inf')
        
        iteration = 0
        
        try:
            while iteration < total_iterations:
                iteration += 1
                
                print(f"\n{'='*60}")
                print(f"Iteration {iteration} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"{'='*60}")
                
                await self.fetch_and_store_prices()
                await self.fetch_and_store_quotes()
                
                # Run arbitrage detection after every fetch cycle
                await run_detection()
                
                # Print database stats every 10 iterations
                if iteration % 10 == 0:
                    stats = self.exporter.get_database_stats()
                    logger.info(f"📦 DB Stats: {stats.get('total_price_records', 0)} price records | "
                                f"{stats.get('total_quote_records', 0)} quote records")
                
                if iteration < total_iterations:
                    logger.info(f"⏳ Waiting {self.interval} seconds until next check...")
                    await asyncio.sleep(self.interval)
        
        except KeyboardInterrupt:
            logger.info("\n⚠️  Monitor stopped by user (Ctrl+C)")
        
        except Exception as e:
            logger.error(f"\n❌ Error in monitoring loop: {e}")
        
        finally:
            logger.info("\n📤 Exporting session data to Excel...")
            self.exporter.export_consolidated_price_history()
            self.exporter.export_consolidated_quotes(self.quote_history)
            self.exporter.export_combined_report(self.price_history)
            
            await self.api.close()
            
            # Final database summary
            stats = self.exporter.get_database_stats()
            
            total_price_points = sum(len(h) for h in self.price_history.values())
            total_quote_points = sum(len(h) for h in self.quote_history.values())
            
            logger.info("✅ Monitoring session complete!")
            logger.info(f"   Total iterations: {iteration}")
            logger.info(f"   Tokens monitored: {len(self.price_history)}")
            logger.info(f"   Pairs monitored: {len(self.quote_history)}")
            logger.info(f"   Price data points this session: {total_price_points}")
            logger.info(f"   Quote data points this session: {total_quote_points}")
            logger.info(f"   Total DB price records (all sessions): {stats.get('total_price_records', 0)}")
            logger.info(f"   Total DB quote records (all sessions): {stats.get('total_quote_records', 0)}")
            logger.info(f"   Database location: {self.exporter.db_path}")
            logger.info(f"   Excel files saved to: {self.exporter.output_dir}")
