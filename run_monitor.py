import asyncio
import os
import sys
from dotenv import load_dotenv
from price_monitor import PriceMonitor

load_dotenv()

def check_api_key():
    """Check if API key is configured"""
    api_key = os.getenv('JUPITER_API_KEY')
    if not api_key or api_key == 'your_jupiter_api_key_here':
        print("\n❌ ERROR: Jupiter API key not configured!")
        print("Please follow these steps:")
        print("1. Get your API key from https://station.jup.ag/")
        print("2. Create or update your .env file with:")
        print("   JUPITER_API_KEY=your_actual_api_key_here")
        print("3. Add MONITOR_INTERVAL_SECONDS=30 (optional)")
        print("\nExample .env file content:")
        print("JUPITER_API_KEY=abc123def456...")
        print("MONITOR_INTERVAL_SECONDS=30")
        return False
    return True

async def main():
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║                                                              ║
    ║           SOLANA DEX PRICE MONITORING BOT                   ║
    ║                   Jupiter API v2/v3                         ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    # Check for API key
    if not check_api_key():
        sys.exit(1)
    
    # Configuration
    interval = int(os.getenv('MONITOR_INTERVAL_SECONDS', 30))
    duration = int(os.getenv('MONITOR_DURATION_MINUTES', 60))
    
    # Display configuration
    print(f"⚙️  Configuration:")
    print(f"   Check interval: {interval} seconds")
    print(f"   Duration: {duration} minutes")
    print(f"   Output folder: price_history/")
    print()
    
    # Display monitored tokens
    from token_registry import TokenRegistry
    high_volume = TokenRegistry.get_tokens_by_category('high')
    mid_volume = TokenRegistry.get_tokens_by_category('mid')
    
    print(f"📊 Monitored Tokens ({len(high_volume) + len(mid_volume)} total):")
    print(f"   High Volume ({len(high_volume)}): {', '.join([t.symbol for t in high_volume])}")
    print(f"   Mid Volume ({len(mid_volume)}): {', '.join([t.symbol for t in mid_volume])}")
    print()
    
    # Display monitored pairs
    from price_monitor import PriceMonitor
    monitor = PriceMonitor(interval_seconds=interval)
    print(f"💱 Monitored Trading Pairs ({len(monitor.quote_pairs)}):")
    for pair in monitor.quote_pairs:
        print(f"   {pair[0]}/{pair[1]}")
    print()
    
    # Ask for confirmation
    print("="*80)
    confirm = input("Press Enter to start monitoring (or type 'exit' to quit): ")
    if confirm.lower() == 'exit':
        print("Exiting...")
        return
    
    print("\n🚀 Starting Jupiter DEX Monitor...")
    print("   Press Ctrl+C to stop at any time")
    print("="*80)
    
    # Create and run monitor
    monitor = PriceMonitor(interval_seconds=interval)
    await monitor.run(duration_minutes=duration)
    
    print("\n" + "="*80)
    print("✅ Monitoring Complete!")
    print("   Excel files created in price_history/ folder")
    print("="*80)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Program interrupted by user")
        print("Exiting gracefully...")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()