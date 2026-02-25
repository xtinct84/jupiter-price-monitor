import httpx
import asyncio
from typing import Dict, List, Optional
from datetime import datetime
import logging
import json
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class JupiterAPI:
    """
    Unified Jupiter API client:
    - Jupiter Price API v3 for prices (requires x-api-key)
    - Jupiter Quote API v1 for quotes (requires x-api-key)
    """
    
    def __init__(self):
        self.api_key = os.getenv('JUPITER_API_KEY')
        if not self.api_key:
            logger.warning("No JUPITER_API_KEY found - prices and quotes will not work")
        else:
            logger.info(f"✅ API key loaded ({len(self.api_key)} chars)")
        
        # Jupiter API endpoints
        self.jupiter_base_url = "https://api.jup.ag"
        
        # Setup Jupiter client with x-api-key header
        if self.api_key:
            self.jupiter_headers = {
                "x-api-key": self.api_key,
                "Accept": "application/json"
            }
            self.jupiter_client = httpx.AsyncClient(
                timeout=30.0,
                headers=self.jupiter_headers
            )
        else:
            self.jupiter_client = httpx.AsyncClient(timeout=30.0)
        
        logger.info("✅ Jupiter API client initialized")
        logger.info("   📊 Prices: Jupiter Price API v3")
        logger.info("   💱 Quotes: Jupiter Swap API v1")
    
    async def get_price(self, token_id: str) -> Optional[Dict]:
        """
        Get price from Jupiter Price API v3 (requires x-api-key)
        
        Args:
            token_id: Token mint address
            
        Returns:
            Price data dictionary
        """
        if not self.api_key:
            logger.error("Cannot get price - no API key configured")
            return None
        
        try:
            response = await self.jupiter_client.get(
                f"{self.jupiter_base_url}/price/v3",
                params={
                    "ids": token_id
                }
            )
            response.raise_for_status()
            data = response.json()
            
            if not data or token_id not in data:
                return None
            
            price_info = data[token_id]
            
            # FIX: Use 'usdPrice' instead of 'price'
            if 'usdPrice' not in price_info:
                logger.warning(f"No 'usdPrice' in response for {token_id[:8]}")
                return None
            
            return {
                'token_id': token_id,
                'price_usd': float(price_info.get('usdPrice', 0)),
                'timestamp': datetime.now(),
                'extra_info': {
                    'vs_token': price_info.get('vsToken', ''),
                    'vs_token_symbol': price_info.get('vsTokenSymbol', ''),
                    'provider': price_info.get('provider', ''),
                    'price_change_24h': float(price_info.get('priceChange24h', 0)),
                    'confidence': float(price_info.get('confidence', 0)),
                    'block_id': price_info.get('blockId'),
                    'decimals': price_info.get('decimals')
                }
            }
            
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching price for {token_id[:8]}: {e.response.status_code} - {e.response.text}")
            return None
        except Exception as e:
            logger.error(f"Error fetching price for {token_id[:8]}: {e}")
            return None
    
    async def get_multiple_prices(self, token_ids: List[str]) -> Dict[str, Dict]:
        """
        Get prices for multiple tokens from Jupiter Price API v3
        
        Args:
            token_ids: List of token mint addresses
            
        Returns:
            Dictionary mapping token_id to price data
        """
        if not self.api_key:
            logger.error("Cannot get prices - no API key configured")
            return {}
        
        # Jupiter API allows batch requests
        try:
            # Create comma-separated list of token IDs
            ids_param = ",".join(token_ids)
            logger.debug(f"Requesting price data for IDs: {ids_param}")

            response = await self.jupiter_client.get(
                f"{self.jupiter_base_url}/price/v3",
                params={
                    "ids": ids_param
                }
            )
            response.raise_for_status()
            data = response.json()
            
            # Log raw response for debugging
            logger.debug(f"Raw API response: {json.dumps(data, indent=2)}")
            
            results = {}
            for token_id in token_ids:
                if token_id in data:
                    price_info = data[token_id]
                    
                    # FIX: Use 'usdPrice' instead of 'price'
                    if 'usdPrice' in price_info:
                        results[token_id] = {
                            'token_id': token_id,
                            'price_usd': float(price_info['usdPrice']),
                            'timestamp': datetime.now(),
                            'extra_info': {
                                'vs_token': price_info.get('vsToken', ''),
                                'vs_token_symbol': price_info.get('vsTokenSymbol', ''),
                                'provider': price_info.get('provider', ''),
                                'price_change_24h': float(price_info.get('priceChange24h', 0)),
                                'confidence': float(price_info.get('confidence', 0)),
                                'block_id': price_info.get('blockId'),
                                'decimals': price_info.get('decimals')
                            }
                        }
                        logger.debug(f"Price for {token_id[:8]}: ${price_info['usdPrice']}")
                    else:
                        logger.warning(f"Token {token_id[:8]} exists in response but has no 'usdPrice' key")
                        results[token_id] = {
                            'token_id': token_id,
                            'price_usd': 0.0,
                            'timestamp': datetime.now(),
                            'extra_info': {
                                'error': 'No usdPrice key in response',
                                'likely_cause': 'API response format issue'
                            }
                        }
                else:
                    logger.warning(f"No price data returned for token: {token_id[:8]}")
                    results[token_id] = {
                        'token_id': token_id,
                        'price_usd': 0.0,
                        'timestamp': datetime.now(),
                        'extra_info': {
                            'error': 'No price data available from Jupiter API',
                            'likely_cause': 'Token not traded recently or flagged by heuristics'
                        }
                    }
            
            valid_prices_count = sum(1 for r in results.values() if r['price_usd'] > 0)
            logger.info(f"Processed {len(results)} tokens, {valid_prices_count} with valid prices")
            return results
            
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching multiple prices: {e.response.status_code} - {e.response.text}")
            return {}
        except Exception as e:
            logger.error(f"Error fetching multiple prices: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {}
    
    async def get_quote(self, input_mint: str, output_mint: str, amount: int,
                       slippage_bps: int = 50) -> Optional[Dict]:
        """
        Get swap quote using Jupiter Swap API v1
        
        Endpoint: GET https://api.jup.ag/swap/v1/quote
        
        Args:
            input_mint: Input token mint address
            output_mint: Output token mint address
            amount: Amount in smallest unit (lamports for SOL)
            slippage_bps: Slippage tolerance in basis points (50 = 0.5%)
            
        Returns:
            Quote data with route information
        """
        if not self.api_key:
            logger.error("Cannot get quote - no API key configured")
            return None
        
        try:
            response = await self.jupiter_client.get(
                f"{self.jupiter_base_url}/swap/v1/quote",
                params={
                    "inputMint": input_mint,
                    "outputMint": output_mint,
                    "amount": str(amount),
                    "slippageBps": str(slippage_bps),
                    "swapMode": "ExactIn",
                    "restrictIntermediateTokens": "true",
                    "maxAccounts": "64"
                }
            )
            response.raise_for_status()
            quote_data = response.json()
            
            return {
                'input_mint': input_mint,
                'output_mint': output_mint,
                'in_amount': int(quote_data['inAmount']),
                'out_amount': int(quote_data['outAmount']),
                'price_impact_pct': float(quote_data.get('priceImpactPct', 0)),
                'slippage_bps': slippage_bps,
                'route_plan': quote_data.get('routePlan', []),
                'timestamp': datetime.now()
            }
            
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching quote: {e.response.status_code} - {e.response.text}")
            return None
        except Exception as e:
            logger.error(f"Error fetching quote: {e}")
            return None
    
    async def close(self):
        """Close the HTTP clients"""
        await self.jupiter_client.aclose()
        logger.info("API client closed")