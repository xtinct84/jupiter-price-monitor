from dataclasses import dataclass
from typing import Dict, List

@dataclass
class TokenInfo:
    symbol: str
    mint: str
    decimals: int
    name: str
    category: str  # 'high', 'mid', 'low' volume

class TokenRegistry:
    """Registry of Solana tokens organized by trading volume"""
    
    # High volume tokens
    HIGH_VOLUME_TOKENS = {
        'SOL': TokenInfo('SOL', 'So11111111111111111111111111111111111111112', 9, 'Solana', 'high'),
        'USDC': TokenInfo('USDC', 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 6, 'USD Coin', 'high'),
        'USDT': TokenInfo('USDT', 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', 6, 'Tether USD', 'high'),
        'JUP': TokenInfo('JUP', 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN', 6, 'Jupiter', 'high'),
    }
    
    # Mid volume tokens
    MID_VOLUME_TOKENS = {
        'RAY': TokenInfo('RAY', '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R', 6, 'Raydium', 'mid'),
        'BONK': TokenInfo('BONK', 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263', 5, 'Bonk', 'mid'),
        'JTO': TokenInfo('JTO', 'jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL', 9, 'Jito', 'mid'),
        'PYTH': TokenInfo('PYTH', 'HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3', 6, 'Pyth Network', 'mid'),
        'WIF': TokenInfo('WIF', 'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm', 6, 'dogwifhat', 'mid'),
        'POPCAT': TokenInfo('POPCAT', '7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr', 9, 'Popcat', 'mid'),
        'MOUTAI': TokenInfo('MOUTAI', '45EgCwcPXYagBC7KqBin4nCFg8WNdhG6ewM7ktrWmWqT', 9, 'Moutai', 'mid'),
        'MYRO': TokenInfo('MYRO', 'MyroWeY4e6kHnTbw8a14Cjc5PZFWJzBQqnd8NCZKHpq', 9, 'Myro', 'mid'),
        'WEN': TokenInfo('WEN', 'WENWENvqqNya429ubCdR81ZmD69brwQaaBYY6g3bsZg', 5, 'WEN', 'mid'),
    }
    
    # Combine all tokens
    ALL_TOKENS = {**HIGH_VOLUME_TOKENS, **MID_VOLUME_TOKENS}
    
    @classmethod
    def get_token(cls, symbol: str) -> TokenInfo:
        """Get token by symbol"""
        return cls.ALL_TOKENS.get(symbol.upper())
    
    @classmethod
    def get_token_by_mint(cls, mint: str) -> TokenInfo:
        """Get token by mint address"""
        for token in cls.ALL_TOKENS.values():
            if token.mint == mint:
                return token
        return None
    
    @classmethod
    def get_tokens_by_category(cls, category: str) -> List[TokenInfo]:
        """Get all tokens in a category (high/mid/low)"""
        return [t for t in cls.ALL_TOKENS.values() if t.category == category]
    
    @classmethod
    def get_all_mints(cls) -> List[str]:
        """Get all token mint addresses"""
        return [t.mint for t in cls.ALL_TOKENS.values()]
    
    @classmethod
    def get_high_and_mid_volume_mints(cls) -> List[str]:
        """Get high and mid volume token mints"""
        tokens = list(cls.HIGH_VOLUME_TOKENS.values()) + list(cls.MID_VOLUME_TOKENS.values())
        return [t.mint for t in tokens]
    
    @classmethod
    def get_high_and_mid_volume_tokens(cls) -> List[TokenInfo]:
        """Get high and mid volume token objects"""
        return list(cls.HIGH_VOLUME_TOKENS.values()) + list(cls.MID_VOLUME_TOKENS.values())
    
    @classmethod
    def get_all_symbols(cls) -> List[str]:
        """Get all token symbols"""
        return list(cls.ALL_TOKENS.keys())