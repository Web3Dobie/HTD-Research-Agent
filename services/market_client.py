# hedgefund_agent/services/market_client.py
import logging
import aiohttp
from typing import Optional, Dict, List

# Import config
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import MARKET_DATA_SERVICE_URL

logger = logging.getLogger(__name__)

class MarketClient:
    """HTTP client for Market Data Service (Phase 1)"""
    
    def __init__(self):
        self.base_url = MARKET_DATA_SERVICE_URL
        logger.info(f"📈 Market client initialized: {self.base_url}")
    
    async def get_price(self, ticker: str) -> Optional[Dict]:
        """Get price data for a single ticker"""
        try:
            async with aiohttp.ClientSession() as session:
                # Correct endpoint: /prices/{symbol}
                url = f"{self.base_url}/prices/{ticker}"
                
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.debug(f"💰 Got price for {ticker}: ${data.get('price', 0):.2f}")
                        return data
                    else:
                        logger.warning(f"⚠️ Market service returned {response.status} for {ticker}")
                        return None
                        
        except Exception as e:
            logger.warning(f"⚠️ Failed to get price for {ticker}: {e}")
            return None
    
    async def get_bulk_prices(self, tickers: List[str]) -> Dict[str, Dict]:
        """Get prices for multiple tickers using bulk endpoint"""
        try:
            async with aiohttp.ClientSession() as session:
                # Use the bulk endpoint: /prices/bulk
                url = f"{self.base_url}/prices/bulk"
                payload = {
                    "symbols": tickers,
                    "include_volume": True,
                    "include_market_cap": False
                }
                
                async with session.post(url, json=payload, timeout=15) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Convert list response to dict for easier lookup
                        result = {}
                        for price_data in data.get('data', []):
                            symbol = price_data.get('symbol')
                            result[symbol] = price_data
                        
                        logger.info(f"📊 Got bulk prices for {len(result)} tickers")
                        if data.get('failed_symbols'):
                            logger.warning(f"⚠️ Failed symbols: {data['failed_symbols']}")
                        
                        return result
                    else:
                        logger.warning(f"⚠️ Bulk price request failed: {response.status}")
                        return {}
                        
        except Exception as e:
            logger.warning(f"⚠️ Bulk price request failed: {e}")
            return {}