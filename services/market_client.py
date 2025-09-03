# hedgefund_agent/services/market_client.py
"""
Unified HTTP client for the Market Data Service.
Handles all endpoints: prices, news, calendars, and macroeconomic data.
This module replaces the previous, separate market_client.py and news_client.py.
"""
import logging
import aiohttp
import asyncio
from typing import List, Dict, Optional, Any
from datetime import datetime

from config.settings import MARKET_DATA_SERVICE_URL

logger = logging.getLogger(__name__)

class MarketClient:
    """Unified HTTP client for all Market Data Service endpoints."""

    def __init__(self):
        self.base_url = MARKET_DATA_SERVICE_URL
        logger.info(f"📈 Unified Market Client initialized for: {self.base_url}")

    # --- Price Methods ---

    async def get_price(self, ticker: str, max_retries: int = 2) -> Optional[Dict]:
        """Get price data for a single ticker with retry logic."""
        for attempt in range(max_retries + 1):
            try:
                async with aiohttp.ClientSession() as session:
                    url = f"{self.base_url}/api/v1/prices/{ticker}"
                    async with session.get(url, timeout=45) as response:
                        if response.status == 200:
                            data = await response.json()
                            logger.debug(f"Got price for {ticker}: ${data.get('price', 0):.2f}")
                            return data
                        elif response.status == 404 and attempt < max_retries:
                            logger.warning(f"404 for {ticker}, retrying... (attempt {attempt + 1})")
                            await asyncio.sleep(2)
                            continue
                        else:
                            logger.warning(f"Market service returned {response.status} for {ticker}")
                            return None
            except Exception as e:
                logger.warning(f"Price fetch failed for {ticker}: {e}")
                return None
        return None

    async def get_bulk_prices(self, tickers: List[str]) -> Dict[str, Dict]:
        """Get prices for multiple tickers using the bulk endpoint."""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/api/v1/prices/bulk"
                payload = {"symbols": tickers}
                async with session.post(url, json=payload, timeout=1200) as response:
                    if response.status == 200:
                        data = await response.json()
                        result = {p_data.get('symbol'): p_data for p_data in data.get('data', [])}
                        logger.info(f"📊 Got bulk prices for {len(result)} tickers")
                        return result
                    else:
                        logger.warning(f"⚠️ Bulk price request failed: {response.status}")
                        return {}
        except Exception as e:
            logger.warning(f"⚠️ Bulk price request failed: {e}")
            return {}

    # --- News Methods ---

    async def get_company_news(self, symbol: str, days: int = 2) -> List[Dict[str, Any]]:
        """Get company news for a single symbol."""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/api/v1/news/company/{symbol}"
                params = {"days": days}
                async with session.get(url, params=params, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get("articles", [])
                    else:
                        logger.warning(f"News API returned {response.status} for {symbol}")
                        return []
        except Exception as e:
            logger.warning(f"Failed to get news for {symbol}: {e}")
            return []

    async def get_market_news(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get general market news."""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/api/v1/news/market"
                params = {"limit": limit, "category": "general"}
                async with session.get(url, params=params, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get("articles", [])
                    else:
                        logger.warning(f"Market news API returned {response.status}")
                        return []
        except Exception as e:
            logger.warning(f"Failed to get market news: {e}")
            return []

    async def get_news_for_symbols(self, symbols: List[str], days: int = 2) -> Dict[str, List[Dict]]:
        """Get news for multiple symbols by making individual calls."""
        if not symbols:
            return {}
        tasks = [self.get_company_news(symbol, days=days) for symbol in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        news_map = {}
        for symbol, result in zip(symbols, results):
            if isinstance(result, list) and result:
                news_map[symbol] = result # no limit
        logger.info(f"Got news for {len(news_map)} symbols.")
        return news_map

    # --- Calendar Methods ---
    
    async def get_calendar_data(self, days_ahead: int = 7) -> Dict[str, List[Dict]]:
        """Gets IPO and earnings calendar data concurrently."""
        async with aiohttp.ClientSession() as session:
            ipo_url = f"{self.base_url}/api/v1/calendar/ipo?days={days_ahead}"
            earnings_url = f"{self.base_url}/api/v1/calendar/earnings?days={days_ahead}"
            
            try:
                ipo_task = asyncio.create_task(session.get(ipo_url, timeout=30))
                earnings_task = asyncio.create_task(session.get(earnings_url, timeout=30))
                
                ipo_response, earnings_response = await asyncio.gather(ipo_task, earnings_task)
                
                calendar = {"ipo_events": [], "earnings_events": []}
                if ipo_response.status == 200:
                    calendar["ipo_events"] = (await ipo_response.json()).get("events", [])
                
                if earnings_response.status == 200:
                    calendar["earnings_events"] = (await earnings_response.json()).get("events", [])
                
                logger.info(f"Fetched {len(calendar['ipo_events'])} IPOs and {len(calendar['earnings_events'])} earnings.")
                return calendar

            except Exception as e:
                logger.error(f"Failed to get calendar data: {e}", exc_info=True)
                return {"ipo_events": [], "earnings_events": []}

    # --- Macroeconomic Methods (NEW) ---

    async def get_macro_indicators(self) -> Dict[str, Any]:
        """
        Fetches key macroeconomic indicators from the FRED endpoints concurrently.
        """
        series_names = ["cpi", "gdp", "unemployment", "fedfunds", "pmi"]
        macro_data = {}

        async with aiohttp.ClientSession() as session:
            tasks = []
            for series in series_names:
                url = f"{self.base_url}/api/v1/macro/{series}"
                tasks.append(asyncio.create_task(session.get(url, timeout=30)))

            responses = await asyncio.gather(*tasks, return_exceptions=True)

            for series, response in zip(series_names, responses):
                if isinstance(response, Exception):
                    logger.warning(f"Macro indicator '{series.upper()}' failed: {response}")
                    continue
                
                if response.status == 200:
                    macro_data[series.upper()] = await response.json()
                else:
                    logger.warning(f"Macro indicator '{series.upper()}' returned status {response.status}")
        
        logger.info(f"📈 Fetched {len(macro_data)} macroeconomic indicators.")
        return macro_data