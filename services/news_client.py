# hedgefund_agent/services/news_client.py
"""
News client for briefings module - integrates with Market Data Service Finnhub endpoints
Replaces old utils/fetch_stock_data.py news functions
"""

import logging
import aiohttp
import asyncio
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta

from config.settings import MARKET_DATA_SERVICE_URL

logger = logging.getLogger(__name__)

class NewsClient:
    """HTTP client for Market Data Service news endpoints"""
    
    def __init__(self):
        self.base_url = MARKET_DATA_SERVICE_URL
        logger.info(f"News client initialized: {self.base_url}")
    
    async def get_company_news(self, symbol: str, days: int = 2) -> List[Dict[str, Any]]:
        """
        Get company news for a single symbol
        
        Args:
            symbol: Stock ticker symbol
            days: Days to look back
            
        Returns:
            List of news articles
        """
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/api/v1/news/company/{symbol}"
                params = {"days": days}
                
                async with session.get(url, params=params, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        articles = data.get("articles", [])
                        logger.debug(f"Got {len(articles)} news articles for {symbol}")
                        return articles
                    else:
                        logger.warning(f"News API returned {response.status} for {symbol}")
                        return []
                        
        except Exception as e:
            logger.warning(f"Failed to get news for {symbol}: {e}")
            return []
    
    async def get_market_news(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get general market news
        
        Args:
            limit: Maximum number of articles
            
        Returns:
            List of market news articles
        """
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/api/v1/news/market"
                params = {"limit": limit, "category": "general"}
                
                async with session.get(url, params=params, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        articles = data.get("articles", [])
                        logger.debug(f"Got {len(articles)} market news articles")
                        return articles
                    else:
                        logger.warning(f"Market news API returned {response.status}")
                        return []
                        
        except Exception as e:
            logger.warning(f"Failed to get market news: {e}")
            return []
    
    async def get_news_for_symbols(self, symbols: List[str], days: int = 2) -> Dict[str, List[Dict]]:
        """
        Get news for multiple symbols individually - flexible approach
        
        Args:
            symbols: List of symbols to get news for
            days: Days to look back
            
        Returns:
            Dictionary mapping symbol -> list of news articles
        """
        if not symbols:
            return {}
        
        result = {}
        
        # Make individual calls - more flexible than bulk endpoint
        for symbol in symbols:
            try:
                articles = await self.get_company_news(symbol, days=days)
                if articles:  # Only include symbols with news
                    # Limit to 2 articles per symbol for briefings
                    result[symbol] = articles[:2]
                    
            except Exception as e:
                logger.warning(f"Failed to get news for {symbol}: {e}")
                continue
        
        total_articles = sum(len(articles) for articles in result.values())
        logger.info(f"Got news for {len(result)} symbols ({total_articles} total articles)")
        return result
    
    async def get_calendar_data(self, days_ahead: int = 7) -> Dict[str, List[Dict]]:
        """
        Gets IPO and earnings calendar data concurrently and parses the response
        using the correct 'events' key.
        """
        async with aiohttp.ClientSession() as session:
            ipo_url = f"{self.base_url}/api/v1/calendar/ipo?days={days_ahead}"
            earnings_url = f"{self.base_url}/api/v1/calendar/earnings?days={days_ahead}"
            
            try:
                ipo_task = asyncio.create_task(session.get(ipo_url, timeout=60))
                earnings_task = asyncio.create_task(session.get(earnings_url, timeout=60))
                
                ipo_response, earnings_response = await asyncio.gather(ipo_task, earnings_task)
                
                ipo_events = []
                if ipo_response.status == 200:
                    ipo_data = await ipo_response.json()
                    # --- FIX: Look for the 'events' key ---
                    ipo_events = ipo_data.get("events", []) 
                    logger.info(f"Successfully fetched {len(ipo_events)} IPO events.")
                else:
                    logger.warning(f"IPO calendar API returned status {ipo_response.status}")

                earnings_events = []
                if earnings_response.status == 200:
                    earnings_data = await earnings_response.json()
                    # --- FIX: Look for the 'events' key ---
                    earnings_events = earnings_data.get("events", [])
                    logger.info(f"Successfully fetched {len(earnings_events)} earnings events.")
                else:
                    logger.warning(f"Earnings calendar API returned status {earnings_response.status}")

                return {"ipo_events": ipo_events, "earnings_events": earnings_events}

            except Exception as e:
                logger.error(f"Failed to get calendar data: {e}", exc_info=True)
                return {"ipo_events": [], "earnings_events": []}
    
    def _empty_briefing_data(self) -> Dict[str, Any]:
        """Return empty briefing data structure"""
        return {
            "news_by_symbol": {},
            "market_news": [],
            "ipo_calendar": [],
            "earnings_calendar": [],
            "request_info": {
                "generated_at": datetime.utcnow().isoformat(),
                "status": "error"
            }
        }

# Convenience functions for backward compatibility with old briefings code
async def fetch_stock_news(ticker: str, start_date: str, end_date: str) -> List[Dict]:
    """
    Backward compatibility function for old briefings code
    
    Args:
        ticker: Stock ticker
        start_date: Start date (YYYY-MM-DD) - will calculate days from this
        end_date: End date (YYYY-MM-DD) - ignored, uses days calculation
        
    Returns:
        List of news articles in old format
    """
    try:
        # Calculate days from start_date to now
        start_dt = datetime.fromisoformat(start_date)
        now = datetime.utcnow()
        days = max(1, (now - start_dt).days)
        
        client = NewsClient()
        articles = await client.get_company_news(ticker, days=days)
        
        # Convert to old format expected by briefings code
        formatted_articles = []
        for article in articles:
            formatted_articles.append({
                "headline": article.get("headline"),
                "source": article.get("source"),
                "date": article.get("timestamp"),
                "url": article.get("url"),
                "summary": article.get("summary")
            })
        
        logger.info(f"Legacy fetch_stock_news: {len(formatted_articles)} articles for {ticker}")
        return formatted_articles
        
    except Exception as e:
        logger.error(f"Legacy fetch_stock_news failed for {ticker}: {e}")
        return []
    
    def _empty_briefing_data(self) -> Dict[str, Any]:
        """Return empty briefing data structure"""
        return {
            "news_by_symbol": {},
            "market_news": [],
            "ipo_calendar": [],
            "earnings_calendar": [],
            "request_info": {
                "generated_at": datetime.utcnow().isoformat(),
                "status": "error"
            }
        }

# Convenience functions for backward compatibility with old briefings code
async def fetch_stock_news(ticker: str, start_date: str, end_date: str) -> List[Dict]:
    """
    Backward compatibility function for old briefings code
    
    Args:
        ticker: Stock ticker
        start_date: Start date (YYYY-MM-DD) - will calculate days from this
        end_date: End date (YYYY-MM-DD) - ignored, uses days calculation
        
    Returns:
        List of news articles in old format
    """
    try:
        # Calculate days from start_date to now
        start_dt = datetime.fromisoformat(start_date)
        now = datetime.utcnow()
        days = max(1, (now - start_dt).days)
        
        client = NewsClient()
        articles = await client.get_company_news(ticker, days=days)
        
        # Convert to old format expected by briefings code
        formatted_articles = []
        for article in articles:
            formatted_articles.append({
                "headline": article.get("headline"),
                "source": article.get("source"),
                "date": article.get("timestamp"),
                "url": article.get("url"),
                "summary": article.get("summary")
            })
        
        logger.info(f"Legacy fetch_stock_news: {len(formatted_articles)} articles for {ticker}")
        return formatted_articles
        
    except Exception as e:
        logger.error(f"Legacy fetch_stock_news failed for {ticker}: {e}")
        return []