# services/enrichment_service.py
import logging
import re
import asyncio
from typing import List, Union, Tuple, Dict

# Adjust the path if your project structure is different
from core.models import MarketData
from services.market_client import MarketClient

logger = logging.getLogger(__name__)

class MarketDataEnrichmentService:
    """
    A dedicated service to enrich text with real-time market data for cashtags.
    It centralizes the logic for fetching and formatting price data.
    """
    def __init__(self, market_client: MarketClient):
        self.market_client = market_client
        self.RETRY_ATTEMPTS = 2
        self.RETRY_DELAY_SECONDS = 2.5
        self.ENRICHMENT_TIMEOUT_SECONDS = 30

    async def enrich_content(self, content: Union[str, List[str]]) -> Tuple[Union[str, List[str]], List[MarketData]]:
        """
        Primary public method to enrich content.
        Accepts a single string or a list of strings and returns data in the same format.
        """
        is_list = isinstance(content, list)
        text_parts = content if is_list else [content]

        try:
            enriched_parts, market_data = await asyncio.wait_for(
                self._fetch_and_replace_data(text_parts),
                timeout=self.ENRICHMENT_TIMEOUT_SECONDS
            )
            
            final_content = enriched_parts if is_list else enriched_parts[0]
            return final_content, market_data

        except asyncio.TimeoutError:
            logger.error(f"🚨 Enrichment timed out after {self.ENRICHMENT_TIMEOUT_SECONDS}s. Returning original content.")
            return content, []
        except Exception as e:
            logger.error(f"🚨 An unexpected error occurred during enrichment: {e}", exc_info=True)
            return content, []

    async def _fetch_and_replace_data(self, text_parts: List[str]) -> Tuple[List[str], List[MarketData]]:
        # 1. Extract all unique cashtags from all parts
        all_text = " ".join(text_parts)
        cashtags = set(re.findall(r'\$([A-Z]{1,5})\b', all_text))
        valid_tickers = sorted(list(cashtags))

        if not valid_tickers:
            return text_parts, []

        # 2. Fetch prices in bulk with retries
        prices = await self._get_all_prices_robustly(valid_tickers)
        
        if not prices:
            logger.warning("⚠️ No price data was fetched, returning original content.")
            return text_parts, []

        # 3. Create enriched text and market data objects
        enriched_parts = []
        market_data_objects = list(prices.values())

        for part in text_parts:
            enriched_part = part
            for ticker_symbol, data in prices.items():
                cashtag = f"${ticker_symbol}"
                # Format: $AAPL ($150.25, +1.25%)
                enriched_format = f"{cashtag} (${data.price:.2f}, {data.change_percent:+.2f}%)"
                # Use regex for safe, whole-word-only replacement
                pattern = rf"{re.escape(cashtag)}(?![a-zA-Z0-9])"
                enriched_part = re.sub(pattern, enriched_format, enriched_part)
            enriched_parts.append(enriched_part)

        return enriched_parts, market_data_objects

    # services/enrichment_service.py

    # services/enrichment_service.py

    async def _get_all_prices_robustly(self, tickers: List[str]) -> Dict[str, MarketData]:
        """
        Fetches prices for a list of tickers with a "best-effort" approach.
        It retries for missing tickers but will return any data it successfully fetches.
        """
        prices: Dict[str, MarketData] = {}
        valid_market_data_keys = MarketData.__dataclass_fields__.keys()

        for attempt in range(self.RETRY_ATTEMPTS):
            try:
                # On each attempt, only fetch tickers that we haven't already found.
                tickers_to_fetch = [t for t in tickers if t not in prices]
                if not tickers_to_fetch:
                    break  # Exit early if we've already found everything.

                bulk_prices = await self.market_client.get_bulk_prices(tickers_to_fetch)
                
                for ticker_symbol, data_dict in bulk_prices.items():
                    if data_dict and data_dict.get('price', 0) > 0 and ticker_symbol not in prices:
                        if 'symbol' in data_dict:
                            data_dict['ticker'] = data_dict.pop('symbol')

                        sanitized_data = {
                            key: value for key, value in data_dict.items() 
                            if key in valid_market_data_keys
                        }
                        prices[ticker_symbol] = MarketData(**sanitized_data)

                # If we have all prices, we can stop retrying.
                if len(prices) == len(tickers):
                    logger.info(f"✅ Fetched all {len(tickers)} prices on attempt {attempt + 1}.")
                    break

                logger.info(f"Attempt {attempt + 1}: Fetched {len(prices)}/{len(tickers)} prices so far.")
                if attempt < self.RETRY_ATTEMPTS - 1:
                    await asyncio.sleep(self.RETRY_DELAY_SECONDS)

            except Exception as e:
                logger.error(f"❌ Error on attempt {attempt + 1} fetching prices: {e}")
                if attempt < self.RETRY_ATTEMPTS - 1:
                    await asyncio.sleep(self.RETRY_DELAY_SECONDS)
        
        # After all retries, log the final status.
        if not prices:
             logger.error(f"🚨 Failed to fetch any prices for {tickers} after {self.RETRY_ATTEMPTS} attempts.")
        elif len(prices) < len(tickers):
            missing = set(tickers) - set(prices.keys())
            # Log as a WARNING, not an error, since partial data is now acceptable.
            logger.warning(f"⚠️ Could not fetch all prices. Got {len(prices)}/{len(tickers)}. Missing: {missing}")
        
        # Return whatever we managed to get.
        return prices