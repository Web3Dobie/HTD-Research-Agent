# services/briefing_data_service.py

import logging
from typing import Dict, Any, List
from datetime import datetime

# Import the specialized clients this service will orchestrate
from services.news_client import NewsClient
from services.market_client import MarketClient
from services.database_service import DatabaseService
# Import the symbol configuration
from config.briefing_symbols import MORNING_BRIEFING_CONFIG

class BriefingDataService:
    def __init__(self, news_client: NewsClient, market_client: MarketClient, db_service: DatabaseService):
        """
        Initializes the service with its required dependencies for news, market data, and DB access.
        """
        self.news_client = news_client
        self.market_client = market_client
        self.db_service = db_service
        self.logger = logging.getLogger(__name__)

    async def get_morning_briefing_data(self) -> Dict[str, Any]:
        """
        Orchestrates calls to dependent services to gather all data for the morning briefing.
        """
        self.logger.info("Gathering all data for the morning briefing...")
        try:
            # 1. Get headlines from our local database via DatabaseService
            top_headlines = await self.db_service.get_top_headlines_since_midnight(limit=5)
            
            # 2. Get calendar events from the Market Data API via NewsClient
            calendar_data = await self.news_client.get_calendar_data(days_ahead=1)
            
            # 3. Get market overview blocks using the MarketClient
            market_blocks = await self.get_market_overview_blocks()
            
            # 4. Structure the final data object
            return {
                "top_headlines": [h.to_dict() for h in top_headlines],
                "ipo_calendar": calendar_data.get("ipo_events", []),
                "earnings_calendar": calendar_data.get("earnings_events", []),
                "market_blocks": market_blocks,
                "generated_at": datetime.utcnow().isoformat()
            }
        except Exception as e:
            self.logger.error(f"❌ Failed to gather briefing data: {e}", exc_info=True)
            return {"error": str(e)}

    async def get_market_overview_blocks(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Fetches price data for predefined market blocks by correctly parsing the
        nested briefing config and using a single bulk API call.
        """
        self.logger.info("Fetching market overview blocks...")
        
        all_symbols = []
        # Correctly parse the nested config structure
        sections = MORNING_BRIEFING_CONFIG.get('market_data_sections', {})
        
        # 1. Gather all symbols from the 'symbols' key in each section
        for section_name, section_config in sections.items():
            symbols_in_section = section_config.get('symbols')
            if isinstance(symbols_in_section, list):
                all_symbols.extend(symbols_in_section)
            else:
                self.logger.warning(f"Section '{section_name}' is missing a 'symbols' list.")

        unique_symbols = list(set(all_symbols))

        if not unique_symbols:
            self.logger.warning("No symbols found in briefing config to fetch prices for.")
            return {}

        # 2. Make one bulk call for all unique symbols
        self.logger.info(f"Making bulk price request for {len(unique_symbols)} unique symbols.")
        bulk_prices = await self.market_client.get_bulk_prices(unique_symbols)

        # 3. Map the results back to their original blocks
        market_data_blocks = {}
        for block_name, section_config in sections.items():
            symbols_in_block = section_config.get('symbols', [])
            # Filter the results from the bulk call for the current block
            block_data = [bulk_prices.get(symbol) for symbol in symbols_in_block if bulk_prices.get(symbol)]
            market_data_blocks[block_name] = block_data
        
        self.logger.info("Successfully fetched and structured data for market overview blocks.")
        return market_data_blocks