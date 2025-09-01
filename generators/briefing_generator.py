from core.models import BriefingPayload
from services.market_sentiment_service import ComprehensiveMarketSentimentService
from services.briefing_config_service import ConfigService
from services.market_client import MarketClient
from services.prompt_augmentation_service import PromptAugmentationService
from services.gpt_service import GPTService
from typing import Dict, List
import asyncio
import logging

logger = logging.getLogger(__name__)

class BriefingGenerator:
    """
    Orchestrates the complete briefing generation by fetching and assembling all data,
    then passing it to the appropriate services for analysis and publishing.
    """
    
    def __init__(self, config_service: ConfigService, sentiment_service: ComprehensiveMarketSentimentService, market_client: MarketClient, db_service, gpt_service: GPTService, prompt_augmentation_service: PromptAugmentationService):
        self.config_service = config_service
        self.sentiment_service = sentiment_service
        self.market_client = market_client
        self.db_service = db_service # For fetching headlines
        self.gpt_service = gpt_service # For commenting on the headlines
        self.prompt_augmentation_service = prompt_augmentation_service

    async def create(self, briefing_key: str) -> BriefingPayload:
        """
        Orchestrates the complete briefing generation following the 6-step flow.
        """
        logger.info(f"BriefingGenerator: Starting orchestration for '{briefing_key}'...")
        
        try:
            # Step 1: Get briefing configuration
            config = await self.config_service.build_briefing_config(briefing_key)
            market_sections = config.get('market_data_sections', {})
            logger.info(f"Step 1: Retrieved config with {len(market_sections)} market sections")
            
            # Step 2: Fetch all raw market data for configured symbols
            raw_market_data = await self._fetch_all_market_data(market_sections)
            logger.info(f"Step 2: Successfully processed market data for {len(raw_market_data)} sections")
            
            # Step 3-5: Fetch data in parallel, now including macro data
            macro_task = self.market_client.get_macro_indicators()
            calendar_task = self.market_client.get_calendar_data(days_ahead=3)
            headlines_task = self.db_service.get_top_headlines_since_midnight(limit=10)
            
            macro_data, calendar_data, top_headlines = await asyncio.gather(
                macro_task, calendar_task, headlines_task
            )
            
            logger.info(f"DEBUG: Macro data received by BriefingGenerator: {macro_data}")

            # Generate commentary for each headline
            if top_headlines:
                logger.info(f"Generating AI commentary for {len(top_headlines)} headlines...")
                commentary_tasks = []
                for headline_obj in top_headlines:
                    # Use the existing method from gpt_service
                    task = asyncio.to_thread(
                        self.gpt_service.generate_institutional_comment,
                        headline=headline_obj.headline,
                        category=headline_obj.category or 'general'
                    )
                    commentary_tasks.append(task)
                
                # Run all commentary generation concurrently
                commentaries = await asyncio.gather(*commentary_tasks)
                
                # Attach the new commentary to each headline object
                for headline_obj, commentary_text in zip(top_headlines, commentaries):
                    headline_obj.commentary = commentary_text

            # NEW: Create the reusable context block
            context_block = self.prompt_augmentation_service.create_context_block(
                macro_data=macro_data,
                headlines=top_headlines
            )

            # Pass the context block to the sentiment service
            market_analysis = await self.sentiment_service.analyze_market_sentiment(
                raw_market_data=raw_market_data, 
                briefing_config=config,
                factual_context=context_block # <-- Pass the new context
            )
            
            # Step 6: Build complete payload
            payload = BriefingPayload(
                market_analysis=market_analysis,
                raw_market_data=raw_market_data,
                earnings_calendar=calendar_data.get('earnings_events', []),
                ipo_calendar=calendar_data.get('ipo_events', []),
                top_headlines=top_headlines,
                config=config
            )
            
            logger.info(f"Step 6: BriefingPayload assembled successfully for '{briefing_key}'")
            return payload
            
        except Exception as e:
            logger.error(f"BriefingGenerator orchestration failed for '{briefing_key}': {e}", exc_info=True)
            raise

    async def _fetch_all_market_data(self, market_sections: Dict) -> Dict[str, List[Dict]]:
        """
        Makes a single bulk request for all symbols and parses the direct
        symbol-to-data dictionary response from the microservice.
        """
        raw_market_data = {}
        all_symbols = list(set([symbol for section in market_sections.values() for symbol in section.get('symbols', [])]))
        
        if not all_symbols:
            return {}

        logger.info(f"Fetching market data for {len(all_symbols)} total symbols...")
        try:
            # This response is the dictionary of all price data, keyed by symbol
            price_data_map = await self.market_client.get_bulk_prices(all_symbols)
            
            # Reconstruct the data for each section using the map
            for section_name, section_config in market_sections.items():
                section_data_list = []
                for symbol in section_config.get('symbols', []):
                    # --- THIS IS THE KEY FIX ---
                    # Directly look up the symbol in the response dictionary
                    symbol_info = price_data_map.get(symbol)
                    
                    if symbol_info and symbol_info.get('price', 0) > 0:
                        section_data_list.append(symbol_info)
                
                if section_data_list:
                    raw_market_data[section_name] = section_data_list
            
            return raw_market_data
            
        except Exception as e:
            logger.error(f"Failed to fetch or process bulk market data: {e}", exc_info=True)
            return {}