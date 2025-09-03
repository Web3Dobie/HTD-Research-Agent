from core.models import BriefingPayload, Headline
from services.market_sentiment_service import ComprehensiveMarketSentimentService
from services.briefing_config_service import ConfigService
from services.market_client import MarketClient
from services.prompt_augmentation_service import PromptAugmentationService
from services.gpt_service import GPTService
from typing import Dict, List
from datetime import datetime, timedelta, time, timezone
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
        self.logger = logging.getLogger(__name__)

    async def create(self, briefing_key: str) -> BriefingPayload:
        """
        Orchestrates briefing generation with custom logic based on the briefing_key.
        """
        self.logger.info(f"BriefingGenerator: Starting orchestration for '{briefing_key}'...")

        # Step 1: Get static config and initialize data structures
        config = await self.config_service.build_briefing_config(briefing_key)
        market_sections = config.get('market_data_sections', {})
        raw_market_data = {}
        top_headlines = []
        news_by_symbol = {}
        top_gainers = []
        top_losers = []

        # Step 2: Handle briefing-specific dynamic data and headline sources
        if briefing_key in ['pre_market_briefing', 'us_close_briefing']:
            top_gainers, top_losers = await self._fetch_and_process_top_movers()
            if top_gainers:
                raw_market_data['top_gainers'] = top_gainers
                market_sections['top_gainers'] = {'title': '📈 Top 5 Gainers', 'symbols': [g['symbol'] for g in top_gainers]}
            if top_losers:
                raw_market_data['top_losers'] = top_losers
                market_sections['top_losers'] = {'title': '📉 Top 5 Losers', 'symbols': [l['symbol'] for l in top_losers]}

            target_symbols = market_sections.get('top_gainers', {}).get('symbols', []) + \
                             market_sections.get('top_losers', {}).get('symbols', [])
            if target_symbols:
                news_by_symbol = await self.market_client.get_news_for_symbols(target_symbols)
        
        elif briefing_key == 'eu_close_briefing':
            since_time = time(7, 0)
            start_datetime = datetime.combine(datetime.now(timezone.utc).date(), since_time, tzinfo=timezone.utc)
            top_headlines = await self.db_service.get_top_headlines(since_datetime=start_datetime, limit=10)
        
        else: # Default for morning_briefing
            start_datetime = datetime.combine(datetime.now(timezone.utc).date(), time.min, tzinfo=timezone.utc)
            top_headlines = await self.db_service.get_top_headlines(since_datetime=start_datetime, limit=10)

        # Step 3: Fetch all remaining data in parallel
        static_symbols = [s for section, cfg in market_sections.items() if section not in ['top_gainers', 'top_losers'] for s in cfg.get('symbols', [])]
        prices_task = self.market_client.get_bulk_prices(static_symbols)
        calendar_task = self.market_client.get_calendar_data(days_ahead=3)
        macro_task = self.market_client.get_macro_indicators()
        
        static_data, calendar_data, macro_data = await asyncio.gather(prices_task, calendar_task, macro_task)

        # Step 4: Assemble all raw market data
        for section_name, section_config in market_sections.items():
            if section_name not in ['top_gainers', 'top_losers']:
                section_data = [static_data.get(s) for s in section_config.get('symbols', []) if static_data.get(s)]
                if section_data:
                    raw_market_data[section_name] = section_data
        
        # Step 5: Generate AI commentary for the morning and EU Close briefings
        if top_headlines and briefing_key in ['morning_briefing', 'eu_close_briefing']:
            self.logger.info(f"Generating AI commentary for {len(top_headlines)} headlines...")
            commentary_tasks = [
                asyncio.to_thread(self.gpt_service.generate_institutional_comment, headline=h.headline, category=getattr(h, 'category', 'general'))
                for h in top_headlines
            ]
            commentaries = await asyncio.gather(*commentary_tasks)
            for headline_obj, commentary_text in zip(top_headlines, commentaries):
                headline_obj.commentary = commentary_text
        
        # Step 6: Create the factual context block for the main AI summary
        context_block = self.prompt_augmentation_service.create_context_block(
            macro_data=macro_data,
            headlines=top_headlines
        )

        # Step 7: Run final sentiment analysis
        sentiment_market_data = {
            section_name: section_data 
            for section_name, section_data in raw_market_data.items() 
            if section_name not in ['top_gainers', 'top_losers']
        }

        self.logger.info(f"Sentiment analysis using {len(sentiment_market_data)} sections (excluding top movers)")

        market_analysis = await self.sentiment_service.analyze_market_sentiment(
            raw_market_data=sentiment_market_data,  # ← Filtered data without top movers
            briefing_config=config,
            factual_context=context_block
        )

        # Step 8: Build the final payload
        payload = BriefingPayload(
            market_analysis=market_analysis, raw_market_data=raw_market_data,
            earnings_calendar=calendar_data.get('earnings_events', []),
            ipo_calendar=calendar_data.get('ipo_events', []),
            top_headlines=top_headlines,
            top_gainers=top_gainers,                 
            top_losers=top_losers,
            stock_specific_news=news_by_symbol,
            config=config
        )
        
        self.logger.info(f"BriefingPayload assembled successfully for '{briefing_key}'")
        return payload

    async def _fetch_and_process_top_movers(self) -> tuple[List[Dict], List[Dict]]:
        """
        Fetches prices for all equities, de-duplicates by EPIC to find the
        true top 5 gainers and losers, and returns their primary symbols.
        """
        self.logger.info("Fetching and processing top market movers...")
        
        # 1. Get all equity symbols with their metadata
        symbols_meta = self.db_service.get_all_equity_symbols()
        if not symbols_meta:
            self.logger.warning("No equity symbols found for top movers screening.")
            return [], []

        symbol_to_meta = {item['symbol']: item for item in symbols_meta}
        all_symbols = list(symbol_to_meta.keys())

        # 2. Get bulk prices for all symbols
        price_data = await self.market_client.get_bulk_prices(all_symbols)
        if not price_data:
            self.logger.error("Failed to fetch bulk prices for top movers screening.")
            return [], []

        # 3. Group results by EPIC and find the best performing symbol data for each EPIC
        best_performer_by_epic = {}
        for symbol, data in price_data.items():
            if 'change_percent' not in data: continue

            meta = symbol_to_meta.get(symbol)
            if not meta or not meta.get('epic'): continue
            
            epic = meta['epic']
            
            # If we haven't seen this EPIC, or if this symbol is a better performer, store its data
            if epic not in best_performer_by_epic or \
               abs(data['change_percent']) > abs(best_performer_by_epic[epic]['change_percent']):
                best_performer_by_epic[epic] = data

        # 4. For each EPIC, determine its primary symbol.
        # This handles non-duplicated symbols by treating them as their own primary.
        primary_symbols_by_epic = {}
        epics_to_symbols_meta = {}
        for item in symbols_meta:
            epic = item.get('epic')
            if epic:
                if epic not in epics_to_symbols_meta:
                    epics_to_symbols_meta[epic] = []
                epics_to_symbols_meta[epic].append(item)
        
        for epic, symbol_meta_list in epics_to_symbols_meta.items():
            # Default to the first symbol if no primary is explicitly set
            primary_symbol = symbol_meta_list[0]['symbol']
            for meta_item in symbol_meta_list:
                if meta_item.get('is_primary_symbol'):
                    primary_symbol = meta_item['symbol']
                    break # Found the explicit primary
            primary_symbols_by_epic[epic] = primary_symbol


        # 5. Build the final list using the determined primary symbol for each top-performing EPIC
        final_movers = []
        for epic, performance_data in best_performer_by_epic.items():
            primary_symbol = primary_symbols_by_epic.get(epic)
            
            if primary_symbol and primary_symbol in price_data:
                # Get the data for the primary symbol (name, price, etc.)
                primary_symbol_data = price_data[primary_symbol].copy()
                # But overwrite its performance with the best performance from its EPIC group
                primary_symbol_data['change_percent'] = performance_data['change_percent']
                primary_symbol_data['change'] = performance_data.get('change', 0)
                # Keep the original symbol for logging/debugging if needed
                primary_symbol_data['original_best_performer_symbol'] = performance_data['symbol']
                final_movers.append(primary_symbol_data)

        # 6. Sort the final, de-duplicated list
        sorted_movers = sorted(final_movers, key=lambda x: x['change_percent'], reverse=True)
        
        top_gainers = sorted_movers[:5]
        top_losers = sorted_movers[-5:]
        top_losers.reverse()
        
        self.logger.info(f"Identified Top 5 Gainers and Losers after de-duplicating by EPIC.")
        return top_gainers, top_losers
    
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