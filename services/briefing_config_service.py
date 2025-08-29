import json
import asyncio
from typing import Dict, Any
from services.database_service import DatabaseService

class ConfigService:
    def __init__(self, db_service: DatabaseService, sentiment_config: dict):
        self.db_service = db_service
        self.sentiment_config = sentiment_config

    async def build_briefing_config(self, briefing_key: str) -> Dict[str, Any]:
        """
        Builds a config object by calling the new specific methods on the DatabaseService.
        """
        # Run the synchronous DB calls in a separate thread to avoid blocking
        briefing_def = await asyncio.to_thread(
            self.db_service.get_briefing_definition_by_key, briefing_key
        )
        if not briefing_def:
            raise ValueError(f"Briefing '{briefing_key}' not found in database.")

        linked_sections = await asyncio.to_thread(
            self.db_service.get_linked_sections_by_briefing_id, briefing_def['id']
        )
        
        market_data_sections = {}
        for row in linked_sections:
            symbols_to_use = row['custom_symbols'] if row['custom_symbols'] is not None else row['default_symbols']
            # The display_order_map is a dict in the DB, no need for json.loads
            display_map = row['display_order_map']
            market_data_sections[row['section_key']] = {
                'title': row['title'],
                'symbols': symbols_to_use,
                'display_order': [display_map[s] for s in symbols_to_use if s in display_map]
            }

        return {
            'briefing_title': briefing_def['title'],
            'publishing_config': {
                'agent_owner': briefing_def['agent_owner'],
                'notion_database_id': briefing_def['notion_database_id'],
                'notion_property_name': briefing_def['notion_property_name']
            },
            'market_data_sections': market_data_sections,
            'sentiment_config': self.sentiment_config
        }