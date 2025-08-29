import json
from typing import Dict, Any

class ConfigService:
    """
    Builds briefing configurations by fetching definitions dynamically
    from the PostgreSQL database.
    """
    def __init__(self, db_connection):
        """
        Initializes the service with an active database connection.
        
        Args:
            db_connection: An asyncpg connection object (or similar).
        """
        self.db = db_connection

    async def build_briefing_config(self, briefing_key: str) -> Dict[str, Any]:
        """
        Assembles a complete, ready-to-use briefing configuration object
        for a given briefing key (e.g., 'morning_briefing').
        """
        # 1. Fetch the core briefing definition
        briefing_def = await self.db.fetchrow(
            "SELECT * FROM hedgefund_agent.briefing_definitions WHERE briefing_key = $1", 
            briefing_key
        )
        if not briefing_def:
            raise ValueError(f"Briefing '{briefing_key}' not found in database.")

        # 2. Fetch all market sections linked to this briefing
        linked_sections = await self.db.fetch(
            """
            SELECT 
                ms.section_key, ms.title, ms.default_symbols, ms.display_order_map,
                bs.custom_symbols
            FROM hedgefund_agent.briefing_sections bs
            JOIN hedgefund_agent.market_sections ms ON bs.section_id = ms.id
            WHERE bs.briefing_id = $1
            ORDER BY ms.id
            """, briefing_def['id']
        )

        market_data_sections = {}
        for row in linked_sections:
            # Use custom symbols if provided, otherwise use the default list
            symbols_to_use = row['custom_symbols'] if row['custom_symbols'] else row['default_symbols']
            
            # The display_order_map is a JSON string in the DB, so we parse it
            display_map = json.loads(row['display_order_map'])

            market_data_sections[row['section_key']] = {
                'title': row['title'],
                'symbols': symbols_to_use,
                'display_order': [display_map[s] for s in symbols_to_use if s in display_map]
            }
        
        # 3. Assemble the final config dictionary in the expected format
        return {
            'briefing_title': briefing_def['title'],
            'publishing_config': {
                'agent_owner': briefing_def['agent_owner'],
                'notion_database_id': briefing_def['notion_database_id'],
                'notion_property_name': briefing_def['notion_property_name']
            },
            'market_data_sections': market_data_sections
            'sentiment_config': self.sentiment_config
        }