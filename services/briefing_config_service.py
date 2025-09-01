# services/briefing_config_service.py (CLEAN NEW SCHEMA VERSION)
"""
ConfigService - FOCUSED on building briefing configurations only.
Uses the new clean schema (stock_universe + market_blocks_config).
No database maintenance or symbol management - those are separate services.
"""

import asyncio
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class ConfigService:
    """
    Builds briefing configurations using the new clean schema.
    Single responsibility: convert database briefing configs into usable config dicts.
    """
    
    def __init__(self, db_service, sentiment_config: dict):
        self.db_service = db_service
        self.sentiment_config = sentiment_config
        logger.info("✅ ConfigService initialized (new schema)")

    async def build_briefing_config(self, briefing_key: str) -> Dict[str, Any]:
        """
        Build briefing config using new clean schema.
        
        Args:
            briefing_key: The briefing to build (e.g. 'morning_briefing')
            
        Returns:
            Config dict with market_data_sections, publishing_config, etc.
        """
        logger.info(f"🔧 Building config for '{briefing_key}' (new schema)")
        
        # Get briefing definition
        briefing_def = await asyncio.to_thread(
            self._get_briefing_definition, briefing_key
        )
        if not briefing_def:
            raise ValueError(f"Briefing '{briefing_key}' not found")

        # Get market sections  
        market_sections = await asyncio.to_thread(
            self._get_market_sections, briefing_key
        )
        
        logger.info(f"✅ Config built: {len(market_sections)} sections")
        
        return {
            'briefing_title': briefing_def['title'],
            'publishing_config': {
                'agent_owner': briefing_def['agent_owner'],
                'notion_database_id': briefing_def['notion_database_id'],
                'notion_property_name': briefing_def['notion_property_name']
            },
            'market_data_sections': market_sections,
            'sentiment_config': self.sentiment_config
        }
    
    def _get_briefing_definition(self, briefing_key: str) -> Dict[str, Any]:
        """Get briefing definition from new table"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT id, briefing_key, title, agent_owner, 
                       notion_database_id, notion_property_name
                FROM hedgefund_agent.briefing_definitions_new 
                WHERE briefing_key = %s AND active = TRUE
            """, (briefing_key,))
            
            row = cursor.fetchone()
            if not row:
                return None
            
            return {
                'id': row[0],
                'briefing_key': row[1], 
                'title': row[2],
                'agent_owner': row[3],
                'notion_database_id': row[4],
                'notion_property_name': row[5]
            }
            
        finally:
            cursor.close()
    
    def _get_market_sections(self, briefing_key: str) -> Dict[str, Dict]:
        """Get market sections using new clean schema"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            # Simple join - no complex JSONB manipulation!
            cursor.execute("""
                SELECT 
                    mbc.block_name,
                    su.symbol,
                    su.display_name,
                    mbc.priority
                FROM hedgefund_agent.market_blocks_config mbc
                JOIN hedgefund_agent.stock_universe su ON mbc.stock_universe_id = su.id
                WHERE mbc.briefing_type = %s 
                AND mbc.active = TRUE
                AND su.active = TRUE
                ORDER BY mbc.priority, mbc.block_name
            """, (briefing_key,))
            
            rows = cursor.fetchall()
            
            # Group by block_name
            sections = {}
            for row in rows:
                block_name, symbol, display_name, priority = row
                
                if block_name not in sections:
                    sections[block_name] = {
                        'title': self._format_section_title(block_name),
                        'symbols': [],
                        'display_order': []
                    }
                
                sections[block_name]['symbols'].append(symbol)
                sections[block_name]['display_order'].append(display_name)
            
            return sections
            
        finally:
            cursor.close()
    
    def _format_section_title(self, block_name: str) -> str:
        """Convert block_name to display title"""
        title_mapping = {
            'us_futures': '🇺🇸 U.S. Market Futures',
            'european_futures': '🇪🇺 European Market Snapshot', 
            'asian_focus': '🌏 Asian Market Focus',
            'crypto': '🪙 Crypto Market',
            'fx': '💱 Forex Market',
            'rates': '💵 Interest Rates', 
            'volatility': '📉 Market Volatility',
            'commodities': '🏗️ Commodities'
        }
        
        return title_mapping.get(block_name, block_name.replace('_', ' ').title())