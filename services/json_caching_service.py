import logging
from typing import List, Dict, Any
from datetime import datetime
from core.models import BriefingPayload

# A helper to create the rich_text structure the frontend expects
def _create_rich_text(text: str) -> List[Dict]:
    """Creates a simple, unformatted rich_text list object."""
    return [{"type": "text", "text": text, "annotations": {"bold": False, "italic": False, "strikethrough": False, "underline": False, "code": False, "color": "default"}, "href": None}]

# A helper to create a table from headers and data, and then sort it.
def _create_sorted_data_table(table_name: str, headers: List[str], data_rows: List[Dict]) -> Dict:
    """Creates a complete, sorted table block for the JSON cache."""
    if not data_rows:
        return None

    # Helper to clean and parse performance strings (e.g., "+1.5%", "-10 bps")
    def clean_and_parse(text: str) -> float:
        try:
            cleaned = text.lower().replace('%', '').replace('bps', '').strip()
            return float(cleaned)
        except (ValueError, TypeError):
            return -float('inf') # Return a very small number if parsing fails

    # Find the performance column to sort by (e.g., 'Change', 'Change in Yield')
    perf_column_key = next((h for h in headers if 'change' in h.lower()), headers[-1])
    perf_column_index = headers.index(perf_column_key)

    # Sort the data rows
    sorted_data_rows = sorted(data_rows, key=lambda x: clean_and_parse(x.get(perf_column_key, '')), reverse=True)

    # Build the table structure
    header_row = {
        "type": "table_row",
        "content": {"cells": [_create_rich_text(h) for h in headers]}
    }
    
    table_children = [header_row]
    for row_data in sorted_data_rows:
        table_children.append({
            "type": "table_row",
            "content": {"cells": [_create_rich_text(str(row_data.get(h, ''))) for h in headers]}
        })

    return {
        "type": "table",
        "content": {"hasColumnHeader": True},
        "children": table_children
    }

class JSONCachingService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def generate_json_from_payload(self, payload: Any, briefing_id: str, notion_page_id: str, final_website_url: str, tweet_url: str) -> Dict[str, Any]:
        """
        Takes the generated BriefingPayload and transforms it into the final
        JSON structure expected by the frontend, ready for caching.
        """
        self.logger.info(f"Generating JSON cache for briefing_id: {briefing_id}")
        
        content_blocks = []
        
        # 1. Headlines
        if payload.headlines:
            content_blocks.append({"type": "heading_2", "content": {"richText": _create_rich_text("Key Headlines")}})
            for h in payload.headlines:
                content_blocks.append({"type": "paragraph", "content": {"richText": _create_rich_text(h.get('title', ''))}})
                if h.get('commentary'):
                    content_blocks.append({"type": "quote", "content": {"richText": _create_rich_text(h['commentary'])}})
            content_blocks.append({"type": "divider", "content": {}})
        
        # 2. Market Performance Dashboard (as columns)
        # This assumes market_analysis contains keys like 'us_market_futures' which are lists of dicts
        market_tables = []
        if payload.market_analysis:
            # EU & US Futures
            eu_table = _create_sorted_data_table("EU Market Snapshot", ["Instrument", "Level", "Change"], payload.market_analysis.get('eu_market_futures', []))
            us_table = _create_sorted_data_table("US Market Futures", ["Instrument", "Level", "Change"], payload.market_analysis.get('us_market_futures', []))
            if eu_table or us_table:
                market_tables.append([
                    {"name": "EU European Market Snapshot", "table": eu_table},
                    {"name": "US U.S. Market Futures", "table": us_table}
                ])
            # Add other pairs of tables here (e.g., Forex & Interest Rates)

        # Build columns for the market tables
        for table_pair in market_tables:
            columns = []
            for item in table_pair:
                if item["table"]:
                    columns.append({
                        "type": "column",
                        "children": [
                            {"type": "heading_3", "content": {"richText": _create_rich_text(item["name"])}},
                            item["table"]
                        ]
                    })
            if columns:
                content_blocks.append({"type": "column_list", "children": columns})

        # 3. Top Movers (as columns)
        gainers_table = _create_sorted_data_table("Top Gainers", ["Symbol", "Price", "Change"], payload.top_gainers)
        losers_table = _create_sorted_data_table("Top Losers", ["Symbol", "Price", "Change"], payload.top_losers)

        if gainers_table or losers_table:
            content_blocks.append({"type": "divider", "content": {}})
            mover_columns = [
                {"type": "column", "children": [{"type": "heading_2", "content": {"richText": _create_rich_text("Top Gainers")}}, gainers_table]},
                {"type": "column", "children": [{"type": "heading_2", "content": {"richText": _create_rich_text("Top Losers")}}, losers_table]},
            ]
            content_blocks.append({"type": "column_list", "children": [c for c in mover_columns if c['children'][1]]})


        # Final JSON Assembly, matching the structure from route.ts
        final_json = {
            "id": notion_page_id,
            "title": payload.config.get('briefing_title', 'Market Briefing'),
            "period": payload.config.get('briefing_period', ''),
            "date": datetime.utcnow().strftime('%Y-%m-%d'),
            "pageUrl": final_website_url,
            "tweetUrl": tweet_url,
            "marketSentiment": payload.market_sentiment or "",
            "content": [block for block in content_blocks if block] # Filter out any None tables
        }
        
        return final_json
