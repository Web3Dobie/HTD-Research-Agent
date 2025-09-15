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

    def generate_json_from_payload(self, payload: BriefingPayload, briefing_id: int, notion_page_id: str, final_website_url: str, tweet_url: str) -> Dict[str, Any]:
        """
        FIXED: Takes the generated BriefingPayload and transforms it into the final
        JSON structure expected by the frontend, ready for caching.
        Now uses the correct BriefingPayload structure.
        """
        self.logger.info(f"Generating JSON cache for briefing_id: {briefing_id}")
        
        content_blocks = []
        
        # 1. Market Sentiment Header
        if payload.market_analysis:
            sentiment_emoji = {
                "BULLISH": "🐂", "BEARISH": "🐻", 
                "NEUTRAL": "📊", "MIXED": "⚖️"
            }.get(payload.market_analysis.sentiment.value, "📊")
            
            content_blocks.append({
                "type": "heading_1", 
                "content": {"richText": _create_rich_text(f"Global Market Sentiment: {payload.market_analysis.sentiment.value} {sentiment_emoji}")}
            })
            
            if payload.market_analysis.market_summary:
                content_blocks.append({
                    "type": "quote", 
                    "content": {"richText": _create_rich_text(payload.market_analysis.market_summary)}
                })
        
        # 2. Market Performance Dashboard
        content_blocks.append({
            "type": "heading_2", 
            "content": {"richText": _create_rich_text("Market Performance Dashboard")}
        })
        
        if payload.raw_market_data:
            # Create sections for each market data category (excluding top movers)
            for section_name, section_data in payload.raw_market_data.items():
                if section_name in ['top_gainers', 'top_losers']:
                    continue  # Handle these separately
                    
                if section_data:
                    # Get section title from config or format section name
                    section_config = payload.config.get('market_data_sections', {}).get(section_name, {})
                    section_title = section_config.get('title', section_name.replace('_', ' ').title())
                    
                    content_blocks.append({
                        "type": "heading_3", 
                        "content": {"richText": _create_rich_text(section_title)}
                    })
                    
                    # Convert market data to table format
                    is_rates_section = 'yield' in section_title.lower() or section_name == 'rates'
                    
                    if is_rates_section:
                        headers = ["Instrument", "Change in Yield"]
                        table_data = []
                        for item in section_data:
                            display_name = item.get('display_name', item.get('symbol', 'N/A'))
                            change_percent = item.get('change_percent', 0)
                            
                            # Estimate basis points change for rates
                            if '2Y' in display_name: 
                                multiplier = -40
                            elif '10Y' in display_name: 
                                multiplier = -17
                            else: 
                                multiplier = -20
                            change_bps = change_percent * multiplier
                            
                            table_data.append({
                                "Instrument": display_name,
                                "Change in Yield": f"{change_bps:+.0f} bps"
                            })
                    else:
                        headers = ["Instrument", "Level", "Change"]
                        table_data = []
                        for item in section_data:
                            display_name = item.get('display_name', item.get('symbol', 'N/A'))
                            price = item.get('price', 0)
                            change_percent = item.get('change_percent', 0)
                            
                            # Format price with currency symbol if appropriate
                            if 'USD' in item.get('symbol', '') or item.get('symbol', '').startswith('$'):
                                price_formatted = f"${price:,.2f}"
                            else:
                                price_formatted = f"{price:,.2f}"
                            
                            table_data.append({
                                "Instrument": display_name,
                                "Level": price_formatted,
                                "Change": f"{change_percent:+.2f}%"
                            })
                    
                    table = _create_sorted_data_table(section_name, headers, table_data)
                    if table:
                        content_blocks.append(table)

        # 3. Key Market Drivers
        if payload.market_analysis and payload.market_analysis.key_drivers:
            content_blocks.append({
                "type": "heading_2", 
                "content": {"richText": _create_rich_text("Key Market Drivers")}
            })
            
            for driver in payload.market_analysis.key_drivers:
                content_blocks.append({
                    "type": "bulleted_list_item",
                    "content": {"richText": _create_rich_text(driver)}
                })

        # 4. Top Headlines (FIXED: use top_headlines)
        if payload.top_headlines:
            content_blocks.append({
                "type": "heading_2", 
                "content": {"richText": _create_rich_text("Top Headlines")}
            })
            
            for headline in payload.top_headlines:
                content_blocks.append({
                    "type": "heading_3", 
                    "content": {"richText": _create_rich_text(headline.headline)}
                })
                
                if hasattr(headline, 'commentary') and headline.commentary:
                    content_blocks.append({
                        "type": "quote", 
                        "content": {"richText": _create_rich_text(headline.commentary)}
                    })
                
                # Add summary and source in a toggle
                if headline.summary or headline.url or headline.score:
                    toggle_children = []
                    if headline.summary:
                        toggle_children.append({
                            "type": "paragraph",
                            "content": {"richText": _create_rich_text(f"Summary: {headline.summary}")}
                        })
                    if headline.url:
                        toggle_children.append({
                            "type": "paragraph",
                            "content": {"richText": [{"type": "text", "text": "Source Link", "href": headline.url}]}
                        })
                    if headline.score:
                        toggle_children.append({
                            "type": "paragraph",
                            "content": {"richText": _create_rich_text(f"Impact Score: {headline.score}/10")}
                        })
                    
                    if toggle_children:
                        content_blocks.append({
                            "type": "toggle",
                            "content": {"richText": _create_rich_text("Details & Source")},
                            "children": toggle_children
                        })
                
                content_blocks.append({"type": "divider", "content": {}})

        # 5. Top Movers (FIXED: use correct attributes)
        if payload.top_gainers or payload.top_losers:
            content_blocks.append({
                "type": "heading_2", 
                "content": {"richText": _create_rich_text("Top Market Movers")}
            })
            
            # Create two-column layout for gainers and losers
            mover_columns = []
            
            if payload.top_gainers:
                gainers_data = []
                for item in payload.top_gainers:
                    gainers_data.append({
                        "Symbol": item.get('symbol', 'N/A'),
                        "Price": f"${item.get('price', 0):.2f}",
                        "Change": f"{item.get('change_percent', 0):+.2f}%"
                    })
                
                gainers_table = _create_sorted_data_table("Top Gainers", ["Symbol", "Price", "Change"], gainers_data)
                if gainers_table:
                    gainers_column = {
                        "type": "column",
                        "children": [
                            {"type": "heading_3", "content": {"richText": _create_rich_text("📈 Top 5 Gainers")}},
                            gainers_table
                        ]
                    }
                    mover_columns.append(gainers_column)

            if payload.top_losers:
                losers_data = []
                for item in payload.top_losers:
                    losers_data.append({
                        "Symbol": item.get('symbol', 'N/A'),
                        "Price": f"${item.get('price', 0):.2f}",
                        "Change": f"{item.get('change_percent', 0):+.2f}%"
                    })
                
                losers_table = _create_sorted_data_table("Top Losers", ["Symbol", "Price", "Change"], losers_data)
                if losers_table:
                    losers_column = {
                        "type": "column",
                        "children": [
                            {"type": "heading_3", "content": {"richText": _create_rich_text("📉 Top 5 Losers")}},
                            losers_table
                        ]
                    }
                    mover_columns.append(losers_column)
            
            # Add empty column if only one mover type exists
            if len(mover_columns) == 1:
                mover_columns.append({"type": "column", "children": []})
            
            if mover_columns:
                content_blocks.append({"type": "column_list", "children": mover_columns})

        # 6. Economic Calendar
        if payload.earnings_calendar or payload.ipo_calendar:
            content_blocks.append({
                "type": "heading_2", 
                "content": {"richText": _create_rich_text("📅 Economic Calendar")}
            })
            
            calendar_columns = []
            
            # Earnings Calendar
            if payload.earnings_calendar:
                earnings_data = []
                for event in payload.earnings_calendar[:10]:  # Limit to 10 events
                    symbol = event.get('symbol', 'N/A')
                    date = event.get('date', 'TBD')
                    estimate = event.get('estimate')
                    
                    # Format date
                    try:
                        formatted_date = datetime.fromisoformat(date.replace('T00:00:00', '')).strftime('%b %d')
                    except (ValueError, TypeError):
                        formatted_date = date
                    
                    earnings_data.append({
                        "Symbol": symbol,
                        "EPS Est": f"{estimate:.4f}" if isinstance(estimate, (int, float)) else "N/A",
                        "Date": formatted_date
                    })
                
                earnings_table = _create_sorted_data_table("Earnings", ["Symbol", "EPS Est", "Date"], earnings_data)
                if earnings_table:
                    calendar_columns.append({
                        "type": "column",
                        "children": [
                            {"type": "heading_3", "content": {"richText": _create_rich_text("📊 Earnings Calendar")}},
                            earnings_table
                        ]
                    })
            
            # IPO Calendar
            if payload.ipo_calendar:
                ipo_data = []
                for event in payload.ipo_calendar[:10]:  # Limit to 10 events
                    symbol = event.get('symbol', 'N/A')
                    date = event.get('date', 'TBD')
                    price_range = event.get('priceRange', 'TBD')
                    
                    # Format date
                    try:
                        formatted_date = datetime.fromisoformat(date.replace('T00:00:00', '')).strftime('%b %d')
                    except (ValueError, TypeError):
                        formatted_date = date
                    
                    ipo_data.append({
                        "Symbol": symbol,
                        "Price Range": price_range,
                        "Date": formatted_date
                    })
                
                ipo_table = _create_sorted_data_table("IPOs", ["Symbol", "Price Range", "Date"], ipo_data)
                if ipo_table:
                    calendar_columns.append({
                        "type": "column",
                        "children": [
                            {"type": "heading_3", "content": {"richText": _create_rich_text("🗓️ IPO Calendar")}},
                            ipo_table
                        ]
                    })
            
            # Add empty column if only one calendar type exists
            if len(calendar_columns) == 1:
                calendar_columns.append({"type": "column", "children": []})
            
            if calendar_columns:
                content_blocks.append({"type": "column_list", "children": calendar_columns})

        # Final JSON Assembly (FIXED: use correct attributes)
        final_json = {
            "id": notion_page_id,
            "title": payload.config.get('briefing_title', 'Market Briefing'),
            "period": payload.config.get('briefing_title', ''),
            "date": datetime.utcnow().strftime('%Y-%m-%d'),
            "pageUrl": final_website_url,
            "tweetUrl": tweet_url,
            "marketSentiment": payload.market_analysis.sentiment.value if payload.market_analysis else "",
            "content": [block for block in content_blocks if block]
        }
        
        self.logger.info(f"Generated JSON with {len(content_blocks)} content blocks")
        return final_json