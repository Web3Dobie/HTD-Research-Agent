# services/json_caching_service.py
"""
JSONCachingService - Complete rewrite for full briefing data capture.
Generates JSON structure matching the exact Notion page layout for frontend consumption.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from core.models import BriefingPayload, Headline
from services.market_sentiment_service import SectionAnalysis

logger = logging.getLogger(__name__)


def _create_rich_text(text: str, href: Optional[str] = None, bold: bool = False, italic: bool = False, color: str = "default") -> List[Dict]:
    """Creates a rich_text structure matching Notion's format."""
    rich_text_obj = {
        "type": "text",
        "text": text,
        "annotations": {
            "bold": bold,
            "italic": italic,
            "strikethrough": False,
            "underline": False,
            "code": False,
            "color": color
        },
        "href": href
    }
    return [rich_text_obj]


def _create_sorted_data_table(headers: List[str], data_rows: List[Dict]) -> Optional[Dict]:
    """Creates a complete, sorted table block with color support."""
    if not data_rows:
        return None

    def clean_and_parse(value) -> float:
        """Parse performance values for sorting."""
        # Handle both string and dict values
        text = value.get('text', value) if isinstance(value, dict) else value
        try:
            cleaned = str(text).lower().replace('%', '').replace('bps', '').replace('market closed', '0').strip()
            return float(cleaned)
        except (ValueError, TypeError):
            return -float('inf')

    # Find the performance column to sort by
    perf_column_key = next((h for h in headers if 'change' in h.lower()), headers[-1])
    
    # Sort the data rows by performance (descending)
    sorted_data_rows = sorted(data_rows, key=lambda x: clean_and_parse(x.get(perf_column_key, '')), reverse=True)

    # Build header row
    header_row = {
        "type": "table_row",
        "content": {"cells": [_create_rich_text(h, bold=True) for h in headers]}
    }
    
    # Build data rows
    table_children = [header_row]
    for row_data in sorted_data_rows:
        cells = []
        for header in headers:
            value = row_data.get(header, '')
            
            # Check if value is a dict with color metadata
            if isinstance(value, dict):
                text = value.get('text', '')
                color = value.get('color', 'default')
                cells.append(_create_rich_text(str(text), color=color))
            else:
                cells.append(_create_rich_text(str(value)))
        
        table_children.append({
            "type": "table_row",
            "content": {"cells": cells}
        })

    return {
        "type": "table",
        "content": {"hasColumnHeader": True},
        "children": table_children
    }


class JSONCachingService:
    """Service to generate cached JSON from BriefingPayload matching exact Notion page layout."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def generate_json_from_payload(
        self, 
        payload: BriefingPayload, 
        briefing_id: int, 
        notion_page_id: str, 
        final_website_url: str, 
        tweet_url: str
    ) -> Dict[str, Any]:
        """
        Generates complete JSON structure from BriefingPayload.
        Matches exact Notion page layout order.
        """
        self.logger.info(f"🔧 Generating JSON cache for briefing_id: {briefing_id}")
        
        content_blocks = []
        
        # ============================================================
        # 1. GLOBAL MARKET SENTIMENT
        # ============================================================
        content_blocks.extend(self._build_sentiment_header(payload))
        
        # ============================================================
        # 2. MARKET PERFORMANCE DASHBOARD
        # ============================================================
        content_blocks.extend(self._build_market_performance_dashboard(payload))
        
        # ============================================================
        # 3. TOP GAINERS/LOSERS (for pre-market and US close only)
        # ============================================================
        if payload.top_gainers or payload.top_losers:
            content_blocks.extend(self._build_top_movers_section(payload))
        
        # ============================================================
        # 4. KEY MARKET DRIVERS
        # ============================================================
        content_blocks.extend(self._build_key_drivers(payload))
        
        # ============================================================
        # 5. ECONOMIC CALENDAR (Earnings + IPO)
        # ============================================================
        content_blocks.extend(self._build_calendar_section(payload))
        
        # ============================================================
        # 6. ECONOMIC CALENDAR WIDGET
        # ============================================================
        content_blocks.extend(self._build_calendar_widget())
        
        # ============================================================
        # 7. HEADLINES SECTION (Stock-Specific News OR General Headlines)
        # ============================================================
        content_blocks.extend(self._build_headlines_section(payload))
        
        # ============================================================
        # 8. DETAILED SECTION ANALYSIS
        # ============================================================
        content_blocks.extend(self._build_detailed_analysis(payload))
        
        # ============================================================
        # 9. DISCLAIMER/FOOTER
        # ============================================================
        content_blocks.extend(self._build_disclaimer())
        
        # ============================================================
        # FINAL JSON ASSEMBLY
        # ============================================================
        enhanced_summary = self._build_enhanced_briefing_summary(payload)

        final_json = {
            "id": notion_page_id,
            "title": payload.config.get('briefing_title', 'Market Briefing'),
            "period": payload.config.get('briefing_title', ''),
            "date": datetime.utcnow().strftime('%Y-%m-%d'),
            "pageUrl": final_website_url,
            "tweetUrl": tweet_url,
            "marketSentiment": payload.market_analysis.sentiment.value if payload.market_analysis else "NEUTRAL",
            "content": [block for block in content_blocks if block],  # Filter out None blocks
            "enhanced_summary": enhanced_summary  # FIXED: correct property name
        }
        
        self.logger.info(f"✅ Generated JSON with {len(content_blocks)} content blocks")
        return final_json

    # ============================================================
    # SECTION BUILDERS
    # ============================================================

    def _build_sentiment_header(self, payload: BriefingPayload) -> List[Dict]:
        """Builds the Global Market Sentiment header section."""
        blocks = []
        
        if not payload.market_analysis:
            return blocks
        
        analysis = payload.market_analysis
        sentiment_emoji = {
            "BULLISH": "🐂",
            "BEARISH": "🐻",
            "NEUTRAL": "📊",
            "MIXED": "⚖️"
        }.get(analysis.sentiment.value, "📊")
        
        # Sentiment heading
        blocks.append({
            "type": "heading_1",
            "content": {"richText": _create_rich_text(
                f"Global Market Sentiment: {analysis.sentiment.value} {sentiment_emoji}",
                bold=True
            )}
        })
        
        # Market summary quote
        if analysis.market_summary:
            blocks.append({
                "type": "quote",
                "content": {"richText": _create_rich_text(analysis.market_summary)}
            })
        
        return blocks

    def _build_market_performance_dashboard(self, payload: BriefingPayload) -> List[Dict]:
        """Builds the Market Performance Dashboard with all market data sections."""
        blocks = []
        
        blocks.append({
            "type": "heading_2",
            "content": {"richText": _create_rich_text("Market Performance Dashboard", bold=True)}
        })
        
        if not payload.raw_market_data:
            blocks.append({
                "type": "paragraph",
                "content": {"richText": _create_rich_text("Market data not available.")}
            })
            return blocks
        
        # Build tables for each market section (excluding top movers)
        section_blocks = []
        for section_name, section_data in payload.raw_market_data.items():
            if section_name in ['top_gainers', 'top_losers']:
                continue  # These are handled separately
            
            if not section_data:
                continue
            
            section_config = payload.config.get('market_data_sections', {}).get(section_name, {})
            section_title = section_config.get('title', section_name.replace('_', ' ').title())
            
            # Section heading
            section_block_group = [{
                "type": "heading_3",
                "content": {"richText": _create_rich_text(section_title, bold=True)}
            }]
            
            # Build table
            is_rates_section = 'yield' in section_title.lower() or section_name == 'rates'
            
            if is_rates_section:
                headers = ["Instrument", "Change in Yield"]
                table_data = []
                for item in section_data:
                    display_name = item.get('display_name', item.get('symbol', 'N/A'))
                    change_percent = item.get('change_percent', 0)
                    market_status = item.get('market_status', 'OPEN').upper()
                    
                    if market_status == 'CLOSED':
                        change_formatted = "Market Closed"
                        change_color = "gray"
                    else:
                        # Estimate basis points change
                        if '2Y' in display_name:
                            multiplier = -40
                        elif '10Y' in display_name:
                            multiplier = -17
                        else:
                            multiplier = -20
                        change_bps = change_percent * multiplier
                        change_formatted = f"{change_bps:+.0f} bps"
                        # Color: green if negative bps (yields down = bond prices up), red if positive
                        change_color = "green" if change_bps < 0 else "red"
                    
                    table_data.append({
                        "Instrument": display_name,
                        "Change in Yield": {
                            "text": change_formatted,
                            "color": change_color  # Add color metadata
                        }
                    })
            else:
                headers = ["Instrument", "Level", "Change"]
                table_data = []
                for item in section_data:
                    display_name = item.get('display_name', item.get('symbol', 'N/A'))
                    price = item.get('price', 0)
                    change_percent = item.get('change_percent', 0)
                    market_status = item.get('market_status', 'OPEN').upper()
                    
                    # Format price
                    if 'USD' in item.get('symbol', '') or item.get('symbol', '').startswith('$'):
                        price_formatted = f"${price:,.2f}"
                    else:
                        price_formatted = f"{price:,.2f}"
                    
                    # Format change with color
                    if market_status == 'CLOSED':
                        change_formatted = "Market Closed"
                        change_color = "gray"
                    else:
                        change_formatted = f"{change_percent:+.2f}%"
                        change_color = "green" if change_percent > 0 else "red"
                    
                    table_data.append({
                        "Instrument": display_name,
                        "Level": price_formatted,
                        "Change": {
                            "text": change_formatted,
                            "color": change_color  # Add color metadata
                        }
                    })
            
            table = _create_sorted_data_table(headers, table_data)
            if table:
                section_block_group.append(table)
            
            section_blocks.append(section_block_group)
        
        # Create two-column layout for sections
        if section_blocks:
            column_layout = self._create_two_column_layout(section_blocks)
            blocks.extend(column_layout)
        
        return blocks

    def _build_top_movers_section(self, payload: BriefingPayload) -> List[Dict]:
        """Builds the Top Gainers & Losers section with colored changes."""
        blocks = []
        
        blocks.append({
            "type": "heading_2",
            "content": {"richText": _create_rich_text("Top Market Movers", bold=True)}
        })
        
        mover_columns = []
        
        # Top Gainers
        if payload.top_gainers:
            gainers_blocks = [{
                "type": "heading_3",
                "content": {"richText": _create_rich_text("📈 Top 5 Gainers", bold=True)}
            }]
            
            gainers_data = []
            for item in payload.top_gainers:
                change_pct = item.get('change_percent', 0)
                gainers_data.append({
                    "Symbol": item.get('symbol', 'N/A'),
                    "Price": f"${item.get('price', 0):.2f}",
                    "Change": {
                        "text": f"{change_pct:+.2f}%",
                        "color": "green" if change_pct > 0 else "red"
                    }
                })
            
            gainers_table = _create_sorted_data_table(["Symbol", "Price", "Change"], gainers_data)
            if gainers_table:
                gainers_blocks.append(gainers_table)
            
            mover_columns.append({"type": "column", "children": gainers_blocks})
        
        # Top Losers (same pattern)
        if payload.top_losers:
            losers_blocks = [{
                "type": "heading_3",
                "content": {"richText": _create_rich_text("📉 Top 5 Losers", bold=True)}
            }]
            
            losers_data = []
            for item in payload.top_losers:
                change_pct = item.get('change_percent', 0)
                losers_data.append({
                    "Symbol": item.get('symbol', 'N/A'),
                    "Price": f"${item.get('price', 0):.2f}",
                    "Change": {
                        "text": f"{change_pct:+.2f}%",
                        "color": "green" if change_pct > 0 else "red"
                    }
                })
            
            losers_table = _create_sorted_data_table(["Symbol", "Price", "Change"], losers_data)
            if losers_table:
                losers_blocks.append(losers_table)
            
            mover_columns.append({"type": "column", "children": losers_blocks})
        
        # Add empty column if only one type exists
        if len(mover_columns) == 1:
            mover_columns.append({"type": "column", "children": []})
        
        if mover_columns:
            blocks.append({"type": "column_list", "children": mover_columns})
        
        return blocks

    def _build_key_drivers(self, payload: BriefingPayload) -> List[Dict]:
        """Builds the Key Market Drivers section."""
        blocks = []
        
        if not payload.market_analysis or not payload.market_analysis.key_drivers:
            return blocks
        
        blocks.append({
            "type": "heading_2",
            "content": {"richText": _create_rich_text("Key Market Drivers", bold=True)}
        })
        
        for driver in payload.market_analysis.key_drivers:
            blocks.append({
                "type": "bulleted_list_item",
                "content": {"richText": _create_rich_text(driver)}
            })
        
        return blocks

    def _build_calendar_section(self, payload: BriefingPayload) -> List[Dict]:
        """Builds the Earnings & IPO Calendar section."""
        blocks = []
        
        if not payload.earnings_calendar and not payload.ipo_calendar:
            return blocks
        
        blocks.append({
            "type": "heading_2",
            "content": {"richText": _create_rich_text("📅 Economic Calendar", bold=True)}
        })
        
        calendar_columns = []
        
        # Earnings Calendar
        if payload.earnings_calendar:
            earnings_blocks = [{
                "type": "heading_3",
                "content": {"richText": _create_rich_text("📊 Earnings Calendar", bold=True)}
            }]
            
            earnings_data = []
            for event in payload.earnings_calendar[:10]:  # First 10 events
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
            
            earnings_table = _create_sorted_data_table(["Symbol", "EPS Est", "Date"], earnings_data)
            if earnings_table:
                earnings_blocks.append(earnings_table)
            
            # Add remaining events in toggle if more than 10
            if len(payload.earnings_calendar) > 10:
                remaining = payload.earnings_calendar[10:]
                toggle_children = []
                for event in remaining:
                    symbol = event.get('symbol', 'N/A')
                    date = event.get('date', 'TBD')
                    estimate = event.get('estimate')
                    
                    try:
                        formatted_date = datetime.fromisoformat(date.replace('T00:00:00', '')).strftime('%b %d, %Y')
                    except (ValueError, TypeError):
                        formatted_date = date
                    
                    details = f"EPS Est: {estimate:.4f}" if isinstance(estimate, (int, float)) else "EPS Est: N/A"
                    
                    toggle_children.append({
                        "type": "bulleted_list_item",
                        "content": {"richText": _create_rich_text(f"{symbol}: {details} ({formatted_date})")}
                    })
                
                if toggle_children:
                    earnings_blocks.append({
                        "type": "toggle",
                        "content": {"richText": _create_rich_text(f"Show {len(remaining)} More Events...")},
                        "children": toggle_children
                    })
            
            calendar_columns.append({"type": "column", "children": earnings_blocks})
        
        # IPO Calendar
        if payload.ipo_calendar:
            ipo_blocks = [{
                "type": "heading_3",
                "content": {"richText": _create_rich_text("🗓️ IPO Calendar", bold=True)}
            }]
            
            ipo_data = []
            for event in payload.ipo_calendar[:10]:  # First 10 events
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
            
            ipo_table = _create_sorted_data_table(["Symbol", "Price Range", "Date"], ipo_data)
            if ipo_table:
                ipo_blocks.append(ipo_table)
            
            # Add remaining events in toggle if more than 10
            if len(payload.ipo_calendar) > 10:
                remaining = payload.ipo_calendar[10:]
                toggle_children = []
                for event in remaining:
                    symbol = event.get('symbol', 'N/A')
                    date = event.get('date', 'TBD')
                    price_range = event.get('priceRange', 'TBD')
                    
                    try:
                        formatted_date = datetime.fromisoformat(date.replace('T00:00:00', '')).strftime('%b %d, %Y')
                    except (ValueError, TypeError):
                        formatted_date = date
                    
                    toggle_children.append({
                        "type": "bulleted_list_item",
                        "content": {"richText": _create_rich_text(f"{symbol}: {price_range} ({formatted_date})")}
                    })
                
                if toggle_children:
                    ipo_blocks.append({
                        "type": "toggle",
                        "content": {"richText": _create_rich_text(f"Show {len(remaining)} More Events...")},
                        "children": toggle_children
                    })
            
            calendar_columns.append({"type": "column", "children": ipo_blocks})
        
        # Add empty column if only one calendar exists
        if len(calendar_columns) == 1:
            calendar_columns.append({"type": "column", "children": []})
        
        if calendar_columns:
            blocks.append({"type": "column_list", "children": calendar_columns})
        
        return blocks

    def _build_calendar_widget(self) -> List[Dict]:
        """Builds the Economic Calendar Widget placeholder."""
        return [
            {
                "type": "heading_2",
                "content": {"richText": _create_rich_text("🗓️ Economic Calendar", bold=True)}
            },
            {
                "type": "callout",
                "content": {
                    "richText": _create_rich_text("ECONOMIC_CALENDAR_WIDGET"),
                    "icon": {"emoji": "⚙️"}
                }
            }
        ]

    def _build_headlines_section(self, payload: BriefingPayload) -> List[Dict]:
        """
        Builds the headlines section.
        Shows stock-specific news for pre-market/US close, general headlines for others.
        """
        blocks = []
        
        # Route based on whether we have stock-specific news
        if payload.stock_specific_news:
            blocks.extend(self._build_stock_specific_news(payload))
        elif payload.top_headlines:
            blocks.extend(self._build_general_headlines(payload))
        else:
            blocks.append({
                "type": "heading_2",
                "content": {"richText": _create_rich_text("Top Headlines", bold=True)}
            })
            blocks.append({
                "type": "paragraph",
                "content": {"richText": _create_rich_text("No headlines available.")}
            })
        
        return blocks

    def _build_stock_specific_news(self, payload: BriefingPayload) -> List[Dict]:
        """Builds stock-specific news section for gainers/losers."""
        blocks = []
        
        blocks.append({
            "type": "heading_2",
            "content": {"richText": _create_rich_text("Top Gainers & Losers News", bold=True)}
        })
        
        # Get ordered symbols (gainers first, then losers)
        ordered_symbols = []
        market_sections = payload.config.get('market_data_sections', {})
        
        if 'top_gainers' in market_sections:
            ordered_symbols.extend(market_sections['top_gainers'].get('symbols', []))
        if 'top_losers' in market_sections:
            ordered_symbols.extend(market_sections['top_losers'].get('symbols', []))
        
        for symbol in ordered_symbols:
            articles = payload.stock_specific_news.get(symbol, [])
            if not articles:
                continue
            
            # Symbol heading
            blocks.append({
                "type": "heading_3",
                "content": {"richText": _create_rich_text(f"📰 News for {symbol}", bold=True)}
            })
            
            # First 2 articles shown directly
            for article in articles[:2]:
                headline_text = article.get('headline', '')
                summary_text = article.get('summary', '')
                article_url = article.get('url', '')
                
                if headline_text:
                    blocks.append({
                        "type": "heading_3",
                        "content": {"richText": _create_rich_text(headline_text, href=article_url if article_url else None)}
                    })
                
                if summary_text:
                    blocks.append({
                        "type": "paragraph",
                        "content": {"richText": _create_rich_text(summary_text, color="gray")}
                    })
            
            # Remaining articles in toggle
            if len(articles) > 2:
                toggle_children = []
                for article in articles[2:]:
                    headline_text = article.get('headline', '')
                    summary_text = article.get('summary', '')
                    article_url = article.get('url', '')
                    
                    if headline_text:
                        toggle_children.append({
                            "type": "heading_3",
                            "content": {"richText": _create_rich_text(headline_text, href=article_url if article_url else None)}
                        })
                    
                    if summary_text:
                        toggle_children.append({
                            "type": "paragraph",
                            "content": {"richText": _create_rich_text(summary_text, color="gray")}
                        })
                
                if toggle_children:
                    blocks.append({
                        "type": "toggle",
                        "content": {"richText": _create_rich_text(f"Show {len(articles) - 2} more headlines...")},
                        "children": toggle_children
                    })
            
            # Divider between symbols
            blocks.append({"type": "divider", "content": {}})
        
        return blocks

    def _build_general_headlines(self, payload: BriefingPayload) -> List[Dict]:
        """Builds general headlines section for morning/EU close briefings."""
        blocks = []
        
        blocks.append({
            "type": "heading_2",
            "content": {"richText": _create_rich_text("Top Headlines", bold=True)}
        })
        
        for headline in payload.top_headlines:
            headline_text = headline.headline
            url = headline.url or ""
            summary = headline.summary or ""
            score = headline.score or 0
            commentary = getattr(headline, 'commentary', None)
            
            # Headline
            blocks.append({
                "type": "heading_3",
                "content": {"richText": _create_rich_text(headline_text, href=url if url else None, bold=True)}
            })
            
            # Commentary (if available)
            if commentary:
                blocks.append({
                    "type": "quote",
                    "content": {"richText": _create_rich_text(commentary, color="blue")}
                })
            
            # Details in toggle
            toggle_children = []
            if summary:
                toggle_children.append({
                    "type": "paragraph",
                    "content": {"richText": _create_rich_text(f"Summary: {summary}")}
                })
            if url:
                toggle_children.append({
                    "type": "paragraph",
                    "content": {"richText": _create_rich_text("Source Link", href=url)}
                })
            if score:
                toggle_children.append({
                    "type": "paragraph",
                    "content": {"richText": _create_rich_text(f"Impact Score: {score}/10")}
                })
            
            if toggle_children:
                blocks.append({
                    "type": "toggle",
                    "content": {"richText": _create_rich_text("Details & Source")},
                    "children": toggle_children
                })
            
            blocks.append({"type": "divider", "content": {}})
        
        return blocks

    def _build_detailed_analysis(self, payload: BriefingPayload) -> List[Dict]:
        """Builds the Detailed Section Analysis section."""
        blocks = []
        
        if not payload.market_analysis or not payload.market_analysis.section_analyses:
            return blocks
        
        blocks.append({
            "type": "heading_2",
            "content": {"richText": _create_rich_text("Detailed Section Analysis", bold=True)}
        })
        
        for section in payload.market_analysis.section_analyses:
            section_title = section.section_name.replace('_', ' ').title()
            toggle_header = f"{section_title}: {section.section_sentiment} ({section.avg_performance:+.2f}%)"
            
            toggle_children = []
            for mover in section.key_movers:
                toggle_children.append({
                    "type": "bulleted_list_item",
                    "content": {"richText": _create_rich_text(mover)}
                })
            
            blocks.append({
                "type": "toggle",
                "content": {"richText": _create_rich_text(toggle_header)},
                "children": toggle_children
            })
        
        return blocks

    def _build_disclaimer(self) -> List[Dict]:
        """Builds the disclaimer/footer section."""
        return [
            {"type": "divider", "content": {}},
            {
                "type": "quote",
                "content": {"richText": _create_rich_text(
                    "Market data sourced from Binance, IG-Index, Mexc, and Finnhub. "
                    "Economic calendar via TradingView, macro data via FRED.",
                    italic=True
                )}
            }
        ]

    def _create_two_column_layout(self, content_groups: List[List[Dict]]) -> List[Dict]:
        """
        Creates a two-column layout from a list of content block groups.
        Handles odd numbers by adding an empty second column.
        """
        layout_blocks = []
        
        for i in range(0, len(content_groups), 2):
            pair = content_groups[i:i+2]
            
            column_list_children = []
            for group in pair:
                column_list_children.append({
                    "type": "column",
                    "children": group
                })
            
            # Add empty column if odd number
            if len(pair) == 1:
                column_list_children.append({
                    "type": "column",
                    "children": []
                })
            
            layout_blocks.append({
                "type": "column_list",
                "children": column_list_children
            })
        
        return layout_blocks

    # ============================================================
    # ENHANCED SUMMARY METHODS (NEW)
    # ============================================================

    def _build_enhanced_briefing_summary(self, payload: BriefingPayload) -> Dict:
        """Build enhanced summary data for LatestBriefingCard component."""
        
        analysis = payload.market_analysis
        if not analysis:
            return {}
        
        # Calculate market momentum indicators
        momentum_data = self._calculate_momentum_indicators(analysis.section_analyses)
        
        # Get top sector movers
        sector_movers = self._get_sector_highlights(analysis.section_analyses)
        
        # Create visual sentiment data
        sentiment_visual = {
            'sentiment': analysis.sentiment.value,
            'confidence': analysis.confidence_score,
            'emoji': self._get_sentiment_emoji(analysis.sentiment.value),
            'color': self._get_sentiment_color(analysis.sentiment.value),
            'description': self._get_sentiment_description(analysis.sentiment.value)
        }
        
        return {
            'sentiment_visual': sentiment_visual,
            'momentum_indicators': momentum_data,
            'sector_highlights': sector_movers,
            'key_insights': analysis.key_drivers[:3] if analysis.key_drivers else [],
            'market_summary_short': analysis.market_summary[:150] + "..." if len(analysis.market_summary or '') > 150 else analysis.market_summary or '',
            'confidence_level': self._map_confidence_to_level(analysis.confidence_score),
            'market_health_score': self._calculate_market_health_score(analysis.section_analyses)
        }

    def _calculate_momentum_indicators(self, section_analyses: List[SectionAnalysis]) -> Dict:
        """Calculate momentum indicators for visual display."""
        
        if not section_analyses:
            return {}
        
        bullish_count = sum(1 for s in section_analyses if s.section_sentiment == "BULLISH")
        bearish_count = sum(1 for s in section_analyses if s.section_sentiment == "BEARISH")
        total_sections = len(section_analyses)
        
        return {
            'bullish_percentage': (bullish_count / total_sections) * 100,
            'bearish_percentage': (bearish_count / total_sections) * 100,
            'neutral_percentage': ((total_sections - bullish_count - bearish_count) / total_sections) * 100,
            'momentum_direction': 'bullish' if bullish_count > bearish_count else 'bearish' if bearish_count > bullish_count else 'neutral'
        }

    def _get_sector_highlights(self, section_analyses: List[SectionAnalysis]) -> List[Dict]:
        """Get top performing and underperforming sectors."""
        
        if not section_analyses:
            return []
        
        # Sort by performance
        sorted_sections = sorted(section_analyses, key=lambda x: x.avg_performance, reverse=True)
        
        highlights = []
        
        # Top performer
        if len(sorted_sections) > 0:
            top = sorted_sections[0]
            highlights.append({
                'type': 'top_performer',
                'name': top.section_name.replace('_', ' ').title(),
                'performance': top.avg_performance,
                'emoji': self._get_sector_emoji(top.section_name)
            })
        
        # Bottom performer  
        if len(sorted_sections) > 1:
            bottom = sorted_sections[-1]
            highlights.append({
                'type': 'underperformer',
                'name': bottom.section_name.replace('_', ' ').title(), 
                'performance': bottom.avg_performance,
                'emoji': self._get_sector_emoji(bottom.section_name)
            })
        
        return highlights

    def _get_sentiment_emoji(self, sentiment: str) -> str:
        """Get emoji for sentiment."""
        emoji_map = {
            'BULLISH': '🐂',
            'BEARISH': '🐻', 
            'MIXED': '⚖️',
            'NEUTRAL': '😐'
        }
        return emoji_map.get(sentiment, '📊')

    def _get_sentiment_color(self, sentiment: str) -> str:
        """Get color code for sentiment."""
        color_map = {
            'BULLISH': '#22c55e',  # Green
            'BEARISH': '#ef4444',  # Red
            'MIXED': '#f59e0b',    # Orange
            'NEUTRAL': '#6b7280'   # Gray
        }
        return color_map.get(sentiment, '#6b7280')

    def _get_sector_emoji(self, sector_name: str) -> str:
        """Get emoji for sector."""
        emoji_map = {
            'us_futures': '🇺🇸',
            'european_futures': '🇪🇺', 
            'asian_focus': '🌏',
            'crypto': '🪙',
            'fx': '💱',
            'rates': '💵',
            'volatility': '📉',
            'commodities': '🏗️'
        }
        return emoji_map.get(sector_name, '📊')

    def _get_sentiment_description(self, sentiment: str) -> str:
        """Get description for sentiment."""
        description_map = {
            'BULLISH': 'Markets showing strong upward momentum',
            'BEARISH': 'Markets under pressure with downward bias',
            'MIXED': 'Markets showing conflicting signals across sectors',
            'NEUTRAL': 'Markets in consolidation with limited direction'
        }
        return description_map.get(sentiment, 'Market direction unclear')

    def _map_confidence_to_level(self, confidence_score: float) -> str:
        """Map confidence score to human readable level."""
        if confidence_score >= 0.8:
            return 'Very High'
        elif confidence_score >= 0.6:
            return 'High'
        elif confidence_score >= 0.4:
            return 'Moderate'
        elif confidence_score >= 0.2:
            return 'Low'
        else:
            return 'Very Low'

    def _calculate_market_health_score(self, section_analyses) -> float:
        """Calculate overall market health score (0-100)."""
        if not section_analyses:
            return 50.0
        
        # Base score on percentage of bullish vs bearish sections
        bullish_count = sum(1 for s in section_analyses if s.section_sentiment == "BULLISH")
        bearish_count = sum(1 for s in section_analyses if s.section_sentiment == "BEARISH")
        total_sections = len(section_analyses)
        
        # Calculate score: 50 is neutral, higher is healthier
        if total_sections == 0:
            return 50.0
        
        bullish_ratio = bullish_count / total_sections
        bearish_ratio = bearish_count / total_sections
        
        # Health score: bullish sections add points, bearish subtract
        health_score = 50 + (bullish_ratio * 50) - (bearish_ratio * 50)
        
        # Clamp between 0 and 100
        return max(0.0, min(100.0, health_score))