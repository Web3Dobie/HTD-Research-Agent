# services/notion_publisher.py
"""
NotionPublisher - Publishes tweets to the HedgeFund Tweet Notion database.
Integrates with the website's existing database structure and APIs.
"""

import logging
import os
import asyncio
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone, time

from notion_client import Client

from core.models import GeneratedContent, BriefingPayload, Headline
from services.publishing_service import TwitterResult
from config.settings import NOTION_CONFIG


class NotionPublisher:
    """
    Publishes tweets to the HedgeFund Tweet Notion database.
    Maintains compatibility with existing website APIs.
    """
    
    BRIEFING_COVER_URL = "https://i.ibb.co/GfM5Tffm/HTD-Research-Banner.png"
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Get credentials from config
        self.notion_api_key = NOTION_CONFIG['api_key']
        self.hedgefund_tweet_db_id = NOTION_CONFIG['hedgefund_tweet_db_id']
        
        if not self.notion_api_key:
            self.logger.error("NOTION_API_KEY not found in environment variables")
            self.client = None
        else:
            try:
                self.client = Client(auth=self.notion_api_key)
                self.logger.info("✅ Notion client initialized successfully")
            except Exception as e:
                self.logger.error(f"❌ Failed to initialize Notion client: {e}")
                self.client = None
        
        if not self.hedgefund_tweet_db_id:
            self.logger.error("HEDGEFUND_TWEET_DB_ID not found in environment variables")
    
    def publish_tweet_to_notion(
        self, 
        content: GeneratedContent, 
        twitter_result: TwitterResult
    ) -> Optional[str]:
        """
        Publish a tweet to the HedgeFund Tweet Notion database.
        
        Args:
            content: The generated content that was tweeted
            twitter_result: Result from Twitter publishing with tweet_id, url, etc.
            
        Returns:
            str: Notion page ID if successful, None if failed
        """
        if not self.client or not self.hedgefund_tweet_db_id:
            self.logger.error("Notion client or database ID not available")
            return None
        
        if not twitter_result.success:
            self.logger.warning("Twitter result was not successful, skipping Notion publish")
            return None
        
        try:
            # Prepare properties for HedgeFund Tweet database
            # CRITICAL: Use "Category" not "Type" for HedgeFund database
            properties = {
                "Tweet ID": {
                    "title": [{"text": {"content": twitter_result.tweet_id}}]
                },
                "Text": {
                    "rich_text": [{"text": {"content": content.text}}]
                },
                "Date": {
                    "date": {"start": twitter_result.timestamp or datetime.now(timezone.utc).isoformat()}
                },
                "Category": {  # CRITICAL: HedgeFund uses "Category" not "Type"
                    "select": {"name": self._format_content_category(content)}
                },
                "Theme": {  # Theme from the generated content
                    "rich_text": [{"text": {"content": content.theme}}]
                },
                "URL": {
                    "url": twitter_result.url
                },
                "Likes": {
                    "number": 0  # Will be updated by engagement sync later
                },
                "Retweets": {
                    "number": 0  # Will be updated by engagement sync later
                },
                "Replies": {
                    "number": 0  # Will be updated by engagement sync later
                },
                "Engagement Score": {
                    "number": 0  # Will be updated by engagement sync later
                }
            }
            
            # Create the page in Notion
            response = self.client.pages.create(
                parent={"database_id": self.hedgefund_tweet_db_id},
                properties=properties
            )
            
            page_id = response["id"]
            self.logger.info(f"✅ Tweet published to Notion: {page_id}")
            self.logger.info(f"   Tweet ID: {twitter_result.tweet_id}")
            self.logger.info(f"   Theme: {content.theme}")
            self.logger.info(f"   Category: {self._format_content_category(content)}")
            self.logger.info(f"   URL: {twitter_result.url}")
            
            return page_id
            
        except Exception as e:
            self.logger.error(f"❌ Failed to publish tweet to Notion: {e}")
            self.logger.error(f"   Tweet ID: {twitter_result.tweet_id}")
            self.logger.error(f"   Content: {content.text[:100]}...")
            return None
    
    async def publish_briefing(self, payload: BriefingPayload, briefing_key: str) -> Optional[Dict]:
        """
        Creates a comprehensive briefing page, now including the briefing period.
        Returns a dictionary with page_id and page_url.
        """
        if not self.client:
            self.logger.error("Notion client not available for briefing publish")
            return None

        publishing_config = payload.config.get('publishing_config', {})
        database_id = publishing_config.get('notion_database_id')
        
        if not database_id:
            self.logger.error("Briefing Notion Database ID not found in config")
            return None
        
        try:
            analysis = payload.market_analysis
            
            # Create page title with date
            page_title = f"{payload.config.get('briefing_title', 'Market Briefing')} - {datetime.now().strftime('%Y-%m-%d')}"
            
            # Step 1: Create page properties
            page_properties = {
                "Name": {"title": [{"text": {"content": page_title}}]},
                "Date": {"date": {"start": datetime.now(timezone.utc).isoformat()}},
                "Sentiment": {"select": {"name": analysis.sentiment.value}},
                "Period": {"select": {"name": "Morning Briefing"}}
            }
            
            # Step 2: Build comprehensive page content
            page_blocks = self._build_complete_briefing_blocks(payload)
            self.logger.info(f"Generated {len(page_blocks)} blocks for Notion page")
            # Step 3: Create the page
            created_page = self.client.pages.create(
                parent={"database_id": database_id},
                properties=page_properties,
                children=page_blocks,
                cover={ 
                    "type": "external",
                    "external": {
                        "url": self.BRIEFING_COVER_URL
                    }
                }
            )
            
            page_id = created_page["id"]
            page_url = created_page.get("url")
            self.logger.info(f"Enhanced briefing page created: {page_id}")

            # Step 4: Update with public URL for website integration
            if page_url:
                await asyncio.sleep(1)
                self.client.pages.update(
                    page_id=page_id,
                    properties={"PDF Link": {"url": page_url}}
                )
                self.logger.info(f"Updated PDF Link with public URL: {page_url}")

            return {"page_id": page_id, "page_url": page_url}

        except Exception as e:
            self.logger.error(f"Failed to publish briefing to Notion: {e}")
            return None

    def update_briefing_with_tweet(self, notion_page_id: str, tweet_url: str):
        """Updates an existing Notion briefing page with the promotional tweet URL."""
        if not self.client:
            return

        try:
            self.client.pages.update(
                page_id=notion_page_id,
                properties={
                    "Tweet_URL": {"url": tweet_url} # Assumes you have a 'Tweet_URL' property in Notion
                }
            )
            self.logger.info(f"Successfully updated Notion page {notion_page_id} with tweet URL.")
        except Exception as e:
            self.logger.error(f"Failed to update Notion page with tweet URL: {e}")

    def _build_two_column_layout(self, content_groups: List[List[Dict]]) -> List[Dict]:
        """
        Takes a list of block groups and arranges them in a two-column layout.
        Handles an odd number of items by adding an empty second column.
        """
        layout_blocks = []
        for i in range(0, len(content_groups), 2):
            pair = content_groups[i:i+2]
            
            column_list_children = []
            for group in pair:
                column_list_children.append({
                    "type": "column",
                    "column": { "children": group }
                })

            # --- START OF FIX ---
            # If there's only one item in the pair (from an odd-numbered list),
            # we must add a second, empty column to satisfy the API.
            if len(pair) == 1:
                column_list_children.append({
                    "type": "column",
                    "column": { "children": [] }
                })
            # --- END OF FIX ---

            layout_blocks.append({
                "type": "column_list",
                "column_list": { "children": column_list_children }
            })
        return layout_blocks
    
    def _format_content_category(self, content: GeneratedContent) -> str:
        """
        Format content category for Notion select field.
        Maps content types and categories to website-compatible values.
        """
        # Map content types to categories for the HedgeFund database
        if content.category:
            # Use the category if provided
            category_mapping = {
                "MACRO": "macro",
                "EQUITY": "equity", 
                "POLITICAL": "political",
                "CRYPTO": "crypto"
            }
            category_str = content.category.value if hasattr(content.category, 'value') else str(content.category)
            return category_mapping.get(category_str.upper(), category_str.lower())
        else:
            # Fall back to content type
            content_type_mapping = {
                "COMMENTARY": "commentary",
                "DEEP_DIVE": "analysis",
                "BRIEFING": "briefing"
            }
            content_type_str = content.content_type.value if hasattr(content.content_type, 'value') else str(content.content_type)
            return content_type_mapping.get(content_type_str.upper(), "commentary")
    
    def _build_complete_briefing_blocks(self, payload: BriefingPayload) -> list:
        """
        Builds the complete Notion page structure matching the PDF format.
        """
        analysis = payload.market_analysis
        
        # Sentiment emoji
        sentiment_emoji = {
            "BULLISH": "🐂", "BEARISH": "🐻", 
            "NEUTRAL": "📊", "MIXED": "⚖️"
        }.get(analysis.sentiment.value, "📊")
        
        blocks = []
        
        # === HEADER SECTION ===
        blocks.extend([
            {
                "type": "heading_1", 
                "heading_1": {
                    "rich_text": [{
                        "type": "text", 
                        "text": {"content": f"Global Market Sentiment: {analysis.sentiment.value} {sentiment_emoji}"}
                    }]
                }
            },
            {
                "type": "quote", 
                "quote": {
                    "rich_text": [{
                        "type": "text", 
                        "text": {"content": analysis.market_summary}
                    }]
                }
            }
        ])
        
        # === MARKET DATA SECTIONS ===
        blocks.append({
            "type": "heading_2", 
            "heading_2": {
                "rich_text": [{
                    "type": "text", 
                    "text": {"content": "Market Performance Dashboard"}
                }]
            }
        })
        
        # Step 1: Generate all individual market data blocks into a temporary list
        market_sections = payload.config.get('market_data_sections', {})
        all_market_section_groups = [] # This will be a list of lists
        for section_name, section_config in market_sections.items():
            section_blocks = self._build_market_data_table(
                section_name, 
                section_config, 
                payload.raw_market_data.get(section_name, [])
            )
            all_market_section_groups.append(section_blocks)


        market_data_columns = self._build_two_column_layout(all_market_section_groups)
        blocks.extend(market_data_columns)

        # === KEY MARKET DRIVERS ===
        if analysis.key_drivers:
            blocks.append({
                "type": "heading_2", 
                "heading_2": {
                    "rich_text": [{
                        "type": "text", 
                        "text": {"content": "Key Market Drivers"}
                    }]
                }
            })
            
            for driver in analysis.key_drivers:
                blocks.append({
                    "type": "bulleted_list_item",
                    "bulleted_list_item": {
                        "rich_text": [{
                            "type": "text", 
                            "text": {"content": driver}
                        }]
                    }
                })
        
        # === CALENDAR EVENTS (Updated to use columns) ===
        # First, generate the content for the IPO and Earnings sections separately
        earnings_blocks = self._build_calendar_section(
            title="📊 Earnings Calendar",
            events=payload.earnings_calendar,
            event_type='earnings'
        )
        ipo_blocks = self._build_calendar_section(
            title="🗓️ IPO Calendar",
            events=payload.ipo_calendar,
            event_type='ipo'
        )
        
        # Now, arrange them in a two-column layout
        calendar_columns = self._build_two_column_layout([earnings_blocks, ipo_blocks])
        blocks.extend(calendar_columns)
        
        # === ECONOMIC CALENDAR WIDGET TRIGGER ===
        blocks.extend([
            {
                "type": "heading_2", 
                "heading_2": { "rich_text": [{"type": "text", "text": {"content": "🗓️ Economic Calendar"}}]}
            },
            {
                # Replaced the old 'embed' block with this 'callout' block
                "type": "callout",
                "callout": {
                    "rich_text": [{
                        "type": "text",
                        "text": {
                            "content": "ECONOMIC_CALENDAR_WIDGET".strip()
                        }
                    }],
                    "icon": {
                        "type": "emoji",
                        "emoji": "⚙️"
                    }
                }
            }
        ])
        
        # === TOP HEADLINES ===
        headlines_blocks = self._build_headlines_section(payload.top_headlines)
        blocks.extend(headlines_blocks)
        
        # === DETAILED SECTION ANALYSIS ===
        blocks.append({
            "type": "heading_2", 
            "heading_2": {
                "rich_text": [{
                    "type": "text", 
                    "text": {"content": "Detailed Section Analysis"}
                }]
            }
        })
        
        for section in analysis.section_analyses:
            section_title = section.section_name.replace('_', ' ').title()
            toggle_header = f"{section_title}: {section.section_sentiment} ({section.avg_performance:+.2f}%)"
            
            blocks.append({
                "type": "toggle",
                "toggle": {
                    "rich_text": [{
                        "type": "text", 
                        "text": {"content": toggle_header}
                    }],
                    "children": [
                        {
                            "type": "bulleted_list_item", 
                            "bulleted_list_item": {
                                "rich_text": [{
                                    "type": "text", 
                                    "text": {"content": mover}
                                }]
                            }
                        } for mover in section.key_movers
                    ]
                }
            })
        
        # === FOOTER ===
        blocks.extend([
            {"type": "divider", "divider": {}},
            {
                "type": "quote",
                "quote": {
                    "rich_text": [{
                        "type": "text",
                        "text": {"content": "Market data sourced from Binance, IG-Index, Mexc, and Finnhub. Economic calendar via TradingView, macro data via FRED."},
                        "annotations": {"italic": True}
                    }]
                }
            }
        ])
        
        return blocks

    def _build_market_data_table(self, section_name: str, section_config: dict, raw_data: List[Dict]) -> List[Dict]:
        """
        Builds a list of blocks for a market data section, with special 2-column
        formatting and yield change estimation for interest rates.
        """
        section_title = section_config.get('title', section_name.replace('_', ' ').title())
        blocks = [{"type": "heading_3", "heading_3": {"rich_text": [{"type": "text", "text": {"content": section_title}}]}}]

        if not raw_data:
            blocks.append({"type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": "Data not available."}}]}})
            return blocks

        table_rows = []
        is_rates_section = 'Yield' in section_config.get('title', '') or section_name == 'rates'

        for symbol_data in raw_data:
            display_name = symbol_data.get('display_name', symbol_data.get('symbol', 'N/A'))
            price = symbol_data.get('price', 0)
            market_status = symbol_data.get('market_status', 'OPEN').upper()

            if market_status == 'CLOSED':
                change_formatted = "Market Closed"
                change_color = "gray"
                price_formatted = f"{price:,.2f}" # Keep last price for context
            elif is_rates_section:
                price_formatted = f"{price:,.2f}" # This is the futures price
                change_percent = symbol_data.get('change_percent', 0)
                if '2Y' in display_name: multiplier = -40
                elif '10Y' in display_name: multiplier = -17
                else: multiplier = -20
                change_bps = change_percent * multiplier
                change_formatted = f"{change_bps:+.0f} bps"
                change_color = "green" if change_bps < 0 else "red"
            else:
                price_formatted = f"{price:,.2f}"
                if 'USD' in symbol_data.get('symbol', '') or symbol_data.get('symbol', '').startswith('$'):
                    price_formatted = f"${price:,.2f}"
                change_percent = symbol_data.get('change_percent', 0)
                change_formatted = f"{change_percent:+.2f}%"
                change_color = "green" if change_percent > 0 else "red"

            # --- Conditionally build the table row cells ---
            if is_rates_section:
                # 2-column row: Instrument, Change in Yield
                cells = [
                    [{"type": "text", "text": {"content": display_name}, "annotations": {"bold": True}}],
                    [{"type": "text", "text": {"content": change_formatted}, "annotations": {"color": change_color}}]
                ]
            else:
                # 3-column row: Instrument, Level, Change
                cells = [
                    [{"type": "text", "text": {"content": display_name}, "annotations": {"bold": True}}],
                    [{"type": "text", "text": {"content": price_formatted}}],
                    [{"type": "text", "text": {"content": change_formatted}, "annotations": {"color": change_color}}]
                ]
            
            table_rows.append({"type": "table_row", "table_row": {"cells": cells}})

        # --- Conditionally build the table structure ---
        if is_rates_section:
            # 2-column table
            table_width = 2
            header_cells = [
                [{"type": "text", "text": {"content": "Instrument"}, "annotations": {"bold": True}}],
                [{"type": "text", "text": {"content": "Change in Yield"}, "annotations": {"bold": True}}]
            ]
        else:
            # 3-column table
            table_width = 3
            header_cells = [
                [{"type": "text", "text": {"content": "Instrument"}, "annotations": {"bold": True}}],
                [{"type": "text", "text": {"content": "Level"}, "annotations": {"bold": True}}],
                [{"type": "text", "text": {"content": "Change"}, "annotations": {"bold": True}}]
            ]
            
        table_block = {
            "type": "table",
            "table": {
                "table_width": table_width,
                "has_column_header": True,
                "has_row_header": False,
                "children": [
                    {"type": "table_row", "table_row": {"cells": header_cells}},
                    *table_rows
                ]
            }
        }
        blocks.append(table_block)
        return blocks


    def _build_calendar_section(self, title: str, events: List[Dict], event_type: str) -> List[Dict]:
        """Builds a list of blocks for a calendar section (heading + list items)."""
        # Start with a heading block
        blocks = [{
            "type": "heading_2",
            "heading_2": {"rich_text": [{"type": "text", "text": {"content": title}}]}
        }]

        if not events or not isinstance(events, list):
            blocks.append({
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": f"No upcoming {event_type} events."}}]}
            })
        else:
            for event in events[:25]:  # Limit events
                if not isinstance(event, dict): continue
                
                symbol = event.get('symbol', 'N/A')
                date = event.get('date', 'TBD')
                details = event.get('priceRange', 'TBD') if event_type == 'ipo' else f"EPS Est: {event.get('estimate', 'N/A')}"
                
                blocks.append({
                    "type": "bulleted_list_item",
                    "bulleted_list_item": {"rich_text": [{"type": "text", "text": {"content": f"{symbol}: {details} ({date})"}}] }
                })
        
        # Return the list of blocks directly, without wrapping them in a toggle
        return blocks

    def _build_headlines_section(self, headlines: List[Headline]) -> list:
        """Build top headlines section from database headlines"""
        blocks = [{
            "type": "heading_2",
            "heading_2": {
                "rich_text": [{
                    "type": "text",
                    "text": {"content": "Top Headlines"}
                }]
            }
        }]
        
        if not headlines:
            blocks.append({
                "type": "callout",
                "callout": {
                    "rich_text": [{
                        "type": "text",
                        "text": {"content": "No headlines available since midnight"}
                    }],
                    "icon": {"emoji": "📰"}
                }
            })
            return blocks
        
        # Display headlines as expandable toggles
        for headline in headlines:
            headline_text = headline.headline
            url = headline.url or ""
            summary = headline.summary or ""
            score = headline.score or 0
            commentary = getattr(headline, 'commentary', None) # Safely get the new commentary
            
            # 1. Add the headline itself as a heading
            blocks.append({
                "type": "heading_3",
                "heading_3": {
                    "rich_text": [{"type": "text", "text": {"content": headline_text}, "annotations": {"bold": True}}]
                }
            })

            # 2. Add the AI Commentary as a quote (always visible)
            if commentary:
                blocks.append({
                    "type": "quote",
                    "quote": {
                        "rich_text": [{"type": "text", "text": {"content": commentary}, "annotations": {"color": "blue"}}]
                    }
                })
            
            # 3. Build the list of items to put inside the toggle
            toggle_children = []
            if summary:
                toggle_children.append({
                    "type": "paragraph",
                    "paragraph": {"rich_text": [{"type": "text", "text": {"content": f"Summary: {summary}"}}]}
                })
            if url:
                toggle_children.append({
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{
                            "type": "text",
                            "text": {
                                "content": "Source Link",
                                "link": {"url": url} # <-- The 'link' object is now INSIDE the 'text' object
                            }
                        }]
                    }
                })
            if score:
                toggle_children.append({
                    "type": "paragraph",
                    "paragraph": {"rich_text": [{"type": "text", "text": {"content": f"Impact Score: {score}/10"}}]}
                })

            # 4. Add the toggle only if there are details to hide
            if toggle_children:
                blocks.append({
                    "type": "toggle",
                    "toggle": {
                        "rich_text": [{"type": "text", "text": {"content": "Details & Source"}}],
                        "children": toggle_children
                    }
                })
            
            # 5. Add a divider to separate from the next headline
            blocks.append({"type": "divider", "divider": {}})

        return blocks

    def update_engagement_metrics(
        self, 
        notion_page_id: str, 
        likes: int, 
        retweets: int, 
        replies: int
    ) -> bool:
        """
        Update engagement metrics for an existing Notion page.
        Used by engagement sync service.
        
        Args:
            notion_page_id: ID of the Notion page to update
            likes: Current like count
            retweets: Current retweet count  
            replies: Current reply count
            
        Returns:
            bool: True if successful, False if failed
        """
        if not self.client:
            self.logger.error("Notion client not available for engagement update")
            return False
        
        try:
            # Calculate engagement score (simple formula)
            engagement_score = likes + (retweets * 2) + (replies * 3)
            
            # Update the page properties
            self.client.pages.update(
                page_id=notion_page_id,
                properties={
                    "Likes": {"number": likes},
                    "Retweets": {"number": retweets},
                    "Replies": {"number": replies},
                    "Engagement Score": {"number": engagement_score}
                }
            )
            
            self.logger.info(f"✅ Updated engagement for Notion page {notion_page_id}")
            self.logger.info(f"   Likes: {likes}, Retweets: {retweets}, Replies: {replies}")
            self.logger.info(f"   Engagement Score: {engagement_score}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to update engagement for {notion_page_id}: {e}")
            return False

    
    
    def get_client_status(self) -> Dict[str, Any]:
        """
        Get the status of the Notion client for monitoring.
        
        Returns:
            Dict with client status information
        """
        return {
            "client_initialized": self.client is not None,
            "api_key_configured": bool(self.notion_api_key),
            "database_id_configured": bool(self.hedgefund_tweet_db_id),
            "database_id": self.hedgefund_tweet_db_id,
            "last_check": datetime.now(timezone.utc).isoformat()
        }


# Convenience function for testing
def test_notion_connection():
    """Test Notion connection and database access"""
    publisher = NotionPublisher()
    status = publisher.get_client_status()
    
    print("🧪 Testing Notion connection...")
    print(f"Status: {status}")
    
    if not status["client_initialized"]:
        print("❌ Notion client not initialized")
        return False
    
    if not status["database_id_configured"]:
        print("❌ HEDGEFUND_TWEET_DB_ID not configured")
        return False
    
    try:
        # Test database access by querying it
        response = publisher.client.databases.retrieve(
            database_id=publisher.hedgefund_tweet_db_id
        )
        print(f"✅ Database accessible: {response['title'][0]['plain_text']}")
        return True
        
    except Exception as e:
        print(f"❌ Database access failed: {e}")
        return False


if __name__ == "__main__":
    test_notion_connection()