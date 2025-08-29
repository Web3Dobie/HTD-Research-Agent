# services/notion_publisher.py
"""
NotionPublisher - Publishes tweets to the HedgeFund Tweet Notion database.
Integrates with the website's existing database structure and APIs.
"""

import logging
import os
from typing import Optional, Dict, Any
from datetime import datetime, timezone

from notion_client import Client

from core.models import GeneratedContent, BriefingPayload
from services.publishing_service import TwitterResult
from config.settings import NOTION_CONFIG


class NotionPublisher:
    """
    Publishes tweets to the HedgeFund Tweet Notion database.
    Maintains compatibility with existing website APIs.
    """
    
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
    
    async def publish_briefing(self, analysis, config: dict) -> Optional[str]:
        """
        Creates a rich Notion page, then updates it with its own public URL
        in the 'PDF Link' property for website integration.
        """
        if not self.client:
            self.logger.error("Notion client not available for briefing publish")
            return None

        publishing_config = config.get('publishing_config', {})
        database_id = publishing_config.get('notion_database_id')
        
        if not database_id:
            self.logger.error("Briefing Notion Database ID not found in config")
            return None
        
        try:
            # Step 1: Create the page with its content and initial properties
            page_properties = {
                "Name": {"title": [{"text": {"content": config.get('briefing_title')}}]},
                "Date": {"date": {"start": datetime.now(timezone.utc).isoformat()}},
                "Sentiment": {"select": {"name": analysis.sentiment.value}}
            }
            page_blocks = self._build_briefing_blocks(analysis)

            created_page = self.client.pages.create(
                parent={"database_id": database_id},
                properties=page_properties,
                children=page_blocks
            )
            
            page_id = created_page["id"]
            page_url = created_page.get("url")
            self.logger.info(f"✅ Briefing page created in Notion: {page_id}")

            # Step 2: Update the page with its own URL in the 'PDF Link' property
            if page_url:
                await asyncio.sleep(1) # Small delay to ensure the page is ready for an update
                self.client.pages.update(
                    page_id=page_id,
                    properties={
                        "PDF Link": {"url": page_url} # <-- This is the key change
                    }
                )
                self.logger.info(f"   Updated 'PDF Link' property with public URL: {page_url}")

            return page_url

        except Exception as e:
            self.logger.error(f"❌ Failed to publish briefing to Notion: {e}")
            return None

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
    
    def _build_briefing_blocks(self, analysis) -> list:
        """Converts a SentimentAnalysis object into a list of Notion blocks."""
        sentiment_emoji = "🐂" if analysis.sentiment.value == "BULLISH" else "🐻" if analysis.sentiment.value == "BEARISH" else "📊"
        blocks = [
            {"type": "heading_1", "heading_1": {"rich_text": [{"type": "text", "text": {"content": f"Global Market Sentiment: {analysis.sentiment.value} {sentiment_emoji}"}}]}},
            {"type": "quote", "quote": {"rich_text": [{"type": "text", "text": {"content": analysis.market_summary}}]}},
            {"type": "heading_2", "heading_2": {"rich_text": [{"type": "text", "text": {"content": "Key Market Drivers"}}]}},
            *[{
                "type": "bulleted_list_item",
                "bulleted_list_item": {"rich_text": [{"type": "text", "text": {"content": driver}}]}
            } for driver in analysis.key_drivers],
            {"type": "heading_2", "heading_2": {"rich_text": [{"type": "text", "text": {"content": "Detailed Section Analysis"}}]}}
        ]
        
        for section in analysis.section_analyses:
            section_title = section.section_name.replace('_', ' ').title()
            toggle_header = f"{section_title}: {section.section_sentiment} ({section.avg_performance:+.2f}%)"
            
            blocks.append({
                "type": "toggle",
                "toggle": {
                    "rich_text": [{"type": "text", "text": {"content": toggle_header}}],
                    "children": [
                        {"type": "bulleted_list_item", "bulleted_list_item": {"rich_text": [{"type": "text", "text": {"content": mover}}]}} 
                        for mover in section.key_movers
                    ]
                }
            })
        
        # --- ADD THE FOOTNOTE ---
        blocks.extend([
            {"type": "divider", "divider": {}},
            {
                "type": "quote",
                "quote": {
                    "rich_text": [{
                        "type": "text",
                        "text": {"content": "Market data and calendars are sourced from Binance, IG-Index, Mexc, and Finnhub."},
                        "annotations": {"italic": True}
                    }]
                }
            }
        ])
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