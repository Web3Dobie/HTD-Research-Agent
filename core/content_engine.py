# core/content_engine.py
"""
ContentEngine - Complete content generation and publishing pipeline.
Integrates headline processing, GPT generation, market data, and multi-platform publishing.
"""

import logging
import asyncio
import aiohttp
from typing import Optional, Dict, Any
from datetime import datetime, timezone

from core.models import GeneratedContent, ContentRequest, ContentType, ContentCategory, BriefingPayload
from services.database_service import DatabaseService
from services.gpt_service import GPTService
from services.market_client import MarketClient
from services.publishing_service import PublishingService
from services.notion_publisher import NotionPublisher
from services.telegram_notifier import TelegramNotifier
from generators.commentary_generator import CommentaryGenerator
from generators.deep_dive_generator import DeepDiveGenerator
from generators.briefing_generator import BriefingGenerator
from config.settings import DATABASE_CONFIG, AGENT_NAME
from config.sentiment_config import SENTIMENT_CONFIG
from services.briefing_config_service import ConfigService
from services.market_sentiment_service import ComprehensiveMarketSentimentService
from services.prompt_augmentation_service import PromptAugmentationService

class ContentEngine:
    """
    Main orchestrator for content generation and publishing.
    Handles the complete pipeline from headline to published content.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Initialize all services with proper configuration
        self.database_service = DatabaseService(DATABASE_CONFIG)
        self.gpt_service = GPTService()
        self.market_client = MarketClient()
        self.prompt_augmentation_service = PromptAugmentationService()

        # Publishing services
        self.publishing_service = PublishingService()
        self.notion_publisher = NotionPublisher()
        self.telegram_notifier = TelegramNotifier()
        
        # Content generators
        try:
            # CommentaryGenerator expects: data_service, gpt_service, market_client, config
            commentary_config = { "agent_name": AGENT_NAME, "include_market_data": True }
            self.commentary_generator = CommentaryGenerator(
                data_service=self.database_service,
                gpt_service=self.gpt_service,
                market_client=self.market_client,
                config=commentary_config
            )
            self.logger.info("✅ CommentaryGenerator initialized successfully")
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize CommentaryGenerator: {e}")
            self.commentary_generator = None
            
        try:
            # DeepDiveGenerator follows the same pattern
            deep_dive_config = { "agent_name": AGENT_NAME, "include_market_data": True }
            self.deep_dive_generator = DeepDiveGenerator(
                data_service=self.database_service,
                gpt_service=self.gpt_service,
                market_client=self.market_client,
                config=deep_dive_config
            )
            self.logger.info("✅ DeepDiveGenerator initialized successfully")
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize DeepDiveGenerator: {e}")
            self.deep_dive_generator = None

        try:
            self.briefing_config_service = ConfigService(self.database_service, SENTIMENT_CONFIG)
            self.sentiment_service = ComprehensiveMarketSentimentService(self.gpt_service)

            # The new briefing generator
            self.briefing_generator = BriefingGenerator(
                config_service=self.briefing_config_service,
                sentiment_service=self.sentiment_service,
                market_client=self.market_client,
                db_service=self.database_service,
                gpt_service=self.gpt_service,
                prompt_augmentation_service=self.prompt_augmentation_service
            )
            self.logger.info("✅ BriefingGenerator initialized successfully with orchestrator pattern")
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize BriefingGenerator: {e}")
            self.briefing_generator = None
            self.news_client = None
        
        self.logger.info("✅ ContentEngine initialized with all services")
    
    async def generate_and_publish_content(self, request: ContentRequest) -> Dict[str, Any]:
        """
        Complete pipeline: generate content and publish to all platforms.
        
        Args:
            request: ContentRequest specifying what type of content to generate
            
        Returns:
            Dict containing generation and publishing results
        """
        start_time = datetime.now(timezone.utc)
        
        # Notify start via Telegram
        await self.telegram_notifier.notify_job_start(
            f"Generate {request.content_type.value.title()}", 
            f"Category: {request.category.value if request.category else 'Any'}"
        )
        
        try:
            # Step 1: Generate content
            self.logger.info(f"🚀 Starting content generation: {request.content_type.value}")
            content = await self.generate_content(request)
            
            if not content:
                duration = (datetime.now(timezone.utc) - start_time).total_seconds()
                await self.telegram_notifier.notify_job_failure(
                    f"Generate {request.content_type.value.title()}", 
                    "Content generation failed",
                    duration
                )
                return {
                    "success": False,
                    "error": "Content generation failed",
                    "duration": duration
                }
            
            # Step 2: Publish to Twitter (handles threads vs single tweets)
            self.logger.info(f"📢 Publishing content: {content.theme}")

            if content.content_type == ContentType.DEEP_DIVE:
                twitter_result = self.publishing_service.publish_thread(content)
            else:
                twitter_result = self.publishing_service.publish_tweet(content)
            
            if not twitter_result.success:
                duration = (datetime.now(timezone.utc) - start_time).total_seconds()
                await self.telegram_notifier.notify_job_failure(
                    f"Generate {request.content_type.value.title()}", 
                    f"Twitter publishing failed: {twitter_result.error}",
                    duration
                )
                return {
                    "success": False,
                    "error": f"Twitter publishing failed: {twitter_result.error}",
                    "content": self._content_to_dict(content),
                    "duration": duration
                }
            
            # Step 3: Publish to Notion (for website)
            notion_page_id = self.notion_publisher.publish_tweet_to_notion(content, twitter_result)
            
            # Step 4: Log to database
            await self._log_content_and_results(content, twitter_result, notion_page_id)
            
            # Step 5: Send success notification
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            result_summary = f"✅ Published successfully\n🔗 {twitter_result.url}"
            await self.telegram_notifier.notify_job_success(
                f"Generate {request.content_type.value.title()}", 
                duration, 
                result_summary
            )
            
            await self.telegram_notifier.notify_content_published(
                content_type=request.content_type.value,
                theme=content.theme,
                url=twitter_result.url
            )
            
            # Step 6: Prepare response
            response = {
                "success": True,
                "content": self._content_to_dict(content),
                "publishing": {
                    "twitter": {
                        "success": twitter_result.success,
                        "tweet_id": twitter_result.tweet_id,
                        "url": twitter_result.url,
                        "timestamp": twitter_result.timestamp
                    },
                    "notion": {
                        "success": bool(notion_page_id),
                        "page_id": notion_page_id
                    }
                },
                "duration": duration,
                "timestamp": start_time.isoformat()
            }
            
            self.logger.info(f"✅ Content pipeline completed successfully in {duration:.2f}s")
            return response
            
        except Exception as e:
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            error_msg = f"Content pipeline failed: {e}"
            self.logger.error(f"❌ {error_msg}")
            
            # Notify failure
            await self.telegram_notifier.notify_job_failure(
                f"Generate {request.content_type.value.title()}", 
                str(e),
                duration
            )
            
            return {
                "success": False,
                "error": error_msg,
                "duration": duration,
                "timestamp": start_time.isoformat()
            }
    
    async def generate_content(self, request: ContentRequest) -> Optional[GeneratedContent]:
        """
        Generate content based on request type.
        
        Args:
            request: ContentRequest specifying content parameters
            
        Returns:
            GeneratedContent object or None if generation failed
        """
        try:
            if request.content_type == ContentType.COMMENTARY:
                if not self.commentary_generator:
                    self.logger.error("CommentaryGenerator not available")
                    return None
                return await self.commentary_generator.generate(request)

            elif request.content_type == ContentType.DEEP_DIVE:
                if not self.deep_dive_generator:
                    self.logger.error("DeepDiveGenerator not available")
                    return None
                return await self.deep_dive_generator.generate(request)

            else:
                self.logger.error(f"Unknown content type: {request.content_type}")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Content generation failed: {e}")
            return None

    async def run_briefing_pipeline(self, briefing_key: str = 'morning_briefing', publish_tweet: bool = True): # <-- Add the parameter here
        """
        Executes the complete, end-to-end pipeline for generating and publishing a briefing.
        """
        self.logger.info(f"--- 🚀 Starting {briefing_key} pipeline (Publish Tweet: {publish_tweet}) ---")
        if not self.briefing_generator:
            self.logger.error("BriefingGenerator not available. Aborting.")
            return

        try:
            # Step 1: Generate the briefing content payload
            payload = await self.briefing_generator.create(briefing_key)
            self.logger.info("Step 1/7: Briefing payload generated successfully.")

            # Step 2: Publish to Notion to get the internal page_id
            notion_result = await self.notion_publisher.publish_briefing(payload, briefing_key)
            if not notion_result or 'page_id' not in notion_result:
                raise Exception("Failed to publish to Notion or get page_id.")
            notion_page_id = notion_result['page_id']
            self.logger.info(f"Step 2/7: Published to Notion, page_id: {notion_page_id}")

            # Step 3: Create a record in our database to get a clean, permanent ID
            briefing_id = self.database_service.create_briefing_record(
                briefing_key=briefing_key,
                notion_page_id=notion_page_id,
                title=payload.config.get('briefing_title', 'Market Briefing')
            )
            self.logger.info(f"Step 3/7: Created database record, briefing_id: {briefing_id}")

            # Step 4: Construct the final, public-facing URL
            final_website_url = f"https://www.dutchbrat.com/briefings?briefing_id={briefing_id}"
            self.logger.info(f"Step 4/7: Constructed public URL: {final_website_url}")

            # Step 5, 6, 7: Conditionally publish tweet and update URLs
            if publish_tweet:
                self.logger.info("publish_tweet is True. Proceeding with tweet publication.")
                
                # Step 5: Generate the promotional tweet
                tweet_text = await self._generate_briefing_promo_tweet(
                    payload=payload,
                    briefing_url=final_website_url
                )
                self.logger.info("Step 5/7: Generated promotional tweet text.")

                # Step 6: Publish the tweet
                tweet_content = GeneratedContent(text=tweet_text, content_type=ContentType.BRIEFING, theme="Market Briefing")
                tweet_result = self.publishing_service.publish_tweet(tweet_content)
                if not tweet_result or not tweet_result.success:
                    raise Exception(f"Failed to publish tweet: {tweet_result.error}")
                self.logger.info(f"Step 6/7: Published tweet: {tweet_result.url}")
                
                # Step 7: Update Notion Page and Database with URLs
                self.notion_publisher.update_briefing_with_tweet(
                    notion_page_id=notion_page_id,
                    tweet_url=tweet_result.url
                )
                self.database_service.update_briefing_urls(
                    briefing_id=briefing_id,
                    website_url=final_website_url,
                    tweet_url=tweet_result.url
                )
                self.logger.info("Step 7/7: Updated Notion page and database with final URLs.")
            else:
                self.logger.warning("publish_tweet is False. Skipping Twitter post and URL updates.")
                self.database_service.update_briefing_urls(
                    briefing_id=briefing_id,
                    website_url=final_website_url,
                    tweet_url="" # Pass an empty string for the tweet_url
                )

            # Step 8 (New): Fetch the parsed JSON and cache it in the database
                try:
                    self.logger.info(f"Fetching parsed JSON from website API to cache for briefing ID: {briefing_id}")
                    # The agent calls its own website's API to get the fully parsed content
                    async with aiohttp.ClientSession() as session:
                        website_api_url = f"https://www.dutchbrat.com/api/briefings?briefingId={notion_page_id}"
                        async with session.get(website_api_url) as response:
                            if response.ok:
                                # We need to get the briefing from the 'data' array
                                api_response = await response.json()
                                briefing_json = api_response.get('data', [{}])[0]
                                # Save the parsed content to our new cache column
                                self.database_service.update_briefing_json_content(briefing_id, briefing_json)
                            else:
                                self.logger.error("Failed to fetch parsed JSON for caching.")
                except Exception as e:
                    self.logger.error(f"Failed during caching step: {e}")

                    self.logger.info(f"--- ✅ {briefing_key} pipeline completed successfully ---")

        except Exception as e:
            self.logger.error(f"--- ❌ Briefing pipeline failed for '{briefing_key}': {e} ---", exc_info=True)
            await self.telegram_notifier.send_message(f"ALERT: Briefing pipeline for {briefing_key} failed. Error: {e}")
    
    async def _generate_briefing_promo_tweet(self, payload: BriefingPayload, briefing_url: str) -> str:
        """
        Generates a promotional tweet by creating a GPT-powered blurb
        based on the briefing's Key Market Drivers, with a specific structure.
        """
        analysis = payload.market_analysis
        key_drivers = analysis.key_drivers

        if not key_drivers:
            # Fallback tweet
            return f"Today's market briefing is now live! See the full analysis of today's price action.\n\nRead more here:\n{briefing_url}\n\n#MarketBriefing #Investing"

        # Step 1: Generate the AI blurb from the key drivers
        drivers_str = ", ".join(key_drivers)
        prompt = f"Based on these key market drivers: '{drivers_str}', write a single, catchy summary sentence for a tweet (under 120 characters)."
        blurb = await asyncio.to_thread(
            self.gpt_service.generate_text,
            prompt,
            max_tokens=45,
            temperature=0.8
        )

        # Step 2: Format the Key Drivers list with emojis
        section_emoji_map = {
            'us_futures': '🇺🇸', 'european_futures': '🇪🇺', 'asian_focus': '🌏',
            'crypto': '🪙', 'fx': '💱', 'rates': '💵', 'volatility': '📉'
        }
        formatted_drivers = []
        for driver in key_drivers[:3]:
            emoji = '➡️'
            for section_key, emoji_char in section_emoji_map.items():
                if section_key.replace('_', ' ').lower() in driver.lower():
                    emoji = emoji_char
                    break
            formatted_drivers.append(f"{emoji} {driver}")
        
        drivers_text = "\n".join(formatted_drivers)
        hashtags = "#MarketAnalysis #Investing #Finance"
        
        # --- Step 3: Assemble the final tweet in your desired order ---
        # 1. Key Drivers
        # 2. Link
        # 3. AI Blurb
        # 4. Hashtags
        tweet_text = (
            f"{drivers_text}\n\n"
            f"Detailed analysis here:\n{briefing_url}\n\n"
            f"{blurb.strip()}\n\n"
            f"{hashtags}"
        )
        
        return tweet_text

    async def _log_content_and_results(
        self, 
        content: GeneratedContent, 
        twitter_result, 
        notion_page_id: Optional[str]
    ):
        """Log content and publishing results to database"""
        try:
            # Save to database for analytics
            content_record = {
                'content_type': content.content_type.value,
                'category': content.category.value if content.category else None,
                'theme': content.theme,
                'text': content.text,
                'headline_id': content.headline_used.id if content.headline_used else None,
                'market_data': [data.__dict__ if hasattr(data, '__dict__') else data for data in content.market_data] if content.market_data else [],
                'published': twitter_result.success,
                'tweet_id': twitter_result.tweet_id,
                'tweet_url': twitter_result.url,
                'notion_page_id': notion_page_id,
                'created_at': datetime.now(timezone.utc)
            }
            
            await self.database_service.log_content_generation(content_record)
            
            # Update theme usage if content was published successfully
            if twitter_result.success:
                await self.database_service.update_theme_usage(content.theme)
            
            self.logger.info(f"📝 Logged content and publishing results to database")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to log content results: {e}")
    
    def _content_to_dict(self, content: GeneratedContent) -> Dict[str, Any]:
        """Convert GeneratedContent to dictionary for API responses"""
        return {
            "text": content.text,
            "theme": content.theme,
            "category": content.category.value if content.category else None,
            "type": content.content_type.value,
            "market_data_count": len(content.market_data) if content.market_data else 0,
            "headline_used": {
                "id": content.headline_used.id,
                "headline": content.headline_used.headline
            } if content.headline_used else None
        }

    async def generate_commentary_now(self, category: Optional[ContentCategory] = None) -> Dict[str, Any]:
        """
        Convenience method to generate and publish commentary immediately.
        """
        request = ContentRequest(
            content_type=ContentType.COMMENTARY,
            category=category,
            include_market_data=True
        )
        return await self.generate_and_publish_content(request)


    async def generate_deep_dive_now(self, category: Optional[ContentCategory] = None) -> Dict[str, Any]:
        """
        Convenience method to generate and publish a deep dive immediately.
        
        Args:
            category: Optional category filter for headlines
            
        Returns:
            Complete pipeline results
        """
        request = ContentRequest(
            content_type=ContentType.DEEP_DIVE,
            category=category,
            include_market_data=True
        )
        
        return await self.generate_and_publish_content(request)
    
    async def get_pipeline_status(self) -> Dict[str, Any]:
        """
        Get current status of the content pipeline and all services.
        
        Returns:
            Dict with status information for monitoring
        """
        try:
            # Check service health
            twitter_status = self.publishing_service.get_client_status()
            
            try:
                notion_status = self.notion_publisher.get_client_status()
            except Exception as e:
                notion_status = {"status": "error", "error": str(e)}
            
            try:
                telegram_status = self.telegram_notifier.get_status()
            except Exception as e:
                telegram_status = {"status": "error", "error": str(e)}
            
            # Test database connection
            db_status = {"status": "unknown"}
            try:
                if await self.database_service.test_connection():
                    db_status = {"status": "healthy"}
                else:
                    db_status = {"status": "unhealthy", "error": "Connection test failed"}
            except Exception as e:
                db_status = {"status": "unhealthy", "error": str(e)}

            # Test market service
            try:
                test_prices = await self.market_client.get_bulk_prices(["SPY"])
                market_status = {
                    "status": "healthy" if test_prices else "degraded",
                    "test_response": bool(test_prices)
                }
            except Exception as e:
                market_status = {"status": "unhealthy", "error": str(e)}
            
            return {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "services": {
                    "database": db_status,
                    "market_data": market_status,
                    "twitter": twitter_status,
                    "notion": notion_status,
                    "telegram": telegram_status
                },
                "generators": {
                    "commentary": "active" if self.commentary_generator else "inactive",
                    "deep_dive": "active" if self.deep_dive_generator else "inactive",
                    "briefing": "pending_implementation"
                }
            }
            
        except Exception as e:
            self.logger.error(f"❌ Failed to get pipeline status: {e}")
            return {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "error": str(e),
                "status": "unhealthy"
            }

async def publish_commentary_now(category: Optional[str] = None) -> Dict[str, Any]:
    """
    Quick function to generate and publish commentary.
    """
    engine = ContentEngine()
    category_enum = None
    if category:
        try:
            category_enum = ContentCategory[category.upper()]
        except KeyError:
            logging.warning(f"Unknown category '{category}', proceeding without category filter.")
    
    return await engine.generate_commentary_now(category_enum)

async def publish_deep_dive_now(category: Optional[str] = None) -> Dict[str, Any]:
    """
    Quick function to generate and publish a deep dive.
    """
    engine = ContentEngine()
    category_enum = None
    if category:
        try:
            category_enum = ContentCategory[category.upper()]
        except KeyError:
            logging.warning(f"Unknown category '{category}', proceeding without category filter.")

    return await engine.generate_deep_dive_now(category_enum)

async def get_system_health() -> Dict[str, Any]:
    """Get complete system health status"""
    engine = ContentEngine()
    return await engine.get_pipeline_status()

# Example usage for testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    async def test_content_engine():
        """Test the complete content engine"""
        print("🧪 Testing ContentEngine...")
        
        engine = ContentEngine()
        
        print("\n📊 Getting pipeline status...")
        status = await engine.get_pipeline_status()
        import json
        print(json.dumps(status, indent=2))
        
        # --- Example: Generate and publish a deep dive ---
        # print("\n\n🚀 Testing Deep Dive Generation (Equity)...")
        # result_deep_dive = await publish_deep_dive_now(category="equity")
        # print(json.dumps(result_deep_dive, indent=2))
        
        # --- Example: Generate and publish commentary ---
        # print("\n\n💬 Testing Commentary Generation (Macro)...")
        # result_commentary = await publish_commentary_now(category="macro")
        # print(json.dumps(result_commentary, indent=2))
    
    asyncio.run(test_content_engine())