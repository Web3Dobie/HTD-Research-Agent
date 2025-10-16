# core/content_engine.py
"""
ContentEngine - Complete content generation and publishing pipeline.
Integrates headline processing, GPT generation, market data, and multi-platform publishing.
"""

import logging
import asyncio
import aiohttp
from typing import Optional, Dict, Any, List, Tuple
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
from services.json_caching_service import JSONCachingService
from services.chart_generation_service import ChartGenerationService

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
        self.chart_service = ChartGenerationService()

        # Publishing services
        self.publishing_service = PublishingService()
        self.notion_publisher = NotionPublisher()
        self.telegram_notifier = TelegramNotifier()
        self.json_caching_service = JSONCachingService()

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

    async def _generate_chart_for_tweet(self, payload: BriefingPayload) -> Optional[str]:
        """Generate chart for tweet if conditions are met."""
        try:
            if not payload.market_analysis or not payload.market_analysis.section_analyses:
                return None
            
            # Only generate charts for certain briefing types or conditions
            briefing_type = payload.config.get('briefing_title', '')
            
            # Generate chart for morning briefings or when volatility is high
            should_generate_chart = (
                'Morning' in briefing_type or 
                self._assess_volatility_level(payload.market_analysis.section_analyses) in ['high', 'elevated']
            )
            
            if not should_generate_chart:
                return None
            
            # Choose chart type based on data
            if len(payload.market_analysis.section_analyses) >= 4:
                chart_path = self.chart_service.generate_sentiment_chart(payload.market_analysis)
            else:
                chart_path = self.chart_service.generate_performance_summary_chart(payload.market_analysis.section_analyses)
            
            return chart_path
            
        except Exception as e:
            self.logger.error(f"Failed to generate chart for tweet: {e}")
            return None

    async def run_briefing_pipeline(self, briefing_key: str = 'morning_briefing', publish_tweet: bool = True, include_charts: bool = True):
        """
        Executes the complete, end-to-end pipeline for generating and publishing a briefing.
        Added include_charts parameter for chart generation control.
        """
        self.logger.info(f"--- 🚀 Starting {briefing_key} pipeline (Publish Tweet: {publish_tweet}, Charts: {include_charts}) ---")
        if not self.briefing_generator:
            self.logger.error("BriefingGenerator not available. Aborting.")
            return

        chart_path = None  # Track chart for cleanup
        
        try:
            # Step 1: Generate the briefing content payload
            payload = await self.briefing_generator.create(briefing_key)
            self.logger.info("Step 1/8: Briefing payload generated successfully.")

            # Step 2: Publish to Notion to get the internal page_id
            notion_result = await self.notion_publisher.publish_briefing(payload, briefing_key)
            if not notion_result or 'page_id' not in notion_result:
                raise Exception("Failed to publish to Notion or get page_id.")
            notion_page_id = notion_result['page_id']
            self.logger.info(f"Step 2/8: Published to Notion, page_id: {notion_page_id}")

            # Step 3: Create a record in our database to get a clean, permanent ID
            briefing_id = self.database_service.create_briefing_record(
                briefing_key=briefing_key,
                notion_page_id=notion_page_id,
                title=payload.config.get('briefing_title', 'Market Briefing')
            )
            self.logger.info(f"Step 3/8: Created database record, briefing_id: {briefing_id}")

            # Step 4: Construct the final, public-facing URL
            final_website_url = f"https://www.dutchbrat.com/briefings?briefing_id={briefing_id}"
            self.logger.info(f"Step 4/8: Constructed public URL: {final_website_url}")
            
            tweet_url = ""
            
            # Step 5-7: Enhanced tweet publishing with optional charts
            if publish_tweet:
                self.logger.info("publish_tweet is True. Proceeding with enhanced tweet publication.")
                
                # Step 5: Generate tweet text and optionally chart
                if include_charts:
                    tweet_text, chart_path = await self._generate_briefing_promo_tweet_with_chart(
                        payload=payload,
                        briefing_url=final_website_url
                    )
                    if chart_path:
                        self.logger.info(f"Step 5/8: Generated tweet with chart: {chart_path}")
                    else:
                        self.logger.info("Step 5/8: Generated tweet text (no chart generated)")
                else:
                    tweet_text = await self._generate_briefing_promo_tweet(
                        payload=payload,
                        briefing_url=final_website_url
                    )
                    chart_path = None
                    self.logger.info("Step 5/8: Generated tweet text (charts disabled)")

                # Step 6: Publish the tweet (with or without media)
                tweet_content = GeneratedContent(text=tweet_text, content_type=ContentType.BRIEFING, theme="Market Briefing")
                
                # Use appropriate publishing method based on whether we have a chart
                if chart_path and hasattr(self.publishing_service, 'publish_tweet_with_media'):
                    tweet_result = self.publishing_service.publish_tweet_with_media(tweet_content, chart_path)
                else:
                    # Fallback to regular tweet if no chart or media method not available
                    if chart_path:
                        self.logger.warning("Chart generated but publish_tweet_with_media not available, publishing text-only")
                    tweet_result = self.publishing_service.publish_tweet(tweet_content)
                    
                if not tweet_result or not tweet_result.success:
                    raise Exception(f"Failed to publish tweet: {tweet_result.error}")
                self.logger.info(f"Step 6/8: Published tweet: {tweet_result.url}")
                
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
                tweet_url = tweet_result.url
                self.logger.info("Step 7/8: Updated Notion page and database with final URLs.")
            else:
                self.logger.warning("publish_tweet is False. Skipping Twitter post and URL updates.")
                self.database_service.update_briefing_urls(
                    briefing_id=briefing_id,
                    website_url=final_website_url,
                    tweet_url=""
                )

            # Step 8: Generate and cache the JSON content locally
            try:
                self.logger.info(f"Step 8/8: Generating and caching JSON for briefing ID: {briefing_id}")
                
                briefing_json = self.json_caching_service.generate_json_from_payload(
                    payload=payload,
                    briefing_id=briefing_id,
                    notion_page_id=notion_page_id,
                    final_website_url=final_website_url,
                    tweet_url=tweet_url
                )

                if briefing_json:
                    self.logger.info("Attempting to save JSON to database...")
                    self.database_service.update_briefing_json_content(briefing_id, briefing_json)
                else:
                    self.logger.error("JSON content generation resulted in an empty object. Caching skipped.")

            except Exception as e:
                self.logger.error(f"CRITICAL: Failed during local JSON caching step: {e}", exc_info=True)

        except Exception as e:
            self.logger.error(f"--- ❌ Briefing pipeline failed for '{briefing_key}': {e} ---", exc_info=True)
            await self.telegram_notifier.send_message(f"ALERT: Briefing pipeline for {briefing_key} failed. Error: {e}")
        
        finally:
            # Clean up chart file if it was generated
            if chart_path and hasattr(self.chart_service, 'cleanup_chart'):
                try:
                    self.chart_service.cleanup_chart(chart_path)
                    self.logger.debug(f"Cleaned up chart file: {chart_path}")
                except Exception as e:
                    self.logger.warning(f"Failed to cleanup chart: {e}")
                    
    async def _generate_briefing_promo_tweet(self, payload: BriefingPayload, briefing_url: str) -> str:
        """
        Enhanced promotional tweet with visual sentiment integration and custom AI hooks.
        """
        analysis = payload.market_analysis
        
        if not analysis:
            return self._fallback_tweet(briefing_url)
        
        # Step 1: Generate custom AI hook based on market conditions
        custom_hook = await self._generate_custom_hook(analysis, payload.config)
        
        # Step 2: Create visual sentiment indicator
        sentiment_visual = self._create_sentiment_visual(analysis)
        
        # Step 3: Format key drivers with performance context
        key_drivers_formatted = self._format_key_drivers_with_performance(analysis, payload.raw_market_data)
        
        # Step 4: Generate market insight from section performance
        market_insight = self._generate_market_insight(analysis.section_analyses)
        
        # Step 5: Create engaging hashtags based on sentiment
        dynamic_hashtags = self._generate_dynamic_hashtags(analysis.sentiment, payload.config)
        
        # Step 6: Assemble the enhanced tweet
        tweet_text = self._assemble_enhanced_tweet(
            custom_hook=custom_hook,
            sentiment_visual=sentiment_visual,
            briefing_title=payload.config.get('briefing_title', 'Market Briefing'),
            key_drivers=key_drivers_formatted,
            market_insight=market_insight,
            briefing_url=briefing_url,
            hashtags=dynamic_hashtags
        )
        
        return tweet_text

    async def _generate_custom_hook(self, analysis, config: Dict) -> str:
        """Generate AI-powered custom opening hook based on market conditions."""
        
        # Create context for hook generation
        market_context = {
            'sentiment': analysis.sentiment.value,
            'confidence': analysis.confidence_score,
            'key_drivers': analysis.key_drivers[:2],  # Top 2 drivers
            'briefing_period': config.get('briefing_title', ''),
            'volatility_level': self._assess_volatility_level(analysis.section_analyses)
        }
        
        prompt = f"""
        Generate a compelling, attention-grabbing opening line for a financial market tweet based on these conditions:
        
        Market Sentiment: {market_context['sentiment']} (confidence: {market_context['confidence']:.1f})
        Period: {market_context['briefing_period']}
        Key Drivers: {', '.join(market_context['key_drivers'])}
        Market Volatility: {market_context['volatility_level']}
        
        Requirements:
        - Maximum 60 characters
        - Create urgency and intrigue
        - Professional but engaging tone
        - No generic phrases like "markets are mixed"
        - Include action words or market-moving implications
        
        Examples of good hooks:
        - "🚨 Critical shift in futures ahead of open"
        - "⚡ Crypto crash ripples through risk assets"
        - "🔥 Explosive sector rotation underway"
        - "⚠️ Fed signals trigger bond market upheaval"
        """
        
        hook = await asyncio.to_thread(
            self.gpt_service.generate_text,
            prompt,
            max_tokens=25,
            temperature=0.9
        )
        
        return hook.strip()

    def _create_sentiment_visual(self, analysis) -> str:
        """Create visual sentiment indicator with emojis and formatting."""
        
        sentiment_config = {
            'BULLISH': {
                'emoji': '🐂',
                'indicator': '📈',
                'color_hint': '🟢',
                'prefix': 'BULLISH'
            },
            'BEARISH': {
                'emoji': '🐻', 
                'indicator': '📉',
                'color_hint': '🔴',
                'prefix': 'BEARISH'
            },
            'MIXED': {
                'emoji': '⚖️',
                'indicator': '📊',
                'color_hint': '🟡', 
                'prefix': 'MIXED'
            },
            'NEUTRAL': {
                'emoji': '😐',
                'indicator': '➡️',
                'color_hint': '⚪',
                'prefix': 'NEUTRAL'
            }
        }
        
        config = sentiment_config.get(analysis.sentiment.value, sentiment_config['NEUTRAL'])
        confidence_bars = '█' * min(int(analysis.confidence_score * 5), 5)
        
        return f"{config['color_hint']} {config['prefix']} {config['emoji']} {config['indicator']} [{confidence_bars}]"

    def _format_key_drivers_with_performance(self, analysis, raw_market_data: Dict) -> str:
        """Format key drivers with actual performance context."""
        
        if not analysis.key_drivers:
            return "Mixed signals across markets"
        
        # Extract performance numbers from section analyses
        section_performance = {
            section.section_name: section.avg_performance 
            for section in analysis.section_analyses
        }
        
        formatted_drivers = []
        for driver in analysis.key_drivers[:2]:  # Top 2 drivers
            # Try to find corresponding performance data
            performance_text = ""
            for section_name, performance in section_performance.items():
                if section_name.replace('_', ' ').lower() in driver.lower():
                    performance_text = f" ({performance:+.1f}%)"
                    break
            
            # Add appropriate emoji based on content
            if 'crypto' in driver.lower():
                emoji = '🪙'
            elif 'europe' in driver.lower():
                emoji = '🇪🇺'
            elif 'us' in driver.lower() or 'futures' in driver.lower():
                emoji = '🇺🇸'
            elif 'fx' in driver.lower() or 'dollar' in driver.lower():
                emoji = '💱'
            elif 'bond' in driver.lower() or 'yield' in driver.lower():
                emoji = '💵'
            else:
                emoji = '📊'
            
            formatted_drivers.append(f"{emoji} {driver}{performance_text}")
        
        return '\n'.join(formatted_drivers)

    def _generate_market_insight(self, section_analyses) -> str:
        """Generate a concise market insight from section performance."""
        
        if not section_analyses:
            return "Markets showing mixed directional signals"
        
        # Find most significant movers
        strongest_section = max(section_analyses, key=lambda x: abs(x.avg_performance))
        
        if strongest_section.avg_performance > 1.0:
            return f"{strongest_section.section_name.replace('_', ' ').title()} surging +{strongest_section.avg_performance:.1f}%"
        elif strongest_section.avg_performance < -1.0:
            return f"{strongest_section.section_name.replace('_', ' ').title()} dropping {strongest_section.avg_performance:.1f}%"
        else:
            # Look for divergence pattern
            bullish_count = sum(1 for s in section_analyses if s.section_sentiment == "BULLISH")
            bearish_count = sum(1 for s in section_analyses if s.section_sentiment == "BEARISH")
            
            if bullish_count > bearish_count:
                return "Risk-on momentum building across sectors"
            elif bearish_count > bullish_count:
                return "Defensive positioning emerging"
            else:
                return "Cross-asset divergence creating opportunities"

    def _generate_dynamic_hashtags(self, sentiment, config: Dict) -> str:
        """Generate hashtags based on sentiment and briefing type."""
        
        base_tags = "#MarketAnalysis #Investing"
        
        sentiment_tags = {
            'BULLISH': "#BullMarket #RiskOn",
            'BEARISH': "#BearMarket #RiskOff", 
            'MIXED': "#MarketRotation #Divergence",
            'NEUTRAL': "#Consolidation #RangeTrading"
        }
        
        period_tags = {
            'Morning Briefing': "#PreMarket #MarketOpen",
            'EU Close Briefing': "#EuropeanClose #GlobalMarkets",
            'US Close Briefing': "#MarketClose #AfterHours"
        }
        
        sentiment_tag = sentiment_tags.get(sentiment.value, "")
        period_tag = period_tags.get(config.get('briefing_title', ''), "#Finance")
        
        return f"{base_tags} {sentiment_tag} {period_tag}"

    def _assemble_enhanced_tweet(self, custom_hook: str, sentiment_visual: str, briefing_title: str, 
                            key_drivers: str, market_insight: str, briefing_url: str, hashtags: str) -> str:
        """Assemble the final enhanced tweet structure."""
        
        tweet_structure = (
            f"{custom_hook}\n\n"
            f"{sentiment_visual}\n"
            f"📊 {briefing_title}\n\n"
            f"{key_drivers}\n\n"
            f"💡 {market_insight}\n\n"
            f"🔗 Full analysis: {briefing_url}\n\n"
            f"{hashtags}"
        )
        
        return tweet_structure

    def _assess_volatility_level(self, section_analyses) -> str:
        """Assess overall market volatility level."""
        if not section_analyses:
            return "low"
        
        avg_abs_performance = sum(abs(s.avg_performance) for s in section_analyses) / len(section_analyses)
        
        if avg_abs_performance > 2.0:
            return "high"
        elif avg_abs_performance > 1.0:
            return "elevated"
        else:
            return "moderate"

    def _fallback_tweet(self, briefing_url: str) -> str:
        """Fallback tweet when analysis data is unavailable."""
        return (
            f"🚨 Latest market briefing is live!\n\n"
            f"📊 Complete analysis of today's price action\n\n"
            f"🔗 Read more: {briefing_url}\n\n"
            f"#MarketAnalysis #Investing #Finance"
        )

    async def _log_content_and_results(
        self, 
        content: GeneratedContent, 
        twitter_result, 
        notion_page_id: Optional[str]
    ):
        """Log content and publishing results to database"""
        try:
            # This dictionary now serves as the rich 'details' blob for our log.
            # It's good practice to make datetime objects JSON serializable.
            content_record = {
                'content_type': content.content_type.value,
                'category': content.category.value if content.category else None,
                'theme': content.theme,
                'text': content.text,
                'headline_id': content.headline_used.id if content.headline_used else None,
                'market_data': [data.__dict__ for data in content.market_data] if content.market_data else [],
                'success': twitter_result.success,
                'tweet_id': twitter_result.tweet_id,
                'tweet_url': twitter_result.url,
                'notion_page_id': notion_page_id,
                'created_at': datetime.now(timezone.utc).isoformat()
            }
            
            # FIX: Call the database service method with the correct arguments as defined
            # in database_service.py. The entire 'content_record' is passed as 'details'.
            await asyncio.to_thread(
                self.database_service.log_content_generation,
                content_type=content.content_type.value,
                theme=content.theme,
                headline_id=content.headline_used.id if content.headline_used else None,
                success=twitter_result.success,
                url=twitter_result.url,
                details=content_record
            )
            
            if twitter_result.success:
                await asyncio.to_thread(
                    self.database_service.track_theme, 
                    content.theme
                )
                
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

    async def _generate_briefing_promo_tweet_with_chart(self, payload: BriefingPayload, briefing_url: str) -> Tuple[str, Optional[str]]:
        """
        Enhanced promotional tweet with optional chart generation.
        Returns (tweet_text, chart_path).
        """
        # Generate the text part (your existing method)
        tweet_text = await self._generate_briefing_promo_tweet(payload, briefing_url)
        
        # Optionally generate chart
        chart_path = await self._generate_chart_for_tweet(payload)
        
        # If we have a chart, modify tweet text to reference it
        if chart_path:
            tweet_text += "\n\n📊 Chart attached 👇"
        
        return tweet_text, chart_path

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