# Add the new imports
from core.models import BriefingPayload
from services.news_client import NewsClient
from services.market_sentiment_service import ComprehensiveMarketSentimentService
from services.briefing_config_service import ConfigService
import asyncio

class BriefingGenerator:
    # Update the constructor to accept the NewsClient
    def __init__(self, config_service: ConfigService, sentiment_service: ComprehensiveMarketSentimentService, news_client: NewsClient):
        self.config_service = config_service
        self.sentiment_service = sentiment_service
        self.news_client = news_client

    async def create(self, briefing_key: str) -> BriefingPayload:
        """
        Assembles all data required for a briefing from various services.
        """
        print(f"BriefingGenerator: Assembling all data for '{briefing_key}'...")
        
        # 1. Get the briefing configuration
        config = await self.config_service.build_briefing_config(briefing_key)
        
        # 2. Fetch market analysis, news, and calendars in parallel
        analysis_task = self.sentiment_service.analyze_briefing_sentiment(config)
        news_task = self.news_client.get_market_news(limit=5)
        calendar_task = self.news_client.get_calendar_data(days_ahead=3) # Fetches both IPO and earnings

        market_analysis, market_news, calendar_data = await asyncio.gather(
            analysis_task, news_task, calendar_task
        )
        
        # 3. Create and return the final payload object
        payload = BriefingPayload(
            market_analysis=market_analysis,
            market_news=market_news,
            earnings_calendar=calendar_data.get('earnings_events', []),
            config=config
        )
        
        print(f"BriefingGenerator: Data payload assembled successfully.")
        return payload