# generators/briefing_generator.py
from services.briefing_config_service import ConfigService
from services.market_sentiment_service import ComprehensiveMarketSentimentService

class BriefingGenerator:
    """
    Knows how to generate a complete briefing by orchestrating the
    config and sentiment analysis services.
    """
    def __init__(self, config_service: ConfigService, sentiment_service: ComprehensiveMarketSentimentService):
        self.config_service = config_service
        self.sentiment_service = sentiment_service

    async def create(self, briefing_key: str):
        """
        Generates the analysis for a specific briefing.

        Returns:
            A tuple of (analysis_result, briefing_config)
        """
        print(f"BriefingGenerator: Creating content for '{briefing_key}'...")
        
        # 1. Build the configuration from the database
        config = await self.config_service.build_briefing_config(briefing_key)
        
        # 2. Run the analysis using that configuration
        analysis = await self.sentiment_service.analyze_briefing_sentiment(config)
        
        print(f"BriefingGenerator: Content created for '{briefing_key}'.")
        return analysis, config