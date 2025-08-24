# hedgefund_agent/generators/deep_dive_generator.py
import logging
import re
from typing import Optional, List

# Import services and models from the new architecture
from core.models import (
    ContentType, ContentCategory, ContentRequest,
    GeneratedContent, Headline, MarketData
)
from services.database_service import DatabaseService
from services.gpt_service import GPTService
from services.market_client import MarketClient

logger = logging.getLogger(__name__)

class DeepDiveGenerator:
    """Generates multi-part deep dive threads with a hedge fund perspective."""

    def __init__(self, data_service: DatabaseService, gpt_service: GPTService, market_client: MarketClient, config: dict):
        """Initializes the generator with dependency injection, same as CommentaryGenerator."""
        self.data_service = data_service
        self.gpt_service = gpt_service
        self.market_client = market_client
        self.config = config

    async def generate(self, request: Optional[ContentRequest] = None) -> GeneratedContent:
        """
        Orchestrates the generation of a deep dive thread.

        This method follows the same flow as CommentaryGenerator:
        1.  Get a headline.
        2.  Determine category and theme.
        3.  Generate content using GPT.
        4.  Enrich with market data.
        5.  Return a structured GeneratedContent object.
        """
        try:
            logger.info("📊 Generating hedge fund deep dive thread")

            # 1. Get a suitable headline for the deep dive
            headline = self.data_service.get_unused_headline_today(min_score=8) # Prefer high-score for deep dives
            if not headline:
                raise Exception("No suitable high-scoring headline available for a deep dive.")

            # 2. Determine category and theme for deduplication
            category = self._determine_category(request, headline)
            theme = self.data_service.extract_and_validate_theme(headline.headline)

            # 3. Build the prompt and generate the thread using GPTService
            prompt = self._build_deep_dive_prompt(headline, category)
            # Use the existing gpt_service method to generate a thread
            thread_parts = self.gpt_service.generate_thread(prompt, max_parts=3)

            if not thread_parts:
                raise Exception("GPT thread generation failed or returned no parts.")

            # 4. Enrich all thread parts with market data
            enriched_parts, market_data = await self._enrich_with_market_data(thread_parts)

            # 5. Combine parts into a single string for the GeneratedContent object
            # The PublishingService will handle splitting it for posting.
            final_text = "\n\n---\n\n".join(enriched_parts)

            # 6. Mark headline as used and track the theme
            self.data_service.mark_headline_used(headline.id, "deep_dive")
            self.data_service.track_theme(theme)

            logger.info(f"✅ Generated deep dive thread on '{theme}' ({len(enriched_parts)} parts)")

            return GeneratedContent(
                text=final_text,
                content_type=ContentType.DEEP_DIVE,
                category=category,
                theme=theme,
                market_data=market_data,
                headline_used=headline,
                # Store the individual parts for potential use by the publisher
                parts=enriched_parts
            )

        except Exception as e:
            logger.error(f"❌ Deep dive generation failed: {e}")
            raise

    def _build_deep_dive_prompt(self, headline: Headline, category: ContentCategory) -> str:
        """Builds a GPT prompt for a deep dive thread, adapted from old script."""
        context = f"Headline: {headline.headline.strip()}\n"
        if headline.summary:
            context += f"Summary: {headline.summary.strip()}"

        # Instruction to include cashtags for later enrichment
        base_instruction = (
            "Whenever you mention a stock ticker (e.g., $AAPL), include the cashtag; "
            "my system will insert the latest price and percent change."
        )

        return (
            f"You are a hedge fund analyst. Write a 3-part Twitter thread analyzing this news.\n\n"
            f"**News Context:**\n{context}\n\n"
            f"**Instructions:**\n{base_instruction}\n"
            "Structure the thread as follows:\n"
            "1. **The News:** Briefly explain what happened and why it's significant.\n"
            "2. **Market Impact:** Analyze what the market cares about. Focus on second-order effects.\n"
            "3. **Your Take:** Provide a sharp, analytical conclusion (e.g., macro implications, sector rotation, stock-specific view).\n\n"
            "Be institutional, analytical, and avoid hype."
        )

    async def _enrich_with_market_data(self, parts: List[str]) -> tuple[List[str], List[MarketData]]:
        """
        Finds all unique cashtags across all thread parts, fetches their market data,
        and replaces the cashtags with enriched data in each part.
        """
        # Extract all unique cashtags from the entire thread to make one bulk API call
        all_cashtags = set()
        for part in parts:
            cashtags_in_part = re.findall(r'\$[A-Z]{1,5}\b', part)
            for tag in cashtags_in_part:
                all_cashtags.add(tag)

        valid_tickers = [tag.strip("$") for tag in all_cashtags if len(tag) > 1 and tag[1:].isalpha()]
        if not valid_tickers:
            return parts, []

        logger.info(f"💰 Enriching deep dive with market data for: {valid_tickers}")
        market_data_map = await self.market_client.get_bulk_prices(valid_tickers)
        market_data_objects = [md for md in market_data_map.values()]

        # Replace cashtags in each part with the fetched data
        enriched_parts = []
        for part in parts:
            enriched_part = part
            for ticker, data in market_data_map.items():
                cashtag = f"${ticker}"
                if data:
                    enriched_format = f"{cashtag} (${data.price:.2f}, {data.change_percent:+.2f}%)"
                    # Use regex for safe replacement (whole word only)
                    pattern = rf"\${ticker}(?=\s|$|[^\w])"
                    enriched_part = re.sub(pattern, enriched_format, enriched_part, flags=re.IGNORECASE)
            enriched_parts.append(enriched_part)

        return enriched_parts, market_data_objects

    def _determine_category(self, request: Optional[ContentRequest], headline: Headline) -> ContentCategory:
        """
        Determines the content category.
        This logic is reused from CommentaryGenerator for consistency.
        """
        if request and request.category:
            return request.category
        if headline.category:
            try:
                return ContentCategory(headline.category.lower())
            except ValueError:
                pass
        # Fallback to keyword classification
        headline_lower = headline.headline.lower()
        if any(kw in headline_lower for kw in ["earnings", "stock", "revenue", "ipo"]):
            return ContentCategory.EQUITY
        if any(kw in headline_lower for kw in ["trump", "biden", "election", "congress", "policy"]):
            return ContentCategory.POLITICAL
        return ContentCategory.MACRO # Default