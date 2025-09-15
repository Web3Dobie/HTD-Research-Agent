# hedgefund_agent/generators/deep_dive_generator.py
import logging
import re
from typing import Optional, List
from datetime import datetime
from services.enrichment_service import MarketDataEnrichmentService

# Import services and models from the new architecture
from core.models import (
    ContentType, ContentCategory, ContentRequest,
    GeneratedContent, Headline, MarketData
)

logger = logging.getLogger(__name__)

class DeepDiveGenerator:
    """Generates multi-part deep dive threads with a hedge fund perspective."""

    def __init__(self, data_service, gpt_service, market_client, config):
        """Initializes the generator with dependency injection, same as CommentaryGenerator."""
        self.data_service = data_service
        self.gpt_service = gpt_service
        self.market_client = market_client
        self.config = config
        self.enrichment_service = MarketDataEnrichmentService(self.market_client)
        
        # Category rotation tracking (same as CommentaryGenerator)
        self.last_used_category = None
        
        # Category classification keywords (reuse from CommentaryGenerator)
        self.category_keywords = {
            ContentCategory.MACRO: [
                "fed", "federal reserve", "inflation", "gdp", "unemployment", 
                "recession", "interest rate", "monetary policy", "powell"
            ],
            ContentCategory.POLITICAL: [
                "trump", "biden", "election", "congress", "tariff", 
                "trade", "sanctions", "policy", "government"
            ],
            ContentCategory.EQUITY: [
                "earnings", "stock", "revenue", "guidance", "merger",
                "acquisition", "ipo", "dividend", "buyback"
            ]
        }

    async def generate(self, request: Optional[ContentRequest] = None) -> GeneratedContent:
        """
        Orchestrates the generation of a deep dive thread.

        This method follows the same flow as CommentaryGenerator:
        1. Get a high-scoring headline.
        2. Determine category and theme.
        3. Generate thread content using GPT.
        4. Enrich with market data.
        5. Return a structured GeneratedContent object.
        """
        try:
            logger.info("📊 Generating hedge fund deep dive thread")

            # 1. Get a high-scoring headline for the deep dive (FIXED: use new method)
            headline = self._get_headline_for_content(request)
            if not headline:
                raise Exception("No suitable high-scoring headline available for deep dive")

            # 2. Determine category and theme for deduplication
            category = self._determine_category(request, headline)
            theme = self._extract_and_validate_theme(headline.headline)

            # 3. Build the prompt and generate the thread using GPTService
            prompt = self._build_deep_dive_prompt(headline, category)
            # Use the existing gpt_service method to generate a thread
            thread_parts = self.gpt_service.generate_thread(prompt, max_parts=3)

            if not thread_parts:
                raise Exception("GPT thread generation failed or returned no parts")

            # 4. Enrich all thread parts with market data
            enriched_parts, market_data = await self.enrichment_service.enrich_content(thread_parts)

            # 5. Add mentions and disclaimer to the last part only
            enriched_parts = self._finalize_thread_parts(enriched_parts)

            # 6. Combine parts into a single string for the main text field
            final_text = "\n\n---\n\n".join(enriched_parts)

            # 7. Mark headline as used and track the theme
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
                parts=enriched_parts  # Store individual parts for thread posting
            )

        except Exception as e:
            logger.error(f"❌ Deep dive generation failed: {e}")
            raise

    def _get_headline_for_content(self, request: Optional[ContentRequest]) -> Optional[Headline]:
        """Get top scoring unused headline for deep dive (FIXED: use new method)"""
        if request and request.specific_headline:
            return request.specific_headline
            
        # Get top scoring unused headline with min_score=9 for deep dives
        return self.data_service.get_top_unused_headline_today(min_score=9)

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
            "1. The News 📰: Briefly explain what happened and why it's significant.\n"
            "2. Market Impact ➡️: Analyze what the market cares about. Focus on second-order effects.\n"
            "3. Our Take 🧐: Provide a sharp, analytical conclusion (e.g., macro implications, sector rotation, stock-specific view).\n\n"
            "Be institutional, analytical, and avoid hype."
        )

    def _finalize_thread_parts(self, parts: List[str]) -> List[str]:
        """Add mentions and disclaimer to thread parts (only disclaimer on last part)"""
        finalized_parts = []
        
        for i, part in enumerate(parts):
            # Add mentions to each part (placeholder - implement as needed)
            finalized_part = part
            
            # Add disclaimer only to the last part
            if i == len(parts) - 1:
                # Clean any existing disclaimers first
                finalized_part = re.sub(
                    r"This is my opinion\.? ?Not financial advice\.?",
                    "",
                    finalized_part,
                    flags=re.IGNORECASE
                ).strip()
                
                # Add disclaimer
                disclaimer = self.config.get('default_disclaimer', "This is my opinion. Not financial advice.")
                finalized_part += f"\n\n{disclaimer}"
            
            finalized_parts.append(finalized_part)
        
        return finalized_parts

    def _determine_category(self, request: Optional[ContentRequest], headline: Headline) -> ContentCategory:
        """
        Determines the content category (same logic as CommentaryGenerator).
        """
        # If request specifies category, use it
        if request and request.category:
            return request.category
            
        # If headline already has category, convert to enum
        if headline.category:
            try:
                return ContentCategory(headline.category.lower())
            except ValueError:
                pass
        
        # Classify based on content using keywords
        classified = self._classify_headline_content(headline.headline)
        
        # Apply category rotation (avoid same category twice in a row)
        if classified == self.last_used_category:
            # Rotate to next category
            categories = list(ContentCategory)
            current_index = categories.index(classified)
            next_index = (current_index + 1) % len(categories)
            selected_category = categories[next_index]
            logger.info(f"🔄 Category rotated from {classified.value} to {selected_category.value}")
        else:
            selected_category = classified
        
        self.last_used_category = selected_category
        return selected_category

    def _classify_headline_content(self, headline: str) -> ContentCategory:
        """Classify headline using keyword matching (same as CommentaryGenerator)"""
        headline_lower = headline.lower()
        
        # Count keyword matches for each category
        category_scores = {}
        for category, keywords in self.category_keywords.items():
            score = sum(1 for keyword in keywords if keyword in headline_lower)
            if score > 0:
                category_scores[category] = score
        
        # Return category with highest score, or default to MACRO
        if category_scores:
            return max(category_scores, key=category_scores.get)
        else:
            return ContentCategory.MACRO

    def _extract_and_validate_theme(self, headline: str) -> str:
        """Extract and validate theme for deduplication (same as CommentaryGenerator)"""
        # Extract meaningful words (remove stopwords)
        stopwords = {"the", "in", "of", "and", "to", "a", "after", "on", "for", "with", "is", "are", "will", "has", "have"}
        words = headline.split()
        
        theme_candidates = []
        for word in words:
            word_clean = re.sub(r'[^\w]', '', word.lower())
            if len(word_clean) > 3 and word_clean not in stopwords:
                theme_candidates.append(word_clean)
        
        # Create theme from first significant word or fallback
        if theme_candidates:
            base_theme = theme_candidates[0]
        else:
            base_theme = "market_update"
        
        # Check for duplicates and modify if needed
        theme = base_theme
        is_duplicate = self.data_service.is_duplicate_theme(theme)
        
        if is_duplicate:
            # Add timestamp to make unique
            theme = f"{base_theme}_{datetime.now().strftime('%H%M')}"
            logger.info(f"🔄 Theme modified to avoid duplicate: {theme}")
        
        return theme