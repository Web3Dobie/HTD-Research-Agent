# hedgefund_agent/generators/deep_dive_generator.py
import logging
import re
from typing import Optional, List
from datetime import datetime
from services.enrichment_service import MarketDataEnrichmentService
from services.semantic_theme_service import SemanticThemeService
from services.content_similarity_service import ContentSimilarityService
from services.article_writer import ArticleWriter

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
        self.semantic_theme_service = SemanticThemeService(self.data_service)
        self.content_similarity_service = ContentSimilarityService(
            self.data_service,
            self.semantic_theme_service
        )

        try:
            self.article_writer = ArticleWriter("/app/articles")
            logger.info("✅ ArticleWriter initialized successfully")
        except Exception as e:
            logger.error(f"❌ Failed to initialize ArticleWriter: {e}")
            self.article_writer = None
        
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
        """Generate deep dive thread with semantic similarity checking and article generation"""
        MAX_HEADLINE_ATTEMPTS = 10
        
        try:
            logger.info("📊 Generating hedge fund deep dive thread with semantic intelligence")

            # === RETRY LOOP FOR HEADLINES ===
            for attempt in range(1, MAX_HEADLINE_ATTEMPTS + 1):
                logger.info(f"🔄 Headline attempt {attempt}/{MAX_HEADLINE_ATTEMPTS}")
                
                # 1. Get a high-scoring headline for the deep dive
                headline = self._get_headline_for_content(request)
                if not headline:
                    if attempt == MAX_HEADLINE_ATTEMPTS:
                        raise Exception("No suitable high-scoring headline available for deep dive")
                    logger.warning(f"⚠️ No headline found, trying again...")
                    continue

                logger.info(f"📰 Trying headline: {headline.headline[:60]}...")

                # 2. Determine category
                category = self._determine_category(request, headline)

                # === SEMANTIC SIMILARITY CHECK ===
                # 3. Extract semantic theme from headline
                semantic_theme = self.semantic_theme_service.extract_theme(headline.headline)
                logger.info(f"🧠 Semantic theme extracted: {semantic_theme[:50]}...")
                
                # 4. Check if content is too similar to today's deep dives
                is_too_similar, similar_content = self.content_similarity_service.is_content_too_similar_today(
                    text=headline.headline,
                    similarity_threshold=0.50,
                    content_type="deep_dive"
                )
                
                if is_too_similar:
                    logger.warning(f"🚫 Headline too similar to today's deep dives (>{similar_content['similarity']:.0%})")
                    logger.warning(f"   Similar to: {similar_content['content'][:80]}...")
                    logger.info(f"⏭️ Skipping to next headline (attempt {attempt}/{MAX_HEADLINE_ATTEMPTS})")
                    
                    # Mark headline as checked
                    self.data_service.mark_headline_used(headline.id, "deep_dive_rejected")
                    continue
                
                logger.info("✅ Semantic similarity check passed - deep dive topic is unique")
                # === END SEMANTIC CHECK ===

                # 5. Build the prompt and generate the thread using GPTService
                prompt = self._build_deep_dive_prompt(headline, category)
                thread_parts = self.gpt_service.generate_thread(prompt, max_parts=3)

                if not thread_parts:
                    raise Exception("GPT thread generation failed or returned no parts")

                # 6. Enrich all thread parts with market data
                enriched_parts, market_data = await self.enrichment_service.enrich_content(thread_parts)

                # 7. Add mentions and disclaimer to the last part only
                enriched_parts = self._finalize_thread_parts(enriched_parts)

                # 8. Combine parts into a single string for the main text field
                final_text = "\n\n---\n\n".join(enriched_parts)

                # === SEMANTIC THEME TRACKING ===
                # 9. Mark headline as used
                self.data_service.mark_headline_used(headline.id, "deep_dive")
                
                # 10. Track semantic theme with full thread content
                self.semantic_theme_service.track_theme(
                    theme_text=semantic_theme,
                    full_content=final_text,
                    content_type="deep_dive",
                    category=category.value
                )
                logger.info(f"✅ Semantic theme tracked for deep dive thread")
                # === END SEMANTIC TRACKING ===

                # 11. Create the GeneratedContent object (EXISTING)
                generated_content = GeneratedContent(
                    text=final_text,
                    content_type=ContentType.DEEP_DIVE,
                    category=category,
                    theme=semantic_theme,
                    market_data=market_data,
                    headline_used=headline,
                    parts=enriched_parts
                )
                
                # === NEW: ARTICLE GENERATION ===
                # 12. Generate article if ArticleWriter is available
                article_id = None
                article_url = None
                
                if self.article_writer:
                    try:
                        logger.info("📝 Generating deep dive article...")
                        
                        # Prepare research data for article expansion
                        research_data = {
                            'prompt_used': prompt,
                            'category': category.value,
                            'generation_time': datetime.now(),
                            'market_data_count': len(market_data),
                            'attempt_number': attempt,
                            'semantic_theme': semantic_theme
                        }
                        
                        # Generate the article
                        article_id = await self.article_writer.write_deep_dive_article(
                            generated_content, 
                            research_data
                        )
                        
                        # Generate public URL
                        article_url = self.article_writer.get_article_url(article_id)
                        
                        logger.info(f"✅ Article generated: {article_id}")
                        logger.info(f"🔗 Article URL: {article_url}")
                        
                        # NEW: Add article link to the final thread part
                        if enriched_parts and article_url:
                            logger.info("🔗 Adding article link to final thread part...")
                            
                            # Use helper method to properly handle disclaimer placement
                            enriched_parts = self._add_article_link_to_final_part(enriched_parts, article_url)
                            
                            # Regenerate final_text with the updated parts
                            final_text = "\n\n---\n\n".join(enriched_parts)
                            
                            # Update the generated_content object
                            generated_content.text = final_text
                            generated_content.parts = enriched_parts
                            
                            logger.info("✅ Article link added to final thread part")
                        
                    except Exception as e:
                        logger.error(f"❌ Article generation failed: {e}")
                        # Don't fail the whole process if article generation fails
                else:
                    logger.warning("⚠️ ArticleWriter not available - skipping article generation")

                # 13. Add article information to the generated content
                generated_content.article_id = article_id
                generated_content.article_url = article_url
                # === END ARTICLE GENERATION ===

                logger.info(f"✅ Generated deep dive thread on '{semantic_theme[:30]}...' ({len(enriched_parts)} parts)")
                if article_id:
                    logger.info(f"📄 With companion article: {article_id}")
                
                return generated_content
            
            # If we exhausted all attempts
            raise Exception(f"Could not find unique deep dive topic after {MAX_HEADLINE_ATTEMPTS} attempts")

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

    def _add_article_link_to_final_part(self, parts: List[str], article_url: str) -> List[str]:
        """Add article link to the final thread part, handling disclaimer placement"""
        if not parts or not article_url:
            return parts
        
        modified_parts = parts.copy()
        final_part = modified_parts[-1]
        
        # Check if disclaimer exists
        disclaimer_pattern = r"(This is my opinion\.? ?Not financial advice\.?)$"
        disclaimer_match = re.search(disclaimer_pattern, final_part, re.IGNORECASE)
        
        if disclaimer_match:
            # Insert article link before disclaimer
            disclaimer_text = disclaimer_match.group(1)
            part_without_disclaimer = final_part[:disclaimer_match.start()].strip()
            modified_parts[-1] = f"{part_without_disclaimer}\n\n📄 Read the full analysis: {article_url}\n\n{disclaimer_text}"
        else:
            # No disclaimer, just append article link
            modified_parts[-1] = f"{final_part}\n\n📄 Read the full analysis: {article_url}"
        
        return modified_parts

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