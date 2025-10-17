# generators/commentary_generator.py (ENHANCED VERSION)

import logging
import re
import random
import os
from typing import Optional, List, Dict, Tuple
from datetime import datetime, timedelta
from collections import defaultdict
from services.enrichment_service import MarketDataEnrichmentService
from services.semantic_theme_service import SemanticThemeService
from services.content_similarity_service import ContentSimilarityService

# Notion client for memory
#try:
    #from notion_client import Client
    #NOTION_AVAILABLE = True
#except ImportError:
    #NOTION_AVAILABLE = False
    #logging.warning("notion-client not available, memory features disabled")

# Import our services and models
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.models import (
    ContentType, ContentCategory, ContentRequest, 
    GeneratedContent, Headline, MarketData
)

logger = logging.getLogger(__name__)

class CommentaryGenerator:
    """Generates single-tweet hedge fund commentary with intelligent diversity"""
    
    def __init__(self, data_service, gpt_service, market_client, config):
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
        
        # Initialize Notion memory if available
        #self.notion_client = None
        #self.hedgefund_tweet_db_id = None
        
        #if NOTION_AVAILABLE:
            #try:
                #notion_api_key = os.getenv("NOTION_API_KEY")
                #.hedgefund_tweet_db_id = os.getenv("HEDGEFUND_TWEET_DB_ID")
                
                #if notion_api_key and self.hedgefund_tweet_db_id:
                    #self.notion_client = Client(auth=notion_api_key)
                    #logger.info("✅ Notion memory enabled for content diversity")
                #else:
                    #logger.warning("⚠️ Notion credentials missing, memory disabled")
            #except Exception as e:
                #logger.warning(f"⚠️ Notion memory initialization failed: {e}")
        
        # Category rotation tracking (fallback only)
        self.last_used_category = None
        
        # Enhanced category classification keywords
        self.category_keywords = {
            ContentCategory.MACRO: [
                "fed", "federal reserve", "inflation", "gdp", "unemployment", 
                "recession", "interest rate", "monetary policy", "powell",
                "economy", "economic", "central bank", "fiscal", "treasury",
                "bonds", "yield", "rates", "monetary", "deflation"
            ],
            ContentCategory.POLITICAL: [
                "trump", "biden", "election", "congress", "tariff", "tariffs",
                "trade", "sanctions", "policy", "government", "senate",
                "diplomacy", "geopolitical", "war", "china", "europe",
                "regulation", "regulatory", "political", "administration"
            ],
            ContentCategory.EQUITY: [
                "earnings", "stock", "revenue", "guidance", "merger",
                "acquisition", "ipo", "dividend", "buyback", "shares",
                "profit", "loss", "eps", "valuation", "ceo", "cfo",
                "quarterly", "annual", "results", "beat", "miss"
            ]
        }
        
        # Diversity settings
        self.max_category_concentration = 0.6
        self.keyword_repeat_limit = 2
    
    async def generate(self, request: Optional[ContentRequest] = None) -> GeneratedContent:
        """Generate commentary content with semantic similarity checking"""
        MAX_HEADLINE_ATTEMPTS = 10  # Try up to 10 headlines
        
        try:
            logger.info("🐂 Generating hedge fund commentary with semantic intelligence")
            
            # 1. Analyze recent content diversity (existing Notion-based logic)
            diversity_analysis = self._analyze_recent_content()
            logger.info(f"📊 Recent categories: {diversity_analysis['categories']}")
            
            # === NEW: RETRY LOOP FOR HEADLINES ===
            for attempt in range(1, MAX_HEADLINE_ATTEMPTS + 1):
                logger.info(f"🔄 Headline attempt {attempt}/{MAX_HEADLINE_ATTEMPTS}")
                
                # 2. Get headline for content generation
                headline = self._get_headline_for_content(request)
                if not headline:
                    if attempt == MAX_HEADLINE_ATTEMPTS:
                        raise Exception("No suitable headlines available")
                    logger.warning(f"⚠️ No headline found, trying again...")
                    continue
                
                logger.info(f"📰 Trying headline: {headline.headline[:60]}...")
                
                # 3. Determine category with diversity logic
                category = self._determine_category_smart(request, headline, diversity_analysis)
                logger.info(f"📂 Selected category: {category.value}")
                
                # === SEMANTIC SIMILARITY CHECK ===
                # 4. Extract semantic theme from headline
                semantic_theme = self.semantic_theme_service.extract_theme(headline.headline)
                logger.info(f"🧠 Semantic theme extracted: {semantic_theme[:50]}...")
                
                # 5. Check if content is too similar to today's posts
                is_too_similar, similar_content = self.content_similarity_service.is_content_too_similar_today(
                    text=headline.headline,
                    similarity_threshold=0.50,
                    content_type="commentary"
                )
                
                if is_too_similar:
                    logger.warning(f"🚫 Headline too similar to today's content (>{similar_content['similarity']:.0%})")
                    logger.warning(f"   Similar to: {similar_content['content'][:80]}...")
                    logger.info(f"⏭️ Skipping to next headline (attempt {attempt}/{MAX_HEADLINE_ATTEMPTS})")
                    
                    # Mark headline as used so we don't try it again
                    self.data_service.mark_headline_used(headline.id, "commentary_rejected")
                    continue  # Try next headline
                
                logger.info("✅ Semantic similarity check passed - content is unique")
                # === END SEMANTIC CHECK ===
                
                # 6. Generate base commentary using GPT
                prompt = self._build_commentary_prompt(headline, category)
                base_text = self.gpt_service.generate_tweet(prompt)
                
                if not base_text:
                    raise Exception("GPT generation failed")
                
                # 7. Check for keyword diversity and regenerate if needed
                base_text = self._ensure_keyword_diversity(base_text, prompt, diversity_analysis)
                
                # 8. Enrich with market data if requested
                enriched_text = base_text
                market_data = []
                
                if request and request.include_market_data:
                    enriched_text, market_data = await self.enrichment_service.enrich_content(base_text)
                
                # 9. Add mentions and disclaimer
                final_text = self._finalize_text(enriched_text)
                
                # === SEMANTIC THEME TRACKING ===
                # 10. Mark headline as used
                self.data_service.mark_headline_used(headline.id, "commentary")
                
                # 11. Track semantic theme
                self.semantic_theme_service.track_theme(
                    theme_text=semantic_theme,
                    full_content=final_text,
                    content_type="commentary",
                    category=category.value
                )
                logger.info(f"✅ Semantic theme tracked with embedding")
                # === END SEMANTIC TRACKING ===
                
                logger.info(f"✅ Generated diverse commentary: {final_text[:50]}...")
                
                return GeneratedContent(
                    text=final_text,
                    content_type=ContentType.COMMENTARY,
                    category=category,
                    theme=semantic_theme,
                    market_data=market_data,
                    headline_used=headline
                )
            
            # If we exhausted all attempts
            raise Exception(f"Could not find unique headline after {MAX_HEADLINE_ATTEMPTS} attempts. All headlines were too similar to today's content.")
            
        except Exception as e:
            logger.error(f"❌ Commentary generation failed: {e}")
            raise
    
    def _analyze_recent_content(self) -> dict:
        """Analyze today's content for diversity tracking."""
        try:
            with self.data_service.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT category, theme_text
                        FROM hedgefund_agent.semantic_themes
                        WHERE DATE(created_at) = CURRENT_DATE
                        AND content_type = 'commentary'
                        ORDER BY created_at
                    """)
                    
                    results = cur.fetchall()
                    
                    # Count categories
                    categories = {}
                    keywords = {}
                    
                    for row in results:
                        category = row[0]
                        theme_text = row[1]
                        
                        if category:
                            categories[category] = categories.get(category, 0) + 1
                        
                        words = theme_text.lower().split()
                        for word in words:
                            if len(word) > 4:
                                keywords[word] = keywords.get(word, 0) + 1
                    
                    # Calculate total posts
                    total_posts = len(results)
                    
                    # Find least used categories for recommendations
                    all_categories = ['macro', 'equity', 'political']
                    category_usage = {cat: categories.get(cat, 0) for cat in all_categories}
                    recommended_categories = sorted(category_usage.keys(), key=lambda x: category_usage[x])
                    
                    logger.info(f"📊 Today's content: {total_posts} posts, {len(categories)} categories")
                    
                    return {
                        "categories": categories,
                        "keywords": keywords,
                        "total_posts": total_posts,  # ✅ Added
                        "recommended_categories": recommended_categories  # ✅ Added
                    }
                    
        except Exception as e:
            logger.error(f"❌ Error analyzing today's content: {e}")
            return {
                "categories": {}, 
                "keywords": {},
                "total_posts": 0,  # ✅ Added
                "recommended_categories": []  # ✅ Added
            }
    
    def _determine_category_smart(self, request: Optional[ContentRequest], 
                                 headline: Headline, diversity_analysis: Dict) -> ContentCategory:
        """Smart category selection with diversity awareness.
        Now uses database diversity analysis instead of Notion."""
        
        # If request specifies category, use it unless severely overused
        if request and request.category:
            category_name = request.category.value.lower()
            category_usage = diversity_analysis['categories'].get(category_name, 0)
            total_posts = diversity_analysis['total_posts']
            
            if total_posts == 0 or category_usage / total_posts <= 0.8:  # Allow unless >80% usage
                return request.category
            else:
                logger.info(f"🔄 Overriding requested category '{category_name}' due to overuse")
        
        # Classify headline content
        classified = self._classify_headline_content(headline.headline)
        
        # Check if classified category should be avoided
        classified_name = classified.value.lower()
        category_usage = diversity_analysis['categories'].get(classified_name, 0)
        total_posts = diversity_analysis['total_posts']
        
        if total_posts > 1:  # Only apply diversity logic if we have some history
            concentration = category_usage / total_posts
            
            if concentration > self.max_category_concentration:
                logger.info(f"🚫 Avoiding overused category '{classified_name}' (concentration: {concentration:.2f})")
                
                # Try to find alternative category that also matches the headline
                alternative_categories = [cat for cat in ContentCategory if cat != classified]
                
                for alt_category in alternative_categories:
                    if self._headline_matches_category(headline.headline, alt_category):
                        alt_name = alt_category.value.lower()
                        alt_usage = diversity_analysis['categories'].get(alt_name, 0)
                        alt_concentration = alt_usage / total_posts
                        
                        if alt_concentration < concentration:
                            logger.info(f"✅ Using alternative category '{alt_name}' instead")
                            return alt_category
                
                # If no good alternative, use least used category overall
                recommended = diversity_analysis['recommended_categories']
                if recommended:
                    fallback_name = recommended[0]
                    for cat in ContentCategory:
                        if cat.value.lower() == fallback_name:
                            logger.info(f"✅ Using least used category '{fallback_name}' as fallback")
                            return cat
        
        # Default to classified category
        return classified
    
    def _ensure_keyword_diversity(self, text: str, original_prompt: str, 
                                 diversity_analysis: Dict) -> str:
        """Check keyword diversity and regenerate if needed"""
        text_keywords = self._extract_keywords(text)
        overused_keywords = []
        
        for keyword in text_keywords:
            usage_count = diversity_analysis['keywords'].get(keyword, 0)
            if usage_count >= self.keyword_repeat_limit:
                overused_keywords.append(keyword)
        
        # Only regenerate if we have multiple overused keywords
        if len(overused_keywords) > 1:
            logger.info(f"🔄 Regenerating due to overused keywords: {overused_keywords}")
            
            # Add diversity instruction to prompt
            diversity_prompt = (
                f"{original_prompt}\n\n"
                f"IMPORTANT: Avoid focusing heavily on these recently covered topics: "
                f"{', '.join(overused_keywords)}. Find a fresh angle or different aspect."
            )
            
            regenerated_text = self.gpt_service.generate_tweet(diversity_prompt)
            
            if regenerated_text:
                logger.info("✅ Successfully regenerated with better keyword diversity")
                return regenerated_text
            else:
                logger.warning("⚠️ Regeneration failed, using original text")
        
        return text
    
    def _extract_keywords(self, text: str) -> List[str]:
        """Extract important keywords from text"""
        if not text:
            return []
        
        # Financial/economic keywords to track
        important_keywords = [
            'trump', 'biden', 'fed', 'powell', 'inflation', 'rates', 'tariff', 'tariffs',
            'china', 'india', 'europe', 'earnings', 'stock', 'market', 'trade',
            'supply chain', 'margin', 'revenue', 'guidance', 'acquisition', 'merger',
            'tech', 'ai', 'nvidia', 'apple', 'tesla', 'bitcoin', 'crypto', 'recession',
            'election', 'policy', 'regulation', 'sanctions', 'geopolitical'
        ]
        
        text_lower = text.lower()
        found_keywords = []
        
        for keyword in important_keywords:
            if keyword in text_lower:
                found_keywords.append(keyword)
        
        # Extract cashtags
        cashtags = re.findall(r'\$[A-Z]{1,5}', text)
        found_keywords.extend([tag.upper() for tag in cashtags])
        
        return found_keywords
    
    # Helper methods for Notion data extraction
    #def _extract_notion_select(self, select_prop):
        #if not select_prop or not select_prop.get('select'):
            #return None
        #return select_prop['select']['name']
    
    #def _extract_notion_rich_text(self, rich_text_prop):
        #if not rich_text_prop or not rich_text_prop.get('rich_text'):
            #return ''
        #return ''.join([item['plain_text'] for item in rich_text_prop['rich_text']])
    
    # Keep all your existing methods below (unchanged)
    def _get_headline_for_content(self, request: Optional[ContentRequest]) -> Optional[Headline]:
        """Get unused headline for content generation"""
        if request and request.specific_headline:
            return request.specific_headline
            
        # Get unused headline from today, preferably high-scoring
        return self.data_service.get_unused_headline_today()
    
    def _classify_headline_content(self, headline: str) -> ContentCategory:
        """Classify headline content by keywords"""
        headline_lower = headline.lower()
        
        category_scores = {}
        for category, keywords in self.category_keywords.items():
            score = sum(1 for keyword in keywords if keyword in headline_lower)
            if score > 0:
                category_scores[category] = score
        
        if category_scores:
            return max(category_scores, key=category_scores.get)
        else:
            return ContentCategory.MACRO  # Default
    
    def _headline_matches_category(self, headline: str, category: ContentCategory) -> bool:
        """Check if headline matches a specific category"""
        headline_lower = headline.lower()
        keywords = self.category_keywords.get(category, [])
        return any(keyword in headline_lower for keyword in keywords)
      
    def _build_commentary_prompt(self, headline: Headline, category: ContentCategory) -> str:
        """Build GPT prompt for commentary generation"""
        
        # Prepare context
        context = f"Headline: {headline.headline.strip()}\n\n"
        if headline.summary:
            context += f"Summary: {headline.summary.strip()}\n\n"
        else:
            context += "Summary: [No summary available]\n\n"
        
        # Base instruction about market data
        base_instruction = (
            "Whenever you mention a stock ticker (cashtag like $AAPL), include the cashtag; "
            "my system will insert price and percent change.\n\n"
        )
        
        # Category-specific prompts with diversity encouragement
        category_prompts = {
            ContentCategory.MACRO: (
                f"{base_instruction}"
                f"Write a tweet as a hedge fund investor about this macro/economic news:\n\n{context}"
                f"Focus on: market implications, economic trends, policy impacts. "
                f"Be analytical and professional, not hype-driven. "
                f"Find a unique angle or contrarian perspective."
            ),
            ContentCategory.POLITICAL: (
                f"{base_instruction}"
                f"Write a tweet as a hedge fund investor about this political/policy news:\n\n{context}"
                f"Focus on: market implications, policy impacts, sector effects. "
                f"Be analytical and avoid partisan language. "
                f"Consider second-order effects or unintended consequences."
            ),
            ContentCategory.EQUITY: (
                f"{base_instruction}"
                f"Write a tweet as a hedge fund investor about this equity/company news:\n\n{context}"
                f"Focus on: stock implications, sector impact, fundamental analysis. "
                f"Be analytical and research-driven. "
                f"Look for competitive dynamics or industry-wide implications."
            )
        }
        
        return category_prompts.get(category, category_prompts[ContentCategory.MACRO])
    
    def _finalize_text(self, text: str) -> str:
        """Add mentions and disclaimer to final text"""
        # Remove any duplicate disclaimers
        text = re.sub(
            r"This is my opinion\. Not financial advice\..*",
            "",
            text,
            flags=re.IGNORECASE
        ).strip()
        
        # Add final disclaimer
        text += "\n\nThis is my opinion. Not financial advice."
        
        return text