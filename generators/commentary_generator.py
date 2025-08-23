# hedgefund_agent/generators/commentary_generator.py
import logging
import re
import random
from typing import Optional, List
from datetime import datetime

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
    """Generates single-tweet hedge fund commentary"""
    
    def __init__(self, data_service, gpt_service, market_client, config):
        self.data_service = data_service
        self.gpt_service = gpt_service
        self.market_client = market_client
        self.config = config
        
        # Category rotation tracking
        self.last_used_category = None
        
        # Category classification keywords (from original)
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
        """Generate commentary content"""
        try:
            logger.info("🐂 Generating hedge fund commentary")
            
            # 1. Get headline for content generation (FIXED: removed await)
            headline = self._get_headline_for_content(request)
            if not headline:
                raise Exception("No suitable headline available")
            
            # 2. Determine category if not specified
            category = self._determine_category(request, headline)
            
            # 3. Extract theme for deduplication (FIXED: removed await)
            theme = self._extract_and_validate_theme(headline.headline)
            
            # 4. Generate base commentary using GPT
            prompt = self._build_commentary_prompt(headline, category)
            base_text = self.gpt_service.generate_tweet(prompt)
            
            if not base_text:
                raise Exception("GPT generation failed")
            
            # 5. Enrich with market data if requested
            enriched_text = base_text
            market_data = []
            
            if request and request.include_market_data:
                enriched_text, market_data = await self._enrich_with_market_data(base_text)
            
            # 6. Add mentions and disclaimer
            final_text = self._finalize_text(enriched_text)
            
            # 7. Mark headline as used and track theme
            self.data_service.mark_headline_used(headline.id, "commentary")
            self.data_service.track_theme(theme)
            
            logger.info(f"✅ Generated commentary: {final_text[:50]}...")
            
            return GeneratedContent(
                text=final_text,
                content_type=ContentType.COMMENTARY,
                category=category,
                theme=theme,
                market_data=market_data,
                headline_used=headline
            )
            
        except Exception as e:
            logger.error(f"❌ Commentary generation failed: {e}")
            raise
    
    def _get_headline_for_content(self, request: Optional[ContentRequest]) -> Optional[Headline]:
        """Get unused headline for content generation (FIXED: removed async)"""
        if request and request.specific_headline:
            return request.specific_headline
            
        # Get unused headline from today, preferably high-scoring
        return self.data_service.get_unused_headline_today()
    
    def _determine_category(self, request: Optional[ContentRequest], headline: Headline) -> ContentCategory:
        """Determine content category with rotation logic (from original)"""
        
        # If request specifies category, use it
        if request and request.category:
            return request.category
        
        # If headline already has category, convert to enum
        if headline.category:
            try:
                return ContentCategory(headline.category.lower())
            except ValueError:
                pass
        
        # Classify based on content
        classified = self._classify_headline_content(headline.headline)
        
        # Apply category rotation (avoid same category twice in a row)
        if classified == self.last_used_category:
            # Try to pick a different category
            other_categories = [cat for cat in ContentCategory if cat != classified]
            if other_categories:
                # Check if any other category also matches
                for cat in other_categories:
                    if self._headline_matches_category(headline.headline, cat):
                        classified = cat
                        break
        
        self.last_used_category = classified
        return classified
    
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
    
    def _extract_and_validate_theme(self, headline: str) -> str:
        """Extract theme and check for duplicates (FIXED: removed async)"""
        # Simple theme extraction - first few words or key topic
        words = headline.split()
        
        # Look for key financial terms
        theme_candidates = []
        for word in words[:8]:  # Check first 8 words
            word_clean = re.sub(r'[^\w]', '', word.lower())
            if len(word_clean) > 3 and word_clean in headline.lower():
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
            # Add timestamp or modifier to make unique
            theme = f"{base_theme}_{datetime.now().strftime('%H%M')}"
            logger.info(f"🔄 Theme modified to avoid duplicate: {theme}")
        
        return theme
    
    def _build_commentary_prompt(self, headline: Headline, category: ContentCategory) -> str:
        """Build GPT prompt for commentary generation (based on original style)"""
        
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
        
        # Category-specific prompts (from original logic)
        category_prompts = {
            ContentCategory.MACRO: (
                f"{base_instruction}"
                f"Write a tweet as a hedge fund investor about this macro/economic news:\n\n{context}"
                f"Focus on: market implications, economic trends, policy impacts. "
                f"Be analytical and professional, not hype-driven."
            ),
            ContentCategory.POLITICAL: (
                f"{base_instruction}"
                f"Write a tweet as a hedge fund investor about this political/policy news:\n\n{context}"
                f"Focus on: market implications, policy impacts, sector effects. "
                f"Be analytical and avoid partisan language."
            ),
            ContentCategory.EQUITY: (
                f"{base_instruction}"
                f"Write a tweet as a hedge fund investor about this equity/company news:\n\n{context}"
                f"Focus on: stock implications, sector impact, fundamental analysis. "
                f"Be analytical and research-driven."
            )
        }
        
        return category_prompts.get(category, category_prompts[ContentCategory.MACRO])
    
    async def _enrich_with_market_data(self, text: str) -> tuple[str, List[MarketData]]:
        """Enrich text with market data (STAYS ASYNC - calls market service)"""
        # Extract cashtags from text
        cashtags = self._extract_cashtags(text)
        valid_tickers = [tag.strip("$") for tag in cashtags if self._is_valid_ticker(tag.strip("$"))]
        
        if not valid_tickers:
            return text, []
        
        logger.info(f"💰 Enriching with market data for: {valid_tickers}")
        
        # Fetch market data
        market_data = []
        enriched_text = text
        
        for ticker in valid_tickers:
            try:
                logger.debug(f"🔍 Fetching price for {ticker}...")
                
                # Call your Market Data Service (from Phase 1)
                price_data = await self.market_client.get_price(ticker)
                
                logger.debug(f"📊 Price data response for {ticker}: {price_data}")
                
                if price_data and price_data.get('price') is not None and price_data.get('price') > 0:
                    # Create market data object
                    market_info = MarketData(
                        ticker=ticker,
                        price=price_data['price'],
                        change_percent=price_data['change_percent'],
                        volume=price_data.get('volume')
                    )
                    market_data.append(market_info)
                    
                    # Replace cashtag with enriched version in text
                    cashtag = f"${ticker}"
                    enriched_format = f"${ticker} (${price_data['price']:.2f}, {price_data['change_percent']:+.2f}%)"
                    
                    # Use regex to replace cashtag (word boundary)
                    pattern = rf"\${ticker}(?=\s|$|[^\w])"
                    enriched_text = re.sub(pattern, enriched_format, enriched_text, flags=re.IGNORECASE)
                    
                    logger.info(f"💹 Enriched {ticker}: ${price_data['price']:.2f} ({price_data['change_percent']:+.2f}%)")
                else:
                    logger.warning(f"⚠️ No valid price data for {ticker}: {price_data}")
                
            except Exception as e:
                logger.error(f"❌ Failed to get price for {ticker}: {str(e)}")
                # Log the full traceback for debugging
                import traceback
                logger.debug(f"Full traceback for {ticker}: {traceback.format_exc()}")
                continue
        
        logger.info(f"✅ Successfully enriched {len(market_data)} tickers")
        return enriched_text, market_data
    
    def _extract_cashtags(self, text: str) -> List[str]:
        """Extract cashtags from text (from original)"""
        pattern = r'\$[A-Z]{1,5}\b'
        return re.findall(pattern, text, re.IGNORECASE)
    
    def _is_valid_ticker(self, ticker: str) -> bool:
        """Basic ticker validation (from original)"""
        return (len(ticker) >= 1 and len(ticker) <= 5 and 
                ticker.isalpha() and ticker.isupper())
    
    def _finalize_text(self, text: str) -> str:
        """Add mentions and disclaimer (from original style)"""
        # Insert mentions (placeholder - you can implement mention logic)
        final_text = text
        
        # Clean any existing disclaimers
        final_text = re.sub(
            r"This is my opinion\.? ?Not financial advice\.?",
            "",
            final_text,
            flags=re.IGNORECASE
        ).strip()
        
        # Add disclaimer
        disclaimer = self.config.get('default_disclaimer', "This is my opinion. Not financial advice.")
        final_text += f"\n\n{disclaimer}"
        
        return final_text