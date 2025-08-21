# hedgefund_agent/services/scoring_service.py
import logging
import re
from typing import List, Dict
from datetime import datetime

# Import our GPT service
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)

class ScoringService:
    """GPT-based headline scoring for hedge fund relevance"""
    
    def __init__(self, gpt_service):
        self.gpt_service = gpt_service
        
        # Category classification keywords (keep for basic classification)
        self.category_keywords = {
            'macro': ['fed', 'federal reserve', 'inflation', 'gdp', 'unemployment', 
                     'recession', 'economy', 'monetary policy', 'fiscal policy', 'interest rate'],
            'equity': ['earnings', 'stock', 'shares', 'ipo', 'dividend', 'buyback',
                      'guidance', 'revenue', 'profit', 'loss', 'acquisition', 'merger'],
            'political': ['trump', 'biden', 'election', 'congress', 'senate',
                         'tariff', 'trade', 'sanctions', 'policy', 'government']
        }
    
    def score_headline(self, headline_data: Dict) -> Dict:
        """Score a single headline using GPT"""
        headline = headline_data.get('headline', '')
        summary = headline_data.get('summary', '') or ''
        
        # Get GPT score for market impact
        score = self._get_gpt_score(headline, summary)
        
        # Classify category
        category = self._classify_category(headline, summary)
        
        # Update the headline data
        scored_headline = headline_data.copy()
        scored_headline.update({
            'score': score,
            'category': category,
            'scored_at': datetime.now()
        })
        
        logger.debug(f"📊 GPT scored '{headline[:50]}...' = {score} ({category})")
        return scored_headline
    
    def score_headlines(self, headlines: List[Dict]) -> List[Dict]:
        """Score multiple headlines using GPT"""
        logger.info(f"📊 GPT scoring {len(headlines)} headlines")
        
        scored_headlines = []
        failed_count = 0
        
        for headline_data in headlines:
            try:
                scored = self.score_headline(headline_data)
                scored_headlines.append(scored)
            except Exception as e:
                logger.warning(f"⚠️ Failed to score headline: {e}")
                failed_count += 1
                continue
        
        # Sort by score (highest first)
        scored_headlines.sort(key=lambda x: x.get('score', 0), reverse=True)
        
        logger.info(f"✅ GPT scored {len(scored_headlines)} headlines successfully, {failed_count} failed")
        if scored_headlines:
            logger.info(f"📈 Top score: {scored_headlines[0]['score']}")
        
        return scored_headlines
    
    def _get_gpt_score(self, headline: str, summary: str) -> int:
        """Get GPT score for headline market impact"""
        
        # Build prompt based on original scoring logic
        content_summary = summary if summary else "[No summary available]"
        
        prompt = (
            "As a hedge fund analyst, rate this story's market impact from 1-10.\n\n"
            f"Headline:\n{headline}\n\n"
            f"Summary:\n{content_summary}\n\n"
            "Score based on:\n"
            "- Immediate price action potential\n"
            "- Broader economic/policy implications\n"
            "- Sector-wide or geopolitical relevance\n"
            "- Unusual or market-moving information\n\n"
            "Score 8-10 for headlines with significant, multi-asset, or urgent impact.\n"
            "Return only the number."
        )
        
        try:
            # Get GPT response
            response = self.gpt_service.generate_text(prompt, max_tokens=10)
            
            if not response or response.strip() == "":
                logger.warning(f"GPT returned empty response for: {headline[:50]}...")
                return 1
            
            # Parse score from response
            score = self._parse_score_from_response(response)
            logger.debug(f"GPT scored '{headline[:30]}...' -> {score}")
            return score
            
        except Exception as e:
            logger.error(f"GPT scoring failed for '{headline[:50]}...': {e}")
            return 1
    
    def _parse_score_from_response(self, response: str) -> int:
        """Parse numerical score from GPT response (from original code)"""
        try:
            # Handle formats like "8/10", "8 out of 10", or just "8"
            match = re.search(r"\b([1-9]|10)\s*(?:/|out of)?\s*(?:10)?\b", response, re.IGNORECASE)
            if match:
                score_raw = int(match.group(1))
                return max(1, min(10, score_raw))  # Clamp to [1..10]
            else:
                # Try parsing as a simple number
                score_raw = float(response.strip())
                return max(1, min(10, int(round(score_raw))))
        except Exception as e:
            logger.warning(f"Failed to parse score from response: '{response}' - {e}")
            return 1
    
    def _classify_category(self, headline: str, summary: str) -> str:
        """Classify headline into category using keywords"""
        content = f"{headline} {summary}".lower()
        
        category_scores = {}
        for category, keywords in self.category_keywords.items():
            score = sum(1 for keyword in keywords if keyword in content)
            if score > 0:
                category_scores[category] = score
        
        if category_scores:
            return max(category_scores, key=category_scores.get)
        else:
            return 'macro'  # Default category