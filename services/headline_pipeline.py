# hedgefund_agent/services/headline_pipeline.py
import logging
from typing import List
from datetime import datetime

# Import our services
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.rss_service import RSSService
from services.scoring_service import ScoringService
from services.gpt_service import GPTService
from services.database_service import DatabaseService
from core.models import Headline

logger = logging.getLogger(__name__)

class HeadlinePipeline:
    """Main pipeline for fetching, scoring (with GPT), and storing headlines"""
    
    def __init__(self, database_service: DatabaseService):
        self.rss_service = RSSService()
        self.gpt_service = GPTService()
        self.scoring_service = ScoringService(self.gpt_service)  # Pass GPT service
        self.database_service = database_service
    
    def run_pipeline(self) -> int:
        """Run the complete headline pipeline with GPT scoring"""
        logger.info("🚀 Starting headline pipeline with GPT scoring")
        
        try:
            # Step 1: Fetch headlines from RSS
            raw_headlines = self.rss_service.fetch_all_headlines()
            if not raw_headlines:
                logger.warning("⚠️ No headlines fetched from RSS feeds")
                return 0
            
            # Step 2: Score headlines using GPT
            scored_headlines = self.scoring_service.score_headlines(raw_headlines)
            
            # Step 3: Filter by minimum score and store in database
            stored_count = self._store_unique_headlines(scored_headlines, min_score=7)
            
            logger.info(f"✅ GPT headline pipeline complete: {stored_count} new headlines stored")
            return stored_count
            
        except Exception as e:
            logger.error(f"❌ Headline pipeline failed: {e}")
            raise
    
    def _store_unique_headlines(self, scored_headlines: List[dict], min_score: int = 7) -> int:
        """Store headlines in database, filtering by score and avoiding duplicates"""
        stored_count = 0
        
        for headline_data in scored_headlines:
            try:
                # Only store headlines that meet minimum score threshold
                if headline_data.get('score', 0) < min_score:
                    logger.debug(f"🔽 Skipped low score ({headline_data['score']}): {headline_data['headline'][:50]}...")
                    continue
                
                # Create Headline model
                headline = Headline(
                    headline=headline_data['headline'],
                    summary=headline_data.get('summary'),
                    score=headline_data['score'],
                    category=headline_data['category'],
                    source=headline_data['source'],
                    url=headline_data.get('url'),
                    created_at=headline_data.get('published_at', datetime.now())
                )
                
                # Check for duplicate (simple headline text check)
                if not self._is_duplicate_headline(headline.headline):
                    headline_id = self.database_service.save_headline(headline)
                    stored_count += 1
                    logger.debug(f"💾 Stored headline {headline_id} (score: {headline.score})")
                else:
                    logger.debug(f"🔄 Skipped duplicate: {headline.headline[:50]}...")
                    
            except Exception as e:
                logger.warning(f"⚠️ Failed to store headline: {e}")
                continue
        
        return stored_count
    
    def _is_duplicate_headline(self, headline_text: str) -> bool:
        """Simple duplicate check - could be enhanced with fuzzy matching"""
        try:
            # For now, just check if exact headline exists
            conn = self.database_service.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM hedgefund_agent.headlines 
                    WHERE headline = %s
                )
            """, (headline_text,))
            
            exists = cursor.fetchone()[0]
            cursor.close()
            return exists
            
        except Exception as e:
            logger.warning(f"⚠️ Duplicate check failed: {e}")
            return False  # If check fails, allow the headline