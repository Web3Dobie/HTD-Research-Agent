"""
Content Similarity Service - Block repetitive content
Prevents posting content that's too similar to recent posts
"""

import logging
import json
from typing import Optional, Dict, Tuple
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class ContentSimilarityService:
    """
    Service for checking content similarity to prevent repetitive posts.
    Uses SemanticThemeService for embedding-based similarity.
    """
    
    def __init__(self, database_service, semantic_theme_service):
        """
        Initialize the content similarity service.
        
        Args:
            database_service: DatabaseService instance
            semantic_theme_service: SemanticThemeService instance
        """
        self.db = database_service
        self.semantic_service = semantic_theme_service
    
    def is_content_too_similar(
        self,
        text: str,
        hours_back: int = 8,
        similarity_threshold: float = 0.50,
        content_type: Optional[str] = None
    ) -> Tuple[bool, Optional[Dict]]:
        """
        Check if content is too similar to recent posts.
        
        Args:
            text: Content text to check
            hours_back: How many hours to look back
            similarity_threshold: Minimum similarity to consider "too similar" (0.0-1.0)
            content_type: Optional filter by content type
            
        Returns:
            Tuple of (is_too_similar: bool, similar_content: Dict or None)
        """
        logger.info(f"🔍 Checking similarity against last {hours_back}h of content...")
        
        # Find similar themes
        similar_themes = self.semantic_service.find_similar_themes(
            text=text,
            threshold=similarity_threshold,
            hours_back=hours_back,
            content_type=content_type
        )
        
        if not similar_themes:
            logger.info("✅ No similar content found - content is unique")
            return False, None
        
        # Get the most similar theme
        most_similar_theme, similarity_score = similar_themes[0]
        
        logger.warning(
            f"⚠️ Found similar content: {similarity_score:.0%} match with "
            f"'{most_similar_theme['theme_text'][:50]}...'"
        )
        
        return True, {
            'theme_id': most_similar_theme['id'],
            'content': most_similar_theme['theme_text'],
            'similarity': similarity_score,
            'content_type': most_similar_theme['content_type'],
            'category': most_similar_theme['category'],
            'posted_at': most_similar_theme['last_used_at']
        }
    
    def store_content_history(
        self,
        content_text: str,
        content_type: str,
        theme_id: Optional[int] = None
    ) -> int:
        """
        Store content in history for future similarity checks.
        
        Args:
            content_text: Full content text
            content_type: Type of content (commentary, deep_dive)
            theme_id: Optional theme ID reference
            
        Returns:
            Content history ID
        """
        # Generate embedding
        embedding = self.semantic_service.get_embedding(content_text)
        embedding_json = json.dumps(embedding.tolist())
        
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO hedgefund_agent.content_history
                        (content_text, content_type, theme_id, embedding_vector, created_at)
                        VALUES (%s, %s, %s, %s::jsonb, NOW())
                        RETURNING id
                    """, (content_text, content_type, theme_id, embedding_json))
                    
                    content_id = cur.fetchone()[0]
                    conn.commit()
                    
                    logger.info(f"✅ Stored content history (ID: {content_id})")
                    return content_id
                    
        except Exception as e:
            logger.error(f"❌ Failed to store content history: {e}")
            raise

    def is_content_too_similar_today(
        self,
        text: str,
        similarity_threshold: float = 0.50,
        content_type: Optional[str] = None
    ) -> Tuple[bool, Optional[Dict]]:
        """
        Check if content is too similar to ANY content posted today.
        
        Args:
            text: Content text to check
            similarity_threshold: Minimum similarity to consider "too similar" (0.0-1.0)
            content_type: Optional filter by content type
            
        Returns:
            Tuple of (is_too_similar: bool, similar_content: Dict or None)
        """
        from datetime import datetime
        
        logger.info(f"🔍 Checking similarity against all content posted today...")
        
        # Find similar themes from TODAY (since 00:00)
        similar_themes = self.semantic_service.find_similar_themes_today(
            text=text,
            threshold=similarity_threshold,
            content_type=content_type
        )
        
        if not similar_themes:
            logger.info("✅ No similar content found today - content is unique")
            return False, None
        
        # Get the most similar theme
        most_similar_theme, similarity_score = similar_themes[0]
        
        logger.warning(
            f"⚠️ Found similar content from today: {similarity_score:.0%} match with "
            f"'{most_similar_theme['theme_text'][:50]}...'"
        )
        
        return True, {
            'theme_id': most_similar_theme['id'],
            'content': most_similar_theme['theme_text'],
            'similarity': similarity_score,
            'content_type': most_similar_theme['content_type'],
            'category': most_similar_theme['category'],
            'posted_at': most_similar_theme['last_used_at']
        }