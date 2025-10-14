"""
Semantic Theme Service - Intelligent theme extraction using embeddings
Replaces the broken theme extraction that generated "gold_0700" type themes
"""

import logging
from typing import Optional, Dict, List, Tuple
from datetime import datetime, timedelta
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

logger = logging.getLogger(__name__)


class SemanticThemeService:
    """
    Service for extracting and tracking semantic themes using embeddings.
    Uses sentence-transformers to create meaningful theme representations.
    """
    
    def __init__(self, database_service):
        """
        Initialize the semantic theme service.
        
        Args:
            database_service: DatabaseService instance for data persistence
        """
        self.db = database_service
        
        # Load the sentence transformer model
        # all-MiniLM-L6-v2: Fast, good quality, 384 dimensions
        logger.info("🤖 Loading sentence transformer model...")
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        logger.info("✅ Semantic model loaded successfully")
    
    def extract_theme(self, text: str, max_length: int = 100) -> str:
        """
        Extract a semantic theme from text.
        
        This creates a meaningful theme by:
        1. Taking the first sentence or meaningful chunk
        2. Cleaning and normalizing it
        3. Truncating to reasonable length
        
        Args:
            text: Input text (headline, content, etc.)
            max_length: Maximum theme length in characters
            
        Returns:
            Extracted theme string
        """
        if not text or not text.strip():
            return "unknown_theme"
        
        # Clean the text
        text = text.strip()
        
        # Try to extract first sentence
        sentences = text.split('.')
        if sentences:
            theme = sentences[0].strip()
        else:
            theme = text
        
        # Truncate to max length
        if len(theme) > max_length:
            theme = theme[:max_length].rsplit(' ', 1)[0]  # Break at word boundary
        
        # Clean up
        theme = theme.strip()
        
        # Fallback if somehow empty
        if not theme:
            theme = text[:max_length].strip()
        
        logger.info(f"🧠 Extracted semantic theme: {theme[:50]}...")
        return theme
    
    def get_embedding(self, text: str) -> np.ndarray:
        """
        Generate embedding vector for text.
        
        Args:
            text: Input text to embed
            
        Returns:
            384-dimensional embedding vector
        """
        embedding = self.model.encode(text, convert_to_numpy=True)
        return embedding
    
    def calculate_similarity(self, text1: str, text2: str) -> float:
        """
        Calculate semantic similarity between two texts.
        
        Args:
            text1: First text
            text2: Second text
            
        Returns:
            Similarity score between 0.0 (completely different) and 1.0 (identical)
        """
        emb1 = self.get_embedding(text1)
        emb2 = self.get_embedding(text2)
        
        # Reshape for sklearn
        emb1 = emb1.reshape(1, -1)
        emb2 = emb2.reshape(1, -1)
        
        similarity = cosine_similarity(emb1, emb2)[0][0]
        return float(similarity)
    
    def track_theme(
        self,
        theme_text: str,
        full_content: str,
        content_type: str,
        category: str
    ) -> int:
        """
        Track a semantic theme with its embedding.
        
        This stores both the theme text and its embedding vector for
        future similarity comparisons.
        
        Args:
            theme_text: Extracted theme text
            full_content: Full content text (for embedding)
            content_type: Type of content (commentary, deep_dive)
            category: Content category
            
        Returns:
            Theme ID from database
        """
        # Generate embedding for the full content
        embedding = self.get_embedding(full_content)
        
        # Convert numpy array to list for JSON storage
        embedding_list = embedding.tolist()
        
        # Store in database
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Insert into semantic_themes table
                    cur.execute("""
                        INSERT INTO hedgefund_agent.semantic_themes 
                        (theme_text, content_type, category, embedding_vector, usage_count, last_used_at)
                        VALUES (%s, %s, %s, %s, 1, NOW())
                        ON CONFLICT (theme_text) 
                        DO UPDATE SET
                            usage_count = hedgefund_agent.semantic_themes.usage_count + 1,
                            last_used_at = NOW()
                        RETURNING id
                    """, (theme_text, content_type, category, embedding_list))
                    
                    theme_id = cur.fetchone()[0]
                    conn.commit()
                    
                    logger.info(f"✅ Tracked semantic theme (ID: {theme_id})")
                    return theme_id
                    
        except Exception as e:
            logger.error(f"❌ Failed to track semantic theme: {e}")
            raise
    
    def get_recent_themes(
        self,
        hours_back: int = 24,
        content_type: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict]:
        """
        Get recent themes with their embeddings.
        
        Args:
            hours_back: How many hours to look back
            content_type: Optional filter by content type
            limit: Maximum number of themes to return
            
        Returns:
            List of theme dictionaries with embeddings
        """
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    if content_type:
                        cur.execute("""
                            SELECT id, theme_text, content_type, category, 
                                   embedding_vector, last_used_at
                            FROM hedgefund_agent.semantic_themes
                            WHERE last_used_at >= NOW() - INTERVAL '%s hours'
                              AND content_type = %s
                            ORDER BY last_used_at DESC
                            LIMIT %s
                        """, (hours_back, content_type, limit))
                    else:
                        cur.execute("""
                            SELECT id, theme_text, content_type, category,
                                   embedding_vector, last_used_at
                            FROM hedgefund_agent.semantic_themes
                            WHERE last_used_at >= NOW() - INTERVAL '%s hours'
                            ORDER BY last_used_at DESC
                            LIMIT %s
                        """, (hours_back, limit))
                    
                    rows = cur.fetchall()
                    
                    themes = []
                    for row in rows:
                        themes.append({
                            'id': row[0],
                            'theme_text': row[1],
                            'content_type': row[2],
                            'category': row[3],
                            'embedding': np.array(row[4]),  # Convert back to numpy
                            'last_used_at': row[5]
                        })
                    
                    return themes
                    
        except Exception as e:
            logger.error(f"❌ Failed to get recent themes: {e}")
            return []
    
    def find_similar_themes(
        self,
        text: str,
        threshold: float = 0.5,
        hours_back: int = 24,
        content_type: Optional[str] = None
    ) -> List[Tuple[Dict, float]]:
        """
        Find themes similar to the given text.
        
        Args:
            text: Text to compare against recent themes
            threshold: Minimum similarity threshold (0.0 to 1.0)
            hours_back: How many hours to look back
            content_type: Optional filter by content type
            
        Returns:
            List of (theme_dict, similarity_score) tuples above threshold
        """
        # Get embedding for input text
        input_embedding = self.get_embedding(text)
        
        # Get recent themes
        recent_themes = self.get_recent_themes(
            hours_back=hours_back,
            content_type=content_type
        )
        
        if not recent_themes:
            return []
        
        # Calculate similarities
        similar_themes = []
        for theme in recent_themes:
            similarity = cosine_similarity(
                input_embedding.reshape(1, -1),
                theme['embedding'].reshape(1, -1)
            )[0][0]
            
            if similarity >= threshold:
                similar_themes.append((theme, float(similarity)))
        
        # Sort by similarity descending
        similar_themes.sort(key=lambda x: x[1], reverse=True)
        
        return similar_themes