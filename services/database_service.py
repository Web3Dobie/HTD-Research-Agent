# hedgefund_agent/services/database_service.py
import psycopg2
import psycopg2.extras
import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional

# Use absolute imports for the core models
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.models import Headline, Theme

logger = logging.getLogger(__name__)

class DatabaseService:
    """Handles all PostgreSQL operations for HedgeFund Agent"""
    
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self._connection = None
    
    def get_connection(self):
        """Get database connection (create if needed)"""
        if self._connection is None or self._connection.closed:
            try:
                self._connection = psycopg2.connect(**self.db_config)
                logger.info("✅ Connected to PostgreSQL")
            except Exception as e:
                logger.error(f"❌ Database connection failed: {e}")
                raise
        return self._connection
    
    def close_connection(self):
        """Close database connection"""
        if self._connection and not self._connection.closed:
            self._connection.close()
            logger.info("🔌 Database connection closed")
    
    # === Headlines Operations ===
    
    def save_headline(self, headline: Headline) -> int:
        """Save headline to database, return ID"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO hedgefund_agent.headlines 
                (headline, summary, score, category, source, url, used, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                headline.headline,
                headline.summary,
                headline.score,
                headline.category,
                headline.source,
                headline.url,
                headline.used,
                headline.created_at
            ))
            
            headline_id = cursor.fetchone()[0]
            conn.commit()
            
            logger.info(f"📰 Saved headline ID {headline_id}: {headline.headline[:50]}...")
            return headline_id
            
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Failed to save headline: {e}")
            raise
        finally:
            cursor.close()
    
    def get_unused_headline_today(self, category: Optional[str] = None) -> Optional[Headline]:
        """Get highest scoring unused headline from today"""
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        try:
            # Base query for unused headlines from today
            query = """
                SELECT * FROM hedgefund_agent.headlines 
                WHERE used = FALSE 
                AND created_at >= CURRENT_DATE
            """
            params = []
            
            # Add category filter if specified
            if category:
                query += " AND category = %s"
                params.append(category)
            
            # Order by score (highest first)
            query += " ORDER BY score DESC LIMIT 1"
            
            cursor.execute(query, params)
            row = cursor.fetchone()
            
            if row:
                headline = Headline(
                    id=row['id'],
                    headline=row['headline'],
                    summary=row['summary'],
                    score=row['score'],
                    category=row['category'],
                    source=row['source'],
                    url=row['url'],
                    used=row['used'],
                    used_at=row['used_at'],
                    created_at=row['created_at']
                )
                logger.info(f"📋 Found unused headline: {headline.headline[:50]}...")
                return headline
            else:
                logger.warning("⚠️ No unused headlines found for today")
                return None
                
        except Exception as e:
            logger.error(f"❌ Failed to get unused headline: {e}")
            raise
        finally:
            cursor.close()
    
    def mark_headline_used(self, headline_id: int, content_type: str):
        """Mark headline as used"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                UPDATE hedgefund_agent.headlines 
                SET used = TRUE, used_at = %s 
                WHERE id = %s
            """, (datetime.now(), headline_id))
            
            conn.commit()
            logger.info(f"✅ Marked headline {headline_id} as used for {content_type}")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Failed to mark headline as used: {e}")
            raise
        finally:
            cursor.close()

    def get_top_headlines_for_website(self, limit: int = 4, hours: int = 48, min_score: int = 7) -> List[dict]:
        """Get top scoring headlines for website display"""
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        try:
            cursor.execute("""
                SELECT 
                    id,
                    headline, 
                    summary,
                    score, 
                    category, 
                    source, 
                    url,
                    created_at
                FROM hedgefund_agent.headlines 
                WHERE created_at >= NOW() - INTERVAL %s
                AND score >= %s
                ORDER BY score DESC, created_at DESC
                LIMIT %s
            """, (f'{hours} hours', min_score, limit))
            
            rows = cursor.fetchall()
            
            headlines = []
            for row in rows:
                headline_data = {
                    "id": row['id'],
                    "headline": row['headline'],
                    "summary": row['summary'],
                    "score": row['score'],
                    "category": row['category'] or "general",
                    "source": row['source'] or "financial_news",
                    "url": row['url'] or "",
                    "created_at": row['created_at']
                }
                headlines.append(headline_data)
                
            logger.info(f"📊 Retrieved {len(headlines)} top headlines for website")
            return headlines
            
        except Exception as e:
            logger.error(f"❌ Failed to get top headlines for website: {e}")
            return []
        finally:
            cursor.close()

    def get_headlines_count(self) -> int:
        """Get total count of headlines for health checks"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT COUNT(*) FROM hedgefund_agent.headlines")
            count = cursor.fetchone()[0]
            logger.debug(f"📊 Total headlines in database: {count}")
            return count
            
        except Exception as e:
            logger.error(f"❌ Failed to get headlines count: {e}")
            return 0
        finally:
            cursor.close()

    def get_recent_headlines_by_category(self, category: str, limit: int = 10, hours: int = 24) -> List[dict]:
        """Get recent headlines by category"""
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        try:
            cursor.execute("""
                SELECT 
                    id,
                    headline, 
                    summary,
                    score, 
                    category, 
                    source, 
                    url,
                    created_at,
                    used
                FROM hedgefund_agent.headlines 
                WHERE category = %s
                AND created_at >= NOW() - INTERVAL %s
                ORDER BY score DESC, created_at DESC
                LIMIT %s
            """, (category, f'{hours} hours', limit))
            
            rows = cursor.fetchall()
            
            headlines = []
            for row in rows:
                headline_data = {
                    "id": row['id'],
                    "headline": row['headline'],
                    "summary": row['summary'],
                    "score": row['score'],
                    "category": row['category'],
                    "source": row['source'],
                    "url": row['url'],
                    "created_at": row['created_at'],
                    "used": row['used']
                }
                headlines.append(headline_data)
                
            logger.info(f"📊 Retrieved {len(headlines)} {category} headlines")
            return headlines
            
        except Exception as e:
            logger.error(f"❌ Failed to get {category} headlines: {e}")
            return []
        finally:
            cursor.close()

    def mark_headline_as_used(self, headline_id: int) -> bool:
        """Mark a headline as used for content generation"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                UPDATE hedgefund_agent.headlines 
                SET used = TRUE, used_at = %s 
                WHERE id = %s
            """, (datetime.now(), headline_id))
            
            conn.commit()
            
            if cursor.rowcount > 0:
                logger.info(f"✅ Marked headline {headline_id} as used")
                return True
            else:
                logger.warning(f"⚠️ No headline found with ID {headline_id}")
                return False
                
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Failed to mark headline {headline_id} as used: {e}")
            return False
        finally:
            cursor.close()
    
    # === Theme Operations ===
    
    def is_duplicate_theme(self, theme: str, hours_back: int = 24) -> bool:
        """Check if theme was used in the last N hours"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cutoff_time = datetime.now() - timedelta(hours=hours_back)
            
            cursor.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM hedgefund_agent.themes 
                    WHERE theme = %s AND last_used_at > %s
                )
            """, (theme, cutoff_time))
            
            is_duplicate = cursor.fetchone()[0]
            
            if is_duplicate:
                logger.info(f"🔄 Theme '{theme}' was used recently (duplicate)")
            else:
                logger.info(f"✅ Theme '{theme}' is unique")
                
            return is_duplicate
            
        except Exception as e:
            logger.error(f"❌ Failed to check theme duplication: {e}")
            raise
        finally:
            cursor.close()
    
    def track_theme(self, theme: str):
        """Track theme usage (insert or update)"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Try to update existing theme
            cursor.execute("""
                UPDATE hedgefund_agent.themes 
                SET usage_count = usage_count + 1, last_used_at = %s 
                WHERE theme = %s
            """, (datetime.now(), theme))
            
            if cursor.rowcount == 0:
                # Insert new theme if it doesn't exist
                cursor.execute("""
                    INSERT INTO hedgefund_agent.themes (theme, first_used_at, usage_count, last_used_at)
                    VALUES (%s, %s, 1, %s)
                """, (theme, datetime.now(), datetime.now()))
                logger.info(f"🆕 Started tracking new theme: '{theme}'")
            else:
                logger.info(f"📈 Updated theme usage: '{theme}'")
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Failed to track theme: {e}")
            raise
        finally:
            cursor.close()

    def get_top_unused_headline_today(self, min_score: int = 9) -> Optional[Headline]:
        """
        Get highest scoring unused headline above minimum threshold.
        Specifically designed for deep dives that need high-quality headlines.
        
        Args:
            min_score: Minimum score threshold (default 9 for deep dives)
            
        Returns:
            Highest scoring Headline above threshold, or None if none found
        """
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        try:
            query = """
                SELECT * FROM hedgefund_agent.headlines 
                WHERE used = FALSE 
                AND created_at >= CURRENT_DATE
                AND score >= %s
                ORDER BY score DESC LIMIT 1
            """
            
            cursor.execute(query, (min_score,))
            row = cursor.fetchone()
            
            if row:
                headline = Headline(
                    id=row['id'],
                    headline=row['headline'],
                    summary=row['summary'],
                    score=row['score'],
                    category=row['category'],
                    source=row['source'],
                    url=row['url'],
                    used=row['used'],
                    used_at=row['used_at'],
                    created_at=row['created_at']
                )
                logger.info(f"📋 Found top scoring headline (score: {headline.score}): {headline.headline[:50]}...")
                return headline
            else:
                logger.warning(f"⚠️ No unused headlines found with score >= {min_score}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Failed to get top unused headline: {e}")
            raise
        finally:
            cursor.close()
    
    # === System Logging ===
    
    def log_system_event(self, service: str, level: str, message: str, metadata: dict = None):
        """Log system events to shared.system_logs"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Convert metadata dict to JSON string for PostgreSQL
            metadata_json = json.dumps(metadata) if metadata else None
            
            cursor.execute("""
                INSERT INTO shared.system_logs (service, level, message, metadata, created_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (service, level, message, metadata_json, datetime.now()))
            
            conn.commit()
            
        except Exception as e:
            # Don't raise exceptions for logging failures
            logger.error(f"❌ Failed to log system event: {e}")
        finally:
            cursor.close()