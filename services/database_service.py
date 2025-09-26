# hedgefund_agent/services/database_service.py
import psycopg2
import psycopg2.extras
import logging
import json
import asyncio
from datetime import datetime, timedelta, time, timezone
from typing import List, Dict, Optional
from psycopg2.extras import Json

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
        self.logger = logging.getLogger(__name__)
    
    def get_connection(self):
        """Get database connection (create if needed)"""
        try:
            # Check if connection exists and is usable
            if self._connection is None or self._connection.closed:
                self._connection = psycopg2.connect(**self.db_config)
                logger.info("✅ Connected to PostgreSQL")
        
            # Test the connection with a simple query
            cursor = self._connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
        
            return self._connection
        
        except psycopg2.errors.InFailedSqlTransaction:
            # Connection is in failed state - force recreation
            logger.warning("🔄 Transaction failed, recreating connection")
            if self._connection:
                try:
                    self._connection.close()
                except:
                    pass
            self._connection = psycopg2.connect(**self.db_config)
            logger.info("✅ Reconnected to PostgreSQL after transaction failure")
            return self._connection
        
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            # Force recreation on any connection error
            if self._connection:
                try:
                    self._connection.close()
                except:
                    pass
            self._connection = None
            
            raise
    
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

    async def get_top_headlines(self, since_datetime: datetime, limit: int = 10) -> List[Headline]:
        """
        Asynchronously fetches the top N headlines since a specific datetime.
        """
        self.logger.info(f"📰 Fetching top {limit} headlines since {since_datetime}")

        try:
            # Run the synchronous DB call in a separate thread for async compatibility
            def db_call():
                conn = self.get_connection()
                # Use RealDictCursor to get dict-like rows, which is safer for the Headline model
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                try:
                    # Corrected to use 'created_at'
                    sql = """
                        SELECT * FROM hedgefund_agent.headlines
                        WHERE created_at >= %s
                        ORDER BY score DESC, created_at DESC
                        LIMIT %s;
                    """
                    cursor.execute(sql, (since_datetime, limit))
                    return cursor.fetchall()
                finally:
                    cursor.close()

            rows = await asyncio.to_thread(db_call)

            # Unpack dictionary rows directly into the Headline dataclass
            headlines = [Headline(**row) for row in rows]
            self.logger.info(f"✅ Successfully fetched {len(headlines)} headlines.")
            return headlines

        except Exception as e:
            self.logger.error(f"❌ Database error while fetching headlines: {e}")
            return []

    def update_briefing_json_content(self, briefing_id: int, json_content: dict):
        """
        Updates a briefing record with the pre-parsed JSON content for caching.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        sql = """
            UPDATE hedgefund_agent.briefings
            SET json_content = %s
            WHERE id = %s;
        """
        try:
            cursor.execute(sql, (Json(json_content), briefing_id))
            conn.commit()
            self.logger.info(f"Successfully updated json_content for briefing_id: {briefing_id}")
        except Exception as e:
            self.logger.error(f"Database error in update_briefing_json_content for briefing_id {briefing_id}: {e}", exc_info=True)
            conn.rollback()
            raise
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
    
    def get_briefing_definition_by_key(self, briefing_key: str) -> Optional[dict]:
        """Fetches a single briefing definition by its unique key."""
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cursor.execute(
                "SELECT * FROM hedgefund_agent.briefing_definitions WHERE briefing_key = %s",
                (briefing_key,)
            )
            row = cursor.fetchone()
            logger.info(f"Found briefing definition for key: {briefing_key}")
            return row
        except Exception as e:
            logger.error(f"Failed to get briefing definition for key {briefing_key}: {e}")
            raise
        finally:
            cursor.close()

    def get_linked_sections_by_briefing_id(self, briefing_id: int) -> List[dict]:
        """Fetches all market sections linked to a specific briefing ID."""
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cursor.execute(
                """
                SELECT 
                    ms.section_key, ms.title, ms.default_symbols, ms.display_order_map,
                    bs.custom_symbols
                FROM hedgefund_agent.briefing_sections bs
                JOIN hedgefund_agent.market_sections ms ON bs.section_id = ms.id
                WHERE bs.briefing_id = %s
                ORDER BY ms.id
                """,
                (briefing_id,)
            )
            rows = cursor.fetchall()
            logger.info(f"Found {len(rows)} linked sections for briefing ID {briefing_id}")
            return rows
        except Exception as e:
            logger.error(f"Failed to get linked sections for briefing ID {briefing_id}: {e}")
            raise
        finally:
            cursor.close()

    def create_briefing_record(self, briefing_key: str, notion_page_id: str, title: str) -> int:
        """
        Inserts a new briefing record into the database and returns its new ID.
        Uses the existing 'briefings' table schema.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            # Use the column names from your existing table
            sql = """
                INSERT INTO hedgefund_agent.briefings (briefing_type, notion_page_id, title)
                VALUES (%s, %s, %s)
                RETURNING id;
            """
            cursor.execute(sql, (briefing_key, notion_page_id, title))
            new_id = cursor.fetchone()[0]
            conn.commit()
            self.logger.info(f"Created new briefing record with ID: {new_id}")
            return new_id
        except Exception as e:
            self.logger.error(f"Failed to create briefing record: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()

    def update_briefing_urls(self, briefing_id: int, website_url: str, tweet_url: str):
        """
        Updates an existing briefing record with the final public URLs.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            sql = """
                UPDATE hedgefund_agent.briefings
                SET website_url = %s, tweet_url = %s
                WHERE id = %s;
            """
            cursor.execute(sql, (website_url, tweet_url, briefing_id))
            conn.commit()
            self.logger.info(f"Updated URLs for briefing record ID: {briefing_id}")
        except Exception as e:
            self.logger.error(f"Failed to update briefing URLs for ID {briefing_id}: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close() 

    def get_all_equity_symbols(self) -> List[Dict]:
        """
        Fetches all active equity symbols along with their EPIC and primary status.
        Returns a list of dictionaries.
        """
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) # Use RealDictCursor
        try:
            sql = """
                SELECT symbol, epic, is_primary_symbol 
                FROM hedgefund_agent.stock_universe 
                WHERE active = 't' AND asset_type = 'stock';
            """
            cursor.execute(sql)
            symbols_data = cursor.fetchall()
            self.logger.info(f"Fetched {len(symbols_data)} active equity symbols for screening.")
            return symbols_data
        finally:
            cursor.close()

    def get_briefings_missing_json(self) -> list[dict]:
        """
        Fetches the id and notion_page_id for all briefings where json_content is NULL.
        """
        conn = self.get_connection()
        # Use RealDictCursor to get results as dictionaries
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            # Assumes your table is named 'briefings' in the 'hedgefund_agent' schema
            sql = """
                SELECT id, notion_page_id 
                FROM hedgefund_agent.briefings 
                WHERE json_content IS NULL;
            """
            cursor.execute(sql)
            rows = cursor.fetchall()
            self.logger.info(f"Found {len(rows)} briefings with no cached JSON.")
            return rows
        except Exception as e:
            self.logger.error(f"Failed to get briefings missing JSON: {e}")
            return []
        finally:
            cursor.close()

    def log_content_generation(self, content_type: str, theme: str, headline_id: Optional[int], success: bool, url: Optional[str] = None, details: Optional[Dict] = None):
        """
        Logs the result of a content generation event to the database.
        This method was missing, causing the 'AttributeError'.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        # Assumes a table named 'content_log' exists in the 'hedgefund_agent' schema
        sql = """
            INSERT INTO hedgefund_agent.content_log 
            (content_type, theme, headline_id, success, url, details, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        try:
            # Convert details dictionary to a JSON string for the database
            details_json = json.dumps(details) if details else None
            
            cursor.execute(sql, (
                content_type, 
                theme, 
                headline_id, 
                success, 
                url, 
                details_json, 
                datetime.now(timezone.utc)
            ))
            conn.commit()
            self.logger.info(f"Logged content generation for theme '{theme}' with status: {success}")
        except Exception as e:
            self.logger.error(f"Failed to log content generation for theme '{theme}': {e}", exc_info=True)
            conn.rollback()
            # We do not re-raise here, as a logging failure should not halt the main process.
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