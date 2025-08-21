# hedgefund_agent/services/rss_service.py
import feedparser
import logging
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urlparse

# Import RSS feeds from config
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import RSS_FEED_URLS

logger = logging.getLogger(__name__)

class RSSService:
    """Handles RSS feed fetching and parsing"""
    
    def __init__(self):
        # Use RSS feeds from config
        self.rss_feeds = RSS_FEED_URLS
        logger.info(f"🗞️ Initialized RSS service with {len(self.rss_feeds)} feeds")
    
    def fetch_headlines_from_feed(self, feed_name: str, feed_url: str) -> List[Dict]:
        """Fetch headlines from a single RSS feed"""
        headlines = []
        
        try:
            logger.info(f"📡 Fetching from {feed_name}: {feed_url}")
            
            # Parse the RSS feed
            feed = feedparser.parse(feed_url)
            
            if feed.bozo:
                logger.warning(f"⚠️ Feed {feed_name} has parsing issues: {feed.bozo_exception}")
            
            # Extract entries
            for entry in feed.entries[:10]:  # Limit to 10 most recent
                try:
                    headline_data = {
                        'headline': entry.title.strip() if hasattr(entry, 'title') else '',
                        'summary': self._extract_summary(entry),
                        'url': entry.link if hasattr(entry, 'link') else '',
                        'source': feed_name,
                        'published_at': self._parse_date(entry),
                    }
                    
                    # Only add if we have a headline
                    if headline_data['headline']:
                        headlines.append(headline_data)
                        
                except Exception as e:
                    logger.warning(f"⚠️ Error parsing entry from {feed_name}: {e}")
                    continue
            
            logger.info(f"✅ Fetched {len(headlines)} headlines from {feed_name}")
            return headlines
            
        except Exception as e:
            logger.error(f"❌ Failed to fetch from {feed_name}: {e}")
            return []
    
    def fetch_all_headlines(self) -> List[Dict]:
        """Fetch headlines from all RSS feeds"""
        all_headlines = []
        
        logger.info(f"🚀 Starting RSS fetch from {len(self.rss_feeds)} feeds")
        
        for feed_name, feed_url in self.rss_feeds.items():
            headlines = self.fetch_headlines_from_feed(feed_name, feed_url)
            all_headlines.extend(headlines)
        
        logger.info(f"📰 Total headlines fetched: {len(all_headlines)}")
        return all_headlines
    
    def _extract_summary(self, entry) -> Optional[str]:
        """Extract summary/description from RSS entry"""
        # Try different fields for summary
        for field in ['summary', 'description', 'content']:
            if hasattr(entry, field):
                content = getattr(entry, field)
                if isinstance(content, str):
                    return content.strip()[:500]  # Limit length
                elif isinstance(content, list) and content:
                    return content[0].get('value', '')[:500]
        return None
    
    def _parse_date(self, entry) -> Optional[datetime]:
        """Parse publication date from RSS entry"""
        try:
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                import time
                timestamp = time.mktime(entry.published_parsed)
                return datetime.fromtimestamp(timestamp)
        except Exception:
            pass
        
        # Fallback to current time
        return datetime.now()