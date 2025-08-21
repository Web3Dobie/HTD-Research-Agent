# hedgefund_agent/services/data_service.py
import logging
from typing import Optional
from datetime import datetime

# Import our services
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.database_service import DatabaseService
from core.models import Headline

logger = logging.getLogger(__name__)

class DataService:
    """High-level data service for content generators"""
    
    def __init__(self, database_service: DatabaseService):
        self.db = database_service
    
    async def get_unused_headline_today(self, category: Optional[str] = None) -> Optional[Headline]:
        """Get highest scoring unused headline from today"""
        try:
            return self.db.get_unused_headline_today(category)
        except Exception as e:
            logger.error(f"Failed to get unused headline: {e}")
            return None
    
    async def mark_headline_used(self, headline_id: int, content_type: str):
        """Mark headline as used"""
        try:
            self.db.mark_headline_used(headline_id, content_type)
        except Exception as e:
            logger.error(f"Failed to mark headline used: {e}")
    
    async def is_duplicate_theme(self, theme: str, hours_back: int = 24) -> bool:
        """Check if theme was used recently"""
        try:
            return self.db.is_duplicate_theme(theme, hours_back)
        except Exception as e:
            logger.error(f"Failed to check theme duplication: {e}")
            return False
    
    async def track_theme(self, theme: str):
        """Track theme usage"""
        try:
            self.db.track_theme(theme)
        except Exception as e:
            logger.error(f"Failed to track theme: {e}")