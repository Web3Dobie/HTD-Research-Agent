# hedgefund_agent/core/models.py
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, List

class ContentType(Enum):
    COMMENTARY = "commentary"
    DEEP_DIVE = "deep_dive"
    BRIEFING = "briefing"

class ContentCategory(Enum):
    MACRO = "macro"
    EQUITY = "equity" 
    POLITICAL = "political"

@dataclass
class Headline:
    """News headline with metadata"""
    headline: str
    summary: Optional[str] = None
    score: Optional[int] = None
    category: Optional[str] = None
    source: Optional[str] = None
    url: Optional[str] = None
    used: bool = False
    used_at: Optional[datetime] = None
    created_at: datetime = field(default_factory=datetime.now)
    id: Optional[int] = None  # Will be set by database

@dataclass
class Theme:
    """Content theme tracking"""
    theme: str
    first_used_at: datetime = field(default_factory=datetime.now)
    usage_count: int = 1
    last_used_at: datetime = field(default_factory=datetime.now)
    id: Optional[int] = None

@dataclass
class MarketData:
    """Market data for a ticker"""
    ticker: str
    price: float
    change_percent: float
    volume: Optional[float] = None

@dataclass
class GeneratedContent:
    """Generated content with metadata"""
    text: str
    content_type: ContentType
    category: Optional[ContentCategory] = None
    theme: Optional[str] = None
    market_data: List[MarketData] = field(default_factory=list)
    headline_used: Optional[Headline] = None
    parts: List[str] = field(default_factory=list)  # For threads
    created_at: datetime = field(default_factory=datetime.now)

@dataclass
class ContentRequest:
    """Request for content generation"""
    content_type: ContentType
    category: Optional[ContentCategory] = None
    include_market_data: bool = True
    specific_headline: Optional[Headline] = None  # Added this missing attribute