# hedgefund_agent/core/models.py
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any
from services.market_sentiment_service import SentimentAnalysis

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
    commentary: Optional[str] = None  # AI-generated commentary

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
    article_id: Optional[str] = None
    article_url: Optional[str] = None

@dataclass
class ContentRequest:
    """Request for content generation"""
    content_type: ContentType
    category: Optional[ContentCategory] = None
    include_market_data: bool = True
    specific_headline: Optional[Headline] = None  # Added this missing attribute

@dataclass
class BriefingPayload:
    """A container for all data needed to generate a briefing document."""
    market_analysis: SentimentAnalysis
    raw_market_data: Dict[str, List[Dict]] = field(default_factory=dict)  # section_name -> [{'symbol': 'SPY', 'price': 455.20, 'change_percent': 1.25, ...}]
    market_news: List[Dict[str, Any]] = field(default_factory=list)  # Future Finnhub market news
    earnings_calendar: List[Dict[str, Any]] = field(default_factory=list)
    ipo_calendar: List[Dict[str, Any]] = field(default_factory=list)  # IPO calendar from MarketClient
    top_headlines: List[Headline] = field(default_factory=list)  # Database headlines from get_top_headlines_since_midnight()
    top_gainers: List[Dict[str, Any]] = field(default_factory=list)  # For pre/post market briefings
    top_losers: List[Dict[str, Any]] = field(default_factory=list)  # For pre/post market briefings
    stock_specific_news: Dict[str, List[Dict]] = field(default_factory=dict)  # symbol -> news articles for gainers/losers
    config: Dict[str, Any] = field(default_factory=dict)