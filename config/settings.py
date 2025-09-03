# config/settings.py
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'agents_platform'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'secure_agents_password')
}

# Only require SSL for remote connections
if DATABASE_CONFIG['host'] not in ['localhost', '127.0.0.1']:
    DATABASE_CONFIG['sslmode'] = 'require'

# Agent configuration
AGENT_NAME = "hedgefund_agent"
TWITTER_HANDLE = "@Dutch_Brat"

# Market Data Service
MARKET_DATA_SERVICE_URL = os.getenv('MARKET_DATA_SERVICE_URL', 'http://localhost:8001')

# Twitter Configuration
TWITTER_CONFIG = {
    'consumer_key': os.getenv('TWITTER_CONSUMER_KEY'),
    'consumer_secret': os.getenv('TWITTER_CONSUMER_SECRET'),
    'access_token': os.getenv('TWITTER_ACCESS_TOKEN'),
    'access_token_secret': os.getenv('TWITTER_ACCESS_TOKEN_SECRET')
}

# Validate Twitter config
if not all(TWITTER_CONFIG.values()):
    raise ValueError("Missing required Twitter API credentials in environment variables")

# Telegram Configuration
TELEGRAM_CONFIG = {
    'bot_token': os.getenv('TELEGRAM_BOT_TOKEN'),
    'chat_id': os.getenv('TELEGRAM_CHAT_ID')
}

# Validate Telegram config
if not TELEGRAM_CONFIG['bot_token']:
    print("Warning: TELEGRAM_BOT_TOKEN not set - notifications will be disabled")

# RSS Feeds (from original HedgeFund Agent config)
RSS_FEED_URLS = {
    # Reuters feeds
    "reuters": "https://feeds.reuters.com/reuters/businessNews",
    
    # CNBC feeds  
    "cnbc": "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    
    # Working feeds
    "marketwatch": "https://feeds.marketwatch.com/marketwatch/topstories/",
    "ft": "https://www.ft.com/?format=rss",
    "bloomberg-markets": "https://feeds.bloomberg.com/markets/news.rss",
    "bloomberg-poli": "https://feeds.bloomberg.com/politics/news.rss",
    "bloomberg-tech": "https://feeds.bloomberg.com/technology/news.rss",
    "bloomberg-wealth": "https://feeds.bloomberg.com/wealth/news.rss",
    "bloomberg-eco": "https://feeds.bloomberg.com/economics/news.rss",
    
    # PRNewswire
    "prnnews": "https://www.prnewswire.com/rss/news-releases/news-releases-list.rss",
    
    # Additional Financial News Sources
    "seeking-alpha": "https://seekingalpha.com/feed.xml",
    "wsj-markets": "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
    "investing-com": "https://www.investing.com/rss/news.rss",
    "benzinga-general": "https://www.benzinga.com/feed",
    "business-insider": "https://markets.businessinsider.com/rss/news",
    "tradingview-news": "https://www.tradingview.com/feed/",
    "yahoo_finance": "https://finance.yahoo.com/news/rssindex",
    "zerohedge": "https://cms.zerohedge.com/fullrss2.xml",
    "marketwatch": "https://www.marketwatch.com/rss/topstories",
}

# Azure OpenAI Configuration (from original HedgeFund Agent)
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_DEPLOYMENT_ID = os.getenv("AZURE_DEPLOYMENT_ID") 
AZURE_API_VERSION = os.getenv("AZURE_API_VERSION", "2024-02-15-preview")
AZURE_RESOURCE_NAME = os.getenv("AZURE_RESOURCE_NAME")

# Notion Configuration
NOTION_CONFIG = {
    'api_key': os.getenv('NOTION_API_KEY'),
    'hedgefund_tweet_db_id': os.getenv('HEDGEFUND_TWEET_DB_ID'),
    'briefings_db_id': os.getenv('NOTION_PDF_DATABASE_ID')
}

# Validate Notion config
if not NOTION_CONFIG['api_key']:
    print("Warning: NOTION_API_KEY not set - Notion publishing will be disabled")


# Validate Azure OpenAI config
if not all([AZURE_OPENAI_API_KEY, AZURE_DEPLOYMENT_ID, AZURE_RESOURCE_NAME]):
    raise ValueError("Missing required Azure OpenAI environment variables")

# Feature toggles
PUBLISH_TWEETS = os.getenv('PUBLISH_TWEETS', 'False').lower() in ('true', '1', 't')