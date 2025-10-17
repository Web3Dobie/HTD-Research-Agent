# hedgefund_http_server.py - Updated with GPTService integration
"""
HTTP server to serve hedge fund news data from PostgreSQL to the DutchBrat website
Now using GPTService for institutional HTD Research commentary
"""

import json
import logging
import os
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,  # Changed from INFO to DEBUG
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Try to import your services
try:
    from services.database_service import DatabaseService
    from services.gpt_service import GPTService
    from config.settings import DATABASE_CONFIG
    
    DB_SERVICE_AVAILABLE = True
    GPT_SERVICE_AVAILABLE = True
    logger.info("✅ Database and GPT services imported successfully")
    
except ImportError as e:
    logger.error(f"❌ Service import failed: {e}")
    DB_SERVICE_AVAILABLE = False
    GPT_SERVICE_AVAILABLE = False
    
    # Fallback database config
    DATABASE_CONFIG = {
        'host': 'localhost',
        'port': 5432,
        'database': 'agents_platform',
        'user': os.getenv('DB_USER', 'admin'),
        'password': os.getenv('DB_PASSWORD', 'secure_agents_password')
    }

class HedgeFundNewsHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        # Initialize services
        self.db_service = None
        self.gpt_service = None
        
        if DB_SERVICE_AVAILABLE:
            try:
                self.db_service = DatabaseService(DATABASE_CONFIG)
                logger.debug("✅ Database service initialized")
            except Exception as e:
                logger.error(f"❌ Database service initialization failed: {e}")
                self.db_service = None
        
        if GPT_SERVICE_AVAILABLE:
            try:
                self.gpt_service = GPTService()
                logger.debug("✅ GPT service initialized")
            except Exception as e:
                logger.error(f"❌ GPT service initialization failed: {e}")
                self.gpt_service = None
        
        super().__init__(*args, **kwargs)
    
    # hedgefund_http_server.py - Add these methods to HedgeFundNewsHandler class

    def do_GET(self):
        """Enhanced GET handler with new briefing endpoints"""
        
        if self.path == '/hedgefund-news-data':
            # Existing news headlines endpoint (unchanged)
            try:
                headlines = self._get_headlines_from_db()
                
                if headlines:
                    response_data = {
                        "success": True,
                        "data": headlines,
                        "lastUpdated": datetime.now().isoformat(),
                        "categories": ["macro", "equity", "political"],
                        "commentGeneration": "gpt_powered" if self.gpt_service else "fallback"
                    }
                    
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.end_headers()
                    self.wfile.write(json.dumps(response_data).encode('utf-8'))
                    
                    logger.info(f"✅ Served {len(headlines)} headlines with {'GPT' if self.gpt_service else 'fallback'} comments")
                else:
                    self._send_empty_headlines_response()
                    
            except Exception as e:
                logger.error(f"❌ Error serving hedge fund news: {e}")
                self._send_error_response(500, "Database error")
        
        elif self.path == '/latest-briefing':
            # NEW: Latest briefing endpoint for LatestBriefingCard
            try:
                briefing_data = self._get_latest_briefing_enhanced()
                
                if briefing_data:
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.end_headers()
                    self.wfile.write(json.dumps(briefing_data).encode('utf-8'))
                    
                    logger.info(f"✅ Served latest briefing: {briefing_data.get('title', 'Unknown')}")
                else:
                    self._send_empty_briefing_response()
                    
            except Exception as e:
                logger.error(f"❌ Error serving latest briefing: {e}")
                self._send_error_response(500, "Briefing data error")
        
        elif self.path == '/briefing-summary':
            # NEW: Compact briefing summary for widget displays
            try:
                summary_data = self._get_briefing_summary()
                
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps(summary_data).encode('utf-8'))
                
                logger.info("✅ Served briefing summary")
                
            except Exception as e:
                logger.error(f"❌ Error serving briefing summary: {e}")
                self._send_error_response(500, "Summary generation error")
                
        elif self.path == '/health':
            # Existing health check (unchanged)
            try:
                if self.db_service:
                    headlines_count = self._get_headlines_count()
                    
                    health_response = {
                        "status": "healthy",
                        "service": "hedgefund-news",
                        "services": {
                            "database": "connected" if self.db_service else "unavailable",
                            "gpt": "available" if self.gpt_service else "fallback_mode",
                            "comment_generation": "institutional_gpt" if self.gpt_service else "static_fallback"
                        },
                        "total_headlines": headlines_count,
                        "endpoints": {
                            "news": "/hedgefund-news-data",
                            "latest_briefing": "/latest-briefing",
                            "briefing_summary": "/briefing-summary",
                            "health": "/health"
                        },
                        "timestamp": datetime.now().isoformat()
                    }
                    
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.end_headers()
                    self.wfile.write(json.dumps(health_response).encode('utf-8'))
                    
                    logger.info(f"✅ Health check: {health_response['status']}")
                else:
                    self._send_error_response(503, "Database unavailable")
                    
            except Exception as e:
                logger.error(f"❌ Health check failed: {e}")
                self._send_error_response(503, "Health check failed")
        else:
            self._send_error_response(404, "Endpoint not found")

    def _get_latest_briefing_enhanced(self):
        """Get the latest briefing with enhanced data for LatestBriefingCard - HANDLES BOTH PROPERTY NAMES"""
        if not self.db_service:
            logger.warning("Database service not available for latest briefing")
            return None
        
        try:
            connection = self.db_service.get_connection()
            if not connection:
                logger.warning("No database connection available for latest briefing")
                return None
            
            # Get the latest briefing with enhanced JSON content
            query = """
            SELECT 
                id,
                briefing_type,
                title,
                website_url,
                tweet_url,
                json_content,
                created_at
            FROM hedgefund_agent.briefings 
            WHERE json_content IS NOT NULL 
            ORDER BY created_at DESC 
            LIMIT 1
            """
            
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                
                if result:
                    briefing_id, briefing_type, title, website_url, tweet_url, json_content, created_at = result
                    logger.info(f"Found latest briefing: ID={briefing_id}, Type={briefing_type}, Title={title}")
                    
                    # Parse the enhanced JSON content - CHECK BOTH PROPERTY NAMES
                    if json_content and isinstance(json_content, dict):
                        # Try new property name first, then fallback to old name
                        enhanced_summary = json_content.get('enhanced_summary', {})
                        if not enhanced_summary:
                            enhanced_summary = json_content.get('enhancedSummary', {})
                            logger.info("Using legacy 'enhancedSummary' property name")
                        else:
                            logger.info("Using new 'enhanced_summary' property name")
                        
                        logger.debug(f"Enhanced summary keys: {list(enhanced_summary.keys())}")
                        
                        # Build response with enhanced data structure
                        response_data = {
                            "success": True,
                            "briefing": {
                                "id": briefing_id,
                                "type": briefing_type,
                                "title": title,
                                "created_at": created_at.isoformat() if created_at else None,
                                "urls": {
                                    "website": website_url,
                                    "twitter": tweet_url
                                }
                            },
                            "sentiment": enhanced_summary.get('sentiment_visual', {}),
                            "momentum": enhanced_summary.get('momentum_indicators', {}),
                            "sectors": enhanced_summary.get('sector_highlights', []),
                            "insights": enhanced_summary.get('key_insights', []),
                            "summary": enhanced_summary.get('market_summary_short', ''),
                            "confidence": enhanced_summary.get('confidence_level', 'moderate'),
                            "health_score": enhanced_summary.get('market_health_score', 50),
                            "lastUpdated": datetime.now().isoformat()
                        }
                        
                        logger.info(f"Enhanced briefing data prepared: {len(enhanced_summary)} summary fields")
                        return response_data
                    else:
                        logger.warning("Latest briefing found but no enhanced JSON content")
                        return self._get_fallback_briefing_data(briefing_id, title, created_at)
                else:
                    logger.warning("No briefings with JSON content found in database")
                    return None
                    
        except Exception as e:
            logger.error(f"Failed to get latest briefing: {e}", exc_info=True)
            return None


    def _get_briefing_summary(self):
        """Get a compact briefing summary for widget displays - HANDLES BOTH PROPERTY NAMES"""
        if not self.db_service:
            return self._get_fallback_summary()
        
        try:
            connection = self.db_service.get_connection()
            if not connection:
                return self._get_fallback_summary()
            
            # FIXED: Separate queries for count and latest briefing
            with connection.cursor() as cursor:
                # Get briefings count for today
                cursor.execute("""
                    SELECT COUNT(*) as total_briefings
                    FROM hedgefund_agent.briefings 
                    WHERE created_at >= NOW() - INTERVAL '24 hours'
                """)
                count_result = cursor.fetchone()
                total_briefings = count_result[0] if count_result else 0
                
                # Get latest briefing with JSON content
                cursor.execute("""
                    SELECT created_at, json_content
                    FROM hedgefund_agent.briefings 
                    WHERE json_content IS NOT NULL 
                    ORDER BY created_at DESC 
                    LIMIT 1
                """)
                latest_result = cursor.fetchone()
                
                if latest_result:
                    latest_time, json_content = latest_result
                    
                    sentiment_info = {"sentiment": "mixed", "emoji": "⚖️", "color": "#f59e0b"}
                    if json_content and isinstance(json_content, dict):
                        # CHECK BOTH PROPERTY NAMES - new first, then legacy
                        enhanced_summary = json_content.get('enhanced_summary', {})
                        if not enhanced_summary:
                            enhanced_summary = json_content.get('enhancedSummary', {})
                            logger.info("Summary endpoint using legacy 'enhancedSummary' property")
                        else:
                            logger.info("Summary endpoint using new 'enhanced_summary' property")
                        
                        sentiment_visual = enhanced_summary.get('sentiment_visual', {})
                        if sentiment_visual:
                            sentiment_info = {
                                "sentiment": sentiment_visual.get('sentiment', 'mixed'),
                                "emoji": sentiment_visual.get('emoji', '⚖️'),
                                "color": sentiment_visual.get('color', '#f59e0b'),
                                "description": sentiment_visual.get('description', 'Market sentiment mixed')
                            }
                            logger.debug(f"Found sentiment data: {sentiment_info['sentiment']}")
                        else:
                            logger.warning("No sentiment_visual data found in enhanced summary")
                    
                    return {
                        "success": True,
                        "briefings_today": total_briefings,
                        "latest_briefing_time": latest_time.isoformat() if latest_time else None,
                        "market_sentiment": sentiment_info,
                        "status": "active",
                        "lastUpdated": datetime.now().isoformat()
                    }
                else:
                    # No briefings with JSON content found
                    return {
                        "success": True,
                        "briefings_today": total_briefings,
                        "latest_briefing_time": None,
                        "market_sentiment": {
                            "sentiment": "unknown",
                            "emoji": "🔄",
                            "color": "#6b7280",
                            "description": "Market analysis in progress"
                        },
                        "status": "processing",
                        "lastUpdated": datetime.now().isoformat()
                    }
                    
        except Exception as e:
            logger.error(f"Failed to get briefing summary: {e}")
            return self._get_fallback_summary()

    def _get_fallback_briefing_data(self, briefing_id, title, created_at):
        """Fallback briefing data when enhanced JSON is not available"""
        return {
            "success": True,
            "briefing": {
                "id": briefing_id,
                "title": title,
                "created_at": created_at.isoformat() if created_at else None,
                "urls": {"website": None, "twitter": None}
            },
            "sentiment": {
                "sentiment": "mixed",
                "emoji": "⚖️",
                "color": "#f59e0b",
                "description": "Market analysis in progress"
            },
            "momentum": {
                "momentum_direction": "neutral",
                "bullish_percentage": 50,
                "bearish_percentage": 50
            },
            "sectors": [],
            "insights": ["Market analysis in progress"],
            "summary": "Comprehensive market analysis available soon.",
            "confidence": "moderate",
            "health_score": 50,
            "lastUpdated": datetime.now().isoformat(),
            "note": "Enhanced data processing in progress"
        }

    def _get_fallback_summary(self):
        """Fallback summary when database is unavailable"""
        return {
            "success": False,
            "briefings_today": 0,
            "latest_briefing_time": None,
            "market_sentiment": {
                "sentiment": "unknown",
                "emoji": "🔄",
                "color": "#6b7280",
                "description": "Market data loading"
            },
            "status": "loading",
            "lastUpdated": datetime.now().isoformat(),
            "error": "Data temporarily unavailable"
        }

    def _send_empty_briefing_response(self):
        """Send empty briefing response"""
        empty_response = {
            "success": True,
            "briefing": None,
            "message": "No recent briefings available",
            "lastUpdated": datetime.now().isoformat()
        }
        
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(empty_response).encode('utf-8'))
        
        logger.warning("⚠️ No briefings found, returned empty response")

    def _send_empty_headlines_response(self):
        """Send empty headlines response (existing method enhanced)"""
        empty_response = {
            "success": True,
            "data": [],
            "message": "HTD Research is analyzing market conditions",
            "lastUpdated": datetime.now().isoformat(),
            "categories": ["macro", "equity", "political"]
        }
        
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(empty_response).encode('utf-8'))
        
        logger.warning("⚠️ No headlines found, returned empty response")
    
    def _get_headlines_from_db(self):
        """Get headlines with smart rotation and GPT-powered institutional comments"""
        if not self.db_service:
            logger.warning("Database service not available")
            return []
        
        try:
            # Your existing headline selection logic (unchanged)
            headlines_30min = self._get_headlines_by_timeframe(30, min_score=7, limit=6)
            
            if len(headlines_30min) >= 4:
                headlines_data = headlines_30min[:6]
                logger.info(f"✅ Using {len(headlines_data)} headlines from last 30 minutes")
            else:
                headlines_1hr = self._get_headlines_by_timeframe(60, min_score=6, limit=10)
                
                if len(headlines_1hr) >= 4:
                    headlines_data = headlines_1hr[:6]
                    logger.info(f"⏰ Expanded to 1 hour: using {len(headlines_data)} headlines")
                else:
                    headlines_data = headlines_1hr
                    logger.warning(f"⚠️ Limited data: only {len(headlines_data)} headlines from last hour")
            
            # Return all headlines for frontend rotation (no server-side rotation)
            if len(headlines_data) > 1:
                current_rotation_index = self._get_current_rotation_index(len(headlines_data))
                logger.info(f"🔄 Found {len(headlines_data)} headlines - returning all to frontend (would be index {current_rotation_index + 1})")
            else:
                logger.info(f"🔄 Found {len(headlines_data)} headline - returning to frontend")
            
            rotated_headlines = headlines_data  # Return ALL headlines
            
            # Format for website API with enhanced GPT comments
            formatted_headlines = []
            for headline_data in rotated_headlines:
                # Generate institutional comment using GPTService
                dutchbrat_comment = self._generate_institutional_comment(
                    headline_data.get('headline', ''),
                    headline_data.get('category', 'macro')
                )
                
                formatted_headline = {
                    "headline": headline_data.get('headline', ''),
                    "url": headline_data.get('url', ''),
                    "score": headline_data.get('score', 0),
                    "timestamp": headline_data.get('created_at', datetime.now()).isoformat() if isinstance(headline_data.get('created_at'), datetime) else datetime.now().isoformat(),
                    "category": headline_data.get('category', 'general'),
                    "source": self._format_source_name(headline_data.get('source', '')),
                    "dutchbratComment": dutchbrat_comment
                }
                formatted_headlines.append(formatted_headline)
            
            return formatted_headlines
            
        except Exception as e:
            logger.error(f"Failed to get headlines: {e}")
            return []
    
    def _generate_institutional_comment(self, headline: str, category: str) -> str:
        """Generate institutional comment using GPTService or fallback"""
        if self.gpt_service and headline:
            try:
                # Use GPTService for institutional commentary
                comment = self.gpt_service.generate_institutional_comment(headline, category)
                logger.debug(f"✅ GPT comment generated for {category} headline")
                return comment
            except Exception as e:
                logger.error(f"❌ GPT comment generation failed: {e}")
                return self._get_static_fallback_comment(category)
        else:
            # Fallback to static comments
            return self._get_static_fallback_comment(category)
    
    def _get_static_fallback_comment(self, category: str) -> str:
        """Static fallback comments when GPT is unavailable"""
        fallbacks = {
            "macro": "Macro policy implications developing. Institutional positioning warranted. — HTD Research 📊",
            "equity": "Sector dynamics shift creates alpha opportunity. Risk assessment ongoing. — HTD Research 📊", 
            "political": "Policy uncertainty creates tactical positioning window. Monitoring regulatory impact. — HTD Research 📊",
            "general": "Market structure development warrants institutional attention. — HTD Research 📊"
        }
        
        return fallbacks.get(category, fallbacks["general"])
    
    # Keep all your existing methods unchanged
    def _get_headlines_by_timeframe(self, minutes: int, min_score: int = 7, limit: int = 6):
        """Get headlines from specified timeframe (unchanged)"""
        try:
            if hasattr(self.db_service, 'get_top_headlines_for_website'):
                return self.db_service.get_top_headlines_for_website(
                    limit=limit, 
                    hours=minutes/60,
                    min_score=min_score
                )
            else:
                # Fallback to direct SQL
                conn = self.db_service.get_connection()
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT headline, summary, score, category, source, url, created_at
                    FROM hedgefund_agent.headlines 
                    WHERE created_at >= NOW() - INTERVAL %s
                    AND score >= %s
                    ORDER BY score DESC, created_at DESC
                    LIMIT %s
                """, (f'{minutes} minutes', min_score, limit))
                
                rows = cursor.fetchall()
                cursor.close()
                
                headlines = []
                for row in rows:
                    headline_data = {
                        'headline': row[0],
                        'summary': row[1], 
                        'score': row[2],
                        'category': row[3],
                        'source': row[4],
                        'url': row[5],
                        'created_at': row[6]
                    }
                    headlines.append(headline_data)
                
                return headlines
                
        except Exception as e:
            logger.error(f"Failed to get headlines for {minutes} minutes: {e}")
            return []
    
    def _get_current_rotation_index(self, total_headlines: int) -> int:
        """Calculate which headline to show based on 5-minute rotation (unchanged)"""
        if total_headlines <= 1:
            return 0
        
        now = datetime.now()
        minutes_since_hour = now.minute
        rotation_cycle = minutes_since_hour // 5
        current_index = rotation_cycle % total_headlines
        
        logger.debug(f"🕐 Time: {now.strftime('%H:%M')}, Minute: {minutes_since_hour}, Cycle: {rotation_cycle}, Index: {current_index}")
        return current_index
    
    def _apply_rotation_logic(self, headlines_data):
        """Apply 5-minute rotation logic to headlines (unchanged)"""
        if len(headlines_data) <= 1:
            return headlines_data
        
        current_index = self._get_current_rotation_index(len(headlines_data))
        return [headlines_data[current_index]]
    
    def _get_headlines_count(self):
        """Get total headlines count for health check (unchanged)"""
        if not self.db_service:
            return 0
            
        try:
            if hasattr(self.db_service, 'get_headlines_count'):
                return self.db_service.get_headlines_count()
            else:
                conn = self.db_service.get_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM hedgefund_agent.headlines")
                count = cursor.fetchone()[0]
                cursor.close()
                return count
        except Exception as e:
            logger.error(f"Failed to get headlines count: {e}")
            return 0
    
    def _format_source_name(self, raw_source):
        """Format source name for display (unchanged)"""
        if not raw_source:
            return "financial_news"
        
        source_mapping = {
            'reuters': 'Reuters',
            'bloomberg': 'Bloomberg', 
            'cnbc': 'CNBC',
            'marketwatch': 'MarketWatch',
            'seeking-alpha': 'Seeking Alpha',
            'tradingview-news': 'TradingView',
            'ft': 'Financial Times'
        }
        
        clean_source = raw_source.lower().replace('-', '_')
        return source_mapping.get(clean_source, raw_source.title())
    
    def _send_error_response(self, status_code, message):
        """Send error response (unchanged)"""
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        
        error_response = {
            "success": False,
            "error": message,
            "timestamp": datetime.now().isoformat()
        }
        self.wfile.write(json.dumps(error_response).encode('utf-8'))
    
    def log_message(self, format, *args):
        # Suppress default HTTP server logs
        pass

def start_hedgefund_news_server(port=3002):
    """Start the HTTP server for hedge fund news with GPT-powered comments - ENHANCED LOGGING"""
    try:
        server_address = ('0.0.0.0', port)
        httpd = HTTPServer(server_address, HedgeFundNewsHandler)
        
        logger.info(f"🌐 Starting HTD Research news server on all interfaces, port {port}")
        logger.info(f"   📡 News endpoint: http://0.0.0.0:{port}/hedgefund-news-data")
        logger.info(f"   📊 Latest briefing: http://0.0.0.0:{port}/latest-briefing")
        logger.info(f"   📈 Briefing summary: http://0.0.0.0:{port}/briefing-summary")
        logger.info(f"   ❤️ Health check: http://0.0.0.0:{port}/health")
        logger.info(f"   💾 Data source: PostgreSQL database")
        logger.info(f"   🤖 Comments: GPT-powered institutional analysis")
        
        print(f"🌐 HTD Research news server running on all interfaces, port {port}")
        print(f"   📡 News: http://localhost:{port}/hedgefund-news-data")
        print(f"   📊 Latest Briefing: http://localhost:{port}/latest-briefing")
        print(f"   📈 Summary: http://localhost:{port}/briefing-summary")
        print(f"   🌍 External: http://74.241.128.114:{port}/latest-briefing")
        print(f"   ❤️ Health: http://localhost:{port}/health")
        print(f"   🤖 Enhanced Briefing Data: Sentiment, Momentum, Sectors")
        
        httpd.serve_forever()
        
    except Exception as e:
        logger.error(f"❌ Failed to start HTTP server: {e}")
        raise

if __name__ == "__main__":
    start_hedgefund_news_server()