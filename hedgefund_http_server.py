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
    
    def do_GET(self):
        if self.path == '/hedgefund-news-data':
            try:
                # Get headlines from database
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
                    # Return empty but valid response
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
                    
            except Exception as e:
                logger.error(f"❌ Error serving hedge fund news: {e}")
                self._send_error_response(500, "Database error")
                
        elif self.path == '/health':
            try:
                # Enhanced health check with service status
                if self.db_service:
                    headlines_count = self._get_headlines_count()
                    
                    # Get rotation info
                    headlines_30min = len(self._get_headlines_by_timeframe(30, min_score=7, limit=6))
                    headlines_1hr = len(self._get_headlines_by_timeframe(60, min_score=6, limit=10))
                    current_rotation = self._get_current_rotation_index(max(headlines_30min, headlines_1hr, 1))
                    
                    health_response = {
                        "status": "healthy",
                        "service": "hedgefund-news",
                        "services": {
                            "database": "connected" if self.db_service else "unavailable",
                            "gpt": "available" if self.gpt_service else "fallback_mode",
                            "comment_generation": "institutional_gpt" if self.gpt_service else "static_fallback"
                        },
                        "total_headlines": headlines_count,
                        "rotation_info": {
                            "headlines_30min": headlines_30min,
                            "headlines_1hr": headlines_1hr,
                            "current_rotation_index": current_rotation,
                            "rotation_interval": "5 minutes",
                            "strategy": "30min priority (score≥7), 1hr fallback (score≥6), 5min rotation"
                        },
                        "timestamp": datetime.now().isoformat()
                    }
                    status_code = 200
                else:
                    health_response = {
                        "status": "degraded", 
                        "service": "hedgefund-news",
                        "services": {
                            "database": "unavailable",
                            "gpt": "unavailable"
                        },
                        "error": "Core services not available",
                        "timestamp": datetime.now().isoformat()
                    }
                    status_code = 503
                
                self.send_response(status_code)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps(health_response).encode('utf-8'))
                
                logger.info(f"✅ Health check: {health_response['status']}")
                
            except Exception as e:
                logger.error(f"❌ Health check failed: {e}")
                self._send_error_response(503, "Health check failed")
        else:
            self._send_error_response(404, "Endpoint not found")
    
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
    """Start the HTTP server for hedge fund news with GPT-powered comments"""
    try:
        server_address = ('0.0.0.0', port)
        httpd = HTTPServer(server_address, HedgeFundNewsHandler)
        
        logger.info(f"🌐 Starting HTD Research news server on all interfaces, port {port}")
        logger.info(f"   📡 News endpoint: http://0.0.0.0:{port}/hedgefund-news-data")
        logger.info(f"   ❤️ Health check: http://0.0.0.0:{port}/health")
        logger.info(f"   💾 Data source: PostgreSQL database")
        logger.info(f"   🤖 Comments: GPT-powered institutional analysis")
        
        print(f"🌐 HTD Research news server running on all interfaces, port {port}")
        print(f"   📡 Local: http://localhost:{port}/hedgefund-news-data")
        print(f"   🌍 External: http://74.241.128.114:{port}/hedgefund-news-data")
        print(f"   ❤️ Health: http://localhost:{port}/health")
        print(f"   🤖 GPT Comments: HTD Research institutional analysis")
        
        httpd.serve_forever()
        
    except Exception as e:
        logger.error(f"❌ Failed to start HTTP server: {e}")
        raise

if __name__ == "__main__":
    start_hedgefund_news_server()