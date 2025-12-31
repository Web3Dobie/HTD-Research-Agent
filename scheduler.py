# hedgefund_agent/scheduler.py
"""
HedgeFund Agent Production Scheduler - TIMEZONE-AWARE VERSION
Fixed timezone handling using Python's zoneinfo for accurate market timing
"""

import schedule
import time
import logging
import asyncio
import threading
from datetime import datetime, timezone, time as dt_time
from typing import Optional, Dict, Tuple
from zoneinfo import ZoneInfo

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import our services and models
from core.content_engine import ContentEngine
from core.models import ContentRequest, ContentType, ContentCategory
from services.telegram_notifier import TelegramNotifier, NotificationLevel

# Import headline pipeline from services
try:
    from services.headline_pipeline import HeadlinePipeline
    from services.database_service import DatabaseService
    from config.settings import DATABASE_CONFIG
    HEADLINE_PIPELINE_AVAILABLE = True
    logger.info("✅ Modern HeadlinePipeline imported successfully from services")
except ImportError as e:
    logger.error(f"❌ Failed to import HeadlinePipeline from services: {e}")
    HEADLINE_PIPELINE_AVAILABLE = False

# Import HTTP server
try:
    from hedgefund_http_server import start_hedgefund_news_server
    HTTP_SERVER_AVAILABLE = True
    logger.info("✅ HTTP News Server imported successfully")
except ImportError as e:
    logger.error(f"❌ Failed to import HTTP News Server: {e}")
    HTTP_SERVER_AVAILABLE = False

class HedgeFundScheduler:
    """Production scheduler for HedgeFund Agent with proper timezone awareness"""
    
    def __init__(self):
        self.content_engine = ContentEngine()
        self.telegram = TelegramNotifier()
        self.deep_dive_days = ["Monday", "Wednesday", "Friday"]
        
        # HTTP Server management
        self.http_server_thread = None
        self.http_server_port = 3002
        self.http_server_status = "stopped"
        
        # Initialize headline pipeline (modern services version only)
        self.headline_pipeline = None
        if HEADLINE_PIPELINE_AVAILABLE:
            try:
                db_service = DatabaseService(DATABASE_CONFIG)
                self.headline_pipeline = HeadlinePipeline(db_service)
                logger.info("✅ Modern HeadlinePipeline initialized with database integration")
            except Exception as e:
                logger.error(f"❌ Failed to initialize HeadlinePipeline: {e}")
                self.headline_pipeline = None
        
        # FIXED MARKET-LOCAL BRIEFING TIMES
        self.london_briefings = {
            "morning_briefing": dt_time(7, 30),      # 07:30 London time
            "eu_close_briefing": dt_time(16, 25)     # 16:25 London time
        }
        
        self.new_york_briefings = {
            "pre_market_briefing": dt_time(9, 7),    # 09:07 Eastern (23 min before 9:30 open)
            "us_close_briefing": dt_time(15, 44)     # 15:44 Eastern (16 min before 16:00 close)
        }
        
        # Commentary times in London time
        self.london_commentary_times = [
            dt_time(7, 0), dt_time(8, 0), dt_time(10, 0), dt_time(11, 0),
            dt_time(15, 30), dt_time(18, 0), dt_time(20, 0), dt_time(22, 0), dt_time(23, 0)
        ]
        
        # Heartbeat configuration
        self.last_heartbeat = time.time()
        self.heartbeat_interval = 3600  # Send heartbeat every hour (3600 seconds)
        self.startup_time = datetime.now(timezone.utc)
        
        # Health metrics
        self.jobs_completed_today = 0
        self.jobs_failed_today = 0
        self.last_job_time = None
        self.last_job_name = None
        
        logger.info("🗓️ HedgeFund Scheduler initialized with timezone-aware scheduling")
        logger.info(f"💓 Heartbeat interval: {self.heartbeat_interval/60:.0f} minutes")
        logger.info(f"🌐 HTTP Server will run on port {self.http_server_port}")
    
    def get_market_times(self) -> Dict[str, datetime]:
        """Get current local times for each market"""
        london_time = datetime.now(ZoneInfo("Europe/London"))
        new_york_time = datetime.now(ZoneInfo("America/New_York"))
        utc_time = datetime.now(timezone.utc)
        
        return {
            "london": london_time,
            "new_york": new_york_time,
            "utc": utc_time
        }
    
    def get_timezone_info(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        """Get comprehensive timezone information for logging"""
        market_times = self.get_market_times()
        
        # Current times
        current_times = {
            "London": market_times["london"].strftime('%Y-%m-%d %H:%M:%S %Z'),
            "New York": market_times["new_york"].strftime('%Y-%m-%d %H:%M:%S %Z'),
            "UTC": market_times["utc"].strftime('%Y-%m-%d %H:%M:%S UTC')
        }
        
        # Timezone abbreviations
        timezone_info = {
            "London": market_times["london"].strftime('%Z'),  # GMT or BST
            "New York": market_times["new_york"].strftime('%Z'),  # EST or EDT
            "UTC Offset London": market_times["london"].strftime('%z'),
            "UTC Offset New York": market_times["new_york"].strftime('%z')
        }
        
        return current_times, timezone_info
    
    def calculate_utc_briefing_times(self) -> Dict[str, str]:
        """Convert market-local briefing times to UTC for scheduling"""
        market_times = self.get_market_times()
        utc_times = {}
        
        # Convert London briefings to UTC
        for briefing_name, local_time in self.london_briefings.items():
            london_dt = market_times["london"].replace(
                hour=local_time.hour, 
                minute=local_time.minute,
                second=0,
                microsecond=0
            )
            utc_dt = london_dt.astimezone(timezone.utc)
            utc_times[briefing_name] = utc_dt.strftime("%H:%M")
        
        # Convert New York briefings to UTC
        for briefing_name, local_time in self.new_york_briefings.items():
            ny_dt = market_times["new_york"].replace(
                hour=local_time.hour,
                minute=local_time.minute, 
                second=0,
                microsecond=0
            )
            utc_dt = ny_dt.astimezone(timezone.utc)
            utc_times[briefing_name] = utc_dt.strftime("%H:%M")
        
        return utc_times
    
    def calculate_utc_commentary_times(self) -> list[str]:
        """Convert London commentary times to UTC for scheduling"""
        market_times = self.get_market_times()
        utc_times = []
        
        for local_time in self.london_commentary_times:
            london_dt = market_times["london"].replace(
                hour=local_time.hour,
                minute=local_time.minute,
                second=0,
                microsecond=0
            )
            utc_dt = london_dt.astimezone(timezone.utc)
            utc_times.append(utc_dt.strftime("%H:%M"))
        
        return utc_times
    
    def start_http_server(self):
        """Start HTTP server in background thread"""
        if not HTTP_SERVER_AVAILABLE:
            logger.error("❌ HTTP server not available - import failed")
            asyncio.run(self.telegram.send_message(
                "🌐 **HTTP Server Startup Failed**\n❌ Import error - check hedgefund_http_server.py",
                NotificationLevel.ERROR
            ))
            return False
        
        try:
            logger.info(f"🌐 Starting HTTP server on port {self.http_server_port}...")
            
            # Start server in daemon thread
            self.http_server_thread = threading.Thread(
                target=self._run_http_server,
                daemon=True,
                name="HedgeFundHTTPServer"
            )
            self.http_server_thread.start()
            
            # Wait a moment for server to start
            time.sleep(2)
            
            # Test if server is responding
            if self._test_http_server():
                self.http_server_status = "running"
                logger.info("✅ HTTP server started successfully")
                
                asyncio.run(self.telegram.send_message(
                    f"🌐 **HTTP News Server Started**\n✅ Port: {self.http_server_port}\n📡 Endpoint: /hedgefund-news-data\n💾 Source: PostgreSQL database",
                    NotificationLevel.SUCCESS
                ))
                return True
            else:
                self.http_server_status = "failed"
                logger.error("❌ HTTP server failed to respond")
                
                asyncio.run(self.telegram.send_message(
                    f"🌐 **HTTP Server Health Check Failed**\n❌ Server not responding on port {self.http_server_port}",
                    NotificationLevel.ERROR
                ))
                return False
                
        except Exception as e:
            self.http_server_status = "failed"
            logger.error(f"❌ HTTP server startup failed: {e}")
            
            asyncio.run(self.telegram.send_message(
                f"🌐 **HTTP Server Startup Failed**\n❌ {str(e)}",
                NotificationLevel.ERROR
            ))
            return False
    
    def _run_http_server(self):
        """Run HTTP server - called in background thread"""
        try:
            start_hedgefund_news_server(port=self.http_server_port)
        except Exception as e:
            logger.error(f"❌ HTTP server thread error: {e}")
            self.http_server_status = "crashed"
    
    def _test_http_server(self) -> bool:
        """Test if HTTP server is responding"""
        try:
            import requests
            response = requests.get(f"http://localhost:{self.http_server_port}/health", timeout=5)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"❌ HTTP server health check failed: {e}")
            return False
    
    def check_http_server_health(self) -> dict:
        """Check HTTP server health status"""
        try:
            if not self.http_server_thread or not self.http_server_thread.is_alive():
                return {
                    "healthy": False,
                    "status": "not_running",
                    "message": "HTTP server thread not running"
                }
            
            if self._test_http_server():
                return {
                    "healthy": True,
                    "status": "running",
                    "message": f"HTTP server healthy on port {self.http_server_port}"
                }
            else:
                return {
                    "healthy": False,
                    "status": "unresponsive", 
                    "message": "HTTP server thread running but not responding"
                }
                
        except Exception as e:
            logger.error(f"❌ HTTP server health check error: {e}")
            return {"healthy": False, "status": "error", "message": str(e)}
    
    def setup_schedule(self):
        """Setup the complete production schedule with timezone-aware times"""
        logger.info("📋 Setting up timezone-aware production schedule...")
        
        # Clear any existing jobs
        schedule.clear()
        
        # Calculate current UTC times based on market timezones
        utc_briefing_times = self.calculate_utc_briefing_times()
        utc_commentary_times = self.calculate_utc_commentary_times()
        
        # === MARKET BRIEFINGS ===
        briefing_mapping = {
            "opening": "morning_briefing",
            "midday": "pre_market_briefing", 
            "afternoon": "eu_close_briefing",
            "close": "us_close_briefing"
        }
        
        weekday_briefings = {
            "monday": ["opening", "midday", "afternoon", "close"],
            "tuesday": ["opening", "midday", "afternoon", "close"], 
            "wednesday": ["opening", "midday", "afternoon", "close"],
            "thursday": ["opening", "midday", "afternoon", "close"],
            "friday": ["opening", "midday", "afternoon", "close"]
        }
        
        for day, briefings in weekday_briefings.items():
            for briefing_type in briefings:
                briefing_key = briefing_mapping[briefing_type]
                if briefing_key in utc_briefing_times:
                    time_str = utc_briefing_times[briefing_key]
                    job_name = f"briefing_{briefing_type}_{day}"
                    
                    getattr(schedule.every(), day).at(time_str).do(
                        self._safe_job_wrapper(job_name, self._run_briefing, briefing_type)
                    )
        
        # === COMMENTARY POSTS ===
        weekday_commentaries = ["monday", "tuesday", "wednesday", "thursday", "friday"]
        for day in weekday_commentaries:
            for i, time_str in enumerate(utc_commentary_times):
                job_name = f"commentary_{day}_{time_str.replace(':', '')}"
                
                getattr(schedule.every(), day).at(time_str).do(
                    self._safe_job_wrapper(job_name, self._run_commentary)
                )
        
        # === DEEP DIVE THREADS ===
        # Deep dives at 22:00 London time
        london_deep_dive = datetime.now(ZoneInfo("Europe/London")).replace(hour=22, minute=0, second=0, microsecond=0)
        utc_deep_dive = london_deep_dive.astimezone(timezone.utc).strftime("%H:%M")
        
        for day in self.deep_dive_days:
            job_name = f"deep_dive_{day.lower()}"
            getattr(schedule.every(), day.lower()).at(utc_deep_dive).do(
                self._safe_job_wrapper(job_name, self._run_deep_dive)
            )
        
        # === WEEKEND SCHEDULE ===
        # Note: Weekend briefings (crypto, technical, fundamental) are placeholders - not implemented yet
        # Only include weekend commentary and deep dives
        weekend_commentary_times = utc_commentary_times[:6]  # 6 commentary posts only
        
        for day in ["saturday", "sunday"]:
            # Weekend commentary (6 instead of 9)
            for i, time_str in enumerate(weekend_commentary_times):
                job_name = f"commentary_{day}_{time_str.replace(':', '')}"
                
                getattr(schedule.every(), day).at(time_str).do(
                    self._safe_job_wrapper(job_name, self._run_commentary)
                )
            
            # Weekend deep dive (both Saturday and Sunday)
            job_name = f"deep_dive_{day}"
            getattr(schedule.every(), day).at(utc_deep_dive).do(
                self._safe_job_wrapper(job_name, self._run_deep_dive)
            )
        
        # Daily maintenance at 23:50 UTC (recurring)
        schedule.every().day.at("23:50").do(
            self._safe_job_wrapper("daily_maintenance", self._daily_maintenance)
        )
        
        # Hourly heartbeat (every hour at :00) - recurring
        schedule.every().hour.at(":00").do(
            self._safe_job_wrapper("heartbeat", self._send_heartbeat)
        )
        
        # === HEADLINE PIPELINE ===
        # Fetch headlines every 30 minutes using modern pipeline
        if HEADLINE_PIPELINE_AVAILABLE and self.headline_pipeline:
            schedule.every().hour.at(":05").do(
                self._safe_job_wrapper("headlines_fetch_05", self._run_headline_pipeline)
            )
            schedule.every().hour.at(":35").do(
                self._safe_job_wrapper("headlines_fetch_35", self._run_headline_pipeline)
            )
        else:
            logger.warning("⚠️ Headline pipeline not available - skipping headline jobs")
        
        # === HTTP SERVER HEALTH CHECKS ===
        # HTTP Server health check every 30 minutes
        schedule.every().hour.at(":15").do(
            self._safe_job_wrapper("http_server_health_15", self._check_http_server_health_job)
        )
        schedule.every().hour.at(":45").do(
            self._safe_job_wrapper("http_server_health_45", self._check_http_server_health_job)
        )
        
        # === DAILY TIMEZONE RECALCULATION ===
        # Recalculate timezone conversions daily at 00:05 UTC to handle DST transitions
        schedule.every().day.at("00:05").do(
            self._safe_job_wrapper("timezone_recalculation", self._recalculate_schedule)
        )
        
        # Log schedule summary
        total_jobs = len(schedule.get_jobs())
        logger.info(f"📋 Schedule loaded: {total_jobs} total jobs")
        
        # Show next job
        next_job = schedule.next_run()
        if next_job:
            logger.info(f"⏰ Next job: {next_job}")
        
        # Log comprehensive timezone info
        current_times, timezone_info = self.get_timezone_info()
        logger.info("🕐 Market Timezone Information:")
        for location, time_str in current_times.items():
            logger.info(f"  {location}: {time_str}")
        
        logger.info("🌍 Timezone Details:")
        for key, value in timezone_info.items():
            logger.info(f"  {key}: {value}")
        
        # Log briefing schedule in local market times
        logger.info("📊 Briefing Schedule (Local Market Times):")
        logger.info(f"  🇬🇧 Morning Briefing: 07:30 London ({utc_briefing_times.get('morning_briefing')} UTC)")
        logger.info(f"  🇺🇸 Pre-Market Briefing: 09:07 Eastern ({utc_briefing_times.get('pre_market_briefing')} UTC)")
        logger.info(f"  🇪🇺 EU Close Briefing: 16:25 London ({utc_briefing_times.get('eu_close_briefing')} UTC)")
        logger.info(f"  🇺🇸 US Close Briefing: 15:44 Eastern ({utc_briefing_times.get('us_close_briefing')} UTC)")
    
    async def _recalculate_schedule(self):
        """Recalculate timezone conversions and update schedule if needed"""
        logger.info("🔄 Recalculating timezone-aware schedule...")
        
        # Get current timezone status for comparison
        current_times, timezone_info = self.get_timezone_info()
        
        # Log timezone status
        await self.telegram.send_message(
            f"🔄 **Daily Timezone Recalculation**\n\n"
            f"🇬🇧 London: {timezone_info['London']}\n"
            f"🇺🇸 New York: {timezone_info['New York']}\n\n"
            f"⏰ Schedule updated for current DST status",
            NotificationLevel.INFO
        )
        
        # Rebuild the entire schedule with new timezone calculations
        self.setup_schedule()
        
        logger.info("✅ Timezone recalculation completed")
    
    def _safe_job_wrapper(self, job_name: str, func, *args, **kwargs):
        """Safe wrapper for all scheduled jobs with proper error handling"""
        def wrapper():
            start_time = datetime.now()
            
            try:
                # Use Telegram notifier's send_message method with proper level
                asyncio.run(self.telegram.send_message(
                    f"Starting: `{job_name}`", 
                    NotificationLevel.START
                ))
                logger.info(f"🚀 Starting job: {job_name}")
                
                # Execute the job - handle both sync and async functions
                if asyncio.iscoroutinefunction(func):
                    result = asyncio.run(func(*args, **kwargs))
                else:
                    result = func(*args, **kwargs)
                
                # Calculate duration
                duration = datetime.now() - start_time
                duration_str = str(duration).split('.')[0]  # Remove microseconds
                
                # Update health metrics
                self.jobs_completed_today += 1
                self.last_job_time = datetime.now()
                self.last_job_name = job_name
                
                # Success notification
                asyncio.run(self.telegram.send_message(
                    f"Completed: `{job_name}` in {duration_str}",
                    NotificationLevel.SUCCESS
                ))
                logger.info(f"✅ Completed job: {job_name} in {duration_str}")
                
                return result
                
            except Exception as e:
                duration = datetime.now() - start_time
                duration_str = str(duration).split('.')[0]
                
                # Update failure metrics
                self.jobs_failed_today += 1
                
                error_msg = f"Job `{job_name}` failed after {duration_str}: {str(e)}"
                
                # Error notification using critical_error method
                asyncio.run(self.telegram.notify_critical_error(
                    f"Scheduler Job: {job_name}",
                    str(e),
                    "Check logs and restart if needed"
                ))
                
                logger.error(f"❌ {error_msg}")
                # Don't re-raise to keep scheduler running
        
        # Store job name as an attribute for analysis
        wrapper.__name__ = f"wrapper_{job_name}"
        wrapper._job_name = job_name
        
        return wrapper
    
    async def _run_briefing(self, briefing_type: str):
        """Generate and publish market briefing, letting the wrapper handle exceptions."""
        
        # Explicit mapping of scheduler names to briefing pipeline keys
        briefing_mapping = {
            "opening": "morning_briefing",
            "midday": "pre_market_briefing",
            "afternoon": "eu_close_briefing",
            "close": "us_close_briefing"
            # Note: Weekend briefings (crypto, technical, fundamental) not implemented yet
        }
        
        if briefing_type not in briefing_mapping:
            logger.info(f"Briefing type '{briefing_type}' not implemented yet")
            return
        
        briefing_key = briefing_mapping[briefing_type]
        await self.content_engine.run_briefing_pipeline(briefing_key)
    
    async def _run_commentary(self):
        """Generate and publish market commentary, allowing the wrapper to handle exceptions."""
        from core.content_engine import publish_commentary_now
        result = await publish_commentary_now()
        
        if result.get('success'):
            logger.info("💬 Commentary published successfully")
            twitter_url = result.get('publishing', {}).get('twitter', {}).get('url')
            return {"success": True, "urls": [twitter_url] if twitter_url else []}
        else:
            # If the job didn't fail but returned a failure status, raise an exception
            # so the wrapper knows it failed.
            raise Exception(f"Commentary job returned failure status: {result.get('error')}")
    
    async def _run_deep_dive(self):
        """Generate and publish deep dive thread, raising an exception on failure."""
        from core.content_engine import publish_deep_dive_now
        result = await publish_deep_dive_now()
        
        if result.get('success'):
            logger.info("🧵 Deep dive published successfully")
            return result
        else:
            raise Exception(f"Deep dive job failed: {result.get('error')}")
    
    async def _run_headline_pipeline(self):
        """Run headline processing pipeline with proper error handling."""
        if not self.headline_pipeline:
            logger.warning("⚠️ Headline pipeline not available - skipping")
            return
        
        try:
            result = await asyncio.to_thread(self.headline_pipeline.run_pipeline)
            logger.info(f"📰 Headline pipeline completed: {result} headlines stored")
            return result
        except Exception as e:
            logger.error(f"❌ Headline pipeline failed: {e}")
            raise
    
    def _check_http_server_health_job(self):
        """Scheduled job to check HTTP server health"""
        try:
            health_status = self.check_http_server_health()
            
            if health_status['healthy']:
                logger.info("✅ HTTP server health check passed")
                return {"success": True, "status": health_status['status']}
            else:
                logger.warning(f"⚠️ HTTP server health check failed: {health_status['message']}")
                
                # Send notification for unhealthy server
                asyncio.run(self.telegram.send_message(
                    f"🌐 **HTTP Server Health Alert**\n⚠️ Status: {health_status['status']}\n📝 {health_status['message']}\n🔧 Website news may be unavailable",
                    NotificationLevel.WARNING
                ))
                
                return {"success": False, "status": health_status['status'], "error": health_status['message']}
                
        except Exception as e:
            logger.error(f"❌ HTTP server health check error: {e}")
            return {"success": False, "error": str(e)}
    
    async def _daily_maintenance(self):
        """Perform daily maintenance tasks"""
        try:
            logger.info("🔧 Running daily maintenance...")
            
            # Reset daily counters
            self.jobs_completed_today = 0
            self.jobs_failed_today = 0
            
            # Check HTTP server health
            http_status = self.check_http_server_health()
            
            if not http_status['healthy']:
                await self.telegram.send_message(
                    f"🔧 **Daily Maintenance - HTTP Server Issue**\n\n"
                    f"⚠️ Status: {http_status['status']}\n📝 {http_status['message']}\n🌐 Website news may be affected",
                    NotificationLevel.WARNING
                )
            else:
                # Create daily summary with timezone info
                current_times, timezone_info = self.get_timezone_info()
                
                today_stats = f"""🔧 **Daily Maintenance Complete**

📅 **Date**: {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}
📊 **Today's Performance**:
   • Jobs completed: {self.jobs_completed_today}
   • Jobs failed: {self.jobs_failed_today}
   • Success rate: {(self.jobs_completed_today/(max(self.jobs_completed_today+self.jobs_failed_today,1)))*100:.1f}%

✅ **System Status**: All systems healthy
🌐 **HTTP Server**: {http_status['status']} (Port {self.http_server_port})
🌍 **Timezones**: {timezone_info['London']} | {timezone_info['New York']}
⏰ **Uptime**: {((datetime.now(timezone.utc) - self.startup_time).total_seconds()/3600):.1f}h"""
                
                await self.telegram.send_message(today_stats, NotificationLevel.SUCCESS)
            
            logger.info("✅ Daily maintenance completed")
            
        except Exception as e:
            logger.error(f"❌ Daily maintenance failed: {e}")
            await self.telegram.notify_critical_error(
                "Daily Maintenance",
                str(e),
                "Manual maintenance check required"
            )
    
    async def _send_heartbeat(self):
        """Send periodic heartbeat with system status"""
        try:
            # Calculate uptime
            uptime = datetime.now(timezone.utc) - self.startup_time
            uptime_hours = uptime.total_seconds() / 3600
            
            # Get next scheduled job
            next_job = schedule.next_run()
            next_job_str = next_job.strftime('%H:%M UTC') if next_job else "None"
            
            # Get today's stats
            today_str = datetime.now().strftime("%A")
            expected_tweets = 11 if today_str in ["Saturday", "Sunday"] else 15
            
            # Calculate success rate
            total_jobs = self.jobs_completed_today + self.jobs_failed_today
            success_rate = 0 if total_jobs == 0 else (self.jobs_completed_today / total_jobs) * 100
            
            # Get system health
            try:
                health = await self.content_engine.get_pipeline_status()
                health_summary = "✅ Healthy"
                if 'error' in health:
                    health_summary = f"⚠️ Issues: {health['error']}"
            except Exception:
                health_summary = "❓ Unknown"
            
            # Get HTTP server status
            http_status = self.check_http_server_health()
            http_summary = "✅ Healthy" if http_status['healthy'] else f"⚠️ {http_status['status']}"
            
            # Get timezone info
            current_times, timezone_info = self.get_timezone_info()
            
            # Create heartbeat message
            heartbeat_msg = f"""💓 **HedgeFund Scheduler Heartbeat**

📊 **Status**: Active & Running
⏰ **Uptime**: {uptime_hours:.1f}h
🗓️ **Today**: {today_str} ({expected_tweets} tweets expected)

📈 **Performance**:
   • Jobs completed: {self.jobs_completed_today}
   • Jobs failed: {self.jobs_failed_today}
   • Success rate: {success_rate:.1f}%

⏱️ **Scheduling**:
   • Next job: {next_job_str}
   • Timezones: {timezone_info['London']} | {timezone_info['New York']}
   • Last job: {self.last_job_name or 'None'}

🏥 **Health**:
   • Content Engine: {health_summary}
   • HTTP Server: {http_summary}"""

            await self.telegram.send_message(heartbeat_msg, NotificationLevel.INFO)
            
        except Exception as e:
            logger.error(f"❌ Heartbeat failed: {e}")
    
    def start_scheduler(self):
        """Start the scheduler loop with proper error handling and heartbeat"""
        logger.info("🚀 Starting HedgeFund Agent Scheduler with Timezone Awareness")
        
        # Start HTTP server first
        http_started = self.start_http_server()
        
        # Get timezone info for startup message
        current_times, timezone_info = self.get_timezone_info()
        
        # Send startup notification using send_message
        try:
            startup_msg = f"""🚀 **HedgeFund Agent Scheduler Started**

📅 **Time**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
🌍 **Mode**: Production (Timezone-Aware)
🕐 **Timezones**: {timezone_info['London']} | {timezone_info['New York']}
💓 **Heartbeat**: Every {self.heartbeat_interval/60:.0f} minutes
📊 **Jobs Loaded**: {len(schedule.get_jobs())} total scheduled jobs

🌐 **HTTP Server**: {'✅ Running' if http_started else '❌ Failed'} (Port {self.http_server_port})
📡 **Website Integration**: {'Enabled' if http_started else 'Disabled'}

🎯 **Ready to generate content at proper market times!**"""
            
            asyncio.run(self.telegram.send_message(startup_msg, NotificationLevel.SUCCESS))
            
        except Exception as e:
            logger.error(f"❌ Startup notification failed: {e}")
        
        # Main scheduler loop
        try:
            logger.info("♻️ Entering scheduler loop...")
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
                
        except KeyboardInterrupt:
            logger.info("🛑 Scheduler stopped by user")
            asyncio.run(self.telegram.send_message(
                "🛑 **HedgeFund Scheduler Stopped**\n📝 Manual shutdown detected",
                NotificationLevel.WARNING
            ))
            
        except Exception as e:
            logger.error(f"💥 Scheduler crashed: {e}")
            asyncio.run(self.telegram.notify_critical_error(
                "Scheduler Crash",
                str(e),
                "Immediate restart required"
            ))


if __name__ == "__main__":
    scheduler = HedgeFundScheduler()
    scheduler.setup_schedule()
    scheduler.start_scheduler()