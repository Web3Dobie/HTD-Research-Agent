# hedgefund_agent/scheduler.py
"""
HedgeFund Agent Production Scheduler - WITH HTTP SERVER
Implements the complete 15-tweet weekday schedule with automatic BST/GMT handling
+ PostgreSQL HTTP news server for website integration
"""

import schedule
import time
import logging
import asyncio
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional

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
    """Production scheduler for HedgeFund Agent with BST/GMT awareness + HTTP Server"""
    
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
        
        # BST/GMT aware scheduling - these are your desired BST times
        self.bst_briefing_times = ["07:30", "14:07", "16:25", "21:40"]
        self.bst_commentary_times = ["07:00", "08:00", "10:00", "11:00", "15:30", "18:00", "20:00", "22:00", "23:00"]
        
        # Calculate BST status once during initialization
        self._bst_active = self._calculate_bst_status()
        
        # Heartbeat configuration
        self.last_heartbeat = time.time()
        self.heartbeat_interval = 3600  # Send heartbeat every hour (3600 seconds)
        self.startup_time = datetime.now(timezone.utc)
        
        # Health metrics
        self.jobs_completed_today = 0
        self.jobs_failed_today = 0
        self.last_job_time = None
        self.last_job_name = None
        
        logger.info("🗓️ HedgeFund Scheduler initialized")
        logger.info(f"💓 Heartbeat interval: {self.heartbeat_interval/60:.0f} minutes")
        logger.info(f"🌐 HTTP Server will run on port {self.http_server_port}")
    
    def _calculate_bst_status(self) -> bool:
        """Calculate if British Summer Time is currently active - called once"""
        now = datetime.now()
        month = now.month
        day = now.day
        
        # BST typically runs from late March to late October
        if month < 3 or month > 10:
            return False
        elif month > 3 and month < 10:
            return True
        elif month == 3:
            return day >= 25  # Rough approximation
        elif month == 10:
            return day <= 25  # Rough approximation
        else:
            return False
    
    def is_bst_active(self) -> bool:
        """Return cached BST status"""
        return self._bst_active
    
    def get_timezone_info(self) -> tuple:
        """Get current timezone information"""
        local_time = datetime.now()
        utc_time = datetime.now(timezone.utc)
        return local_time, utc_time
    
    def bst_to_utc(self, bst_time: str) -> str:
        """Convert BST time to UTC time for scheduling"""
        if self.is_bst_active():
            # BST is UTC+1, so subtract 1 hour
            hour, minute = map(int, bst_time.split(':'))
            utc_hour = (hour - 1) % 24
            return f"{utc_hour:02d}:{minute:02d}"
        else:
            # GMT is UTC+0, no conversion needed
            return bst_time
    
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
            self.http_server_status = "error"
            logger.error(f"❌ Failed to start HTTP server: {e}")
            
            asyncio.run(self.telegram.notify_critical_error(
                "HTTP Server Startup",
                str(e),
                f"Website news integration unavailable on port {self.http_server_port}"
            ))
            return False
    
    def _run_http_server(self):
        """Run HTTP server (called in thread)"""
        try:
            # This will block in the thread
            start_hedgefund_news_server(port=self.http_server_port)
        except Exception as e:
            logger.error(f"❌ HTTP server thread error: {e}")
            self.http_server_status = "crashed"
    
    def _test_http_server(self) -> bool:
        """Test if HTTP server is responding"""
        try:
            import urllib.request
            import socket
            
            # Set a short timeout
            socket.setdefaulttimeout(5)
            
            # Test health endpoint
            url = f"http://localhost:{self.http_server_port}/health"
            with urllib.request.urlopen(url) as response:
                data = response.read()
                return response.status == 200
                
        except Exception as e:
            logger.debug(f"HTTP server test failed: {e}")
            return False
    
    def check_http_server_health(self) -> dict:
        """Check HTTP server health and return status"""
        if not self.http_server_thread or not self.http_server_thread.is_alive():
            return {
                "status": "stopped",
                "message": "HTTP server thread not running",
                "healthy": False
            }
        
        if self._test_http_server():
            return {
                "status": "healthy",
                "message": f"HTTP server responding on port {self.http_server_port}",
                "healthy": True
            }
        else:
            return {
                "status": "unhealthy", 
                "message": "HTTP server thread running but not responding",
                "healthy": False
            }
    
    def setup_schedule(self):
        """Setup the complete production schedule"""
        logger.info("📋 Setting up production schedule...")
        
        # Clear any existing jobs
        schedule.clear()
        
        # Convert BST times to UTC for scheduling
        utc_briefing_times = [self.bst_to_utc(t) for t in self.bst_briefing_times]
        utc_commentary_times = [self.bst_to_utc(t) for t in self.bst_commentary_times]
        
        # === MARKET BRIEFINGS ===
        weekday_briefings = {
            "monday": ["opening", "midday", "afternoon", "close"],
            "tuesday": ["opening", "midday", "afternoon", "close"], 
            "wednesday": ["opening", "midday", "afternoon", "close"],
            "thursday": ["opening", "midday", "afternoon", "close"],
            "friday": ["opening", "midday", "afternoon", "close"]
        }
        
        for day, briefings in weekday_briefings.items():
            for i, briefing_type in enumerate(briefings):
                if i < len(utc_briefing_times):
                    time_str = utc_briefing_times[i]
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
        for day in self.deep_dive_days:
            job_name = f"deep_dive_{day.lower()}"
            getattr(schedule.every(), day.lower()).at("21:00").do(  # 22:00 BST = 21:00 UTC
                self._safe_job_wrapper(job_name, self._run_deep_dive)
            )
        
        # === WEEKEND SCHEDULE ===
        weekend_briefings = ["opening", "midday", "close"]  # 3 briefings only
        weekend_commentary_times = utc_commentary_times[:6]  # 6 commentary posts only
        
        for day in ["saturday", "sunday"]:
            # Weekend briefings
            for i, briefing_type in enumerate(weekend_briefings):
                if i < len(utc_briefing_times):
                    time_str = utc_briefing_times[i]
                    job_name = f"briefing_{briefing_type}_{day}"
                    
                    getattr(schedule.every(), day).at(time_str).do(
                        self._safe_job_wrapper(job_name, self._run_briefing, briefing_type)
                    )
            
            # Weekend commentary
            for i, time_str in enumerate(weekend_commentary_times):
                job_name = f"commentary_{day}_{time_str.replace(':', '')}"
                
                getattr(schedule.every(), day).at(time_str).do(
                    self._safe_job_wrapper(job_name, self._run_commentary)
                )
            
            # Weekend deep dive
            job_name = f"deep_dive_{day}"
            getattr(schedule.every(), day).at("21:00").do(
                self._safe_job_wrapper(job_name, self._run_deep_dive)
            )
        
        # === MAINTENANCE TASKS ===
        if HEADLINE_PIPELINE_AVAILABLE and self.headline_pipeline:
            # Fetch headlines every 30 minutes using modern pipeline
            schedule.every().hour.at(":05").do(
                self._safe_job_wrapper("headlines_fetch_05", self._run_headline_pipeline)
            )
            schedule.every().hour.at(":35").do(
                self._safe_job_wrapper("headlines_fetch_35", self._run_headline_pipeline)
            )
        
        # HTTP Server health check every 30 minutes
        schedule.every().hour.at(":15").do(
            self._safe_job_wrapper("http_server_health_15", self._check_http_server_health_job)
        )
        schedule.every().hour.at(":45").do(
            self._safe_job_wrapper("http_server_health_45", self._check_http_server_health_job)
        )
        
        # Daily maintenance at 23:50 UTC (recurring)
        schedule.every().day.at("23:50").do(
            self._safe_job_wrapper("daily_maintenance", self._daily_maintenance)
        )
        
        # Hourly heartbeat (every hour at :00) - recurring
        schedule.every().hour.at(":00").do(
            self._safe_job_wrapper("heartbeat", self._send_heartbeat)
        )
        
        # Log schedule summary
        total_jobs = len(schedule.get_jobs())
        logger.info(f"📋 Schedule loaded: {total_jobs} total jobs")
        
        # Show next job
        next_job = schedule.next_run()
        if next_job:
            logger.info(f"⏰ Next job: {next_job}")
        
        # Log timezone info
        local_time, utc_time = self.get_timezone_info()
        logger.info("🕐 VM Timezone Information:")
        logger.info(f"Local Time: {local_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"UTC Time: {utc_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        
        # Log BST status
        if self.is_bst_active():
            logger.info("🕐 BST Status: Active - Schedule configured for BST (UTC+1)")
        else:
            logger.info("🕐 GMT Status: Active - Schedule configured for GMT (UTC+0)")
    
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
        }
        
        if briefing_type not in briefing_mapping:
            logger.info(f"Briefing type '{briefing_type}' disabled")
            return
        
        briefing_key = briefing_mapping[briefing_type]
        await self.content_engine.run_briefing_pipeline(briefing_key)
    
    async def _run_commentary(self):
        """
        Generate and publish market commentary, allowing the wrapper to handle exceptions.
        """
        # We removed the try...except block from this method.
        # Now, if publish_commentary_now fails, the exception will be
        # caught by _safe_job_wrapper, which will send the correct failure notification.
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
        request = ContentRequest(
            content_type=ContentType.DEEP_DIVE,
            category=ContentCategory.MACRO,
            include_market_data=True
        )
        
        result = await self.content_engine.generate_and_publish_content(request)
        
        # If the result is not successful, raise an exception for the wrapper to catch
        if not result or not result.get('success'):
            raise Exception(f"Deep dive job returned failure status: {result.get('error')}")
        
        logger.info("🧵 Deep dive thread published")
    
    def _run_headline_pipeline(self):
        """Run modern headline fetching and scoring pipeline"""
        try:
            if not self.headline_pipeline:
                raise Exception("HeadlinePipeline not available")
            
            headlines_stored = self.headline_pipeline.run_pipeline()
            logger.info(f"📰 Headlines pipeline completed: {headlines_stored} headlines stored to database")
            return {"success": True, "headlines_stored": headlines_stored}
                
        except Exception as e:
            logger.error(f"❌ Headlines pipeline failed: {e}")
            return {"success": False, "error": str(e)}
    
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
   • BST active: {self.is_bst_active()}
   • Last job: {self.last_job_name or 'None'} at {self.last_job_time.strftime('%H:%M') if self.last_job_time else 'N/A'}

🔧 **System Health**: {health_summary}
🌐 **HTTP Server**: {http_summary} (Port {self.http_server_port})"""
            
            await self.telegram.send_message(heartbeat_msg, NotificationLevel.HEARTBEAT)
            logger.info("💓 Heartbeat sent successfully")
            
            # Reset daily counters at midnight
            if datetime.now().hour == 0 and datetime.now().minute < 5:
                self.jobs_completed_today = 0
                self.jobs_failed_today = 0
                logger.info("🔄 Daily metrics reset")
            
        except Exception as e:
            logger.error(f"❌ Heartbeat failed: {e}")
    
    def _check_heartbeat_in_loop(self):
        """Check if heartbeat should be sent (for non-scheduled heartbeat)"""
        current_time = time.time()
        
        if current_time - self.last_heartbeat > self.heartbeat_interval:
            try:
                asyncio.run(self._send_heartbeat())
                self.last_heartbeat = current_time
            except Exception as e:
                logger.error(f"❌ Loop heartbeat failed: {e}")
    
    async def _daily_maintenance(self):
        """Perform daily maintenance tasks"""
        try:
            logger.info("🔧 Starting daily maintenance...")
            
            # Check system health
            status = await self.content_engine.get_pipeline_status()
            
            # Check HTTP server health
            http_status = self.check_http_server_health()
            
            if status.get('error') or status.get('status') == 'unhealthy':
                # Use critical_error for health issues
                await self.telegram.notify_critical_error(
                    "Daily Health Check",
                    f"System health check failed: {status.get('error', 'Unknown error')}",
                    "Check system components and restart if needed"
                )
            elif not http_status['healthy']:
                # HTTP server issues
                await self.telegram.send_message(
                    f"🔧 **Daily Maintenance Alert**\n⚠️ HTTP Server: {http_status['status']}\n📝 {http_status['message']}\n🌐 Website news may be affected",
                    NotificationLevel.WARNING
                )
            else:
                # Create daily summary
                today_stats = f"""🔧 **Daily Maintenance Complete**

📅 **Date**: {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}
📊 **Today's Performance**:
   • Jobs completed: {self.jobs_completed_today}
   • Jobs failed: {self.jobs_failed_today}
   • Success rate: {(self.jobs_completed_today/(max(self.jobs_completed_today+self.jobs_failed_today,1)))*100:.1f}%

✅ **System Status**: All systems healthy
🌐 **HTTP Server**: {http_status['status']} (Port {self.http_server_port})
🕐 **BST Active**: {self.is_bst_active()}
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
    
    def start_scheduler(self):
        """Start the scheduler loop with proper error handling and heartbeat"""
        logger.info("🚀 Starting HedgeFund Agent Scheduler with HTTP Server")
        
        # Start HTTP server first
        http_started = self.start_http_server()
        
        # Send startup notification using send_message
        try:
            startup_msg = f"""🚀 **HedgeFund Agent Scheduler Started**

📅 **Time**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
🌍 **Mode**: Production
🕐 **BST Active**: {self.is_bst_active()}
💓 **Heartbeat**: Every {self.heartbeat_interval/60:.0f} minutes
📊 **Jobs Loaded**: {len(schedule.get_jobs())} total scheduled jobs

🌐 **HTTP Server**: {'✅ Running' if http_started else '❌ Failed'} (Port {self.http_server_port})
📡 **Website Integration**: {'Enabled' if http_started else 'Disabled'}

🎯 **Ready to generate content!**"""
            
            asyncio.run(self.telegram.send_message(startup_msg, NotificationLevel.START))
        except Exception as e:
            logger.error(f"Failed to send startup notification: {e}")
        
        # Send initial heartbeat
        try:
            asyncio.run(self._send_heartbeat())
            self.last_heartbeat = time.time()
        except Exception as e:
            logger.error(f"Failed to send initial heartbeat: {e}")
        
        # Main scheduler loop
        while True:
            try:
                # Run scheduled jobs
                schedule.run_pending()
                
                # Check for heartbeat (backup to scheduled heartbeat)
                self._check_heartbeat_in_loop()
                
                # Sleep for 30 seconds (more responsive than 60)
                time.sleep(30)
                
            except KeyboardInterrupt:
                logger.info("👋 Scheduler stopped by user")
                try:
                    http_status = self.check_http_server_health()
                    shutdown_msg = f"""👋 **HedgeFund Agent Scheduler Stopped**

📅 **Time**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
🛑 **Reason**: Manual shutdown (Ctrl+C)
⏰ **Final Uptime**: {((datetime.now(timezone.utc) - self.startup_time).total_seconds()/3600):.1f}h
📊 **Today's Stats**: {self.jobs_completed_today} completed, {self.jobs_failed_today} failed
🌐 **HTTP Server**: {http_status['status']} at shutdown

✅ **Shutdown clean**"""
                    
                    asyncio.run(self.telegram.send_message(shutdown_msg, NotificationLevel.WARNING))
                except Exception:
                    pass  # Don't fail on notification errors during shutdown
                break
                
            except Exception as e:
                logger.error(f"❌ Scheduler error: {e}")
                try:
                    asyncio.run(self.telegram.notify_critical_error(
                        "Scheduler Loop",
                        str(e),
                        "Scheduler continuing but may need restart"
                    ))
                except Exception:
                    pass  # Don't fail on notification errors
                time.sleep(60)  # Wait longer before retrying after errors


def main():
    """Main entry point"""
    try:
        scheduler = HedgeFundScheduler()
        scheduler.setup_schedule()
        scheduler.start_scheduler()
        
    except Exception as e:
        logger.critical(f"Failed to start scheduler: {e}")
        # Try to send critical error notification
        try:
            notifier = TelegramNotifier()
            asyncio.run(notifier.notify_critical_error(
                "Scheduler Startup",
                str(e),
                "Manual restart required"
            ))
        except Exception:
            pass  # Don't fail if notification fails
        raise


if __name__ == "__main__":
    main()