# hedgefund_agent/scheduler.py
"""
HedgeFund Agent Production Scheduler - FIXED VERSION
Implements the complete 15-tweet weekday schedule with automatic BST/GMT handling
"""

import schedule
import time
import logging
import asyncio
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
    from services.headline_pipeline import fetch_and_score_headlines
    HEADLINE_PIPELINE_AVAILABLE = True
    logger.info("✅ Headline pipeline imported successfully from services")
except ImportError as e:
    logger.warning(f"⚠️ Headline pipeline not available: {e}")
    HEADLINE_PIPELINE_AVAILABLE = False

class HedgeFundScheduler:
    """Production scheduler for HedgeFund Agent with BST/GMT awareness"""
    
    def __init__(self):
        self.content_engine = ContentEngine()
        self.telegram = TelegramNotifier()
        self.deep_dive_days = ["Monday", "Wednesday", "Friday"]
        
        # BST/GMT aware scheduling - these are your desired BST times
        self.bst_briefing_times = ["07:30", "14:15", "17:00", "21:45"]
        self.bst_commentary_times = ["07:00", "08:00", "10:00", "11:00", "15:30", "18:00", "20:00", "22:00", "23:00"]
        
        # Calculate BST status once during initialization
        self._bst_active = self._calculate_bst_status()
        
        logger.info("🗓️ HedgeFund Scheduler initialized")
    
    def _calculate_bst_status(self) -> bool:
        """Calculate if British Summer Time is currently active - called once"""
        # Simple and reliable: August is definitely BST
        # You can manually override this or use a more complex calculation later
        now = datetime.now()
        month = now.month
        day = now.day
        
        # BST typically runs from late March to late October
        # Simple approximation for now
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
        # Reduced weekend schedule (11 tweets: 6 commentary + 3 briefings + 2 deep dives)
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
        if HEADLINE_PIPELINE_AVAILABLE:
            # Fetch headlines every 30 minutes
            schedule.every().hour.at(":05").do(
                self._safe_job_wrapper("headlines_fetch_05", fetch_and_score_headlines)
            )
            schedule.every().hour.at(":35").do(
                self._safe_job_wrapper("headlines_fetch_35", fetch_and_score_headlines)
            )
        
        # Daily maintenance at 23:50 UTC
        schedule.every().day.at("23:50").do(
            self._safe_job_wrapper("daily_maintenance", self._daily_maintenance)
        )
        
        # Log schedule summary
        total_jobs = len(schedule.get_jobs())
        jobs_by_type = self._analyze_schedule()
        
        logger.info(f"📋 Schedule loaded: {total_jobs} total jobs")
        logger.info(f"📊 Jobs breakdown: {jobs_by_type}")
        
        # Show next job
        next_job = schedule.next_run()
        if next_job:
            logger.info(f"⏰ Next job: {next_job}")
        
        # Log timezone info once
        local_time, utc_time = self.get_timezone_info()
        logger.info("🕐 VM Timezone Information:")
        logger.info(f"Local Time: {local_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"UTC Time: {utc_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        
        if abs((local_time - utc_time.replace(tzinfo=None)).total_seconds()) < 60:
            logger.info("✅ VM is on UTC timezone")
        
        # Log BST status once
        if self.is_bst_active():
            logger.info("🕐 BST Status: Active")
            logger.info("✅ Schedule configured for BST (UTC+1)")
        else:
            logger.info("🕐 GMT Status: Active") 
            logger.info("✅ Schedule configured for GMT (UTC+0)")
        
        # Log expected tweets
        today = datetime.now().strftime("%A")
        expected_tweets = 11 if today in ["Saturday", "Sunday"] else 15
        logger.info(f"📊 Daily: 9 commentary + 4 briefings + 3 deep dives = {expected_tweets} tweets")
        logger.info("📰 Headlines fetched every 30min: :05 and :35 past each hour, 7 days/week")
    
    def _analyze_schedule(self) -> dict:
        """Analyze loaded schedule and return breakdown"""
        jobs = schedule.get_jobs()
        breakdown = {
            'commentary': 0,
            'briefings': 0, 
            'deep_dives': 0,
            'headlines': 0,
            'maintenance': 0
        }
        
        for job in jobs:
            job_name = getattr(job.job_func, '__name__', 'unknown')
            if 'commentary' in job_name:
                breakdown['commentary'] += 1
            elif 'briefing' in job_name:
                breakdown['briefings'] += 1
            elif 'deep_dive' in job_name:
                breakdown['deep_dives'] += 1
            elif 'headlines' in job_name:
                breakdown['headlines'] += 1
            elif 'maintenance' in job_name:
                breakdown['maintenance'] += 1
        
        return breakdown
    
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
                
                # Execute the job
                result = func(*args, **kwargs)
                
                # Calculate duration
                duration = datetime.now() - start_time
                duration_str = str(duration).split('.')[0]  # Remove microseconds
                
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
                
                error_msg = f"Job `{job_name}` failed after {duration_str}: {str(e)}"
                
                # Error notification using critical_error method
                asyncio.run(self.telegram.notify_critical_error(
                    f"Scheduler Job: {job_name}",
                    str(e),
                    "Check logs and restart if needed"
                ))
                
                logger.error(f"❌ {error_msg}")
                # Don't re-raise to keep scheduler running
        
        return wrapper
    
    async def _run_briefing(self, briefing_type: str):
        """Generate and publish market briefing"""
        request = ContentRequest(
            content_type=ContentType.BRIEFING,
            category=ContentCategory.MARKET_ANALYSIS,
            briefing_type=briefing_type,
            priority="high"
        )
        
        result = await self.content_engine.generate_content(request)
        
        if result.success:
            logger.info(f"📋 {briefing_type} briefing published")
            return {"success": True, "urls": result.published_urls}
        else:
            logger.error(f"❌ {briefing_type} briefing failed: {result.error}")
            return {"success": False, "error": result.error}
    
    async def _run_commentary(self):
        """Generate and publish market commentary"""
        request = ContentRequest(
            content_type=ContentType.COMMENTARY,
            category=ContentCategory.MARKET_ANALYSIS,
            priority="normal"
        )
        
        result = await self.content_engine.generate_content(request)
        
        if result.success:
            logger.info("💬 Commentary published")
            return {"success": True, "urls": result.published_urls}
        else:
            logger.error(f"❌ Commentary failed: {result.error}")
            return {"success": False, "error": result.error}
    
    async def _run_deep_dive(self):
        """Generate and publish deep dive thread"""
        request = ContentRequest(
            content_type=ContentType.THREAD,
            category=ContentCategory.DEEP_ANALYSIS,
            priority="high"
        )
        
        result = await self.content_engine.generate_content(request)
        
        if result.success:
            logger.info("🧵 Deep dive thread published")
            return {"success": True, "urls": result.published_urls}
        else:
            logger.error(f"❌ Deep dive failed: {result.error}")
            return {"success": False, "error": result.error}
    
    async def _daily_maintenance(self):
        """Perform daily maintenance tasks"""
        try:
            logger.info("🔧 Starting daily maintenance...")
            
            # Check system health
            status = self.content_engine.get_health_status()
            
            if not status.get('healthy', False):
                # Use critical_error for health issues
                await self.telegram.notify_critical_error(
                    "Daily Health Check",
                    f"System health check failed: {status.get('error', 'Unknown error')}",
                    "Check system components and restart if needed"
                )
            else:
                # Use send_message for successful maintenance
                await self.telegram.send_message(
                    f"🔧 Daily Maintenance Complete\nTime: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n✅ All systems healthy",
                    NotificationLevel.SUCCESS
                )
            
            logger.info("✅ Daily maintenance completed")
            
        except Exception as e:
            logger.error(f"❌ Daily maintenance failed: {e}")
            await self.telegram.notify_critical_error(
                "Daily Maintenance",
                str(e),
                "Manual maintenance check required"
            )
    
    async def _send_daily_summary(self):
        """Send daily summary"""
        try:
            today = datetime.now().strftime("%A")
            expected_tweets = 11 if today in ["Saturday", "Sunday"] else 15
            
            await self.telegram.send_message(
                f"📊 Daily Summary - {today}\nExpected tweets: {expected_tweets}\nStatus: Scheduler running",
                NotificationLevel.INFO
            )
            
        except Exception as e:
            logger.error(f"❌ Daily summary failed: {e}")
    
    def start_scheduler(self):
        """Start the scheduler loop with proper error handling"""
        logger.info("🚀 Starting HedgeFund Agent Scheduler")
        
        # Send startup notification using send_message
        try:
            asyncio.run(self.telegram.send_message(
                f"🚀 HedgeFund Agent Scheduler Started\nMode: Production\nTime: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\nBST Active: {self.is_bst_active()}",
                NotificationLevel.START
            ))
        except Exception as e:
            logger.error(f"Failed to send startup notification: {e}")
        
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
                
            except KeyboardInterrupt:
                logger.info("👋 Scheduler stopped by user")
                try:
                    asyncio.run(self.telegram.send_message(
                        "👋 HedgeFund Agent Scheduler Stopped\nReason: Manual shutdown",
                        NotificationLevel.WARNING
                    ))
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
                time.sleep(60)  # Wait before retrying


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