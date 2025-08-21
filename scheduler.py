# === SYSTEM MAINTENANCE ===# hedgefund_agent/scheduler.py
"""
HedgeFund Agent Production Scheduler
Implements the complete 15-tweet weekday schedule with automatic BST/GMT handling
"""

import schedule
import time
import logging
import asyncio
from datetime import datetime, timezone
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
from services.telegram_notifier import TelegramNotifier

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
        
        logger.info("🗓️ HedgeFund Scheduler initialized")
    
    def is_bst_active(self) -> bool:
        """Check if British Summer Time is currently active"""
        import datetime
        
        now = datetime.datetime.now()
        year = now.year
        
        # BST starts on last Sunday of March at 01:00 UTC
        # BST ends on last Sunday of October at 01:00 UTC
        
        # Find last Sunday of March
        march_31 = datetime.date(year, 3, 31)
        days_back = (march_31.weekday() + 1) % 7
        bst_start = march_31 - datetime.timedelta(days=days_back)
        
        # Find last Sunday of October  
        oct_31 = datetime.date(year, 10, 31)
        days_back = (oct_31.weekday() + 1) % 7
        bst_end = oct_31 - datetime.timedelta(days=days_back)
        
        current_date = now.date()
        is_bst = bst_start <= current_date < bst_end
        
        logger.info(f"🕐 BST Status: {'Active' if is_bst else 'Inactive (GMT)'}")
        return is_bst
    
    def convert_bst_to_utc_time(self, bst_time_str: str) -> str:
        """Convert BST time string to UTC time string"""
        bst_hour, bst_minute = map(int, bst_time_str.split(':'))
        
        # Calculate UTC offset
        utc_offset = 1 if self.is_bst_active() else 0
        utc_hour = bst_hour - utc_offset
        
        # Handle day rollover
        if utc_hour < 0:
            utc_hour += 24
        
        return f"{utc_hour:02d}:{bst_minute:02d}"
    
    def check_vm_timezone(self):
        """Check VM timezone and display info"""
        import subprocess
        import time
        
        try:
            local_time = datetime.now()
            utc_time = datetime.utcnow()
            
            logger.info("🕐 VM Timezone Information:")
            logger.info(f"Local Time: {local_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"UTC Time: {utc_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            
            # Try to get timezone info
            try:
                result = subprocess.run(['timedatectl'], capture_output=True, text=True)
                if "UTC" in result.stdout:
                    logger.info("✅ VM is on UTC timezone")
                else:
                    logger.warning("⚠️ VM timezone may not be UTC")
            except:
                logger.info("📝 Could not determine timezone details")
                
        except Exception as e:
            logger.error(f"❌ Failed to check timezone: {e}")
    
    def setup_weekday_schedule(self):
        """Setup complete weekday schedule with automatic BST/GMT conversion"""
        
        # Check timezone and BST status
        self.check_vm_timezone()
        
        # Convert BST times to UTC
        briefing_utc_times = [self.convert_bst_to_utc_time(t) for t in self.bst_briefing_times]
        commentary_utc_times = [self.convert_bst_to_utc_time(t) for t in self.bst_commentary_times]
        
        logger.info(f"📋 Briefing times (UTC): {briefing_utc_times}")
        logger.info(f"💬 Commentary times (UTC): {commentary_utc_times}")
        
        # === BRIEFINGS (4 per day) ===
        days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']
        briefing_types = ['morning', 'pre-market', 'midday', 'post-market']
        
        for day in days:
            for i, briefing_type in enumerate(briefing_types):
                getattr(schedule.every(), day).at(briefing_utc_times[i]).do(self._run_briefing, briefing_type)
        
        # === COMMENTARY (9 per day) ===
        categories = [
            ContentCategory.MACRO, ContentCategory.EQUITY, ContentCategory.POLITICAL,
            ContentCategory.MACRO, ContentCategory.EQUITY, ContentCategory.MACRO,
            ContentCategory.EQUITY, ContentCategory.POLITICAL, None  # Last one is any
        ]
        
        for day in days:
            for i in range(min(9, len(commentary_utc_times))):
                getattr(schedule.every(), day).at(commentary_utc_times[i]).do(self._run_commentary, categories[i])
        
        # === DEEP DIVES (3 tweets, Mon/Wed/Fri) ===
        deep_dive_utc_time = self.convert_bst_to_utc_time("14:00")
        schedule.every().monday.at(deep_dive_utc_time).do(self._run_deep_dive)
        schedule.every().wednesday.at(deep_dive_utc_time).do(self._run_deep_dive)
        schedule.every().friday.at(deep_dive_utc_time).do(self._run_deep_dive)
        
        # === HEADLINE PIPELINE (Every 30 minutes, 7 days a week) ===
        # Fetch fresh headlines at :05 and :35 past every hour
        # This runs 48 times per day (24 hours × 2 fetches per hour)
        schedule.every().hour.at(":05").do(self._run_headline_fetch)
        schedule.every().hour.at(":35").do(self._run_headline_fetch)
        maintenance_utc = self.convert_bst_to_utc_time("05:00")
        summary_utc = self.convert_bst_to_utc_time("01:00")
        
        schedule.every().day.at(maintenance_utc).do(self._daily_maintenance)
        schedule.every().day.at(summary_utc).do(self._send_daily_summary)
        
        bst_status = "BST (UTC+1)" if self.is_bst_active() else "GMT (UTC+0)"
        logger.info(f"✅ Schedule configured for {bst_status}")
        logger.info("📊 Daily: 9 commentary + 4 briefings + 3 deep dives = 15 tweets")
        logger.info("📰 Headlines fetched every 30min: :05 and :35 past each hour, 7 days/week")
    
    def _run_headline_fetch(self):
        """Run headline pipeline to fetch and score fresh news"""
        try:
            if not HEADLINE_PIPELINE_AVAILABLE:
                logger.warning("⚠️ Headline pipeline not available, skipping fetch")
                return
                
            current_time = datetime.now().strftime('%H:%M')
            logger.info(f"📰 Starting headline fetch at {current_time}")
            
            # Fetch and score headlines (increased limit for better coverage)
            fetch_and_score_headlines(limit=250)
            
            logger.info(f"✅ Headline fetch completed at {current_time}")
            
            # Send notification only every 2 hours to avoid spam
            current_hour = datetime.now().hour
            if current_hour % 2 == 0 and datetime.now().minute <= 10:  # Only at even hours, :05 fetch
                asyncio.run(self.telegram.notify_system_message(
                    f"📰 Headlines Updated ({current_time})\n"
                    f"Fresh headlines fetched and scored\n"
                    f"Next fetch: {current_hour}:35"
                ))
            
        except Exception as e:
            logger.error(f"❌ Headline fetch failed at {datetime.now().strftime('%H:%M')}: {e}")
            
            # Alert about headline fetch failure
            asyncio.run(self.telegram.notify_system_alert(
                "🚨 Headline Fetch Failed",
                f"Time: {datetime.now().strftime('%H:%M')}\n"
                f"Error: {str(e)}\n"
                f"Content generation may use stale headlines"
            ))
    
    def _run_commentary(self, category: Optional[ContentCategory] = None):
        """Run commentary generation job"""
        try:
            logger.info(f"🐂 Starting commentary - Category: {category.value if category else 'Any'}")
            
            request = ContentRequest(
                content_type=ContentType.COMMENTARY,
                category=category,
                include_market_data=True
            )
            
            result = asyncio.run(self.content_engine.generate_and_publish_content(request))
            
            if result["success"]:
                logger.info(f"✅ Commentary published: {result['content']['theme']}")
            else:
                logger.error(f"❌ Commentary failed: {result['error']}")
                
        except Exception as e:
            logger.error(f"❌ Commentary job crashed: {e}")
    
    def _run_briefing(self, period: str):
        """Run briefing generation job (placeholder)"""
        try:
            logger.info(f"📋 {period.title()} briefing scheduled (pending implementation)")
            
            asyncio.run(self.telegram.notify_system_message(
                f"📋 {period.title()} briefing scheduled but not yet implemented"
            ))
            
        except Exception as e:
            logger.error(f"❌ Briefing job crashed: {e}")
    
    def _run_deep_dive(self):
        """Run deep dive generation job (placeholder)"""
        try:
            current_day = datetime.now().strftime("%A")
            
            if current_day not in self.deep_dive_days:
                logger.info(f"⏭️ Skipping deep dive - not scheduled for {current_day}")
                return
                
            logger.info(f"🧵 Deep dive scheduled (pending implementation)")
            
            asyncio.run(self.telegram.notify_system_message(
                f"🧵 Deep dive thread scheduled but not yet implemented"
            ))
            
        except Exception as e:
            logger.error(f"❌ Deep dive job crashed: {e}")
    
    def _daily_maintenance(self):
        """Daily maintenance tasks"""
        try:
            logger.info("🔧 Running daily maintenance")
            
            status = asyncio.run(self.content_engine.get_pipeline_status())
            
            if status.get("error"):
                asyncio.run(self.telegram.notify_system_alert(
                    "⚠️ System Health Issue",
                    f"Daily health check failed: {status['error']}"
                ))
            else:
                asyncio.run(self.telegram.notify_system_message(
                    f"🔧 Daily Maintenance Complete\n"
                    f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
                ))
            
            logger.info("✅ Daily maintenance completed")
            
        except Exception as e:
            logger.error(f"❌ Daily maintenance failed: {e}")
    
    def _send_daily_summary(self):
        """Send daily summary"""
        try:
            today = datetime.now().strftime("%A")
            expected_tweets = 11 if today in ["Saturday", "Sunday"] else 15
            
            asyncio.run(self.telegram.notify_system_message(
                f"📊 Daily Summary - {today}\n"
                f"Expected tweets: {expected_tweets}\n"
                f"Status: Scheduler running"
            ))
            
        except Exception as e:
            logger.error(f"❌ Daily summary failed: {e}")
    
    def start_scheduler(self):
        """Start the scheduler loop"""
        logger.info("🚀 Starting HedgeFund Agent Scheduler")
        
        asyncio.run(self.telegram.notify_system_message(
            "🚀 HedgeFund Agent Scheduler Started\n"
            f"Mode: Production\n"
            f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ))
        
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)
                
            except KeyboardInterrupt:
                logger.info("👋 Scheduler stopped by user")
                asyncio.run(self.telegram.notify_system_message(
                    "👋 HedgeFund Agent Scheduler Stopped"
                ))
                break
                
            except Exception as e:
                logger.error(f"❌ Scheduler error: {e}")
                asyncio.run(self.telegram.notify_system_alert(
                    "🚨 Scheduler Error",
                    f"Error: {str(e)}\nScheduler continuing..."
                ))
                time.sleep(300)
    
    def get_schedule_info(self):
        """Get schedule information"""
        jobs = schedule.jobs
        
        return {
            "total_jobs": len(jobs),
            "next_job": schedule.next_run() if jobs else None,
            "jobs_by_type": {
                "commentary": len([j for j in jobs if "_run_commentary" in str(j.job_func)]),
                "briefings": len([j for j in jobs if "_run_briefing" in str(j.job_func)]),
                "deep_dives": len([j for j in jobs if "_run_deep_dive" in str(j.job_func)]),
                "headlines": len([j for j in jobs if "_run_headline_fetch" in str(j.job_func)]),
                "maintenance": len([j for j in jobs if "_daily_" in str(j.job_func)])
            }
        }


def main():
    """Main scheduler execution"""
    scheduler = HedgeFundScheduler()
    scheduler.setup_weekday_schedule()
    
    info = scheduler.get_schedule_info()
    logger.info(f"📋 Schedule loaded: {info['total_jobs']} total jobs")
    logger.info(f"📊 Jobs breakdown: {info['jobs_by_type']}")
    logger.info(f"⏰ Next job: {info['next_job']}")
    
    scheduler.start_scheduler()


def test_single_commentary():
    """Test single commentary generation"""
    engine = ContentEngine()
    
    request = ContentRequest(
        content_type=ContentType.COMMENTARY,
        category=ContentCategory.MACRO,
        include_market_data=True
    )
    
    result = asyncio.run(engine.generate_and_publish_content(request))
    print(f"Test result: {result}")


def dry_run_schedule():
    """Show schedule without running"""
    scheduler = HedgeFundScheduler()
    scheduler.setup_weekday_schedule()
    
    info = scheduler.get_schedule_info()
    print("🗓️ HedgeFund Scheduler - Dry Run")
    print(f"Total scheduled jobs: {info['total_jobs']}")
    print(f"Next scheduled: {info['next_job']}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            test_single_commentary()
        elif sys.argv[1] == "dry-run":
            dry_run_schedule()
        else:
            print("Usage: python scheduler.py [test|dry-run]")
    else:
        main()