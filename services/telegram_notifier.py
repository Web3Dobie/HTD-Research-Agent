# services/telegram_notifier.py
"""
TelegramNotifier - Clean system notification service for operational monitoring.
Built from scratch for the new HedgeFund Agent architecture.
"""

import logging
import requests
import asyncio
from typing import Optional, Dict, Any
from datetime import datetime, timezone
from enum import Enum

from config.settings import TELEGRAM_CONFIG


class NotificationLevel(Enum):
    """Notification severity levels with emojis"""
    INFO = "ℹ️"
    SUCCESS = "✅" 
    WARNING = "⚠️"
    ERROR = "❌"
    CRITICAL = "🚨"
    START = "🚀"
    COMPLETE = "🎯"
    HEARTBEAT = "💓"


class TelegramNotifier:
    """
    Clean Telegram notification service for system monitoring.
    Handles operational alerts, job status, and health notifications.
    """
    
    def __init__(self, bot_token: Optional[str] = None, chat_id: Optional[str] = None):
        self.logger = logging.getLogger(__name__)
        
        # Get credentials from config (with optional override)
        self.bot_token = bot_token or TELEGRAM_CONFIG['bot_token']
        self.chat_id = chat_id or TELEGRAM_CONFIG['chat_id']
        
        if not self.bot_token:
            self.logger.warning("No Telegram bot token provided - notifications disabled")
            self.enabled = False
        else:
            self.enabled = True
            self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
            
        self.service_name = "HedgeFund Agent"
        self.startup_time = datetime.now(timezone.utc)
    
    async def send_message(self, message: str, level: NotificationLevel = NotificationLevel.INFO) -> bool:
        """
        Send a message to Telegram with formatting.
        
        Args:
            message: The message text to send
            level: Notification level (affects emoji and formatting)
            
        Returns:
            bool: True if message sent successfully, False otherwise
        """
        if not self.enabled:
            self.logger.debug(f"Telegram disabled - would send: {message}")
            return False
        
        try:
            # Format message with emoji and timestamp
            timestamp = datetime.now().strftime('%H:%M:%S')
            formatted_message = f"{level.value} **{timestamp}** | {message}"
            
            # Prepare request
            url = f"{self.base_url}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': formatted_message,
                'parse_mode': 'Markdown',
                'disable_web_page_preview': True
            }
            
            # Send message
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            
            self.logger.debug(f"Telegram message sent: {level.name}")
            return True
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to send Telegram message: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error sending Telegram message: {e}")
            return False
    
    async def notify_startup(self, components: Optional[list] = None):
        """Send system startup notification"""
        message = f"🚀 **{self.service_name} Started**\n"
        message += f"⏰ System initialized successfully"
        
        if components:
            message += f"\n📦 Components loaded: {len(components)}"
            # Show first few components
            for component in components[:3]:
                message += f"\n   • {component}"
            if len(components) > 3:
                message += f"\n   • ... and {len(components) - 3} more"
        
        await self.send_message(message, NotificationLevel.START)
    
    async def notify_job_start(self, job_name: str, details: Optional[str] = None):
        """Notify when a scheduled job starts"""
        message = f"▶️ **Job Started**\n📋 {job_name}"
        
        if details:
            message += f"\n💡 {details}"
        
        await self.send_message(message, NotificationLevel.INFO)
    
    async def notify_job_success(self, job_name: str, duration: float, result_summary: Optional[str] = None):
        """Notify when a job completes successfully"""
        duration_str = f"{duration:.2f}s" if duration < 60 else f"{duration/60:.1f}m"
        
        message = f"✅ **Job Completed**\n"
        message += f"📋 {job_name}\n"
        message += f"⏱️ Duration: {duration_str}"
        
        if result_summary:
            message += f"\n📊 {result_summary}"
        
        await self.send_message(message, NotificationLevel.SUCCESS)
    
    async def notify_job_failure(self, job_name: str, error: str, duration: Optional[float] = None):
        """Notify when a job fails"""
        message = f"❌ **Job Failed**\n📋 {job_name}\n"
        
        if duration:
            duration_str = f"{duration:.2f}s" if duration < 60 else f"{duration/60:.1f}m"
            message += f"⏱️ Duration: {duration_str}\n"
        
        # Truncate long error messages
        error_preview = error[:200] + "..." if len(error) > 200 else error
        message += f"💥 Error: {error_preview}"
        
        await self.send_message(message, NotificationLevel.ERROR)
    
    async def notify_content_published(self, content_type: str, theme: str, url: Optional[str] = None):
        """Notify when content is successfully published"""
        message = f"📢 **Content Published**\n"
        message += f"📝 Type: {content_type}\n"
        message += f"🎯 Theme: {theme}"
        
        if url:
            message += f"\n🔗 [View Tweet]({url})"
        
        await self.send_message(message, NotificationLevel.SUCCESS)
    
    async def notify_system_health(self, health_status: Dict[str, Any]):
        """Send system health summary"""
        message = f"💓 **System Health Check**\n"
        
        services = health_status.get('services', {})
        healthy_count = 0
        total_count = len(services)
        
        for service_name, status in services.items():
            if isinstance(status, dict):
                service_health = status.get('status', 'unknown')
                if service_health == 'healthy':
                    healthy_count += 1
                    emoji = "✅"
                elif service_health == 'degraded':
                    emoji = "⚠️"
                else:
                    emoji = "❌"
            else:
                # Simple boolean status
                emoji = "✅" if status else "❌"
                if emoji == "✅":
                    healthy_count += 1
            
            message += f"\n{emoji} {service_name.replace('_', ' ').title()}"
        
        # Overall health assessment
        if healthy_count == total_count:
            message += f"\n\n🎯 All systems operational ({healthy_count}/{total_count})"
            level = NotificationLevel.HEARTBEAT
        elif healthy_count > total_count // 2:
            message += f"\n\n⚠️ Some issues detected ({healthy_count}/{total_count})"
            level = NotificationLevel.WARNING
        else:
            message += f"\n\n🚨 Multiple system failures ({healthy_count}/{total_count})"
            level = NotificationLevel.CRITICAL
        
        await self.send_message(message, level)
    
    async def notify_rate_limit_warning(self, platform: str = "Twitter"):
        """Notify when approaching rate limits"""
        message = f"⚠️ **Rate Limit Warning**\n"
        message += f"📱 Platform: {platform}\n"
        message += f"🚫 Daily limit reached - posting paused"
        
        await self.send_message(message, NotificationLevel.WARNING)
    
    async def notify_critical_error(self, component: str, error: str, action_required: Optional[str] = None):
        """Send critical system error alert"""
        message = f"🚨 **CRITICAL ERROR**\n"
        message += f"💥 Component: {component}\n"
        
        # Truncate very long errors
        error_preview = error[:300] + "..." if len(error) > 300 else error
        message += f"❌ Error: {error_preview}\n"
        
        if action_required:
            message += f"🔧 Action: {action_required}"
        else:
            message += f"🔧 Action: Manual intervention required"
        
        await self.send_message(message, NotificationLevel.CRITICAL)
    
    async def notify_maintenance_mode(self, enabled: bool, reason: Optional[str] = None):
        """Notify about maintenance mode changes"""
        if enabled:
            message = f"🔧 **Maintenance Mode ON**\n"
            message += f"⏸️ System operations paused"
            if reason:
                message += f"\n💡 Reason: {reason}"
            level = NotificationLevel.WARNING
        else:
            message = f"✅ **Maintenance Mode OFF**\n"
            message += f"▶️ System operations resumed"
            level = NotificationLevel.SUCCESS
        
        await self.send_message(message, level)
    
    async def notify_performance_summary(self, metrics: Dict[str, Any]):
        """Send performance summary"""
        message = f"📊 **Performance Report**\n"
        
        # Content metrics
        if 'content_generated' in metrics:
            message += f"📝 Generated: {metrics['content_generated']}\n"
        
        if 'content_published' in metrics:
            message += f"📢 Published: {metrics['content_published']}\n"
        
        if 'errors' in metrics:
            message += f"❌ Errors: {metrics['errors']}\n"
        
        # System metrics
        if 'uptime_hours' in metrics:
            message += f"⏱️ Uptime: {metrics['uptime_hours']:.1f}h"
        
        await self.send_message(message, NotificationLevel.INFO)
    
    def get_status(self) -> Dict[str, Any]:
        """Get notification service status"""
        uptime = (datetime.now(timezone.utc) - self.startup_time).total_seconds()
        
        return {
            "enabled": self.enabled,
            "service_name": self.service_name,
            "uptime_seconds": uptime,
            "uptime_hours": uptime / 3600,
            "startup_time": self.startup_time.isoformat(),
            "bot_configured": bool(self.bot_token),
            "chat_configured": bool(self.chat_id)
        }


def job_notification_wrapper(notifier: TelegramNotifier, job_name: str):
    """
    Decorator to automatically notify about job execution.
    Use with your scheduler functions.
    
    Example:
        notifier = TelegramNotifier()
        
        @job_notification_wrapper(notifier, "Generate Commentary")
        async def generate_commentary():
            # Your job logic here
            return {"success": True, "url": "https://..."}
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            start_time = datetime.now()
            
            # Notify job start
            await notifier.notify_job_start(job_name)
            
            try:
                # Execute the job
                result = func(*args, **kwargs)
                
                # Handle both sync and async functions
                if asyncio.iscoroutine(result):
                    result = await result
                
                duration = (datetime.now() - start_time).total_seconds()
                
                # Extract summary from result if available
                summary = None
                if isinstance(result, dict):
                    if result.get('success'):
                        summary = "✅ Success"
                        if 'urls' in result and result['urls']:
                            summary += f" - {result['urls'][0]}"
                    else:
                        summary = f"❌ Failed: {result.get('error', 'Unknown error')}"
                
                await notifier.notify_job_success(job_name, duration, summary)
                return result
                
            except Exception as e:
                duration = (datetime.now() - start_time).total_seconds()
                await notifier.notify_job_failure(job_name, str(e), duration)
                raise  # Re-raise the exception
                
        return wrapper
    return decorator


# Convenience functions for easy integration
async def send_startup_notification(components: Optional[list] = None):
    """Quick function to send startup notification"""
    notifier = TelegramNotifier()
    await notifier.notify_startup(components)


async def send_content_notification(content_type: str, theme: str, url: Optional[str] = None):
    """Quick function to notify about published content"""
    notifier = TelegramNotifier()
    await notifier.notify_content_published(content_type, theme, url)


async def send_error_notification(component: str, error: str, action: Optional[str] = None):
    """Quick function to send critical error alerts"""
    notifier = TelegramNotifier()
    await notifier.notify_critical_error(component, error, action)


# Example usage:
if __name__ == "__main__":
    async def test_notifications():
        """Test the notification service"""
        notifier = TelegramNotifier()
        
        print("Testing Telegram notifications...")
        
        # Test basic notification
        await notifier.send_message("🧪 Testing notification service", NotificationLevel.INFO)
        
        # Test startup notification
        await notifier.notify_startup(["ContentEngine", "DatabaseService", "MarketClient"])
        
        # Test job notifications
        await notifier.notify_job_start("Test Job")
        await asyncio.sleep(1)  # Simulate work
        await notifier.notify_job_success("Test Job", 1.2, "✅ Test completed successfully")
        
        # Test health check
        health_status = {
            "services": {
                "database": {"status": "healthy"},
                "market_data": {"status": "healthy"},
                "publishing": {"status": "degraded"}
            }
        }
        await notifier.notify_system_health(health_status)
        
        print("Test notifications sent!")
    
    # Run the test
    asyncio.run(test_notifications())