#!/usr/bin/env python3
"""
Test Tweet Pipeline - Send test tweet and publish to Notion
Tests the complete PublishingService + NotionPublisher integration
"""

import asyncio
import logging
import sys
import os
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.models import GeneratedContent, ContentType
from services.publishing_service import PublishingService
from services.notion_publisher import NotionPublisher
from services.telegram_notifier import TelegramNotifier

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


async def test_complete_pipeline():
    """Test the complete tweet pipeline: Twitter → Notion → Telegram notification"""
    
    print("🧪 Testing Complete Tweet Pipeline")
    print("=" * 50)
    
    # Initialize services
    print("📱 Initializing services...")
    publishing_service = PublishingService()
    notion_publisher = NotionPublisher()
    telegram_notifier = TelegramNotifier()
    
    # Check service status
    twitter_status = publishing_service.get_client_status()
    notion_status = notion_publisher.get_client_status()
    telegram_status = telegram_notifier.get_status()
    
    print(f"Twitter Status: {twitter_status['status']}")
    print(f"Notion Status: {'✅' if notion_status['client_initialized'] else '❌'}")
    print(f"Telegram Status: {'✅' if telegram_status['enabled'] else '❌'}")
    
    if twitter_status['status'] != 'connected':
        print("❌ Twitter not connected - check credentials")
        return False
    
    if not notion_status['client_initialized']:
        print("❌ Notion not initialized - check credentials")
        return False
    
    # Create test content
    test_text = "🚀 Improved HTD Research Agent coming soon! 🤖✨ Enhanced market analysis and smarter content generation on the way. #AI #FinTech #Innovation"
    
    print(f"\n📝 Test Tweet: {test_text}")
    print(f"Length: {len(test_text)} characters")
    
    # Confirm before posting
    response = input("\n⚠️  This will post to Twitter! Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("❌ Test cancelled by user")
        return False
    
    try:
        # Step 1: Create content object
        content = GeneratedContent(
            text=test_text,
            content_type=ContentType.COMMENTARY,
            theme="system_update",
            category=None,  # Will default to commentary
            market_data=[]
        )
        
        print("\n🐦 Step 1: Publishing to Twitter...")
        
        # Step 2: Publish to Twitter
        twitter_result = publishing_service.publish_tweet(content)
        
        if twitter_result.success:
            print(f"✅ Twitter Success!")
            print(f"   Tweet ID: {twitter_result.tweet_id}")
            print(f"   URL: {twitter_result.url}")
            
            # Step 3: Publish to Notion
            print("\n📝 Step 2: Publishing to Notion...")
            notion_page_id = notion_publisher.publish_tweet_to_notion(content, twitter_result)
            
            if notion_page_id:
                print(f"✅ Notion Success!")
                print(f"   Page ID: {notion_page_id}")
                
                # Step 4: Send Telegram notification
                print("\n📲 Step 3: Sending Telegram notification...")
                await telegram_notifier.notify_content_published(
                    content_type="test_tweet",
                    theme="system_update", 
                    url=twitter_result.url
                )
                print("✅ Telegram notification sent!")
                
                # Summary
                print("\n" + "=" * 50)
                print("🎉 COMPLETE PIPELINE TEST SUCCESSFUL!")
                print(f"📱 Twitter: {twitter_result.url}")
                print(f"📝 Notion: Page {notion_page_id}")
                print(f"📲 Telegram: Notification sent")
                print("=" * 50)
                
                return True
                
            else:
                print("❌ Notion publishing failed")
                return False
        else:
            print(f"❌ Twitter publishing failed: {twitter_result.error}")
            return False
            
    except Exception as e:
        print(f"❌ Pipeline test failed: {e}")
        logger.exception("Pipeline test exception")
        return False


async def test_services_only():
    """Test service initialization without posting"""
    print("🔧 Testing Service Initialization Only")
    print("=" * 50)
    
    # Test PublishingService
    print("🐦 Testing Twitter connection...")
    publishing_service = PublishingService()
    twitter_status = publishing_service.get_client_status()
    print(f"Twitter: {twitter_status}")
    
    # Test NotionPublisher
    print("\n📝 Testing Notion connection...")
    notion_publisher = NotionPublisher()
    notion_status = notion_publisher.get_client_status()
    print(f"Notion: {notion_status}")
    
    # Test TelegramNotifier
    print("\n📲 Testing Telegram connection...")
    telegram_notifier = TelegramNotifier()
    telegram_status = telegram_notifier.get_status()
    print(f"Telegram: {telegram_status}")
    
    # Summary
    print("\n" + "=" * 50)
    services_ok = (
        twitter_status['status'] == 'connected' and
        notion_status['client_initialized'] and
        telegram_status['enabled']
    )
    
    if services_ok:
        print("✅ All services initialized successfully!")
        print("Ready for live tweet testing.")
    else:
        print("❌ Some services need configuration:")
        if twitter_status['status'] != 'connected':
            print("   - Check Twitter API credentials")
        if not notion_status['client_initialized']:
            print("   - Check Notion API key and database ID")
        if not telegram_status['enabled']:
            print("   - Check Telegram bot token")
    
    print("=" * 50)
    return services_ok


async def main():
    """Main test runner"""
    print("🧪 HedgeFund Agent Publishing Pipeline Test")
    print("Choose test type:")
    print("1. Service initialization only (safe)")
    print("2. Complete pipeline with live tweet (posts to Twitter)")
    
    choice = input("\nEnter choice (1 or 2): ")
    
    if choice == "1":
        success = await test_services_only()
    elif choice == "2":
        success = await test_complete_pipeline()
    else:
        print("Invalid choice")
        return
    
    if success:
        print("\n🎉 Test completed successfully!")
    else:
        print("\n❌ Test failed - check logs above")


if __name__ == "__main__":
    # Run the test
    asyncio.run(main())