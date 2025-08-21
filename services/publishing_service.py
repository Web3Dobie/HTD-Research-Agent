# services/publishing_service.py
"""
PublishingService - Clean Twitter posting service.
Starts minimal with single tweet posting, expandable as needed.
"""

import logging
from typing import Optional, List
from dataclasses import dataclass
from datetime import datetime, timezone

import tweepy

from core.models import GeneratedContent
from config.settings import TWITTER_CONFIG


@dataclass
class TwitterResult:
    """Result of a Twitter publishing operation"""
    success: bool
    tweet_id: Optional[str] = None
    url: Optional[str] = None
    error: Optional[str] = None
    timestamp: Optional[str] = None


class PublishingService:
    """
    Clean Twitter publishing service.
    Focused on posting content to Twitter/X platform.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Get Twitter credentials from config
        self.consumer_key = TWITTER_CONFIG['consumer_key']
        self.consumer_secret = TWITTER_CONFIG['consumer_secret']
        self.access_token = TWITTER_CONFIG['access_token']
        self.access_token_secret = TWITTER_CONFIG['access_token_secret']
        
        # Initialize Twitter client
        self.client = None
        self._initialize_client()
        
    def _initialize_client(self):
        """Initialize Twitter client with error handling"""
        try:
            if not all([self.consumer_key, self.consumer_secret, 
                       self.access_token, self.access_token_secret]):
                self.logger.error("Missing Twitter API credentials")
                return
            
            self.client = tweepy.Client(
                consumer_key=self.consumer_key,
                consumer_secret=self.consumer_secret,
                access_token=self.access_token,
                access_token_secret=self.access_token_secret,
                wait_on_rate_limit=True
            )
            
            # Test the connection
            me = self.client.get_me()
            if me and me.data:
                self.logger.info(f"✅ Twitter client initialized for @{me.data.username}")
            else:
                self.logger.warning("⚠️ Twitter client created but user verification failed")
                
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize Twitter client: {e}")
            self.client = None
    
    def publish_tweet(self, content: GeneratedContent) -> TwitterResult:
        """
        Publish a single tweet.
        
        Args:
            content: GeneratedContent object containing the tweet text and metadata
            
        Returns:
            TwitterResult with success status and details
        """
        if not self.client:
            return TwitterResult(
                success=False,
                error="Twitter client not initialized"
            )
        
        try:
            # Post the tweet
            response = self.client.create_tweet(text=content.text)
            
            if not response or not response.data:
                return TwitterResult(
                    success=False,
                    error="No response data from Twitter API"
                )
            
            # Extract tweet details
            tweet_id = response.data['id']
            
            # Get username for URL (assuming we have it from initialization)
            try:
                me = self.client.get_me()
                username = me.data.username if me and me.data else "unknown"
            except:
                username = "unknown"
            
            url = f"https://x.com/{username}/status/{tweet_id}"
            timestamp = datetime.now(timezone.utc).isoformat()
            
            self.logger.info(f"✅ Tweet published successfully: {url}")
            
            return TwitterResult(
                success=True,
                tweet_id=tweet_id,
                url=url,
                timestamp=timestamp
            )
            
        except tweepy.TooManyRequests:
            error_msg = "Twitter rate limit exceeded"
            self.logger.warning(f"⚠️ {error_msg}")
            return TwitterResult(success=False, error=error_msg)
            
        except tweepy.Forbidden as e:
            error_msg = f"Twitter API forbidden: {e}"
            self.logger.error(f"❌ {error_msg}")
            return TwitterResult(success=False, error=error_msg)
            
        except tweepy.HTTPException as e:
            error_msg = f"Twitter API error: {e}"
            self.logger.error(f"❌ {error_msg}")
            return TwitterResult(success=False, error=error_msg)
            
        except Exception as e:
            error_msg = f"Unexpected error publishing tweet: {e}"
            self.logger.error(f"❌ {error_msg}")
            return TwitterResult(success=False, error=error_msg)
    
    def publish_text(self, text: str) -> TwitterResult:
        """
        Convenience method to publish raw text as a tweet.
        Useful for simple testing or direct posting.
        
        Args:
            text: The tweet text to publish
            
        Returns:
            TwitterResult with success status and details
        """
        # Create a minimal GeneratedContent object
        from core.models import GeneratedContent, ContentType
        
        content = GeneratedContent(
            text=text,
            content_type=ContentType.COMMENTARY,
            theme="manual_post",
            category=None,
            market_data=[]
        )
        
        return self.publish_tweet(content)
    
    def get_client_status(self) -> dict:
        """
        Get the status of the Twitter client for monitoring.
        
        Returns:
            Dict with client status information
        """
        if not self.client:
            return {
                "status": "disconnected",
                "error": "Client not initialized",
                "credentials_configured": bool(all([
                    self.consumer_key, self.consumer_secret,
                    self.access_token, self.access_token_secret
                ]))
            }
        
        try:
            # Test API access
            me = self.client.get_me()
            if me and me.data:
                return {
                    "status": "connected",
                    "username": me.data.username,
                    "user_id": me.data.id,
                    "last_check": datetime.now(timezone.utc).isoformat()
                }
            else:
                return {
                    "status": "error",
                    "error": "Failed to get user information"
                }
                
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }


# Convenience function for easy testing
def quick_tweet(text: str) -> TwitterResult:
    """
    Quick function to post a tweet.
    Useful for testing or simple posting.
    
    Args:
        text: The tweet text to publish
        
    Returns:
        TwitterResult with success status and details
    """
    service = PublishingService()
    return service.publish_text(text)


# Example usage and testing
if __name__ == "__main__":
    import asyncio
    
    def test_publishing_service():
        """Test the publishing service"""
        print("🧪 Testing PublishingService...")
        
        # Initialize service
        service = PublishingService()
        
        # Check client status
        status = service.get_client_status()
        print(f"Client Status: {status}")
        
        if status["status"] != "connected":
            print("❌ Twitter client not connected - check credentials")
            return
        
        # Test posting (uncomment when ready to actually post)
        # result = service.publish_text("🧪 Testing new PublishingService - ignore this test tweet")
        # print(f"Tweet Result: {result}")
        
        print("✅ PublishingService test completed")
    
    # Run the test
    test_publishing_service()