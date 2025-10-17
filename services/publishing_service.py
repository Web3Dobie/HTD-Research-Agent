# services/publishing_service.py
"""
PublishingService - Pure Twitter API v2 implementation with media support.
Scalable, future-proof design with proper error handling and rate limiting.
"""

import logging
import os
import mimetypes
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
    Pure Twitter API v2 publishing service with media support.
    Scalable design for high-volume content publishing.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Cache username to avoid repeated API calls
        self.username = None
        self.user_id = None
        
        # Get Twitter credentials from config
        self.consumer_key = TWITTER_CONFIG['consumer_key']
        self.consumer_secret = TWITTER_CONFIG['consumer_secret']
        self.access_token = TWITTER_CONFIG['access_token']
        self.access_token_secret = TWITTER_CONFIG['access_token_secret']
        
        # Initialize Twitter client (API v2 only)
        self.client = None
        self._initialize_client()
        
    def _initialize_client(self):
        """Initialize Twitter API v2 client with error handling"""
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
            
            # Cache username to avoid rate limits (fallback approach)
            self.username = "Dutch_Brat"  # Your actual username
            self.user_id = "unknown"
            self.logger.info("✅ Twitter API v2 client initialized")
                
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize Twitter client: {e}")
            self.client = None

    def upload_media_v2(self, image_path: str) -> Optional[str]:
        """
        Upload media using Twitter API v2 media upload.
        
        Args:
            image_path: Path to the image file
            
        Returns:
            media_id string if successful, None if failed
        """
        try:
            if not os.path.exists(image_path):
                self.logger.error(f"Image file not found: {image_path}")
                return None
            
            # Check file size (Twitter limit is 5MB for images)
            file_size = os.path.getsize(image_path)
            if file_size > 5 * 1024 * 1024:  # 5MB
                self.logger.error(f"Image file too large: {file_size} bytes")
                return None
            
            # Detect media type
            mime_type, _ = mimetypes.guess_type(image_path)
            if not mime_type or not mime_type.startswith('image/'):
                self.logger.error(f"Invalid image file type: {mime_type}")
                return None
            
            # Create OAuth1 handler for media upload (API v2 doesn't support direct media upload yet)
            auth = tweepy.OAuth1UserHandler(
                self.consumer_key, self.consumer_secret,
                self.access_token, self.access_token_secret
            )
            api_v1 = tweepy.API(auth)
            
            # Upload media using v1.1 endpoint (this is still the standard approach)
            with open(image_path, 'rb') as image_file:
                response = api_v1.media_upload(filename=image_path, file=image_file)
                
            if hasattr(response, 'media_id'):
                self.logger.info(f"Successfully uploaded media: {response.media_id}")
                return str(response.media_id)
            else:
                self.logger.error("Media upload response missing media_id")
                return None
                
        except Exception as e:
            self.logger.error(f"Failed to upload media: {e}")
            return None

    def publish_tweet(self, content: GeneratedContent, image_path: str = None) -> TwitterResult:
        """
        Publish a tweet with optional media using pure API v2.
        
        Args:
            content: GeneratedContent object with tweet text
            image_path: Optional path to image file
            
        Returns:
            TwitterResult with success/failure details
        """
        if not self.client:
            return TwitterResult(
                success=False,
                error="Twitter client not initialized"
            )
        
        try:
            # Prepare tweet parameters
            tweet_params = {
                'text': content.text
            }
            
            # Upload media if provided
            if image_path:
                media_id = self.upload_media_v2(image_path)
                if media_id:
                    tweet_params['media_ids'] = [media_id]
                    self.logger.info(f"Added media to tweet: {media_id}")
                else:
                    self.logger.warning("Failed to upload media, posting text-only tweet")
            
            # Publish tweet using API v2
            response = self.client.create_tweet(**tweet_params)
            
            if not response or not response.data:
                return TwitterResult(
                    success=False,
                    error="No response data from Twitter API"
                )
            
            # Extract tweet details
            tweet_id = response.data['id']
            username = self.username or "unknown"
            url = f"https://x.com/{username}/status/{tweet_id}"
            timestamp = datetime.now(timezone.utc).isoformat()
            
            self.logger.info(f"✅ Tweet published successfully: {url}")
            if image_path and 'media_ids' in tweet_params:
                self.logger.info(f"📊 Tweet includes media attachment")
            
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

    def publish_thread(self, content: GeneratedContent) -> TwitterResult:
        """
        Publish a multi-part thread to Twitter using API v2.
        
        Args:
            content: GeneratedContent with parts array for thread
            
        Returns:
            TwitterResult with main tweet details
        """
        try:
            if not self.client:
                return TwitterResult(success=False, error="Twitter client not initialized")
            
            if not content.parts:
                return TwitterResult(success=False, error="No thread parts to publish")
            
            self.logger.info(f"🧵 Publishing {len(content.parts)}-part thread: {content.theme}")
            
            # Post the thread
            thread_tweets = []
            reply_to_id = None
            
            for i, part in enumerate(content.parts):
                try:
                    # Prepare tweet parameters
                    tweet_params = {'text': part}
                    if reply_to_id:
                        tweet_params['in_reply_to_tweet_id'] = reply_to_id
                    
                    # Post each part as a reply to the previous
                    response = self.client.create_tweet(**tweet_params)
                    
                    thread_tweets.append(response)
                    reply_to_id = response.data['id']  # Next tweet replies to this one
                    
                    self.logger.info(f"✅ Posted thread part {i+1}/{len(content.parts)}: {response.data['id']}")
                    
                except Exception as e:
                    self.logger.error(f"❌ Failed to post thread part {i+1}: {e}")
                    # If first tweet fails, return error. If later tweets fail, continue with partial thread
                    if i == 0:
                        return TwitterResult(success=False, error=f"Failed to post thread starter: {e}")
                    else:
                        self.logger.warning(f"⚠️ Thread partially posted - {i} of {len(content.parts)} parts successful")
                        break
            
            if not thread_tweets:
                return TwitterResult(success=False, error="No thread parts were posted successfully")
            
            # Return result based on first tweet (main thread)
            main_tweet = thread_tweets[0]
            username = self.username or "unknown"
            tweet_url = f"https://x.com/{username}/status/{main_tweet.data['id']}"
            
            self.logger.info(f"🎉 Thread published successfully: {tweet_url}")
            
            return TwitterResult(
                success=True,
                tweet_id=main_tweet.data['id'],
                url=tweet_url,
                timestamp=datetime.now(timezone.utc).isoformat()
            )
            
        except Exception as e:
            self.logger.error(f"❌ Failed to publish thread: {e}")
            return TwitterResult(success=False, error=str(e))

    def publish_text(self, text: str) -> TwitterResult:
        """
        Convenience method to publish raw text as a tweet.
        
        Args:
            text: The tweet text to publish
            
        Returns:
            TwitterResult with success status and details
        """
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
                ])),
                "api_version": "v2"
            }
        
        return {
            "status": "connected",
            "username": self.username,
            "user_id": self.user_id,
            "last_check": "cached",
            "api_version": "v2",
            "note": "Using cached credentials to avoid rate limits"
        }

    # Legacy method aliases for backward compatibility
    def upload_media(self, image_path: str) -> Optional[str]:
        """Legacy alias for upload_media_v2"""
        return self.upload_media_v2(image_path)
    
    def publish_tweet_with_media(self, content: GeneratedContent, image_path: str = None) -> TwitterResult:
        """Legacy alias for publish_tweet with media"""
        return self.publish_tweet(content, image_path)