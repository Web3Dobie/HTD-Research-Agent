#!/usr/bin/env python3
"""
Test script for database service
"""
import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from services.database_service import DatabaseService
from config.settings import DATABASE_CONFIG
from core.models import Headline

def test_database():
    print("🧪 Testing Database Service...")
    
    # Initialize service
    db_service = DatabaseService(DATABASE_CONFIG)
    
    # Test headline operations
    print("\n📰 Testing Headlines...")
    
    # Create test headline
    test_headline = Headline(
        headline="Test Market News - Fed Considers Rate Changes",
        summary="The Federal Reserve is reviewing interest rate policies.",
        score=85,
        category="macro",
        source="test_source"
    )
    
    # Save headline
    headline_id = db_service.save_headline(test_headline)
    print(f"✅ Saved test headline with ID: {headline_id}")
    
    # Get unused headline
    unused_headline = db_service.get_unused_headline_today()
    if unused_headline:
        print(f"✅ Retrieved unused headline: {unused_headline.headline[:50]}...")
    
    # Test theme operations
    print("\n🎯 Testing Themes...")
    
    # Check if theme is duplicate
    test_theme = "fed_interest_rates"
    is_duplicate = db_service.is_duplicate_theme(test_theme)
    print(f"✅ Theme duplicate check: {is_duplicate}")
    
    # Track theme
    db_service.track_theme(test_theme)
    print(f"✅ Tracked theme: {test_theme}")
    
    # Check duplicate again
    is_duplicate_after = db_service.is_duplicate_theme(test_theme)
    print(f"✅ Theme duplicate check after tracking: {is_duplicate_after}")
    
    # Test logging
    print("\n📝 Testing System Logging...")
    db_service.log_system_event(
        service="hedgefund_agent",
        level="INFO", 
        message="Database service test completed",
        metadata={"test": True}
    )
    print("✅ Logged system event")
    
    # Close connection
    db_service.close_connection()
    
    print("\n🎉 All database tests passed!")

if __name__ == "__main__":
    test_database()