#!/usr/bin/env python3
"""
Test the commentary generator
"""
import asyncio
import sys
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from generators.commentary_generator import CommentaryGenerator
from services.database_service import DatabaseService
from services.data_service import DataService
from services.gpt_service import GPTService
from services.market_client import MarketClient
from config.settings import DATABASE_CONFIG, AGENT_NAME
from core.models import ContentRequest, ContentType

async def test_commentary_generator():
    print("🐂 Testing Commentary Generator...")
    
    # Initialize services
    db_service = DatabaseService(DATABASE_CONFIG)
    data_service = DataService(db_service)
    gpt_service = GPTService()
    market_client = MarketClient()
    
    # Configuration
    config = {
        'agent_name': AGENT_NAME,
        'default_disclaimer': 'This is my opinion. Not financial advice.'
    }
    
    # Initialize generator
    generator = CommentaryGenerator(
        data_service=data_service,
        gpt_service=gpt_service,
        market_client=market_client,
        config=config
    )
    
    # Test 1: Generate commentary with market data
    print("\n📰 Test 1: Generate commentary with market data")
    request = ContentRequest(
        content_type=ContentType.COMMENTARY,
        include_market_data=True
    )
    
    try:
        content = await generator.generate(request)
        
        print(f"✅ Generated Commentary:")
        print(f"📝 Text: {content.text}")
        print(f"🎯 Theme: {content.theme}")
        print(f"📂 Category: {content.category}")
        print(f"📰 Headline used: {content.headline_used.headline if content.headline_used else 'None'}")
        print(f"💰 Market data: {len(content.market_data)} tickers")
        
        if content.market_data:
            for md in content.market_data:
                print(f"  💹 {md.ticker}: ${md.price:.2f} ({md.change_percent:+.2f}%)")
    
    except Exception as e:
        print(f"❌ Test 1 failed: {e}")
    
    # Test 2: Generate without market data
    print("\n📰 Test 2: Generate commentary without market data")
    request = ContentRequest(
        content_type=ContentType.COMMENTARY,
        include_market_data=False
    )
    
    try:
        content = await generator.generate(request)
        
        print(f"✅ Generated Commentary (No Market Data):")
        print(f"📝 Text: {content.text}")
        print(f"🎯 Theme: {content.theme}")
        
    except Exception as e:
        print(f"❌ Test 2 failed: {e}")
    
    # Show remaining unused headlines
    print("\n📊 Remaining unused headlines:")
    conn = db_service.get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT COUNT(*) as unused_count,
               COUNT(*) FILTER (WHERE score >= 9) as excellent,
               COUNT(*) FILTER (WHERE score >= 7) as good
        FROM hedgefund_agent.headlines 
        WHERE used = false
    """)
    stats = cursor.fetchone()
    
    print(f"  Unused headlines: {stats[0]}")
    print(f"  Excellent (9+): {stats[1]}")
    print(f"  Good (7+): {stats[2]}")
    
    cursor.close()
    db_service.close_connection()

if __name__ == "__main__":
    asyncio.run(test_commentary_generator())