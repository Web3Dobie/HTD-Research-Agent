#!/usr/bin/env python3
"""
Test the headline pipeline
"""
import sys
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from services.headline_pipeline import HeadlinePipeline
from services.database_service import DatabaseService
from config.settings import DATABASE_CONFIG

def test_headline_pipeline():
    print("🗞️ Testing Headline Pipeline...")
    
    # Initialize services
    db_service = DatabaseService(DATABASE_CONFIG)
    pipeline = HeadlinePipeline(db_service)
    
    # Run the pipeline
    headlines_stored = pipeline.run_pipeline()
    
    print(f"✅ Pipeline complete! Stored {headlines_stored} new headlines")
    
    # Show some stats
    conn = db_service.get_connection()
    cursor = conn.cursor()
    
    # Count total headlines
    cursor.execute("SELECT COUNT(*) FROM hedgefund_agent.headlines")
    total_count = cursor.fetchone()[0]
    
    # Show top 5 headlines by score
    cursor.execute("""
        SELECT headline, score, category, source 
        FROM hedgefund_agent.headlines 
        ORDER BY score DESC 
        LIMIT 5
    """)
    top_headlines = cursor.fetchall()
    
    print(f"\n📊 Database Stats:")
    print(f"Total headlines: {total_count}")
    print(f"\n🏆 Top 5 Headlines:")
    for i, (headline, score, category, source) in enumerate(top_headlines, 1):
        print(f"{i}. [{score}] {headline[:60]}... ({category}, {source})")
    
    cursor.close()
    db_service.close_connection()

if __name__ == "__main__":
    test_headline_pipeline()