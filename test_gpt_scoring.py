#!/usr/bin/env python3
"""
Test GPT-based headline scoring
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

def test_gpt_scoring():
    print("🤖 Testing GPT-based Headline Scoring...")
    
    # Initialize services
    db_service = DatabaseService(DATABASE_CONFIG)
    pipeline = HeadlinePipeline(db_service)
    
    # Run the pipeline with GPT scoring
    headlines_stored = pipeline.run_pipeline()
    
    print(f"✅ GPT Pipeline complete! Stored {headlines_stored} high-scoring headlines")
    
    # Show some stats
    conn = db_service.get_connection()
    cursor = conn.cursor()
    
    # Count headlines by score range
    cursor.execute("""
        SELECT 
            CASE 
                WHEN score >= 9 THEN '9-10 (Excellent)'
                WHEN score >= 7 THEN '7-8 (Good)'
                WHEN score >= 5 THEN '5-6 (Fair)'
                ELSE '1-4 (Poor)'
            END as score_range,
            COUNT(*) as count
        FROM hedgefund_agent.headlines 
        GROUP BY 
            CASE 
                WHEN score >= 9 THEN '9-10 (Excellent)'
                WHEN score >= 7 THEN '7-8 (Good)'
                WHEN score >= 5 THEN '5-6 (Fair)'
                ELSE '1-4 (Poor)'
            END
        ORDER BY MIN(score) DESC
    """)
    score_distribution = cursor.fetchall()
    
    # Show top 5 headlines by GPT score
    cursor.execute("""
        SELECT headline, score, category, source 
        FROM hedgefund_agent.headlines 
        ORDER BY score DESC, created_at DESC
        LIMIT 5
    """)
    top_headlines = cursor.fetchall()
    
    print(f"\n📊 GPT Scoring Distribution:")
    for score_range, count in score_distribution:
        print(f"  {score_range}: {count} headlines")
    
    print(f"\n🏆 Top 5 GPT-Scored Headlines:")
    for i, (headline, score, category, source) in enumerate(top_headlines, 1):
        print(f"{i}. [{score}] {headline[:70]}... ({category}, {source})")
    
    cursor.close()
    db_service.close_connection()

if __name__ == "__main__":
    test_gpt_scoring()