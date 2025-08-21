"""
Debug the full enrichment flow
"""
import re
import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from services.market_client import MarketClient

def extract_cashtags(text: str):
    pattern = r'\$[A-Z]{1,5}\b'
    cashtags = re.findall(pattern, text, re.IGNORECASE)
    print(f"🏷️ Found cashtags: {cashtags}")
    return cashtags

def is_valid_ticker(ticker: str) -> bool:
    valid = (len(ticker) >= 1 and len(ticker) <= 5 and 
             ticker.isalpha() and ticker.isupper())
    print(f"✅ Ticker '{ticker}' valid: {valid}")
    return valid

async def debug_enrichment():
    print("🔍 Debug Full Enrichment Flow...")
    
    # Test text from actual commentary
    text = "Fed's Schmid signals steady rates with inflation risks outweighing labor concerns. Bond yields may stabilize, but equities like $SPY face valuation pressure if inflation persists."
    
    print(f"📝 Text: {text}")
    print()
    
    # Step 1: Extract cashtags
    cashtags = extract_cashtags(text)
    
    # Step 2: Validate tickers
    valid_tickers = []
    for tag in cashtags:
        ticker = tag.strip("$").upper()
        print(f"🔍 Processing tag '{tag}' -> ticker '{ticker}'")
        if is_valid_ticker(ticker):
            valid_tickers.append(ticker)
    
    print(f"✅ Valid tickers: {valid_tickers}")
    print()
    
    # Step 3: Test market client
    if valid_tickers:
        client = MarketClient()
        for ticker in valid_tickers:
            print(f"💰 Testing market data for {ticker}...")
            try:
                data = await client.get_price(ticker)
                print(f"📊 Result: {data}")
            except Exception as e:
                print(f"❌ Error: {e}")
                import traceback
                traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(debug_enrichment())