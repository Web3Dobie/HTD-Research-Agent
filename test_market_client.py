#!/usr/bin/env python3
"""
Test market client directly
"""
import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from services.market_client import MarketClient

async def test_market_client():
    print("📈 Testing Market Client...")
    
    client = MarketClient()
    
    # Test 1: Single ticker
    print("\nTest 1: Get single ticker (AAPL)")
    try:
        data = await client.get_price("AAPL")
        print(f"Result: {data}")
    except Exception as e:
        print(f"Error: {e}")
    
    # Test 2: Tickers mentioned in commentary
    print("\nTest 2: Get tickers from commentary (ARKK, XLF)")
    for ticker in ["ARKK", "XLF", "BTC"]:
        try:
            data = await client.get_price(ticker)
            print(f"{ticker}: {data}")
        except Exception as e:
            print(f"{ticker} Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_market_client())