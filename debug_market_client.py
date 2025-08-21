#!/usr/bin/env python3
"""
Debug the market client step by step
"""
import asyncio
import aiohttp
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

async def debug_market_request():
    print("🔍 Debug Market Client Request...")
    
    base_url = "http://localhost:8001"
    ticker = "SPY"
    
    try:
        print(f"Making request to: {base_url}/prices/{ticker}")
        
        async with aiohttp.ClientSession() as session:
            url = f"{base_url}/prices/{ticker}"
            print(f"Full URL: {url}")
            
            async with session.get(url, timeout=10) as response:
                print(f"Response status: {response.status}")
                print(f"Response headers: {dict(response.headers)}")
                
                if response.status == 200:
                    data = await response.json()
                    print(f"Response data: {data}")
                    
                    # Test the specific fields we need
                    price = data.get('price')
                    change_percent = data.get('change_percent')
                    print(f"Extracted - Price: {price}, Change: {change_percent}")
                    
                else:
                    text = await response.text()
                    print(f"Error response: {text}")
                    
    except asyncio.TimeoutError:
        print("❌ Request timed out")
    except Exception as e:
        print(f"❌ Exception: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(debug_market_request())