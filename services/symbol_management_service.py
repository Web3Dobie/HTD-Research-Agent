# services/symbol_management_service.py
"""
SymbolManagementService - Dedicated service for managing symbols in stock_universe.
This is what you wanted - easy control over symbol management!
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional
from services.database_service import DatabaseService

logger = logging.getLogger(__name__)

class SymbolManagementService:
    """
    Dedicated service for managing symbols in the stock_universe table.
    Handles: adding, updating, listing, and organizing symbols.
    """
    
    def __init__(self, db_service: DatabaseService):
        self.db_service = db_service
        logger.info("🔧 SymbolManagementService initialized")
    
    async def update_symbol(self, old_symbol: str, new_symbol: str, 
                           new_display_name: str = None, new_epic: str = None) -> bool:
        """
        Update a symbol - this is what you wanted for easy symbol management!
        
        Args:
            old_symbol: Current symbol (e.g. '^GSPC')
            new_symbol: New symbol (e.g. 'SPY') 
            new_display_name: Optional new display name
            new_epic: Optional new EPIC for IG Index
            
        Returns:
            bool: Success status
        """
        def db_update():
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            try:
                # Build dynamic update query
                update_parts = ["symbol = %s", "last_updated = NOW()"]
                params = [new_symbol]
                
                if new_display_name:
                    update_parts.append("display_name = %s")
                    params.append(new_display_name)
                
                if new_epic:
                    update_parts.append("epic = %s") 
                    params.append(new_epic)
                
                update_sql = f"""
                    UPDATE hedgefund_agent.stock_universe
                    SET {', '.join(update_parts)}
                    WHERE symbol = %s
                """
                params.append(old_symbol)
                
                cursor.execute(update_sql, params)
                rows_affected = cursor.rowcount
                conn.commit()
                
                return rows_affected > 0
                
            except Exception as e:
                conn.rollback()
                logger.error(f"❌ Failed to update symbol {old_symbol} -> {new_symbol}: {e}")
                return False
            finally:
                cursor.close()
        
        try:
            success = await asyncio.to_thread(db_update)
            if success:
                logger.info(f"✅ Updated symbol: {old_symbol} -> {new_symbol}")
                if new_display_name:
                    logger.info(f"   Display name: {new_display_name}")
                if new_epic:
                    logger.info(f"   EPIC: {new_epic}")
            else:
                logger.warning(f"⚠️ Symbol {old_symbol} not found")
            return success
        except Exception as e:
            logger.error(f"❌ Symbol update failed: {e}")
            return False
    
    async def list_symbols(self, asset_type: str = None, discovery_method: str = None) -> List[Dict]:
        """
        List symbols with optional filtering.
        
        Args:
            asset_type: Filter by type ('stock', 'index', 'forex', etc.)
            discovery_method: Filter by how discovered ('migrated', 'auto_discovery', etc.)
            
        Returns:
            List of symbol dicts
        """
        def db_query():
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            try:
                query = """
                    SELECT symbol, display_name, asset_type, epic, discovery_method
                    FROM hedgefund_agent.stock_universe
                    WHERE active = TRUE
                """
                params = []
                
                if asset_type:
                    query += " AND asset_type = %s"
                    params.append(asset_type)
                
                if discovery_method:
                    query += " AND discovery_method = %s"
                    params.append(discovery_method)
                
                query += " ORDER BY asset_type, symbol"
                
                cursor.execute(query, params)
                return cursor.fetchall()
            finally:
                cursor.close()
        
        try:
            rows = await asyncio.to_thread(db_query)
            return [
                {
                    'symbol': row[0],
                    'display_name': row[1],
                    'asset_type': row[2], 
                    'epic': row[3],
                    'discovery_method': row[4]
                }
                for row in rows
            ]
        except Exception as e:
            logger.error(f"❌ Failed to list symbols: {e}")
            return []
    
    async def add_symbol(self, symbol: str, display_name: str, asset_type: str, 
                        epic: str = None) -> bool:
        """Add a new symbol to the universe"""
        def db_insert():
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            try:
                cursor.execute("""
                    INSERT INTO hedgefund_agent.stock_universe 
                    (symbol, display_name, asset_type, epic, discovery_method, discovered_at)
                    VALUES (%s, %s, %s, %s, 'manual', NOW())
                """, (symbol, display_name, asset_type, epic))
                
                conn.commit()
                return True
                
            except Exception as e:
                conn.rollback()
                logger.error(f"❌ Failed to add symbol {symbol}: {e}")
                return False
            finally:
                cursor.close()
        
        try:
            success = await asyncio.to_thread(db_insert)
            if success:
                logger.info(f"✅ Added symbol: {symbol} ({display_name})")
            return success
        except Exception as e:
            logger.error(f"❌ Add symbol failed: {e}")
            return False
    
    async def get_symbols_needing_epics(self) -> List[Dict]:
        """Get symbols that don't have EPIC codes yet"""
        def db_query():
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            try:
                cursor.execute("""
                    SELECT symbol, display_name, asset_type
                    FROM hedgefund_agent.stock_universe
                    WHERE active = TRUE
                    AND epic IS NULL
                    ORDER BY asset_type, symbol
                """)
                return cursor.fetchall()
            finally:
                cursor.close()
        
        try:
            rows = await asyncio.to_thread(db_query)
            return [
                {
                    'symbol': row[0],
                    'display_name': row[1],
                    'asset_type': row[2]
                }
                for row in rows
            ]
        except Exception as e:
            logger.error(f"❌ Failed to get symbols needing EPICs: {e}")
            return []

# Convenience functions for easy symbol management
async def change_symbol(old_symbol: str, new_symbol: str, new_display_name: str = None):
    """Quick function to change a symbol"""
    from config.settings import DATABASE_CONFIG
    
    db_service = DatabaseService(DATABASE_CONFIG)
    symbol_service = SymbolManagementService(db_service)
    
    return await symbol_service.update_symbol(old_symbol, new_symbol, new_display_name)

async def list_yahoo_symbols():
    """List all the weird Yahoo symbols that need fixing"""
    from config.settings import DATABASE_CONFIG
    
    db_service = DatabaseService(DATABASE_CONFIG)
    symbol_service = SymbolManagementService(db_service)
    
    symbols = await symbol_service.list_symbols()
    yahoo_symbols = [s for s in symbols if s['symbol'].startswith('^')]
    
    print(f"📊 Found {len(yahoo_symbols)} Yahoo symbols to fix:")
    for sym in yahoo_symbols:
        print(f"  {sym['symbol']} -> {sym['display_name']}")
    
    return yahoo_symbols

# Example usage
if __name__ == "__main__":
    async def test_symbol_management():
        """Test the symbol management service"""
        print("🧪 Testing Symbol Management...")
        
        # List Yahoo symbols that need fixing
        yahoo_symbols = await list_yahoo_symbols()
        
        # Example: Fix ^GSPC -> SPY
        if any(s['symbol'] == '^GSPC' for s in yahoo_symbols):
            print("\n🔧 Fixing ^GSPC -> SPY...")
            success = await change_symbol('^GSPC', 'SPY', 'S&P 500 ETF')
            print(f"Result: {'✅ Success' if success else '❌ Failed'}")
    
    asyncio.run(test_symbol_management())