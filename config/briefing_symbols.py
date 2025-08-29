MORNING_BRIEFING_CONFIG = {
    'market_data_sections': {
        'asian_focus': {
            'symbols': ['^N225', '^HSI', '000001.SS'],
            'display_order': ['Nikkei 225', 'Hang Seng', 'China A50']
        },
        'european_futures': {
            'symbols': ['^GDAXI', '^FTSE', '^FCHI', '^STOXX50E'],
            'display_order': ['DAX', 'FTSE 100', 'CAC 40', 'Euro Stoxx 50']
        },
        'us_futures': {
            'symbols': ['^GSPC', '^IXIC'],
            'display_order': ['S&P 500', 'Nasdaq']
        },
        'volatility': {
            'symbols': ['^VIX'],
            'display_order': ['VIX']
        },
        'fx': {
            'symbols': ['EURUSD', 'USDJPY', 'GBPUSD', "AUDUSD", "USDCAD"],
            'display_order': ['EUR/USD', 'USD/JPY', 'GBP/USD', 'AUD/USD', 'USD/CAD']
        },
        'rates': {
            'symbols': ['2YEAR', '5YEAR', '10YEAR'],
            'display_order': ['2-year', '5-Year', '10-Year']
        },
        'crypto': {
            'symbols': ['BTC', 'ETH', 'XRP', 'SOL', 'ADA'],
            'display_order': ['Bitcoin', 'Ethereum', 'Ripple', 'Solana', 'Cardano']
    }
}
}
