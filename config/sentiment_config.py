# config/sentiment_config.py
"""
Configuration for the ComprehensiveMarketSentimentService.

This module contains the weights and thresholds used to calculate
overall market sentiment. Separating this allows for easy tuning
without altering the core application logic.
"""

SENTIMENT_CONFIG = {
    'section_weights': {
        'asian_focus': 0.15,      # Asian market influence overnight
        'european_futures': 0.20,  # European market direction
        'us_futures': 0.30,       # US markets have the highest weight
        'volatility': 0.15,       # VIX is a key fear indicator
        'fx': 0.10,               # Currency markets reflect risk appetite
        'rates': 0.07,            # Bond yields provide economic signals
        'crypto': 0.03            # Crypto is a barometer for speculative risk
    },
    'sentiment_thresholds': {
        # Performance thresholds for classifying a section's sentiment
        "strong_bullish": 1.5,   # avg % change > 1.5%
        "bullish": 0.5,          # avg % change > 0.5%
        "bearish": -0.5,         # avg % change < -0.5%
        "strong_bearish": -1.5   # avg % change < -1.5%
    },
    'overall_sentiment_boundaries': {
        # Weighted score boundaries for final sentiment classification
        "bullish": 0.4,
        "bearish": -0.4
    }
}