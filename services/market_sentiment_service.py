# services/market_sentiment_service.py
"""
Comprehensive Market Sentiment Service - Analyzes Full Briefing Dataset
Works with your briefing symbol configurations to provide institutional-grade sentiment analysis
"""

import logging
import asyncio
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum

# Import your existing services
from services.market_client import MarketClient
from services.gpt_service import GPTService

logger = logging.getLogger(__name__)


class MarketSentiment(Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH" 
    NEUTRAL = "NEUTRAL"
    MIXED = "MIXED"


@dataclass
class SectionAnalysis:
    """Analysis for a specific market section (e.g., Asian Focus, European Futures)"""
    section_name: str
    symbols_analyzed: int
    avg_performance: float  # Average % change
    dominant_direction: str  # "up", "down", "mixed"
    key_movers: List[str]  # Top movers in this section
    section_sentiment: str  # BULLISH/BEARISH/NEUTRAL for this section


@dataclass
class SentimentAnalysis:
    """Complete market sentiment analysis result"""
    sentiment: MarketSentiment
    confidence_score: float  # 0.0 to 1.0
    key_drivers: List[str]
    market_summary: str
    sentiment_reasoning: str
    section_analyses: List[SectionAnalysis] = field(default_factory=list)
    total_symbols_analyzed: int = 0
    generated_at: datetime = field(default_factory=datetime.utcnow)


class ComprehensiveMarketSentimentService:
    """
    Analyzes comprehensive briefing market data to generate institutional-grade sentiment
    
    Works with your briefing configurations like MORNING_BRIEFING_CONFIG to analyze:
    - Asian markets (Nikkei, Hang Seng, China A50)
    - European markets (DAX, FTSE, CAC, Euro Stoxx)
    - US indices (S&P 500, Nasdaq)
    - Volatility (VIX)
    - FX markets (EUR/USD, USD/JPY, etc.)
    - Fixed Income (2Y, 5Y, 10Y yields)
    - Crypto (BTC, ETH, XRP, SOL, ADA)
    """
    
    def __init__(self, market_client: MarketClient, gpt_service: GPTService):
        self.market_client = market_client
        self.gpt_service = gpt_service
        
        # Section weights for overall sentiment calculation
        self.section_weights = {
            'asian_focus': 0.15,      # Asian market influence
            'european_futures': 0.20,  # European market weight
            'us_futures': 0.30,       # US markets - highest weight
            'volatility': 0.15,       # VIX and fear indicators
            'fx': 0.10,               # Currency impact
            'rates': 0.07,            # Fixed income signals
            'crypto': 0.03            # Crypto as risk sentiment
        }
        
        # Performance thresholds for sentiment determination
        self.sentiment_thresholds = {
            "strong_bullish": 1.5,   # >1.5% average performance
            "bullish": 0.5,          # >0.5% average performance
            "bearish": -0.5,         # <-0.5% average performance
            "strong_bearish": -1.5   # <-1.5% average performance
        }
    
    async def analyze_briefing_sentiment(self, briefing_config: Dict) -> SentimentAnalysis:
        """
        Analyze sentiment for a complete briefing configuration
        
        Args:
            briefing_config: Your briefing config dict with market_data_sections
            
        Returns:
            SentimentAnalysis with comprehensive market view
        """
        try:
            logger.info("🎯 Starting comprehensive briefing sentiment analysis")
            
            market_data_sections = briefing_config.get('market_data_sections', {})
            
            if not market_data_sections:
                raise Exception("No market_data_sections found in briefing config")
            
            # 1. Analyze each section individually
            section_analyses = await self._analyze_market_sections(market_data_sections)
            
            if not section_analyses:
                raise Exception("Failed to analyze any market sections")
            
            # 2. Calculate overall weighted sentiment
            overall_sentiment, confidence_score, key_drivers = self._calculate_overall_sentiment(section_analyses)
            
            # 3. Generate institutional market summary using GPT
            market_summary = await self._generate_comprehensive_summary(section_analyses, overall_sentiment)
            
            # 4. Create detailed reasoning
            sentiment_reasoning = self._generate_comprehensive_reasoning(section_analyses, overall_sentiment)
            
            total_symbols = sum(len(section.get('symbols', [])) for section in market_data_sections.values())
            symbols_analyzed = sum(analysis.symbols_analyzed for analysis in section_analyses)
            
            analysis = SentimentAnalysis(
                sentiment=overall_sentiment,
                confidence_score=confidence_score,
                key_drivers=key_drivers,
                market_summary=market_summary,
                sentiment_reasoning=sentiment_reasoning,
                section_analyses=section_analyses,
                total_symbols_analyzed=symbols_analyzed
            )
            
            logger.info(f"✅ Comprehensive sentiment: {overall_sentiment.value} (confidence: {confidence_score:.2f})")
            logger.info(f"📊 Analyzed {symbols_analyzed}/{total_symbols} symbols across {len(section_analyses)} sections")
            
            return analysis
            
        except Exception as e:
            logger.error(f"❌ Comprehensive sentiment analysis failed: {e}")
            return self._get_fallback_sentiment()
    
    async def _analyze_market_sections(self, market_data_sections: Dict) -> List[SectionAnalysis]:
        """Analyze each market section from briefing config"""
        
        section_analyses = []
        
        for section_name, section_config in market_data_sections.items():
            try:
                logger.info(f"📈 Analyzing {section_name} section...")
                
                symbols = section_config.get('symbols', [])
                display_names = section_config.get('display_order', symbols)
                
                if not symbols:
                    logger.warning(f"⚠️ No symbols found in {section_name} section")
                    continue
                
                # Get market data for all symbols in this section
                section_data = await self._get_section_market_data(symbols)
                
                if not section_data:
                    logger.warning(f"⚠️ No market data retrieved for {section_name}")
                    continue
                
                # Analyze this section's performance
                analysis = self._analyze_section_performance(
                    section_name, section_data, display_names
                )
                
                section_analyses.append(analysis)
                logger.info(f"✓ {section_name}: {analysis.dominant_direction} ({analysis.avg_performance:+.2f}% avg)")
                
            except Exception as e:
                logger.error(f"❌ Failed to analyze {section_name}: {e}")
                continue
        
        return section_analyses
    
    async def _get_section_market_data(self, symbols: List[str]) -> List[Dict]:
        """Get market data for symbols in a section"""
        
        market_data = []
        
        try:
            # Use bulk endpoint if available
            if hasattr(self.market_client, 'get_bulk_prices'):
                logger.debug(f"📊 Bulk fetching {len(symbols)} symbols")
                bulk_results = await self.market_client.get_bulk_prices(symbols)
                
                for symbol, price_data in zip(symbols, bulk_results):
                    if price_data and hasattr(price_data, 'price') and price_data.price > 0:
                        market_data.append({
                            'symbol': symbol,
                            'price': price_data.price,
                            'change_percent': price_data.change_percent,
                            'change_absolute': price_data.change_absolute,
                            'volume': getattr(price_data, 'volume', None)
                        })
                        
            else:
                # Fallback to individual requests
                logger.debug(f"📊 Individual fetching {len(symbols)} symbols")
                
                # Create semaphore to limit concurrent requests
                semaphore = asyncio.Semaphore(5)  # Limit to 5 concurrent requests
                
                async def get_single_symbol(symbol):
                    async with semaphore:
                        try:
                            price_data = await self.market_client.get_price(symbol)
                            if price_data and price_data.price > 0:
                                return {
                                    'symbol': symbol,
                                    'price': price_data.price,
                                    'change_percent': price_data.change_percent,
                                    'change_absolute': price_data.change_absolute,
                                    'volume': getattr(price_data, 'volume', None)
                                }
                        except Exception as e:
                            logger.warning(f"⚠️ Failed to get {symbol}: {e}")
                            return None
                
                # Execute all requests concurrently
                tasks = [get_single_symbol(symbol) for symbol in symbols]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Filter successful results
                market_data = [result for result in results if result and not isinstance(result, Exception)]
            
            logger.debug(f"✓ Retrieved data for {len(market_data)}/{len(symbols)} symbols")
            return market_data
            
        except Exception as e:
            logger.error(f"❌ Failed to get section market data: {e}")
            return []
    
    def _analyze_section_performance(self, section_name: str, section_data: List[Dict], display_names: List[str]) -> SectionAnalysis:
        """Analyze performance of a market section"""
        
        if not section_data:
            return SectionAnalysis(
                section_name=section_name,
                symbols_analyzed=0,
                avg_performance=0.0,
                dominant_direction="unknown",
                key_movers=[],
                section_sentiment="NEUTRAL"
            )
        
        # Calculate average performance
        changes = [data['change_percent'] for data in section_data]
        avg_performance = sum(changes) / len(changes)
        
        # Determine dominant direction
        positive_count = sum(1 for change in changes if change > 0.1)
        negative_count = sum(1 for change in changes if change < -0.1)
        
        if positive_count > negative_count * 1.5:
            dominant_direction = "up"
        elif negative_count > positive_count * 1.5:
            dominant_direction = "down"
        else:
            dominant_direction = "mixed"
        
        # Find key movers (top 3 by absolute change)
        sorted_data = sorted(section_data, key=lambda x: abs(x['change_percent']), reverse=True)
        key_movers = []
        
        for data in sorted_data[:3]:
            symbol = data['symbol']
            change = data['change_percent']
            direction = "+" if change >= 0 else ""
            
            # Try to use display name if available
            display_name = symbol
            if hasattr(self, '_get_display_name'):
                display_name = self._get_display_name(symbol, display_names) or symbol
            
            key_movers.append(f"{display_name} {direction}{change:.1f}%")
        
        # Determine section sentiment
        if avg_performance >= self.sentiment_thresholds["strong_bullish"]:
            section_sentiment = "BULLISH"
        elif avg_performance >= self.sentiment_thresholds["bullish"]:
            section_sentiment = "BULLISH"
        elif avg_performance <= self.sentiment_thresholds["strong_bearish"]:
            section_sentiment = "BEARISH"
        elif avg_performance <= self.sentiment_thresholds["bearish"]:
            section_sentiment = "BEARISH"
        else:
            section_sentiment = "NEUTRAL"
        
        return SectionAnalysis(
            section_name=section_name,
            symbols_analyzed=len(section_data),
            avg_performance=avg_performance,
            dominant_direction=dominant_direction,
            key_movers=key_movers,
            section_sentiment=section_sentiment
        )
    
    def _calculate_overall_sentiment(self, section_analyses: List[SectionAnalysis]) -> Tuple[MarketSentiment, float, List[str]]:
        """Calculate weighted overall sentiment from section analyses"""
        
        weighted_score = 0.0
        total_weight = 0.0
        key_drivers = []
        
        # Calculate weighted average performance
        for analysis in section_analyses:
            section_weight = self.section_weights.get(analysis.section_name, 0.05)  # Default 5% weight
            
            # Convert section performance to sentiment score
            if analysis.section_sentiment == "BULLISH":
                score = min(abs(analysis.avg_performance), 3.0)  # Cap at 3%
            elif analysis.section_sentiment == "BEARISH":
                score = -min(abs(analysis.avg_performance), 3.0)  # Negative, capped at -3%
            else:
                score = 0.0
            
            weighted_score += score * section_weight
            total_weight += section_weight
            
            # Add significant drivers
            if abs(analysis.avg_performance) > 0.5:  # Only include meaningful moves
                direction = "strength" if analysis.avg_performance > 0 else "weakness"
                driver = f"{analysis.section_name.replace('_', ' ').title()} showing {direction} ({analysis.avg_performance:+.1f}%)"
                key_drivers.append(driver)
        
        # Normalize weighted score
        if total_weight > 0:
            weighted_score = weighted_score / total_weight
        
        # Determine overall sentiment
        if weighted_score >= 1.0:
            sentiment = MarketSentiment.BULLISH
        elif weighted_score >= 0.3:
            sentiment = MarketSentiment.BULLISH  
        elif weighted_score <= -1.0:
            sentiment = MarketSentiment.BEARISH
        elif weighted_score <= -0.3:
            sentiment = MarketSentiment.BEARISH
        else:
            # Check for mixed signals
            bullish_sections = sum(1 for a in section_analyses if a.section_sentiment == "BULLISH")
            bearish_sections = sum(1 for a in section_analyses if a.section_sentiment == "BEARISH")
            
            if bullish_sections > 0 and bearish_sections > 0:
                sentiment = MarketSentiment.MIXED
            else:
                sentiment = MarketSentiment.NEUTRAL
        
        # Calculate confidence based on data completeness and consensus
        total_symbols_requested = sum(self.section_weights.values()) * 20  # Estimate
        symbols_analyzed = sum(a.symbols_analyzed for a in section_analyses)
        
        completeness_score = min(symbols_analyzed / max(total_symbols_requested, 1), 1.0)
        
        # Sentiment strength
        strength_score = min(abs(weighted_score) / 2.0, 1.0)
        
        # Section consensus
        dominant_sentiment = max(
            sum(1 for a in section_analyses if a.section_sentiment == "BULLISH"),
            sum(1 for a in section_analyses if a.section_sentiment == "BEARISH"),
            sum(1 for a in section_analyses if a.section_sentiment == "NEUTRAL")
        )
        consensus_score = dominant_sentiment / max(len(section_analyses), 1)
        
        confidence_score = (
            completeness_score * 0.4 +
            strength_score * 0.3 +
            consensus_score * 0.3
        )
        
        # Sort drivers by impact
        key_drivers.sort(key=lambda x: abs(float(x.split('(')[1].split('%')[0])), reverse=True)
        
        return sentiment, round(confidence_score, 2), key_drivers[:5]
    
    async def _generate_comprehensive_summary(self, section_analyses: List[SectionAnalysis], sentiment: MarketSentiment) -> str:
        """Generate institutional-grade comprehensive market summary"""
        
        # Build detailed market context
        market_context = []
        
        for analysis in section_analyses:
            section_name = analysis.section_name.replace('_', ' ').title()
            context_line = f"- {section_name}: {analysis.avg_performance:+.2f}% average, {analysis.symbols_analyzed} instruments ({analysis.section_sentiment.lower()})"
            market_context.append(context_line)
        
        market_data_text = "\n".join(market_context)
        
        # Get key movers across all sections
        all_movers = []
        for analysis in section_analyses:
            all_movers.extend(analysis.key_movers)
        
        top_movers = sorted(all_movers, key=lambda x: abs(float(x.split()[-1].replace('%', '').replace('+', ''))), reverse=True)[:5]
        movers_text = ", ".join(top_movers)
        
        prompt = f"""As an institutional portfolio manager, write a comprehensive 2-3 paragraph market summary based on this global market data:

MARKET PERFORMANCE BY REGION/ASSET CLASS:
{market_data_text}

KEY MOVERS: {movers_text}

OVERALL SENTIMENT: {sentiment.value}

Write an institutional analysis covering:
1. Global market themes and cross-asset flows (risk-on vs risk-off positioning)
2. Regional performance divergences and their implications for portfolio allocation
3. Tactical positioning recommendations based on the current market environment

Style: Sophisticated institutional perspective, focused on portfolio implications and macro themes.
Avoid retail language. Focus on institutional positioning and risk management.
Length: 2-3 paragraphs, ~250-300 words."""

        try:
            summary = self.gpt_service.generate_text(prompt, max_tokens=500, temperature=0.6)
            if summary:
                logger.debug("✅ GPT comprehensive market summary generated")
                return summary
            else:
                logger.warning("⚠️ GPT returned empty comprehensive summary")
                return self._get_fallback_comprehensive_summary(sentiment)
                
        except Exception as e:
            logger.error(f"❌ GPT comprehensive summary failed: {e}")
            return self._get_fallback_comprehensive_summary(sentiment)
    
    def _generate_comprehensive_reasoning(self, section_analyses: List[SectionAnalysis], sentiment: MarketSentiment) -> str:
        """Generate detailed reasoning for the sentiment decision"""
        
        bullish_sections = [a for a in section_analyses if a.section_sentiment == "BULLISH"]
        bearish_sections = [a for a in section_analyses if a.section_sentiment == "BEARISH"]
        neutral_sections = [a for a in section_analyses if a.section_sentiment == "NEUTRAL"]
        
        reasoning_parts = []
        
        if bullish_sections:
            section_names = [s.section_name.replace('_', ' ') for s in bullish_sections]
            reasoning_parts.append(f"Bullish momentum in {', '.join(section_names)}")
        
        if bearish_sections:
            section_names = [s.section_name.replace('_', ' ') for s in bearish_sections]
            reasoning_parts.append(f"Bearish pressure from {', '.join(section_names)}")
        
        if neutral_sections:
            section_names = [s.section_name.replace('_', ' ') for s in neutral_sections]
            reasoning_parts.append(f"Neutral signals in {', '.join(section_names)}")
        
        base_reasoning = "; ".join(reasoning_parts) if reasoning_parts else "Mixed cross-asset signals"
        
        # Add quantitative context
        total_analyzed = sum(a.symbols_analyzed for a in section_analyses)
        avg_global_performance = sum(a.avg_performance for a in section_analyses) / max(len(section_analyses), 1)
        
        quantitative_context = f"Global average: {avg_global_performance:+.2f}% across {total_analyzed} instruments in {len(section_analyses)} regions/asset classes"
        
        return f"{base_reasoning}. {quantitative_context}"
    
    def _get_fallback_sentiment(self) -> SentimentAnalysis:
        """Return neutral sentiment when comprehensive analysis fails"""
        return SentimentAnalysis(
            sentiment=MarketSentiment.NEUTRAL,
            confidence_score=0.4,
            key_drivers=["Limited market data available for comprehensive analysis"],
            market_summary="Global market conditions remain mixed with cross-currents across regions and asset classes. Institutional positioning neutral pending clearer directional signals from key markets.",
            sentiment_reasoning="Insufficient comprehensive data for full cross-asset analysis",
            section_analyses=[],
            total_symbols_analyzed=0,
            generated_at=datetime.utcnow()
        )
    
    def _get_fallback_comprehensive_summary(self, sentiment: MarketSentiment) -> str:
        """Fallback comprehensive summary when GPT fails"""
        fallback_summaries = {
            MarketSentiment.BULLISH: "Global risk assets demonstrate constructive momentum with broad-based institutional flows supporting equity markets across regions. Cross-asset positioning suggests continued risk-on appetite with defensive hedges being reduced. Portfolio allocation favors growth assets over defensive positioning.",
            
            MarketSentiment.BEARISH: "Global markets exhibit defensive characteristics with institutional flows rotating toward quality and duration. Cross-asset signals suggest risk-off positioning appropriate as volatility metrics elevate. Portfolio construction should emphasize defensive positioning and downside protection.",
            
            MarketSentiment.MIXED: "Global markets present tactical challenges with conflicting regional signals creating cross-currents for institutional positioning. Risk asset performance diverges across geographies, suggesting selective allocation over broad market exposure. Portfolio positioning requires nuanced regional and sector-specific approaches.",
            
            MarketSentiment.NEUTRAL: "Global market conditions remain range-bound with limited directional conviction across major asset classes. Institutional positioning maintains neutral risk posture as fundamental catalysts remain unclear. Portfolio allocation balanced between growth and defensive characteristics pending clearer market direction."
        }
        
        return fallback_summaries.get(sentiment, fallback_summaries[MarketSentiment.NEUTRAL])


# ==============================================================================
# USAGE EXAMPLE WITH YOUR BRIEFING CONFIG
# ==============================================================================

# Example of how this works with your morning briefing config
EXAMPLE_MORNING_BRIEFING_CONFIG = {
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

async def test_comprehensive_sentiment():
    """Test the comprehensive market sentiment service"""
    from services.market_client import MarketClient
    from services.gpt_service import GPTService
    
    # Initialize services
    market_client = MarketClient()
    gpt_service = GPTService()
    
    # Create comprehensive sentiment service
    sentiment_service = ComprehensiveMarketSentimentService(market_client, gpt_service)
    
    # Run analysis with morning briefing config
    try:
        analysis = await sentiment_service.analyze_briefing_sentiment(EXAMPLE_MORNING_BRIEFING_CONFIG)
        
        print(f"\n🎯 COMPREHENSIVE MARKET SENTIMENT ANALYSIS")
        print(f"=" * 60)
        print(f"Overall Sentiment: {analysis.sentiment.value}")
        print(f"Confidence Score: {analysis.confidence_score:.2f}")
        print(f"Symbols Analyzed: {analysis.total_symbols_analyzed}")
        print(f"Key Drivers: {', '.join(analysis.key_drivers[:3])}")
        
        print(f"\n📊 SECTION BREAKDOWN:")
        for section in analysis.section_analyses:
            print(f"- {section.section_name}: {section.section_sentiment} ({section.avg_performance:+.2f}%)")
            print(f"  Movers: {', '.join(section.key_movers[:2])}")
        
        print(f"\n📈 MARKET SUMMARY:")
        print(analysis.market_summary)
        
        print(f"\n🧠 REASONING:")
        print(analysis.sentiment_reasoning)
        
        return analysis
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        return None


if __name__ == "__main__":
    # Run test
    import asyncio
    asyncio.run(test_comprehensive_sentiment())