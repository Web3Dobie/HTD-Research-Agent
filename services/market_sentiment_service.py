# services/market_sentiment_service.py
import logging
import asyncio
from typing import Dict, List, Tuple
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
from services.market_client import MarketClient
from services.gpt_service import GPTService

logger = logging.getLogger(__name__)

# --- Data Classes ---
class MarketSentiment(Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"
    MIXED = "MIXED"

@dataclass
class SectionAnalysis:
    section_name: str
    symbols_analyzed: int
    avg_performance: float
    dominant_direction: str
    key_movers: List[str]
    section_sentiment: str

@dataclass
class SentimentAnalysis:
    sentiment: MarketSentiment
    confidence_score: float
    key_drivers: List[str]
    market_summary: str
    sentiment_reasoning: str
    section_analyses: List[SectionAnalysis] = field(default_factory=list)
    total_symbols_analyzed: int = 0
    generated_at: datetime = field(default_factory=datetime.utcnow)

# --- Main Service Class (Complete & Refactored) ---
class ComprehensiveMarketSentimentService:
    def __init__(self, market_client: MarketClient, gpt_service: GPTService):
        self.market_client = market_client
        self.gpt_service = gpt_service

    async def analyze_briefing_sentiment(self, briefing_config: Dict) -> SentimentAnalysis:
        try:
            logger.info("🎯 Starting comprehensive briefing sentiment analysis")

            market_data_sections = briefing_config.get('market_data_sections', {})
            sentiment_config = briefing_config.get('sentiment_config', {})

            if not market_data_sections or not sentiment_config:
                raise ValueError("Config missing market_data_sections or sentiment_config")

            section_analyses = await self._analyze_market_sections(
                market_data_sections,
                sentiment_config.get('sentiment_thresholds', {})
            )

            # Proceed if we have data for at least 3 sections, otherwise use fallback.
            if len(section_analyses) < 3:
                logger.warning(f"Insufficient data: only got {len(section_analyses)} sections. Using fallback.")
                return self._get_fallback_sentiment()

            overall_sentiment, confidence_score, key_drivers = self._calculate_overall_sentiment(
                section_analyses,
                sentiment_config.get('section_weights', {})
            )

            market_summary = await self._generate_comprehensive_summary(section_analyses, overall_sentiment)
            sentiment_reasoning = self._generate_comprehensive_reasoning(section_analyses, overall_sentiment)
            symbols_analyzed = sum(analysis.symbols_analyzed for analysis in section_analyses)

            return SentimentAnalysis(
                sentiment=overall_sentiment,
                confidence_score=confidence_score,
                key_drivers=key_drivers,
                market_summary=market_summary,
                sentiment_reasoning=sentiment_reasoning,
                section_analyses=section_analyses,
                total_symbols_analyzed=symbols_analyzed
            )
        except Exception as e:
            logger.error(f"❌ Comprehensive sentiment analysis failed: {e}")
            return self._get_fallback_sentiment()

    async def _analyze_market_sections(self, market_data_sections: Dict, thresholds: Dict) -> List[SectionAnalysis]:
        analyses = []
        for section_name, section_config in market_data_sections.items():
            try:
                symbols = section_config.get('symbols', [])
                display_names = section_config.get('display_order', symbols)
                if not symbols: continue
                
                section_data = await self._get_section_market_data(symbols)
                if not section_data: continue
                
                analysis = self._analyze_section_performance(section_name, section_data, display_names, thresholds)
                analyses.append(analysis)
                logger.info(f"✓ {section_name}: {analysis.dominant_direction} ({analysis.avg_performance:+.2f}% avg)")
            except Exception as e:
                logger.error(f"❌ Failed to analyze {section_name}: {e}")
        return analyses

    async def _get_section_market_data(self, symbols: List[str]) -> List[Dict]:
        market_data = []
        try:
            if hasattr(self.market_client, 'get_bulk_prices'):
                bulk_results = await self.market_client.get_bulk_prices(symbols)
                for symbol, price_data in zip(symbols, bulk_results):
                    if price_data and hasattr(price_data, 'price') and price_data.price > 0:
                        market_data.append({'symbol': symbol, 'change_percent': price_data.change_percent})
            else:
                semaphore = asyncio.Semaphore(5)
                async def get_single(symbol):
                    async with semaphore:
                        price_data = await self.market_client.get_price(symbol)
                        if price_data and price_data.price > 0:
                            return {'symbol': symbol, 'change_percent': price_data.change_percent}
                tasks = [get_single(s) for s in symbols]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                market_data = [r for r in results if r and not isinstance(r, Exception)]
        except Exception as e:
            logger.error(f"❌ Failed to get section market data: {e}")
        return market_data

    def _analyze_section_performance(self, section_name: str, section_data: List[Dict], display_names: List[str], thresholds: Dict) -> SectionAnalysis:
        if not section_data:
            return SectionAnalysis(section_name=section_name, symbols_analyzed=0, avg_performance=0.0, dominant_direction="unknown", key_movers=[], section_sentiment="NEUTRAL")

        changes = [data['change_percent'] for data in section_data]
        avg_performance = sum(changes) / len(changes)

        positive_count = sum(1 for c in changes if c > 0.1)
        negative_count = sum(1 for c in changes if c < -0.1)
        if positive_count > negative_count * 1.5:
            dominant_direction = "up"
        elif negative_count > positive_count * 1.5:
            dominant_direction = "down"
        else:
            dominant_direction = "mixed"

        sorted_data = sorted(section_data, key=lambda x: abs(x['change_percent']), reverse=True)
        key_movers = [f"{d['symbol']} {d['change_percent']:+.1f}%" for d in sorted_data[:3]]

        if avg_performance >= thresholds.get("strong_bullish", 1.5):
            section_sentiment = "BULLISH"
        elif avg_performance >= thresholds.get("bullish", 0.5):
            section_sentiment = "BULLISH"
        elif avg_performance <= thresholds.get("strong_bearish", -1.5):
            section_sentiment = "BEARISH"
        elif avg_performance <= thresholds.get("bearish", -0.5):
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

    def _calculate_overall_sentiment(self, section_analyses: List[SectionAnalysis], section_weights: Dict) -> Tuple[MarketSentiment, float, List[str]]:
        weighted_score, total_weight, key_drivers = 0.0, 0.0, []

        for analysis in section_analyses:
            weight = section_weights.get(analysis.section_name, 0.05)
            if analysis.section_sentiment == "BULLISH": score = min(abs(analysis.avg_performance), 3.0)
            elif analysis.section_sentiment == "BEARISH": score = -min(abs(analysis.avg_performance), 3.0)
            else: score = 0.0
            
            weighted_score += score * weight
            total_weight += weight
            
            if abs(analysis.avg_performance) > 0.5:
                direction = "strength" if analysis.avg_performance > 0 else "weakness"
                drivers.append(f"{analysis.section_name.replace('_', ' ').title()} showing {direction} ({analysis.avg_performance:+.1f}%)")
        
        if total_weight > 0: weighted_score /= total_weight

        if weighted_score >= 0.4: sentiment = MarketSentiment.BULLISH
        elif weighted_score <= -0.4: sentiment = MarketSentiment.BEARISH
        else:
            bullish_sections = sum(1 for a in section_analyses if a.section_sentiment == "BULLISH")
            bearish_sections = sum(1 for a in section_analyses if a.section_sentiment == "BEARISH")
            if bullish_sections > 0 and bearish_sections > 0: sentiment = MarketSentiment.MIXED
            else: sentiment = MarketSentiment.NEUTRAL
        
        completeness_score = sum(a.symbols_analyzed for a in section_analyses) / max(len(section_analyses) * 5, 1) # Simple heuristic
        strength_score = min(abs(weighted_score) / 1.5, 1.0)
        confidence_score = (completeness_score * 0.5) + (strength_score * 0.5)
        
        drivers.sort(key=lambda x: abs(float(x.split('(')[1].split('%')[0])), reverse=True)
        
        return sentiment, round(confidence_score, 2), drivers[:5]

    async def _generate_comprehensive_summary(self, section_analyses: List[SectionAnalysis], sentiment: MarketSentiment) -> str:
        market_context = "\n".join([f"- {a.section_name.replace('_', ' ').title()}: {a.avg_performance:+.2f}% avg ({a.section_sentiment.lower()})" for a in section_analyses])
        prompt = f"""As an institutional portfolio manager, write a 2-paragraph market summary.
        Data:\n{market_context}\nOverall Sentiment: {sentiment.value}\n
        Cover global themes, regional divergences, and tactical positioning. Be concise and sophisticated."""
        try:
            summary = self.gpt_service.generate_text(prompt, max_tokens=300, temperature=0.6)
            return summary or self._get_fallback_comprehensive_summary(sentiment)
        except Exception as e:
            logger.error(f"❌ GPT summary failed: {e}")
            return self._get_fallback_comprehensive_summary(sentiment)

    def _generate_comprehensive_reasoning(self, section_analyses: List[SectionAnalysis], sentiment: MarketSentiment) -> str:
        bullish = [a.section_name for a in section_analyses if a.section_sentiment == "BULLISH"]
        bearish = [a.section_name for a in section_analyses if a.section_sentiment == "BEARISH"]
        reasoning = []
        if bullish: reasoning.append(f"Bullish momentum from {', '.join(bullish)}")
        if bearish: reasoning.append(f"Bearish pressure from {', '.join(bearish)}")
        return "; ".join(reasoning) or "Neutral cross-asset signals."

    def _get_fallback_sentiment(self) -> SentimentAnalysis:
        return SentimentAnalysis(
            sentiment=MarketSentiment.NEUTRAL,
            confidence_score=0.2,
            key_drivers=["Limited market data available"],
            market_summary="Global market conditions remain mixed with limited directional conviction.",
            sentiment_reasoning="Insufficient data for full analysis.",
        )

    def _get_fallback_comprehensive_summary(self, sentiment: MarketSentiment) -> str:
        summaries = {
            MarketSentiment.BULLISH: "Global risk assets show constructive momentum, suggesting continued risk-on appetite.",
            MarketSentiment.BEARISH: "Global markets exhibit defensive characteristics, suggesting risk-off positioning is appropriate.",
            MarketSentiment.MIXED: "Global markets present conflicting signals, suggesting selective allocation over broad market exposure.",
            MarketSentiment.NEUTRAL: "Global market conditions are range-bound with limited directional conviction."
        }
        return summaries.get(sentiment, summaries[MarketSentiment.NEUTRAL])