# services/market_sentiment_service.py - Refactored for analysis only

import logging
from typing import Dict, List, Tuple
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
from services.gpt_service import GPTService

logger = logging.getLogger(__name__)

# Keep existing data classes unchanged
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

class ComprehensiveMarketSentimentService:
    """Pure analysis service - no data fetching, only sentiment analysis"""
    
    def __init__(self, gpt_service: GPTService):
        # Removed market_client dependency - no more data fetching
        self.gpt_service = gpt_service

    async def analyze_market_sentiment(
        self,
        raw_market_data: Dict[str, List[Dict]],
        briefing_config: Dict,
        factual_context: str  # <-- Change here
    ) -> SentimentAnalysis:
        """
        Analyze market sentiment from pre-fetched raw market data and a factual context block.
        """
        try:
            logger.info("Starting market sentiment analysis on pre-fetched data")

            sentiment_config = briefing_config.get('sentiment_config', {})
            if not sentiment_config:
                raise ValueError("Config missing sentiment_config")

            # Analyze each section using the raw data provided
            section_analyses = self._analyze_all_sections(
                raw_market_data,
                sentiment_config.get('sentiment_thresholds', {})
            )

            if len(section_analyses) < 3:
                logger.warning(f"Insufficient section data: only got {len(section_analyses)} sections. Using fallback.")
                return self._get_fallback_sentiment()

            # Calculate overall sentiment from section analyses
            overall_sentiment, confidence_score, key_drivers = self._calculate_overall_sentiment(
                section_analyses,
                sentiment_config.get('section_weights', {})
            )

            # Generate comprehensive summary and reasoning using GPT
            market_summary = await self._generate_comprehensive_summary(
                section_analyses=section_analyses,
                sentiment=overall_sentiment,
                factual_context=factual_context  # <-- Change here
            )
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
            logger.error(f"Market sentiment analysis failed: {e}")
            return self._get_fallback_sentiment()

    def _analyze_all_sections(self, raw_market_data: Dict[str, List[Dict]], thresholds: Dict) -> List[SectionAnalysis]:
        """Analyze all market sections using pre-fetched raw data"""
        analyses = []
        
        for section_name, section_data in raw_market_data.items():
            if not section_data:
                logger.warning(f"No market data for section: {section_name}")
                continue
                
            try:
                # Extract display names from the data itself
                display_names = [item.get('symbol', '') for item in section_data]
                
                # Use existing analysis logic but with pre-fetched data
                analysis = self._analyze_section_performance(
                    section_name, 
                    section_data, 
                    display_names, 
                    thresholds
                )
                analyses.append(analysis)
                logger.info(f"Analyzed {section_name}: {analysis.dominant_direction} ({analysis.avg_performance:+.2f}% avg)")
            except Exception as e:
                logger.error(f"Failed to analyze section {section_name}: {e}")
                continue
                
        return analyses

    def _analyze_section_performance(self, section_name: str, section_data: List[Dict], display_names: List[str], thresholds: Dict) -> SectionAnalysis:
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
        """Calculate overall sentiment from section analyses"""
        weighted_score = 0.0
        total_weight = 0.0
        key_drivers = []

        for analysis in section_analyses:
            weight = section_weights.get(analysis.section_name, 0.05)
            
            if analysis.section_sentiment == "BULLISH": 
                score = min(abs(analysis.avg_performance), 3.0)
            elif analysis.section_sentiment == "BEARISH": 
                score = -min(abs(analysis.avg_performance), 3.0)
            else: 
                score = 0.0
            
            weighted_score += score * weight
            total_weight += weight
            
            if abs(analysis.avg_performance) > 0.5:
                direction = "strength" if analysis.avg_performance > 0 else "weakness"
                key_drivers.append(f"{analysis.section_name.replace('_', ' ').title()} showing {direction} ({analysis.avg_performance:+.1f}%)")

        if total_weight > 0: 
            weighted_score /= total_weight

        if weighted_score >= 0.4: 
            sentiment = MarketSentiment.BULLISH
        elif weighted_score <= -0.4: 
            sentiment = MarketSentiment.BEARISH
        else:
            bullish_sections = sum(1 for a in section_analyses if a.section_sentiment == "BULLISH")
            bearish_sections = sum(1 for a in section_analyses if a.section_sentiment == "BEARISH")
            if bullish_sections > 0 and bearish_sections > 0: 
                sentiment = MarketSentiment.MIXED
            else: 
                sentiment = MarketSentiment.NEUTRAL

        completeness_score = sum(a.symbols_analyzed for a in section_analyses) / max(len(section_analyses) * 5, 1)
        strength_score = min(abs(weighted_score) / 1.5, 1.0)
        confidence_score = (completeness_score * 0.5) + (strength_score * 0.5)

        key_drivers.sort(key=lambda x: abs(float(x.split('(')[1].split('%')[0])), reverse=True)

        return sentiment, round(confidence_score, 2), key_drivers[:5]

    async def _generate_comprehensive_summary(
        self, 
        section_analyses: List[SectionAnalysis], 
        sentiment: MarketSentiment,
        factual_context: str # <-- Receives the context here
    ) -> str:
        """Generate market summary using a pre-built context block."""
        
        analysis_context = "\n".join([f"- {a.section_name.replace('_', ' ').title()}: {a.avg_performance:+.2f}% avg ({a.section_sentiment.lower()})" for a in section_analyses])
        
        prompt = f"""{factual_context}
**Internal Analysis:**
- Overall Sentiment: {sentiment.value}
- Section Performance:
{analysis_context}

**Your Task:**
As an institutional portfolio manager, synthesize all the context and analysis above into a sophisticated, exactly 2-paragraph market summary.
- **Paragraph 1:** Focus on the broad macroeconomic and geopolitical drivers.
- **Paragraph 2:** Focus on the specific market and sector performance, including key divergences.
- **Formatting Requirement:** You must separate the two paragraphs with a double line break. The final output must be in the format: [Paragraph 1]\n\n[Paragraph 2].
"""

        summary = self.gpt_service.generate_text(prompt, max_tokens=300, temperature=0.6)
        # Ensure the final output has the double newline, replacing any single ones at the paragraph break
        return summary.replace('\n', '\n\n') if summary.count('\n') < 2 else summary

    def _generate_comprehensive_reasoning(self, section_analyses: List[SectionAnalysis], sentiment: MarketSentiment) -> str:
        """Generate reasoning for sentiment conclusion"""
        bullish = [a.section_name for a in section_analyses if a.section_sentiment == "BULLISH"]
        bearish = [a.section_name for a in section_analyses if a.section_sentiment == "BEARISH"]
        reasoning = []
        if bullish: 
            reasoning.append(f"Bullish momentum from {', '.join(bullish)}")
        if bearish: 
            reasoning.append(f"Bearish pressure from {', '.join(bearish)}")
        return "; ".join(reasoning) or "Neutral cross-asset signals."

    def _get_fallback_sentiment(self) -> SentimentAnalysis:
        """Return fallback sentiment when analysis fails"""
        return SentimentAnalysis(
            sentiment=MarketSentiment.NEUTRAL,
            confidence_score=0.2,
            key_drivers=["Limited market data available"],
            market_summary="Global market conditions remain mixed with limited directional conviction.",
            sentiment_reasoning="Insufficient data for comprehensive analysis.",
            section_analyses=[],
            total_symbols_analyzed=0
        )

    def _get_fallback_comprehensive_summary(self, sentiment: MarketSentiment) -> str:
        """Fallback market summaries"""
        summaries = {
            MarketSentiment.BULLISH: "Global risk assets show constructive momentum, suggesting continued risk-on appetite.",
            MarketSentiment.BEARISH: "Global markets exhibit defensive characteristics, suggesting risk-off positioning is appropriate.",
            MarketSentiment.MIXED: "Global markets present conflicting signals, suggesting selective allocation over broad market exposure.",
            MarketSentiment.NEUTRAL: "Global market conditions are range-bound with limited directional conviction."
        }
        return summaries.get(sentiment, summaries[MarketSentiment.NEUTRAL])
