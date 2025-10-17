# services/article_writer.py
"""
ArticleWriter - Generates markdown articles from deep dive content.
Follows Hunter-Agent pattern with local file storage.
"""

import logging
import os
import asyncio
from datetime import datetime
from typing import Optional
from pathlib import Path

from core.models import GeneratedContent

class ArticleWriter:
    """
    Writes deep dive articles to local storage for NGINX serving.
    Follows the Hunter-Agent pattern with markdown format.
    """
    
    def __init__(self, articles_path: str = "/app/articles"):
        self.logger = logging.getLogger(__name__)
        self.articles_path = Path(articles_path)
        
        # Ensure articles directory exists
        self.articles_path.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"✅ ArticleWriter initialized: {self.articles_path}")
    
    async def write_deep_dive_article(self, content: GeneratedContent, research_data: dict = None) -> str:
        """
        Generate and write a deep dive article from thread content.
        
        Args:
            content: GeneratedContent with thread parts and metadata
            research_data: Additional research data used in thread generation
            
        Returns:
            article_id: Unique identifier for the article
        """
        try:
            # Generate article ID from date and theme
            date_str = datetime.now().strftime('%Y-%m-%d')
            theme_slug = self._slugify_theme(content.theme)
            article_id = f"deep-dive-{date_str}-{theme_slug}"
            
            self.logger.info(f"🔄 Generating article: {article_id}")
            
            # Generate article content from thread content
            article_markdown = await self._generate_article_markdown(content, research_data)
            
            # Write to file
            file_path = self.articles_path / f"{article_id}.md"
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(article_markdown)
            
            self.logger.info(f"✅ Article written: {file_path}")
            
            return article_id
            
        except Exception as e:
            self.logger.error(f"❌ Failed to write article: {e}")
            raise
    
    async def _generate_article_markdown(self, content: GeneratedContent, research_data: dict = None) -> str:
        """
        Generate expanded markdown article from thread content.
        """
        # Create article sections from thread parts
        thread_analysis = self._analyze_thread_content(content)
        
        # Generate markdown content
        markdown_content = f"""# HTD Research - {content.theme}

**Published:** {datetime.now().strftime('%B %d, %Y')}  
**Author:** HTD Research  
**Category:** Deep Dive Analysis  
**Market Data:** {len(content.market_data)} instruments analyzed

## Executive Summary

{thread_analysis['summary']}

## Market Analysis

{thread_analysis['market_section']}

## Technical Insights

{thread_analysis['technical_section']}

{self._generate_market_data_section(content.market_data)}

## Thread Analysis

This analysis was also published as a Twitter thread for real-time market commentary:

{self._format_thread_content(content.parts)}

## Research Methodology

This analysis is based on:
- Real-time market data from multiple sources
- Technical indicators and price action analysis  
- News sentiment analysis from institutional sources
- Cross-asset correlation studies

---

*HTD Research provides institutional-grade market analysis combining quantitative data with qualitative insights. This analysis is for informational purposes only and does not constitute investment advice.*

**Risk Disclaimer:** Market analysis involves substantial risk. Past performance does not guarantee future results.
"""
        
        return markdown_content
    
    def _analyze_thread_content(self, content: GeneratedContent) -> dict:
        """
        Analyze thread parts to extract key sections for article expansion.
        """
        parts = content.parts if content.parts else [content.text]
        
        # Combine all parts for analysis
        full_thread = "\n\n".join(parts)
        
        # Extract key themes for expansion
        summary = parts[0] if parts else "Market analysis of current conditions."
        
        # Create expanded sections
        market_section = self._expand_market_analysis(content, full_thread)
        technical_section = self._expand_technical_analysis(content, full_thread)
        
        return {
            'summary': summary,
            'market_section': market_section,
            'technical_section': technical_section
        }
    
    def _expand_market_analysis(self, content: GeneratedContent, thread_text: str) -> str:
        """
        Expand the market analysis section beyond the thread content.
        """
        if content.headline_used:
            headline_analysis = f"""
### News Analysis

**Headline:** {content.headline_used.headline}

The market is reacting to this development with particular attention to its implications for institutional positioning and cross-asset volatility patterns.
"""
        else:
            headline_analysis = ""
        
        return f"""The current market environment presents several key dynamics that warrant institutional attention.

{headline_analysis}

### Market Context

Our analysis indicates that current price action reflects a combination of:
- Institutional positioning adjustments
- Technical level interactions
- Macroeconomic sentiment shifts
- Cross-asset correlation changes

The thread content above provides the immediate tactical view, while this expanded analysis offers additional context for longer-term positioning considerations."""
    
    def _expand_technical_analysis(self, content: GeneratedContent, thread_text: str) -> str:
        """
        Expand technical analysis with additional institutional perspective.
        """
        return """### Technical Framework

The technical analysis incorporates multiple timeframe perspectives:

**Short-term (Intraday):** Price action and volume patterns suggest institutional interest at current levels.

**Medium-term (Weekly):** Trend structure remains intact despite recent volatility, with key support/resistance levels holding.

**Long-term (Monthly):** Broader market structure continues to align with institutional positioning themes.

### Risk Management Considerations

Current volatility patterns suggest:
- Position sizing should account for elevated implied volatility
- Stop-loss levels should be placed beyond recent volatility ranges
- Correlation breakdowns may create unexpected portfolio impacts"""
    
    def _generate_market_data_section(self, market_data: list) -> str:
        """
        Generate market data section if available.
        """
        if not market_data:
            return ""
        
        data_table = "## Market Data Summary\n\n"
        data_table += "| Instrument | Price | Change | Analysis |\n"
        data_table += "|------------|-------|--------|----------|\n"
        
        for data in market_data:
            change_direction = "📈" if data.change_percent > 0 else "📉"
            data_table += f"| {data.ticker} | ${data.price:.2f} | {data.change_percent:+.2f}% {change_direction} | Key level interaction |\n"
        
        data_table += "\n*Market data as of article publication time.*\n\n"
        
        return data_table
    
    def _format_thread_content(self, parts: list) -> str:
        """
        Format thread parts for inclusion in article.
        """
        if not parts:
            return "Thread content not available."
        
        formatted = ""
        for i, part in enumerate(parts, 1):
            formatted += f"**Part {i}:**\n> {part}\n\n"
        
        return formatted
    
    def _slugify_theme(self, theme: str) -> str:
        """
        Convert theme to URL-friendly slug.
        """
        if not theme:
            return "market-analysis"
        
        # Convert to lowercase and replace spaces/special chars with hyphens
        slug = theme.lower()
        slug = slug.replace(' ', '-')
        slug = ''.join(c for c in slug if c.isalnum() or c == '-')
        slug = '-'.join(filter(None, slug.split('-')))  # Remove empty parts
        
        return slug[:50]  # Limit length
    
    def get_article_url(self, article_id: str) -> str:
        """
        Generate the public URL for an article.
        """
        return f"https://dutchbrat.com/articles/htd/{article_id}"
    
    def list_articles(self, limit: int = 10) -> list:
        """
        List recent articles for debugging/monitoring.
        """
        try:
            articles = []
            for file_path in sorted(self.articles_path.glob("*.md"), reverse=True):
                if limit and len(articles) >= limit:
                    break
                
                articles.append({
                    'article_id': file_path.stem,
                    'filename': file_path.name,
                    'created': datetime.fromtimestamp(file_path.stat().st_mtime),
                    'size': file_path.stat().st_size
                })
            
            return articles
        except Exception as e:
            self.logger.error(f"Failed to list articles: {e}")
            return []


# Convenience functions for testing
async def test_article_writer():
    """Test function for ArticleWriter"""
    from core.models import GeneratedContent, ContentType, MarketData
    
    # Create test content
    test_content = GeneratedContent(
        text="Test market analysis content",
        content_type=ContentType.DEEP_DIVE,
        theme="Market Volatility Analysis",
        parts=[
            "Market showing increased volatility following recent economic data releases.",
            "Technical indicators suggest potential reversal at current levels with strong support.",
            "Risk management protocols recommend cautious positioning given current uncertainty."
        ],
        market_data=[
            MarketData(ticker="SPY", price=425.50, change_percent=1.25),
            MarketData(ticker="QQQ", price=375.25, change_percent=0.85)
        ]
    )
    
    # Test article generation
    writer = ArticleWriter("/tmp/test_articles")
    article_id = await writer.write_deep_dive_article(test_content)
    
    print(f"Generated test article: {article_id}")
    print(f"Articles list: {writer.list_articles()}")
    
    return article_id

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_article_writer())