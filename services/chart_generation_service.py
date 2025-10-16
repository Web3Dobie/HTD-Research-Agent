# services/chart_generation_service.py

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from io import BytesIO
import base64
import os
import tempfile
from typing import Dict, List, Optional, Tuple
import logging
from datetime import datetime

class ChartGenerationService:
    """Generate simple chart images for social media sharing."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # Set style for professional looking charts
        plt.style.use('default')  # Use default instead of seaborn for compatibility
        self.temp_dir = tempfile.mkdtemp()
        
    def generate_sentiment_chart(self, analysis) -> Optional[str]:
        """Generate a sentiment overview chart and return filepath."""
        try:
            fig, ax = plt.subplots(figsize=(10, 6), facecolor='white')
            
            if not analysis.section_analyses:
                self.logger.warning("No section analyses available for chart")
                return None
            
            # Create data for sentiment visualization
            sections = [s.section_name.replace('_', ' ').title() for s in analysis.section_analyses]
            performances = [s.avg_performance for s in analysis.section_analyses]
            colors = ['#22c55e' if p > 0 else '#ef4444' if p < 0 else '#6b7280' for p in performances]
            
            # Create horizontal bar chart
            bars = ax.barh(sections, performances, color=colors, alpha=0.8)
            
            # Customize chart
            ax.set_xlabel('Performance (%)', fontsize=14, fontweight='bold')
            ax.set_title(f'Market Sentiment: {analysis.sentiment.value}', 
                        fontsize=16, fontweight='bold', pad=20)
            ax.axvline(x=0, color='black', linestyle='-', alpha=0.5, linewidth=1)
            
            # Add value labels on bars
            for bar, value in zip(bars, performances):
                label_x = value + (0.1 if value > 0 else -0.1)
                ax.text(label_x, bar.get_y() + bar.get_height()/2, 
                       f'{value:+.1f}%', ha='left' if value > 0 else 'right', 
                       va='center', fontweight='bold', fontsize=12)
            
            # Style improvements
            ax.grid(True, alpha=0.3, axis='x')
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            ax.spines['left'].set_linewidth(0.5)
            ax.spines['bottom'].set_linewidth(0.5)
            
            # Add confidence indicator
            confidence_text = f"Confidence: {analysis.confidence_score:.1%}"
            ax.text(0.02, 0.98, confidence_text, transform=ax.transAxes, 
                   fontsize=10, verticalalignment='top', 
                   bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.7))
            
            plt.tight_layout()
            
            # Save to temporary file
            chart_filename = f"sentiment_chart_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            chart_path = os.path.join(self.temp_dir, chart_filename)
            
            plt.savefig(chart_path, format='png', dpi=300, bbox_inches='tight', 
                       facecolor='white', edgecolor='none')
            plt.close()
            
            self.logger.info(f"Generated sentiment chart: {chart_path}")
            return chart_path
            
        except Exception as e:
            self.logger.error(f"Failed to generate sentiment chart: {e}")
            plt.close()
            return None
    
    def generate_performance_summary_chart(self, section_analyses) -> Optional[str]:
        """Generate a simple performance overview chart."""
        try:
            if not section_analyses:
                return None
                
            fig, ax = plt.subplots(figsize=(8, 8), facecolor='white')
            
            # Create pie chart of sentiment distribution
            sentiments = [s.section_sentiment for s in section_analyses]
            sentiment_counts = {}
            for sentiment in sentiments:
                sentiment_counts[sentiment] = sentiment_counts.get(sentiment, 0) + 1
            
            labels = list(sentiment_counts.keys())
            sizes = list(sentiment_counts.values())
            colors = {
                'BULLISH': '#22c55e',
                'BEARISH': '#ef4444', 
                'NEUTRAL': '#6b7280',
                'MIXED': '#f59e0b'
            }
            chart_colors = [colors.get(label, '#6b7280') for label in labels]
            
            wedges, texts, autotexts = ax.pie(sizes, labels=labels, colors=chart_colors, 
                                            autopct='%1.0f%%', startangle=90,
                                            textprops={'fontsize': 12, 'fontweight': 'bold'})
            
            ax.set_title('Market Sentiment Distribution', fontsize=16, fontweight='bold', pad=20)
            
            plt.tight_layout()
            
            # Save to temporary file
            chart_filename = f"performance_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            chart_path = os.path.join(self.temp_dir, chart_filename)
            
            plt.savefig(chart_path, format='png', dpi=300, bbox_inches='tight',
                       facecolor='white', edgecolor='none')
            plt.close()
            
            self.logger.info(f"Generated performance summary chart: {chart_path}")
            return chart_path
            
        except Exception as e:
            self.logger.error(f"Failed to generate performance summary chart: {e}")
            plt.close()
            return None
    
    def cleanup_chart(self, chart_path: str):
        """Clean up temporary chart file."""
        try:
            if chart_path and os.path.exists(chart_path):
                os.remove(chart_path)
                self.logger.debug(f"Cleaned up chart file: {chart_path}")
        except Exception as e:
            self.logger.warning(f"Failed to cleanup chart file {chart_path}: {e}")
    
    def cleanup_all_charts(self):
        """Clean up all temporary chart files."""
        try:
            for filename in os.listdir(self.temp_dir):
                filepath = os.path.join(self.temp_dir, filename)
                if filepath.endswith('.png'):
                    self.cleanup_chart(filepath)
        except Exception as e:
            self.logger.warning(f"Failed to cleanup all charts: {e}")