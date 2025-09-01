# hedgefund_agent/services/gpt_service.py
import logging
from typing import List
from openai import AzureOpenAI

# Import config
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import (
    AZURE_OPENAI_API_KEY, 
    AZURE_DEPLOYMENT_ID,
    AZURE_API_VERSION, 
    AZURE_RESOURCE_NAME
)

try:
    from core.models import ContentCategory
except ImportError:
    # Fallback if models not available
    from enum import Enum
    class ContentCategory(Enum):
        MACRO = "macro"
        EQUITY = "equity"
        POLITICAL = "political"

logger = logging.getLogger(__name__)

class GPTService:
    """Handles all GPT interactions for HedgeFund Agent"""
    
    def __init__(self):
        # Create Azure OpenAI client
        self.client = AzureOpenAI(
            api_key=AZURE_OPENAI_API_KEY,
            api_version=AZURE_API_VERSION,
            azure_endpoint=f"https://{AZURE_RESOURCE_NAME}.openai.azure.com/",
        )
        
        logger.info("🤖 GPT Service initialized with Azure OpenAI")
    
    def generate_text(self, prompt: str, max_tokens: int = 1800, temperature: float = 0.9) -> str:
        """Generate text using GPT (for scoring, content generation, etc.)"""
        try:
            response = self.client.chat.completions.create(
                model=AZURE_DEPLOYMENT_ID,
                messages=[{"role": "user", "content": prompt}],
                temperature=temperature,
                max_tokens=max_tokens,
                top_p=1.0,
            )
            
            result = response.choices[0].message.content.strip()
            logger.debug(f"GPT generated {len(result)} characters")
            return result
            
        except Exception as e:
            logger.error(f"GPT text generation failed: {e}")
            return ""
    
    def generate_tweet(self, prompt: str, temperature: float = 0.7) -> str:
        """Generate a single tweet with hedge fund perspective"""
        try:
            system_prompt = (
                "You are an expert hedge fund manager. Analyze the topic and return in format:\n"
                "THEME|COMMENTARY\n\n"
                "Requirements for commentary:\n"
                "- Provide sharp, data-driven market analysis\n"
                "- Include specific market implications\n"
                "- Focus on actionable investment insights\n"
                "- Target ~240 chars (max 280)\n"
                "- Theme should be 1-3 key words\n"
                "\nExample format:\n"
                "FED_POLICY|Fed's hawkish pivot signals 75bp hike. Credit spreads widening, "
                "tech multiples compressing. Defensive positioning warranted."
            )
            
            response = self.client.chat.completions.create(
                model=AZURE_DEPLOYMENT_ID,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ],
                temperature=temperature,
                max_tokens=160,
                top_p=1.0,
            )
            
            result = response.choices[0].message.content.strip()
            
            # Parse theme|commentary format
            if "|" in result:
                theme, commentary = result.split("|", 1)
                return commentary.strip()
            else:
                return result[:280]  # Fallback
                
        except Exception as e:
            logger.error(f"GPT tweet generation failed: {e}")
            return ""
    
    def generate_thread(self, prompt: str, max_parts: int = 3, delimiter: str = "---") -> List[str]:
        """Generate a multi-part thread"""
        try:
            system_prompt = (
                f"You are a hedge fund investor. Write exactly {max_parts} tweet-length insights "
                f"separated by '{delimiter}'.\n"
                "Each part should deepen the analysis or add nuance to the macro, political or equity view. "
                "No numbering."
            )
            
            response = self.client.chat.completions.create(
                model=AZURE_DEPLOYMENT_ID,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.85,
                max_tokens=1500,
                top_p=1.0,
            )
            
            raw_content = response.choices[0].message.content.strip()
            parts = raw_content.split(delimiter) if delimiter in raw_content else raw_content.split("\n\n")
            
            # Format each part
            formatted_parts = []
            for i, part in enumerate(parts[:max_parts], start=1):
                part = part.strip()
                if not part:
                    continue
                    
                # Add thread numbering
                formatted_part = f"{part}\n\nPart {i}/{max_parts}"
                
                # Add disclaimer to last part
                if i == max_parts:
                    formatted_part += "\n\nThis is my opinion. Not financial advice."
                
                formatted_parts.append(formatted_part)
            
            return formatted_parts
            
        except Exception as e:
            logger.error(f"GPT thread generation failed: {e}")
            return []
    
    # ========================================
    # NEW: INSTITUTIONAL COMMENT GENERATION
    # ========================================
    
    def generate_institutional_comment(self, headline: str, category: str) -> str:
        """
        Generate HTD Research institutional commentary for hedge fund news
        
        Args:
            headline: The news headline to comment on
            category: Category (macro, equity, political)
            
        Returns:
            Professional institutional comment with HTD Research branding
        """
        try:
            # Map category string to enum if needed
            category_enum = self._map_category_string(category)
            
            # Build institutional prompt
            prompt = self._build_institutional_prompt(headline, category_enum)
            
            # Generate comment using existing generate_text method
            comment = self.generate_text(prompt, max_tokens=250, temperature=0.7)
            
            # Clean and format for institutional use
            formatted_comment = self._format_institutional_comment(comment)
            
            logger.info(f"✅ Generated institutional comment for {category} headline")
            return formatted_comment
            
        except Exception as e:
            logger.error(f"❌ Institutional comment generation failed: {e}")
            return self._get_institutional_fallback(category)
    
    def _build_institutional_prompt(self, headline: str, category: ContentCategory) -> str:
        """Build category-specific institutional prompts"""
        
        base_instruction = (
            "As HTD Research, provide sharp institutional analysis. "
            "Use professional terminology. Keep under 120 characters total. "
            "Be analytical and show market expertise."
        )
        
        category_prompts = {
            ContentCategory.MACRO: f"""
            {base_instruction}
            
            Focus on: policy implications, market structure impacts, duration/credit risk, 
            institutional positioning opportunities.
            
            Headline: {headline}
            Category: Macro/Economic
            
            Institutional Analysis:
            """,
            
            ContentCategory.EQUITY: f"""
            {base_instruction}
            
            Focus on: sector implications, alpha opportunities, earnings impact, 
            institutional flow implications, risk-adjusted positioning.
            
            Headline: {headline} 
            Category: Equity/Corporate
            
            Institutional Analysis:
            """,
            
            ContentCategory.POLITICAL: f"""
            {base_instruction}
            
            Focus on: policy market impact, regulatory implications, sector rotation,
            risk-off/risk-on positioning. Stay objective and institutional.
            
            Headline: {headline}
            Category: Political/Policy  
            
            Institutional Analysis:
            """
        }
        
        return category_prompts.get(category, category_prompts[ContentCategory.MACRO])
    
    def _format_institutional_comment(self, raw_comment: str) -> str:
        """Format and clean institutional commentary"""
        if not raw_comment:
            return self._get_institutional_fallback("macro")
            
        # Clean the comment
        comment = raw_comment.strip()
        
        # Remove any casual language that might slip through
        professional_replacements = {
            "I think": "Analysis suggests",
            "I believe": "Research indicates",
            "guys": "market participants", 
            "folks": "investors",
            "gonna": "will",
            "can't": "cannot"
        }
        
        for casual, professional in professional_replacements.items():
            comment = comment.replace(casual, professional)
        
        # Ensure HTD Research branding (institutional style)
        if "HTD Research" not in comment and "— HTD" not in comment:
            # Add signature without brain emoji for institutional feel
            comment += " — HTD Research 📊"
            
        return comment
    
    def _map_category_string(self, category: str) -> ContentCategory:
        """Map category string to ContentCategory enum"""
        category_mapping = {
            'macro': ContentCategory.MACRO,
            'equity': ContentCategory.EQUITY, 
            'political': ContentCategory.POLITICAL,
            'general': ContentCategory.MACRO  # Default fallback
        }
        
        return category_mapping.get(category.lower(), ContentCategory.MACRO)
    
    def _get_institutional_fallback(self, category: str) -> str:
        """Professional fallback comments by category"""
        fallbacks = {
            'macro': "Fed policy dynamics create asymmetric positioning opportunity. Monitor duration exposure. — HTD Research 📊",
            'equity': "Earnings revision cycle suggests institutional flow implications. Alpha opportunity developing. — HTD Research 📊",
            'political': "Policy uncertainty creates tactical positioning window. Regulatory impact assessment ongoing. — HTD Research 📊",
            'general': "Market structure shift warrants institutional attention. Risk positioning advised. — HTD Research 📊"
        }
        
        return fallbacks.get(category, fallbacks['general'])