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