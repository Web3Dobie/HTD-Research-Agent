import logging
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

class PromptAugmentationService:
    """
    Builds the factual context block for Retrieval-Augmented Generation (RAG) prompts.
    """
    def create_context_block(self, macro_data: Dict[str, Any], headlines: List[Any]) -> str:
        """
        Takes raw data and returns a formatted string for AI prompts,
        now using a narrative backdrop instead of a list.
        """
        logger.info("Creating augmented context block with macro backdrop for AI prompt...")
        
        # Use the new method to get a narrative summary
        macro_backdrop = self._create_macro_backdrop(macro_data)
        formatted_headlines = self._format_headlines(headlines)
        
        context_block = f"""**Economic Backdrop:**
{macro_backdrop}

**Today's Key Headlines:**
{formatted_headlines}
"""
        return context_block

    # services/prompt_augmentation_service.py

    def _create_macro_backdrop(self, macro_data: dict) -> str:
        """
        Generates a single sentence summarizing the economic environment
        to serve as a contextual backdrop for the AI.
        """
        if not macro_data:
            return "The current macroeconomic backdrop is uncertain due to unavailable data."

        # Generate descriptive phrases based on the data
        inflation_data = macro_data.get('CPI', {})
        yoy_change = inflation_data.get('percent_change_year_ago')
        inflation_phrase = f"inflation that is {'elevated' if yoy_change > 3.0 else 'moderating'}" if yoy_change is not None else "an unclear inflation picture"

        unemployment_data = macro_data.get('UNEMPLOYMENT', {})
        unemployment_value = unemployment_data.get('latest_value')
        labor_market_phrase = f"a {'tight' if unemployment_value <= 4.2 else 'softening'} labor market" if unemployment_value is not None else "an uncertain labor market"

        gdp_data = macro_data.get('GDP', {})
        gdp_change = gdp_data.get('percent_change_from_previous')
        growth_phrase = f"{'robust' if gdp_change > 1.0 else 'modest'} economic growth" if gdp_change is not None else "unclear economic growth"
        
        fedfunds_data = macro_data.get('FEDFUNDS', {})
        fedfunds_change = fedfunds_data.get('change_from_previous')
        policy_phrase = f"with monetary policy {'on hold' if fedfunds_change == 0 else 'in transition'}" if fedfunds_change is not None else "with an unclear policy stance"

        # Assemble the final sentence
        backdrop = (
            f"The current economic backdrop is characterized by {inflation_phrase}, "
            f"{labor_market_phrase}, and {growth_phrase}, {policy_phrase}."
        )
        return backdrop
        
    def _format_headlines(self, headlines: list) -> str:
        """Formats the top headlines into a readable string."""
        if not headlines:
            return "No top headlines available.\n"
        
        # Takes the top 5 headlines. Assumes headline objects have a 'headline' attribute.
        headline_texts = [f"- {h.headline}" for h in headlines[:5]]
        return "\n".join(headline_texts) + "\n"