import json
import os
from typing import Any, Dict, Optional

from openai import OpenAI

from src.config.config import DEFAULT_MODEL, DEFAULT_TEMPERATURE, OPENAI_API_KEY


class LLMWrapper:
    """Unified LLM wrapper for all API calls using OpenAI GPT models."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        temperature: float = DEFAULT_TEMPERATURE,
    ):
        """Initialize the LLM wrapper.

        Args:
            api_key: OpenAI API key (defaults to env variable)
            model: Model to use (defaults to MINI_MODEL env variable)
            temperature: Temperature for generation (defaults to 0)
        """
        # Note: Using gpt-5-mini as default model
        self.api_key = api_key or OPENAI_API_KEY
        if not self.api_key:
            raise ValueError(
                "OpenAI API key not found. Set OPENAI_API_KEY environment variable."
            )

        self.model = model or DEFAULT_MODEL
        self.temperature = temperature
        self.client = OpenAI(api_key=self.api_key)

    def complete(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        max_tokens: Optional[int] = None,
        response_format: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> str:
        """Generate a completion from the LLM.

        Args:
            prompt: The user prompt
            system_prompt: Optional system prompt
            max_tokens: Maximum tokens to generate
            response_format: Optional response format (e.g., {"type": "json_object"})
            **kwargs: Additional parameters for the API call

        Returns:
            The generated text response
        """
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        params = {
            "model": self.model,
            "messages": messages,
            "temperature": self.temperature,
        }

        if max_tokens:
            params["max_tokens"] = max_tokens
        if response_format:
            params["response_format"] = response_format

        # Add any additional kwargs
        params.update(kwargs)

        try:
            response = self.client.chat.completions.create(**params)
            return response.choices[0].message.content
        except Exception as e:
            print(f"Error calling OpenAI API: {e}")
            raise

    def complete_json(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        max_tokens: Optional[int] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """Generate a JSON response from the LLM.

        Args:
            prompt: The user prompt
            system_prompt: Optional system prompt
            max_tokens: Maximum tokens to generate
            **kwargs: Additional parameters for the API call

        Returns:
            The parsed JSON response
        """
        # Add instruction to return JSON if not already in prompt
        if "json" not in prompt.lower():
            prompt += "\n\nReturn the response as valid JSON."

        response = self.complete(
            prompt=prompt,
            system_prompt=system_prompt,
            max_tokens=max_tokens,
            response_format={"type": "json_object"},
            **kwargs,
        )

        try:
            return json.loads(response)
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response: {e}")
            print(f"Response: {response}")
            # Try to extract JSON from the response
            try:
                # Find JSON-like content
                import re

                json_match = re.search(r"\{.*\}", response, re.DOTALL)
                if json_match:
                    return json.loads(json_match.group())
            except:
                pass
            raise ValueError(f"Could not parse JSON from response: {response}")

    def analyze_text(
        self,
        text: str,
        analysis_type: str = "general",
        **kwargs,
    ) -> Dict[str, Any]:
        """Analyze text for specific purposes.

        Args:
            text: Text to analyze
            analysis_type: Type of analysis (e.g., "tender", "general")
            **kwargs: Additional parameters

        Returns:
            Analysis results as a dictionary
        """
        prompts = {
            "tender": """Analyze this tender text and extract:
                - CIG (contract identifier)
                - Subject/Object of the tender
                - Category (Illuminazione/Videosorveglianza/etc)
                - Importance (1-10)
                - Key entities and amounts
                Return as JSON.""",
            "general": """Analyze this text and provide a structured summary.
                Return as JSON with relevant fields.""",
        }

        prompt = prompts.get(analysis_type, prompts["general"])
        prompt = f"{prompt}\n\nText: {text[:4000]}"  # Limit text length

        return self.complete_json(prompt, **kwargs)


# Singleton instance for convenience
_default_llm = None


def get_llm(
    api_key: Optional[str] = None,
    model: Optional[str] = None,
    temperature: float = DEFAULT_TEMPERATURE,
) -> LLMWrapper:
    """Get or create a default LLM instance.

    Args:
        api_key: Optional API key override
        model: Optional model override
        temperature: Optional temperature override

    Returns:
        LLMWrapper instance
    """
    global _default_llm
    if _default_llm is None or api_key or model:
        _default_llm = LLMWrapper(api_key=api_key, model=model, temperature=temperature)
    return _default_llm