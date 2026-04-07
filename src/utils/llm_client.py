"""Unified LLM client wrapper with GPT-4o-mini default."""

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Type, TypeVar

from openai import OpenAI
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class LLMClient:
    """Unified LLM client that wraps OpenAI API calls with robust error handling."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        temperature: float = 0.0,
    ):
        """Initialize LLM client.

        Args:
            api_key: OpenAI API key (defaults to env OPENAI_API_KEY)
            model: Model name (defaults to env MINI_MODEL or 'gpt-4o-mini')
            temperature: Temperature for generation
        """
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError(
                "OPENAI_API_KEY must be set in environment or passed as parameter"
            )

        self.model = model or os.getenv("MINI_MODEL", "gpt-4o-mini")
        self.temperature = temperature
        self.client = OpenAI(api_key=self.api_key)

    def parse_response(
        self,
        messages: List[Dict[str, str]],
        response_model: Type[T],
        max_retries: int = 3,
    ) -> T:
        """Parse response using structured output with Pydantic model.

        Args:
            messages: List of messages for the conversation
            response_model: Pydantic model class for parsing
            max_retries: Number of retry attempts

        Returns:
            Parsed response as the specified Pydantic model

        Raises:
            Exception: If parsing fails after all retries
        """
        for attempt in range(max_retries):
            try:
                completion = self.client.beta.chat.completions.parse(
                    model=self.model,
                    messages=messages,
                    response_format=response_model,
                    temperature=self.temperature,
                )

                if completion.choices[0].parsed:
                    return completion.choices[0].parsed
                else:
                    raise ValueError("Failed to parse response")

            except Exception as e:
                if attempt == max_retries - 1:
                    raise Exception(
                        f"Failed to parse after {max_retries} attempts: {e}"
                    )
                print(f"Attempt {attempt + 1} failed: {e}. Retrying...")
                continue

    def simple_completion(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        max_tokens: Optional[int] = None,
    ) -> str:
        """Get a simple text completion.

        Args:
            prompt: User prompt
            system_prompt: Optional system prompt
            max_tokens: Maximum tokens to generate

        Returns:
            Generated text response
        """
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        completion = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=self.temperature,
            max_tokens=max_tokens,
        )

        return completion.choices[0].message.content

    def parse_json_response(
        self, messages: List[Dict[str, str]], max_retries: int = 3
    ) -> Dict[str, Any]:
        """Parse JSON response with robust error handling.

        Args:
            messages: List of messages for the conversation
            max_retries: Number of retry attempts

        Returns:
            Parsed JSON as dictionary
        """
        for attempt in range(max_retries):
            try:
                completion = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    temperature=self.temperature,
                    response_format={"type": "json_object"},
                )

                response_text = completion.choices[0].message.content
                return json.loads(response_text)

            except json.JSONDecodeError as e:
                if attempt == max_retries - 1:
                    raise Exception(
                        f"Failed to parse JSON after {max_retries} attempts: {e}"
                    )
                print(f"JSON parse attempt {attempt + 1} failed: {e}. Retrying...")
                continue
            except Exception as e:
                if attempt == max_retries - 1:
                    raise Exception(
                        f"API call failed after {max_retries} attempts: {e}"
                    )
                print(f"API attempt {attempt + 1} failed: {e}. Retrying...")
                continue


# Global client instance for backward compatibility
_global_client: Optional[LLMClient] = None


def get_llm_client() -> LLMClient:
    """Get or create the global LLM client instance."""
    global _global_client
    if _global_client is None:
        _global_client = LLMClient()
    return _global_client


def set_global_client(client: LLMClient) -> None:
    """Set the global LLM client instance."""
    global _global_client
    _global_client = client
