"""
Client LLM centralizzato per GPT-5/GPT-5-mini.

Offre funzioni helper per inviare prompt e ottenere risposte JSON pulite.
"""

from __future__ import annotations

import json
import re
import time
from typing import Any, Dict, Optional

from openai import OpenAI

from config.settings import config


class LLMClient:
    """
    Wrapper semplice per le chiamate a GPT-5/GPT-5-mini con output JSON.
    """

    def __init__(self, use_full_model: bool = False):
        self.client = OpenAI(api_key=config.OPENAI_API_KEY)
        self.model_cfg = config.get_llm_config(use_full_model=use_full_model)

    @staticmethod
    def _strip_markdown_fences(text: str) -> str:
        if not text:
            return text
        text = re.sub(r"```json\n?", "", text, flags=re.IGNORECASE)
        text = re.sub(r"```\n?", "", text)
        return text.strip()

    def chat_json(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        tool_schema: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Invia una chat completion e restituisce il JSON decodificato.
        """
        model_name = self.model_cfg.get("model")
        # Costruisci kwargs compatibili: GPT-5 usa max_completion_tokens
        is_gpt5 = isinstance(model_name, str) and model_name.startswith("gpt-5")
        # Costruisci kwargs minimal compatibili
        kwargs = {
            "model": model_name,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        # Function calling per output JSON robusto
        if tool_schema:
            kwargs["tools"] = [
                {
                    "type": "function",
                    "function": {
                        "name": "return_json",
                        "description": "Restituisce il risultato come JSON valido",
                        "parameters": tool_schema,
                    },
                }
            ]
            kwargs["tool_choice"] = {
                "type": "function",
                "function": {"name": "return_json"},
            }
        # Parametri avanzati (solo per modelli che li supportano)
        if not is_gpt5:
            kwargs["top_p"] = self.model_cfg.get("top_p", 0.95)
            kwargs["frequency_penalty"] = self.model_cfg.get("frequency_penalty", 0.0)
            kwargs["presence_penalty"] = self.model_cfg.get("presence_penalty", 0.0)
            kwargs["temperature"] = (
                temperature
                if temperature is not None
                else self.model_cfg.get("temperature", 0.0)
            )
        max_tok = (
            max_tokens
            if max_tokens is not None
            else min(800, self.model_cfg.get("max_tokens", 800))
        )
        if is_gpt5:
            kwargs["max_completion_tokens"] = max_tok
            # Forza risposta JSON
            if not tool_schema:
                kwargs["response_format"] = {"type": "json_object"}
        else:
            kwargs["max_tokens"] = max_tok

        last_err = None
        for attempt in range(3):
            try:
                response = self.client.chat.completions.create(**kwargs)
                choice = response.choices[0]
                # Se function calling attivo, usa gli arguments
                tc = getattr(choice.message, "tool_calls", None)
                if tool_schema and tc:
                    args = tc[0].function.arguments if tc[0].function else "{}"
                    return json.loads(args)
                # Altrimenti parse content
                content = choice.message.content or ""
                content = self._strip_markdown_fences(content)
                return json.loads(content)
            except Exception as e:
                last_err = e
                # Backoff esponenziale breve
                time.sleep(0.5 * (2**attempt))
        # Dopo i retry falliti
        raise json.JSONDecodeError("Invalid JSON from LLM", "", 0)

    def chat_text(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: Optional[int] = None,
    ) -> str:
        """
        Invia una chat completion e restituisce il testo grezzo (no JSON).
        Evita parametri non supportati dai modelli GPT-5.
        """
        model_name = self.model_cfg.get("model")
        is_gpt5 = isinstance(model_name, str) and model_name.startswith("gpt-5")
        kwargs = {
            "model": model_name,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        max_tok = (
            max_tokens
            if max_tokens is not None
            else min(200, self.model_cfg.get("max_tokens", 800))
        )
        if is_gpt5:
            kwargs["max_completion_tokens"] = max_tok
        else:
            kwargs["max_tokens"] = max_tok

        response = self.client.chat.completions.create(**kwargs)
        return (response.choices[0].message.content or "").strip()


def get_llm_client(use_full_model: bool = False) -> LLMClient:
    return LLMClient(use_full_model=use_full_model)
