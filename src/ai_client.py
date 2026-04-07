"""
Client unificato per OpenAI o3 con prompt caching.

Questo modulo fornisce un client centralizzato per tutte le chiamate
al modello o3 con gestione ottimizzata del prompt caching.
"""

import hashlib
import json
import logging
import time
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

from openai import OpenAI
from pydantic import BaseModel

from config.settings import config

logger = logging.getLogger(__name__)


class PromptCache:
    """
    Gestione cache dei prompt per ottimizzare le chiamate al modello o3.
    """

    def __init__(self, cache_dir: Path = None):
        """
        Inizializza il sistema di cache.

        Args:
            cache_dir: Directory per salvare la cache
        """
        self.cache_dir = cache_dir or config.CACHE_DIR / "prompts"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_index = self._load_cache_index()

    def _load_cache_index(self) -> Dict[str, Dict]:
        """Carica l'indice della cache da file."""
        index_file = self.cache_dir / "cache_index.json"
        if index_file.exists():
            try:
                with open(index_file, "r") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Errore caricamento cache index: {e}")
        return {}

    def _save_cache_index(self) -> None:
        """Salva l'indice della cache su file."""
        index_file = self.cache_dir / "cache_index.json"
        try:
            with open(index_file, "w") as f:
                json.dump(self.cache_index, f, indent=2)
        except Exception as e:
            logger.error(f"Errore salvataggio cache index: {e}")

    def get_cache_key(self, prompt: str, system: str = "") -> str:
        """
        Genera una chiave univoca per il prompt.

        Args:
            prompt: Prompt utente
            system: Prompt di sistema

        Returns:
            str: Chiave hash del prompt
        """
        combined = f"{system}|||{prompt}"
        return hashlib.sha256(combined.encode()).hexdigest()

    def get(self, prompt: str, system: str = "") -> Optional[str]:
        """
        Recupera una risposta dalla cache se disponibile.

        Args:
            prompt: Prompt utente
            system: Prompt di sistema

        Returns:
            Optional[str]: Risposta cached o None
        """
        if not config.ENABLE_PROMPT_CACHING:
            return None

        cache_key = self.get_cache_key(prompt, system)

        if cache_key in self.cache_index:
            cache_entry = self.cache_index[cache_key]

            # Verifica TTL
            if time.time() - cache_entry["timestamp"] < config.CACHE_TTL_SECONDS:
                cache_file = self.cache_dir / f"{cache_key}.json"
                if cache_file.exists():
                    try:
                        with open(cache_file, "r") as f:
                            data = json.load(f)
                            logger.info(
                                f"✅ Cache hit per prompt (key: {cache_key[:8]}...)"
                            )
                            return data["response"]
                    except Exception as e:
                        logger.warning(f"Errore lettura cache: {e}")

        return None

    def set(self, prompt: str, system: str, response: str) -> None:
        """
        Salva una risposta nella cache.

        Args:
            prompt: Prompt utente
            system: Prompt di sistema
            response: Risposta del modello
        """
        if not config.ENABLE_PROMPT_CACHING:
            return

        cache_key = self.get_cache_key(prompt, system)
        cache_file = self.cache_dir / f"{cache_key}.json"

        # Salva risposta
        try:
            with open(cache_file, "w") as f:
                json.dump(
                    {
                        "prompt": prompt,
                        "system": system,
                        "response": response,
                        "timestamp": time.time(),
                    },
                    f,
                    indent=2,
                )

            # Aggiorna indice
            self.cache_index[cache_key] = {
                "timestamp": time.time(),
                "prompt_preview": prompt[:100],
            }
            self._save_cache_index()

            logger.info(f"💾 Risposta salvata in cache (key: {cache_key[:8]}...)")

            # Pulizia cache se troppo grande
            self._cleanup_old_entries()

        except Exception as e:
            logger.error(f"Errore salvataggio cache: {e}")

    def _cleanup_old_entries(self) -> None:
        """Rimuove le entry scadute dalla cache."""
        current_time = time.time()
        keys_to_remove = []

        for key, entry in self.cache_index.items():
            if current_time - entry["timestamp"] > config.CACHE_TTL_SECONDS:
                keys_to_remove.append(key)
                cache_file = self.cache_dir / f"{key}.json"
                if cache_file.exists():
                    cache_file.unlink()

        for key in keys_to_remove:
            del self.cache_index[key]

        if keys_to_remove:
            self._save_cache_index()
            logger.info(f"🗑️ Rimosse {len(keys_to_remove)} entry scadute dalla cache")


class O3Client:
    """
    Client unificato per OpenAI o3 con gestione ottimizzata.
    """

    _instance = None

    def __new__(cls):
        """Singleton pattern per client unico."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Inizializza il client o3."""
        if not hasattr(self, "initialized"):
            if not config.OPENAI_API_KEY:
                raise ValueError("OPENAI_API_KEY non configurata")

            self.client = OpenAI(api_key=config.OPENAI_API_KEY)
            self.cache = PromptCache()
            self.initialized = True

            logger.info(f"🚀 Client o3 inizializzato - Modello: {config.PRIMARY_MODEL}")
            logger.info(
                f"📦 Prompt caching: {'ATTIVO' if config.ENABLE_PROMPT_CACHING else 'DISATTIVO'}"
            )

    def complete(
        self,
        prompt: str,
        system: str = "",
        use_full_model: bool = False,
        response_model: Optional[BaseModel] = None,
        **kwargs,
    ) -> Any:
        """
        Esegue una completion con il modello o3.

        Args:
            prompt: Prompt utente
            system: Prompt di sistema
            use_full_model: Se True, usa o3 completo invece di o3-mini
            response_model: Modello Pydantic per parsing strutturato
            **kwargs: Parametri aggiuntivi per il modello

        Returns:
            Risposta del modello (stringa o oggetto parsed)
        """
        # Controlla cache
        cached_response = self.cache.get(prompt, system)
        if cached_response and not response_model:
            return cached_response

        # Prepara configurazione
        model_config = config.get_o3_config(use_full_model)
        model_config.update(kwargs)

        # Prepara messaggi
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        try:
            # Chiamata al modello
            if response_model:
                # Usa instructor per parsing strutturato
                from instructor import from_openai

                instructor_client = from_openai(self.client)

                response = instructor_client.chat.completions.create(
                    messages=messages, response_model=response_model, **model_config
                )
                return response
            else:
                # Completion standard
                response = self.client.chat.completions.create(
                    messages=messages, **model_config
                )

                result = response.choices[0].message.content

                # Salva in cache
                self.cache.set(prompt, system, result)

                return result

        except Exception as e:
            logger.error(f"Errore chiamata o3: {e}")
            raise

    def complete_batch(
        self, prompts: List[Dict[str, str]], use_full_model: bool = False, **kwargs
    ) -> List[str]:
        """
        Esegue completions in batch ottimizzato.

        Args:
            prompts: Lista di dict con 'prompt' e opzionale 'system'
            use_full_model: Se True, usa o3 completo
            **kwargs: Parametri aggiuntivi

        Returns:
            Lista di risposte
        """
        results = []

        for prompt_data in prompts:
            prompt = prompt_data.get("prompt", "")
            system = prompt_data.get("system", "")

            result = self.complete(
                prompt=prompt, system=system, use_full_model=use_full_model, **kwargs
            )
            results.append(result)

        return results

    def parse(
        self,
        prompt: str,
        response_model: BaseModel,
        system: str = "",
        use_full_model: bool = False,
        **kwargs,
    ) -> BaseModel:
        """
        Esegue parsing strutturato con modello Pydantic.

        Args:
            prompt: Prompt utente
            response_model: Modello Pydantic per il parsing
            system: Prompt di sistema
            use_full_model: Se True, usa o3 completo
            **kwargs: Parametri aggiuntivi

        Returns:
            Istanza del modello Pydantic
        """
        return self.complete(
            prompt=prompt,
            system=system,
            use_full_model=use_full_model,
            response_model=response_model,
            **kwargs,
        )

    def clear_cache(self) -> None:
        """Pulisce completamente la cache dei prompt."""
        self.cache.cache_index.clear()
        self.cache._save_cache_index()

        # Rimuovi tutti i file cache
        for cache_file in self.cache.cache_dir.glob("*.json"):
            if cache_file.name != "cache_index.json":
                cache_file.unlink()

        logger.info("🗑️ Cache prompt completamente pulita")


# Funzione helper per ottenere il client singleton
@lru_cache(maxsize=1)
def get_o3_client() -> O3Client:
    """
    Restituisce l'istanza singleton del client o3.

    Returns:
        O3Client: Client configurato e pronto
    """
    return O3Client()


# Export per retrocompatibilità
def get_openai_client():
    """
    Funzione di retrocompatibilità per codice esistente.

    Returns:
        OpenAI: Client OpenAI base
    """
    client = get_o3_client()
    return client.client
