"""
Configurazione sicura dell'applicazione.

Questo modulo gestisce tutte le configurazioni dell'applicazione
utilizzando variabili d'ambiente per le informazioni sensibili.
"""

import os
from datetime import datetime
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

# Carica variabili d'ambiente da file .env
load_dotenv()


class Config:
    """
    Classe di configurazione principale.
    
    Gestisce tutte le impostazioni dell'applicazione con validazione
    e valori di default sicuri.
    """
    
    # Percorsi base
    BASE_DIR = Path(__file__).resolve().parent.parent
    DATA_DIR = BASE_DIR / "data"
    TEMP_DIR = DATA_DIR / "temp"
    OUTPUT_DIR = DATA_DIR / "output"
    
    # Crea directory se non esistono
    for dir_path in [DATA_DIR, TEMP_DIR, OUTPUT_DIR]:
        dir_path.mkdir(parents=True, exist_ok=True)
    
    # API Key principale - OpenAI
    OPENAI_API_KEY: Optional[str] = os.getenv("OPENAI_API_KEY")
    
    # Modelli AI principali - OpenAI GPT-5
    LLM_PRIMARY_MODEL = "gpt-5-mini"  # Modello leggero per alto throughput
    LLM_FULL_MODEL = "gpt-5"          # Modello completo per task complessi
    
    # Configurazione prompt caching per GPT-5
    ENABLE_PROMPT_CACHING = True
    CACHE_TEMPERATURE = 0.0  # Temperatura 0 per massima coerenza con cache
    MAX_CACHED_PROMPTS = 100
    CACHE_TTL_SECONDS = 3600  # 1 ora di cache
    
    # Parametri ottimizzati per GPT-5 (mini)
    LLM_CONFIG = {
        "model": LLM_PRIMARY_MODEL,
        "temperature": CACHE_TEMPERATURE,
        "max_tokens": 4096,
        "top_p": 0.95,
        "frequency_penalty": 0.0,
        "presence_penalty": 0.0,
        "seed": 42,  # Per risultati riproducibili
        "response_format": {"type": "json_object"},  # Formato JSON strutturato
    }
    
    # Configurazione GPT-5 per task complessi
    LLM_FULL_CONFIG = {
        "model": LLM_FULL_MODEL,
        "temperature": CACHE_TEMPERATURE,
        "max_tokens": 8192,
        "top_p": 0.95,
        "frequency_penalty": 0.0,
        "presence_penalty": 0.0,
        "seed": 42,
        "response_format": {"type": "json_object"},
    }
    
    # Sistema di prompt caching
    PROMPT_CACHE = {
        "enabled": ENABLE_PROMPT_CACHING,
        "strategy": "semantic",  # Cache basata su similarità semantica
        "similarity_threshold": 0.95,  # Soglia per riuso cache
        "max_cache_size_mb": 100,  # Limite memoria cache
    }
    
    # Directory per i download
    CIG_DIR = DATA_DIR / "cig"
    CIG_DOWNLOAD_DIR = DATA_DIR / "cig_downloads"
    OCDS_DIR = DATA_DIR / "ocds"
    
    # Range temporali
    GAZZETTA_START_YEAR = 2010
    GAZZETTA_LAST_YEAR = datetime.now().year
    
    CIG_START_YEAR = 2007
    CIG_LAST_YEAR = 2023
    CIG_LAST_MONTH = 12
    
    OCDS_START_YEAR = 2021
    OCDS_START_MONTH = 5
    OCDS_LAST_YEAR = datetime.now().year
    OCDS_LAST_MONTH = datetime.now().month
    
    # Nomi file output
    LOTTI_RAW = "Lotti_Raw.xlsx"
    LOTTI_GAZZETTA = "Lotti_Gazzetta.xlsx"
    LOTTI_OCDS = "Lotti_Ocds.xlsx"
    LOTTI_MERGED = "Lotti_Merged.xlsx"
    AGGIUDICAZIONI = "aggiudicazioni_csv.csv"
    AGGIUDICATARI = "aggiudicatari_csv.csv"
    VERBALI_INTERMEDIO = "Verbali_Intermedio.xlsx"
    VERBALI = "Verbali.xlsx"
    FINAL = "Final.xlsx"
    GARE = "Gare.xlsx"
    FILE_CIG = "cig.csv"
    SERVIZIO_LUCE_INTERMEDIO = "servizio_luce.csv"
    SERVIZIO_LUCE_CONSIP_CIG = "ServizioLuceConsip_CIG.xlsx"
    SERVIZIO_LUCE = "ServizioLuce.xlsx"
    
    # Parametri di performance
    MAX_WORKERS = 10
    CHUNK_SIZE = 10**6
    REQUEST_TIMEOUT = 60
    MAX_RETRIES = 5
    
    # Cache settings per dati
    CACHE_ENABLED = True
    CACHE_TTL = 3600  # 1 ora
    CACHE_DIR = TEMP_DIR / "cache"
    CACHE_DIR.mkdir(exist_ok=True)

    # Parametri operativi
    CONFIDENCE_NOT_RELEVANT_THRESHOLD = float(os.getenv("CONFIDENCE_NOT_RELEVANT_THRESHOLD", "0.7"))
    ENABLE_PARQUET = os.getenv("ENABLE_PARQUET", "true").lower() == "true"
    ENABLE_SQLITE = os.getenv("ENABLE_SQLITE", "true").lower() == "true"
    SQLITE_PATH = (OUTPUT_DIR / "gare.sqlite")
    
    @classmethod
    def validate(cls) -> bool:
        """
        Valida la configurazione e verifica che tutte le API key necessarie siano presenti.
        
        Returns:
            bool: True se la configurazione è valida, False altrimenti
        """
        valid = True
        
        if not cls.OPENAI_API_KEY:
            logger.error("OPENAI_API_KEY non configurata - necessaria per i modelli GPT-5")
            valid = False
        else:
            logger.info(f"✅ Configurazione LLM attiva - Modello: {cls.LLM_PRIMARY_MODEL}")
            logger.info(f"✅ Prompt caching: {'ATTIVO' if cls.ENABLE_PROMPT_CACHING else 'DISATTIVO'}")
            
        # Verifica directory
        for dir_name, dir_path in [
            ("CIG", cls.CIG_DIR),
            ("OCDS", cls.OCDS_DIR),
            ("TEMP", cls.TEMP_DIR),
        ]:
            if not dir_path.exists():
                logger.info(f"Creazione directory {dir_name}: {dir_path}")
                dir_path.mkdir(parents=True, exist_ok=True)
        
        return valid
    
    @classmethod
    def get_file_path(cls, filename: str, directory: str = "temp") -> Path:
        """
        Restituisce il percorso completo per un file.
        
        Args:
            filename: Nome del file
            directory: Directory di destinazione (temp, output, data)
            
        Returns:
            Path: Percorso completo del file
        """
        dir_map = {
            "temp": cls.TEMP_DIR,
            "output": cls.OUTPUT_DIR,
            "data": cls.DATA_DIR,
            "cig": cls.CIG_DIR,
            "ocds": cls.OCDS_DIR,
        }
        
        base_dir = dir_map.get(directory, cls.TEMP_DIR)
        return base_dir / filename
    
    @classmethod
    def get_llm_config(cls, use_full_model: bool = False) -> dict:
        """
        Restituisce la configurazione per il modello GPT-5.
        
        Args:
            use_full_model: Se True, usa GPT-5 completo invece di GPT-5-mini
            
        Returns:
            dict: Configurazione del modello
        """
        return cls.LLM_FULL_CONFIG if use_full_model else cls.LLM_CONFIG

    # Back-compat: mantieni alias per eventuali import esistenti
    get_o3_config = get_llm_config


# Istanza singleton della configurazione
config = Config()

# Valida configurazione all'importazione
if not config.validate():
    logger.error("⚠️ Configurazione non valida. I modelli GPT-5 richiedono OPENAI_API_KEY.")