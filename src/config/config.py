import os
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project paths
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data"
TEMP_DIR = DATA_DIR / "temp"
OCDS_DIR = DATA_DIR / "ocds"

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
TEMP_DIR.mkdir(exist_ok=True)
OCDS_DIR.mkdir(exist_ok=True)

# API Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DEFAULT_MODEL = os.getenv("MINI_MODEL", "gpt-5-mini")
DEFAULT_TEMPERATURE = 0

# Processing Configuration
MAX_WORKERS = 20
CHUNK_SIZE = 8192
REQUEST_TIMEOUT = 60

# Gazzetta Scraper Configuration
GAZZETTA_START_YEAR = 2015
GAZZETTA_END_YEAR = 2025

# OCDS Downloader Configuration
OCDS_START_YEAR = 2021
OCDS_START_MONTH = 5

# File paths
LOTTI_RAW_FILE = TEMP_DIR / "Lotti_Raw.xlsx"
LOTTI_ANALIZZATI_FILE = TEMP_DIR / "Lotti_Analizzati.xlsx"
GARE_ANALIZZATE_FILE = TEMP_DIR / "Gare_Analizzate.xlsx"
RISULTATI_ANALISI_FILE = TEMP_DIR / "RisultatiAnalisi.xlsx"