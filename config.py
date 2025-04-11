from datetime import datetime

# Configurazione API
# OPENAI_KEY="" # Inserire la tua chiave API OpenAI qui
GOOGLE_API_KEY="AIzaSyA2UiDjhtUWoF6jNOsIZfrmnUl_w2qYI-A" # Inserire la tua chiave API Google qui
USE_GEMINI = True  # Impostare su True per usare Gemini, False per OpenAI

# Directory di lavoro
TEMP_DIR = "data"

# Directory di base
TEMP_DIR="temp"
CIG_DIR="cig"
CIG_DOWNLOAD_DIR = "cig_downloads"
OCDS_DIR = "json_ocds"

# Configurazione date - Modificato per elaborare un anno alla volta
ANNO_INIZIO = 2022
ANNO_FINE = 2022

# Configurazione per riprendere lo scraping
RIPRENDI_DA_INDICE = 152  # Indice del link da cui riprendere per il 2022

# Configurazione dei file di output
LOTTI_RAW = "lotti_raw.xlsx"
LOTTI_TEMP="Lotti_temp.xlsx"
AGGIUDICAZIONI="aggiudicazioni_csv.csv"
AGGIUDICATARI="aggiudicatari_csv.csv"
VERBALI_INTERMEDIO="Verbali_Intermedio.xlsx"
VERBALI="LottiVerbali.xlsx"
FILE_CIG="cig.csv"
SERVIZIO_LUCE_INTERMEDIO="servizio_luce.csv"
SERVIZIO_LUCE_CONSIP_CIG="ServizioLuceConsip_CIG.xlsx"
SERVIZIO_LUCE_CONSIP_OCDS="ServizioLuceConsip_OCDS.xlsx"

# Configurazione per richieste HTTP
TIMEOUT_DOWNLOAD = 30  # Timeout per il download delle pagine (in secondi)
RETRY_ATTEMPTS = 3     # Numero di tentativi in caso di errore
RETRY_DELAY = 2        # Ritardo iniziale tra i tentativi (in secondi)

# Configurazione per il download dei CIG
CIG_START_YEAR = 2007
CIG_LAST_YEAR = datetime.now().year
CIG_LAST_MONTH = datetime.now().month

# Configurazione per il download degli OCDS
OCDS_START_YEAR = 2021
OCDS_START_MONTH = 5

# Configurazione Gemini
GEMINI_MODEL = "gemini-2.0-flash"  # Modello Gemini da utilizzare
GEMINI_TEMPERATURE = 0.2  # Valore di temperatura per le richieste Gemini
GEMINI_MAX_OUTPUT_TOKENS = 20000  # Numero massimo di token di output

# Configurazione per l'estrazione dei CIG
MIN_CIG_LENGTH = 7
MAX_CIG_LENGTH = 10

