# Sistema di Analisi Gare Pubbliche

Sistema avanzato per l'analisi di bandi di gara pubblici con supporto per resume e logging.

## 🚀 Setup Iniziale

### 1. Installazione Dipendenze

```bash
# Crea ambiente virtuale
python -m venv venv

# Attiva ambiente (macOS/Linux)
source venv/bin/activate

# Attiva ambiente (Windows)
venv\Scripts\activate

# Installa dipendenze
pip install -r requirements.txt
```

### 2. Configurazione API Keys

**⚠️ IMPORTANTE: NON condividere MAI le API keys nel codice!**

1. Copia il file di esempio:
```bash
cp .env.example .env
```

2. Modifica `.env` con le tue chiavi:
```bash
# Apri con il tuo editor preferito
nano .env
# oppure
code .env
```

3. Inserisci la tua API key OpenAI nel file `.env`:
```
OPENAI_API_KEY=la-tua-chiave-qui
```

## 📋 Come Lanciare il Sistema

### Esecuzione Standard
```bash
python src/main.py
```

### Opzioni Disponibili

```bash
# Salta download (usa dati esistenti)
python src/main.py --skip-download

# Salta scraping (usa dati esistenti)
python src/main.py --skip-scraping

# Solo trasformazione finale
python src/main.py --only-transform

# Logging verbose
python src/main.py --verbose --log-file

# Lista sessioni disponibili per resume
python src/main.py --list-sessions

# Resume da sessione precedente
python src/main.py --resume <session_id>
```

## 🔄 Resume da Interruzione

Se il processo viene interrotto:

1. **Lista sessioni disponibili**:
```bash
python src/main.py --list-sessions
```

2. **Riprendi da sessione specifica**:
```bash
python src/main.py --resume pipeline_main_20241206_143022
```

## 📊 Monitoraggio

### Log Files
- `data/logs/gare.log` - Log principale
- `data/logs/errors.log` - Solo errori
- `data/logs/session_*.log` - Log per sessione

### Checkpoint
- `data/checkpoints/` - Stato salvato per resume
- Ogni task ha checkpoint granulare con progresso

## 🛠️ Troubleshooting

### Errore API Key
Se vedi "API key non valida":
1. Verifica che `.env` esista
2. Controlla che la chiave sia corretta
3. Assicurati che la chiave non sia scaduta

### Memoria Insufficiente
Per dataset grandi:
```bash
# Aumenta workers paralleli
export MAX_WORKERS=10
python src/main.py
```

### Download Bloccati
Il sistema ha retry automatico con circuit breaker.
Se persistono problemi:
```bash
# Resume con retry aggressivo
python src/main.py --resume <session_id> --verbose
```

## 📁 Struttura Output

```
data/
├── temp/           # File temporanei e parziali
├── output/         # File finali processati
├── logs/           # Log di esecuzione
└── checkpoints/    # Stati per resume
```

## 🔒 Sicurezza

1. **MAI** committare `.env` su Git
2. **MAI** condividere API keys pubblicamente
3. Usa sempre variabili d'ambiente
4. Revoca e rigenera chiavi compromesse

## 📈 Performance

- Checkpoint automatici ogni fase
- Resume da interruzione senza perdita dati
- Parallel processing con ThreadPoolExecutor
- Circuit breaker per prevenire cascate errori
- Retry con backoff esponenziale

## 🆘 Supporto

Per problemi o domande:
1. Controlla i log in `data/logs/`
2. Usa `--verbose` per debug dettagliato
3. Resume da checkpoint per recuperare stato