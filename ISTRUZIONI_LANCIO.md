# 🚀 ISTRUZIONI PER LANCIARE IL SISTEMA

## ⚠️ IMPORTANTE: Prima di Iniziare

### 1. **SOSTITUISCI LA API KEY**
La chiave OpenAI che hai condiviso è stata esposta pubblicamente:
1. Vai su https://platform.openai.com/api-keys
2. **REVOCA** la chiave vecchia
3. **GENERA** una nuova chiave
4. **AGGIORNA** il file `.env` con la nuova chiave

### 2. Verifica Configurazione
```bash
# Controlla che .env contenga la chiave corretta
cat .env | grep OPENAI_API_KEY
```

## 📋 Come Lanciare il Sistema

### Metodo 1: Script Interattivo (CONSIGLIATO)
```bash
./run.sh
```
Segui il menu interattivo:
- 1 = Esecuzione completa
- 2 = Solo scraping 
- 3 = Solo analisi
- 4 = Lista sessioni
- 5 = Resume sessione
- 6 = Debug mode

### Metodo 2: Comando Diretto
```bash
# Esecuzione completa (download + scraping + analisi)
python3 -m src.main

# Skip download (usa dati esistenti)
python3 -m src.main --skip-download

# Skip download e scraping (solo analisi)
python3 -m src.main --skip-download --skip-scraping

# Debug verbose
python3 -m src.main --verbose --log-file
```

## 🔄 Resume da Interruzione

Se il processo si interrompe:

```bash
# 1. Lista sessioni disponibili
python3 -m src.main --list-sessions

# 2. Resume da sessione specifica
python3 -m src.main --resume pipeline_main_20250806_165122
```

## 📂 Struttura Output

```
data/
├── logs/           # Log di esecuzione
│   ├── gare.log   # Log principale
│   └── errors.log # Solo errori
├── checkpoints/    # Stati per resume
├── temp/          # File temporanei
├── output/        # File finali
├── cig/           # Dati CIG estratti
└── ocds/          # Dati OCDS estratti
```

## 🛠️ Troubleshooting

### Chrome/Selenium non funziona
```bash
# Installa Chrome se mancante
brew install --cask google-chrome

# O usa Chrome già installato
which google-chrome
```

### Memoria insufficiente
```bash
# Riduci workers paralleli
export MAX_WORKERS=3
python3 -m src.main
```

### Download bloccato
- Il sistema ha retry automatico
- Circuit breaker previene cascate di errori
- Resume automatico da checkpoint

## 📊 Monitoraggio Progress

Durante l'esecuzione vedrai:
- Progress bar per ogni operazione
- ETA stimato
- Contatore errori
- Checkpoint automatici

## ✅ Stato Implementazione

### Completato
- ✅ Sistema di checkpoint e resume
- ✅ Logging avanzato con colori
- ✅ Download manager CIG/OCDS
- ✅ Scraper Gazzetta Ufficiale
- ✅ Retry resiliente con circuit breaker
- ✅ Progress tracking

### Da Completare (Stub)
- ⏳ GazzettaAnalyzer
- ⏳ JsonProcessor
- ⏳ VerbaliAnalyzer
- ⏳ ServizioLuceAnalyzer
- ⏳ Concatenator
- ⏳ Transformer

I moduli stub loggano "DA IMPLEMENTARE" quando eseguiti.
Per completare il sistema, migra il codice dai file originali.

## 🎯 Prossimi Passi

1. **Testa download limitato**:
   ```bash
   # Modifica config/settings.py temporaneamente:
   # CIG_LAST_YEAR = 2023
   # CIG_LAST_MONTH = 1  # Solo gennaio
   
   python3 -m src.main
   ```

2. **Monitora i log**:
   ```bash
   tail -f data/logs/gare.log
   ```

3. **Verifica checkpoint**:
   ```bash
   ls -la data/checkpoints/
   ```

## 💡 Tips

- Usa `--verbose` per debug dettagliato
- I checkpoint salvano ogni progresso
- Resume riprende esattamente da dove interrotto
- Circuit breaker previene download loop infiniti
- Log colorati facilitano lettura errori

---

**RICORDA**: Sostituisci la API key prima di lanciare!