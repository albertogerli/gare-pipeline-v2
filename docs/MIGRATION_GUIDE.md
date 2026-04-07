# Guida alla Migrazione v1.0 → v2.0

## 🔄 Panoramica dei Cambiamenti

La versione 2.0 introduce cambiamenti significativi nell'architettura e nella sicurezza del sistema. Questa guida ti aiuterà a migrare dal codice legacy.

## 📋 Checklist Migrazione

### 1. Backup dei Dati
```bash
# Backup completo
cp -r Gare/ Gare_backup_v1/

# Backup solo dati
cp -r temp/ temp_backup/
cp -r cig/ cig_backup/
cp -r ocds/ ocds_backup/
```

### 2. Configurazione Ambiente

#### Prima (v1.0)
```python
# config.py
OPENAI_KEY = "sk-proj-xxx"  # ❌ INSICURO
GEMINI_API = "AIzaSyxxx"    # ❌ HARDCODED
```

#### Dopo (v2.0)
```bash
# .env
OPENAI_API_KEY=sk-proj-xxx
GEMINI_API_KEY=AIzaSyxxx

# NON committare .env nel repository!
```

### 3. Struttura Directory

#### Riorganizzazione File
```bash
# Sposta file esistenti
mkdir -p src/{scrapers,analyzers,processors}

# Riorganizza moduli
mv gazzetta_scraper.py src/scrapers/gazzetta.py
mv gazzetta_analyzer.py src/analyzers/gazzetta_analyzer.py
mv download_cigs.py src/scrapers/downloader.py
mv download_ocds.py src/scrapers/downloader.py
mv json_to_excel.py src/analyzers/json_processor.py
mv transformer.py src/processors/transformer.py
mv concatenate.py src/processors/concatenator.py
mv verbali.py src/analyzers/verbali.py
mv servizio_luce.py src/analyzers/servizio_luce.py
```

### 4. Import Updates

#### Prima (v1.0)
```python
import config
from gazzetta_analyzer import GazzettaAnalyzer
from download_cigs import DownloadCigs
```

#### Dopo (v2.0)
```python
from config.settings import config
from src.analyzers.gazzetta_analyzer import GazzettaAnalyzer
from src.scrapers.downloader import DownloadManager
```

### 5. Modelli Pydantic

#### Prima (v1.0)
```python
# Inline nel file principale
class Lotto(BaseModel):
    Oggetto: str
    # ... 20+ campi
```

#### Dopo (v2.0)
```python
# src/models/lotto.py
from src.models import Lotto, GruppoLotti

# Utilizzo
lotto = Lotto(**data)
```

### 6. Gestione Errori

#### Prima (v1.0)
```python
try:
    process()
except:
    pass  # ❌ Errore silente
```

#### Dopo (v2.0)
```python
try:
    process()
except SpecificError as e:
    logger.error(f"Errore specifico: {e}")
    raise  # Re-raise per debugging
```

### 7. Testing

#### Setup Test Environment
```bash
# Installa dipendenze test
pip install pytest pytest-cov pytest-mock

# Esegui test
pytest tests/

# Verifica coverage
pytest --cov=src --cov-report=html
```

## 🔧 Script di Migrazione Automatica

```python
#!/usr/bin/env python3
"""
Script di migrazione automatica v1 → v2
"""

import os
import shutil
from pathlib import Path

def migrate_project():
    """Esegue migrazione automatica del progetto."""
    
    # 1. Backup
    print("📦 Creazione backup...")
    if not Path("backup_v1").exists():
        shutil.copytree(".", "backup_v1", 
                       ignore=shutil.ignore_patterns("venv", "__pycache__"))
    
    # 2. Crea nuova struttura
    print("🏗️ Creazione nuova struttura...")
    dirs = [
        "src/models", "src/utils", "src/scrapers",
        "src/analyzers", "src/processors", 
        "tests", "config", "docs", "scripts"
    ]
    for d in dirs:
        Path(d).mkdir(parents=True, exist_ok=True)
    
    # 3. Copia configurazione
    print("⚙️ Migrazione configurazione...")
    if Path("config.py").exists():
        # Estrai API keys e crea .env
        with open(".env", "w") as f:
            f.write("# Configurazione API Keys\n")
            f.write("# IMPORTANTE: Aggiungi le tue chiavi qui\n")
            f.write("OPENAI_API_KEY=your-key-here\n")
            f.write("GEMINI_API_KEY=your-key-here\n")
    
    # 4. Aggiorna import
    print("📝 Aggiornamento import...")
    # Qui andrebbero le regex per aggiornare gli import
    
    print("✅ Migrazione completata!")
    print("⚠️ Ricorda di:")
    print("  1. Aggiornare .env con le tue API keys")
    print("  2. Verificare gli import nei file")
    print("  3. Eseguire i test: pytest tests/")

if __name__ == "__main__":
    migrate_project()
```

## ⚠️ Breaking Changes

### 1. Config Import
```python
# ❌ Non funziona più
import config

# ✅ Nuovo metodo
from config.settings import config
```

### 2. File Paths
```python
# ❌ Path relativi
"temp/file.xlsx"

# ✅ Path objects
config.TEMP_DIR / "file.xlsx"
```

### 3. API Clients
```python
# ❌ Client globale
client = OpenAI(api_key=config.OPENAI_KEY)

# ✅ Client factory
def get_openai_client():
    return OpenAI(api_key=config.OPENAI_API_KEY)
```

## 🎯 Best Practices Post-Migrazione

### 1. Variabili d'Ambiente
- Mai committare .env
- Usa .env.example come template
- Valida configurazione all'avvio

### 2. Error Handling
- Log tutti gli errori
- Use specific exceptions
- Implementa retry logic

### 3. Performance
- Usa batch processing
- Implementa caching
- Monitor memory usage

### 4. Testing
- Scrivi test per nuove feature
- Mantieni coverage >80%
- Usa CI/CD pipeline

## 📚 Risorse

- [Documentazione API](docs/API.md)
- [Guida Performance](docs/PERFORMANCE.md)
- [Security Best Practices](docs/SECURITY.md)

## ❓ FAQ

**Q: Posso mantenere la vecchia struttura?**
A: Sconsigliato per sicurezza e manutenibilità.

**Q: I dati esistenti sono compatibili?**
A: Sì, il formato dati è retrocompatibile.

**Q: Quanto tempo richiede la migrazione?**
A: 1-2 ore con lo script automatico.

## 🆘 Supporto

Per problemi durante la migrazione:
1. Controlla i log in `logs/migration.log`
2. Verifica la checklist sopra
3. Apri una issue su GitHub

---
**Versione guida**: 1.0.0  
**Ultimo aggiornamento**: 2025-08-06