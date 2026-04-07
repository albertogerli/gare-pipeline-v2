#!/usr/bin/env python
"""
Gazzetta Analyzer Ottimizzato con Sistema a Due Stadi
Stage 1: GPT-5-mini per filtro e classificazione rapida
Stage 2: GPT-5 per record rilevanti con estrazione dettagliata

Riduzione stimata token: 60-70%
"""

import hashlib
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from pydantic import BaseModel, field_validator

from config.settings import config
from src.llm.client import get_llm_client

llm_mini = get_llm_client(use_full_model=False)
llm_full = get_llm_client(use_full_model=True)

# ==================== LLM CIRCUIT BREAKER ====================
# Se lo Stage 1 LLM fallisce ripetutamente il parsing JSON, disattiva LLM e usa regex.
ALLOW_LLM_STAGE1 = True  # Richiesto: usare GPT-5-mini per pertinenza
STAGE1_LLM_ERRORS = 0
try:
    CIRCUIT_BREAKER_THRESHOLD = int(os.getenv("GAZZETTA_STAGE1_CB_THRESHOLD", "10"))
except Exception:
    CIRCUIT_BREAKER_THRESHOLD = 10

# ==================== CONFIGURAZIONE ====================


class Config:
    """Configurazione centralizzata con supporto .env"""

    # Processing
    MAX_WORKERS = int(os.getenv("MAX_WORKERS", "3"))
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
    USE_TWO_STAGE = os.getenv("USE_TWO_STAGE", "true").lower() == "true"
    CHECKPOINT_EVERY = int(os.getenv("CHECKPOINT_EVERY", "500"))
    # Throttle opzionale tra richieste LLM (ms)
    THROTTLE_MS = int(os.getenv("GAZZETTA_THROTTLE_MS", "0"))

    # Paths
    TEMP_DIR = "data/temp"
    LOTTI_RAW = "Lotti_Raw.xlsx"
    LOTTI_GAZZETTA = "Lotti_Gazzetta_Optimized.xlsx"
    CHECKPOINT_FILE = "checkpoint_optimized.json"


# Client LLM GPT-5/GPT-5-mini
CLIENT_MINI = llm_mini
CLIENT_FULL = llm_full

# ==================== MODELLI PYDANTIC ====================


class FilterResult(BaseModel):
    """Risultato del filtro rapido con GPT-5-mini"""

    is_relevant: bool
    confidence: float
    category: str
    reason: str
    estimated_lots: int


class QuickCategory(str, Enum):
    """Categorie semplificate per filtro rapido"""

    ILLUMINAZIONE = "Illuminazione"
    ENERGIA = "Energia"
    EDIFICI = "Edifici"
    INFRASTRUTTURE = "Infrastrutture"
    TRASPORTI = "Trasporti"
    DIGITALE = "Digitale"
    AMBIENTE = "Ambiente"
    NON_RILEVANTE = "Non Rilevante"


# Riusa gli stessi Enum del file originale
class CategoriaLotto(str, Enum):
    ILLUMINAZIONE = "Illuminazione"
    VIDEOSORVEGLIANZA = "Videosorveglianza"
    GALLERIE = "Gallerie"
    TUNNEL = "Tunnel"
    IMPIANTI = "Impianti"
    EDIFICI = "Edifici"
    TERMICI = "Termici"
    COLONNINE = "Colonnine"
    RICARICA = "Ricarica"
    PARCHEGGI = "Parcheggi"
    STRUTTURE_SPORTIVE = "Strutture Sportive"
    TRASPORTI_PUBBLICI = "Trasporti Pubblici"
    INFRASTRUTTURE_DIGITALI = "Infrastrutture Digitali"
    ACQUEDOTTI = "Acquedotti"
    SCUOLE = "Scuole"
    SANITARIO = "Sanitario"
    RIFIUTI = "Rifiuti"
    ALTRO = ""


class TipoIlluminazione(Enum):
    PUBBLICA = "Pubblica"
    STRADALE = "Stradale"
    SPORTIVA = "Sportiva"
    ARCHITETTURALE = "Architetturale"
    URBANA = "Urbana"
    ALTRO = ""


class TipoEfficientamento(Enum):
    ENERGETICO = "Energetico"
    TECNOLOGICO = "Tecnologico"
    MANUTENZIONE_ORDINARIA = "Manutenzione Ordinaria"
    MANUTENZIONE_STRAORDINARIA = "Manutenzione Straordinaria"
    ILLUMINAZIONE_LED = "Illuminazione LED"
    ALTRO = ""


class TipoAppalto(Enum):
    AFFIDAMENTO = "Affidamento"
    APPALTO = "Appalto"
    CONCESSIONE = "Concessione"
    PROJECT_FINANCING = "Project Financing"
    SERVIZIO = "Servizio"
    FORNITURA = "Fornitura"
    ACCORDO_QUADRO = "Accordo Quadro"
    ALTRO = ""


class TipoIntervento(Enum):
    EFFICIENTAMENTO_ENERGETICO = "Efficientamento Energetico"
    RIQUALIFICAZIONE = "Riqualificazione"
    MANUTENZIONE_ORDINARIA = "Manutenzione Ordinaria"
    MANUTENZIONE_STRAORDINARIA = "Manutenzione Straordinaria"
    GESTIONE_IMPIANTI = "Gestione Impianti"
    ALTRO = ""


class TipoImpianto(Enum):
    PUBBLICA_ILLUMINAZIONE = "Pubblica Illuminazione"
    ILLUMINAZIONE_STRADALE = "Illuminazione Stradale"
    IMPIANTI_ELETTRICI = "Impianti Elettrici"
    VIDEOSORVEGLIANZA = "Videosorveglianza"
    SMART_CITY = "Smart City"
    ALTRO = ""


class TipoEnergia(Enum):
    ENERGIA_ELETTRICA = "Energia Elettrica"
    FONTI_RENEWABLE = "Fonti Rinnovabili"
    RISPARMIO_ENERGETICO = "Risparmio Energetico"
    ALTRO = ""


class TipoOperazione(str, Enum):
    GESTIONE = "Gestione"
    MANUTENZIONE = "Manutenzione"
    LAVORI = "Lavori"
    EFFICIENTAMENTO = "Efficientamento"
    FORNITURE = "Forniture"
    ALTRO = ""


class Lotto(BaseModel):
    """Modello completo per estrazione con o3"""

    Oggetto: str
    Categoria: CategoriaLotto
    TipoIlluminazione: TipoIlluminazione
    TipoEfficientamento: TipoEfficientamento
    TipoAppalto: TipoAppalto
    TipoIntervento: TipoIntervento
    TipoImpianto: TipoImpianto
    TipoEnergia: TipoEnergia
    TipoOperazione: TipoOperazione
    Procedura: str
    AmministrazioneAggiudicatrice: str
    OfferteRicevute: str
    DurataAppalto: str
    Scadenza: str
    ImportoAggiudicazione: str
    DataAggiudicazione: str
    Sconto: str
    Comune: str
    Aggiudicatario: str
    CIG: str
    CUP: str
    CriterioAggiudicazione: str = ""

    @field_validator("Scadenza", "DataAggiudicazione", mode="before")
    def parse_date(cls, v):
        if not v or "non specificat" in v.lower():
            return ""
        # Parsing date logic from original
        return v

    @field_validator("ImportoAggiudicazione", mode="before")
    def extract_amount(cls, v):
        if not v:
            return ""
        matches = re.findall(r"\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?", v)
        if matches:
            # Take largest amount
            amounts = []
            for m in matches:
                try:
                    clean = m.replace(".", "").replace(",", ".")
                    amounts.append(float(clean))
                except:
                    pass
            if amounts:
                return f"{max(amounts):.2f}"
        return ""


class GrouppoLotti(BaseModel):
    Lotti: List[Lotto]


# ==================== FUNZIONI UTILITÀ ====================


def hash_text(text: str) -> str:
    """Genera hash SHA256 del testo"""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def clean_text(text: str) -> str:
    """Pulisce il testo rimuovendo caratteri speciali"""
    text = text.replace("\\n", " ").replace("\n", " ")
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def quick_filter_regex(text: str) -> FilterResult:
    """Fallback regex permissivo: prova comunque Stage 2 con conf. bassa."""
    return FilterResult(
        is_relevant=True,
        confidence=0.5,
        category="ALTRO",
        reason="regex fallback",
        estimated_lots=1,
    )


def load_checkpoint() -> Dict[str, Any]:
    """Carica checkpoint se esiste"""
    checkpoint_path = Path(Config.TEMP_DIR) / Config.CHECKPOINT_FILE
    if checkpoint_path.exists():
        try:
            with open(checkpoint_path, "r") as f:
                checkpoint = json.load(f)
                print(
                    f"✅ Checkpoint caricato: {len(checkpoint.get('processed', []))} record già processati"
                )
                return checkpoint
        except Exception as e:
            print(f"⚠️ Errore caricamento checkpoint: {e}")
            # Backup corrotto e crea nuovo
            backup_path = checkpoint_path.with_suffix(".json.corrupted")
            checkpoint_path.rename(backup_path)
            print(f"   Backup salvato in: {backup_path}")
    return {
        "processed": [],
        "stage1_cache": {},
        "last_index": 0,
        "partial_results": [],
        "timestamp": datetime.now().isoformat(),
    }


def save_checkpoint(checkpoint: Dict[str, Any], force: bool = False):
    """Salva checkpoint con backup atomico"""
    checkpoint_path = Path(Config.TEMP_DIR) / Config.CHECKPOINT_FILE
    checkpoint_path.parent.mkdir(exist_ok=True)

    # Aggiorna timestamp
    checkpoint["timestamp"] = datetime.now().isoformat()
    checkpoint["total_processed"] = len(checkpoint.get("processed", []))

    # Salvataggio atomico con file temporaneo
    temp_path = checkpoint_path.with_suffix(".tmp")
    try:
        with open(temp_path, "w") as f:
            json.dump(checkpoint, f, indent=2)

        # Backup esistente se presente
        if checkpoint_path.exists():
            backup_path = checkpoint_path.with_suffix(".bak")
            checkpoint_path.replace(backup_path)

        # Rinomina atomicamente
        temp_path.replace(checkpoint_path)

        if force:
            print(
                f"💾 Checkpoint forzato: {checkpoint['total_processed']} record salvati"
            )

    except Exception as e:
        print(f"❌ Errore salvataggio checkpoint: {e}")
        if temp_path.exists():
            temp_path.unlink()


# ==================== STAGE 1: FILTRO RAPIDO ====================


def stage1_quick_filter(text: str, use_cache: bool = True) -> FilterResult:
    global STAGE1_LLM_ERRORS, ALLOW_LLM_STAGE1
    """
    Stage 1: Usa GPT-5-mini per filtro rapido e classificazione base
    ~200-300 token per record
    OpenAI Prompt Caching automatico per system messages > 1024 token
    """

    # Se il circuito è aperto, restituisci non rilevante
    if not ALLOW_LLM_STAGE1:
        return FilterResult(
            is_relevant=True,
            confidence=0.8,
            category="ALTRO",
            reason="circuit_breaker_proceed",
            estimated_lots=1,
        )

    # System prompt lungo per attivare caching automatico (>1024 token)
    # Questo verrà automaticamente cached da OpenAI per ridurre costi
    system_prompt = """Sei un assistente specializzato nell'analisi di gare d'appalto italiane per enti pubblici.
Il tuo compito è identificare rapidamente se un testo di gara riguarda infrastrutture pubbliche rilevanti.

CATEGORIE RILEVANTI DA IDENTIFICARE:

1. ILLUMINAZIONE - Include:
   - Illuminazione pubblica stradale, piazze, parchi
   - Sostituzione lampioni con LED
   - Impianti di illuminazione edifici pubblici
   - Illuminazione monumentale e architettonica
   - Sistemi smart lighting e telecontrollo
   - Manutenzione impianti illuminazione

2. ENERGIA - Include:
   - Efficientamento energetico edifici pubblici
   - Impianti fotovoltaici e rinnovabili
   - Cogenerazione e trigenerazione
   - Audit e certificazioni energetiche
   - Gestione calore e servizio energia
   - Riqualificazione centrali termiche

3. EDIFICI - Include:
   - Riqualificazione scuole e asili
   - Ristrutturazione ospedali e ASL
   - Manutenzione edifici comunali
   - Adeguamento sismico strutture pubbliche
   - Restauro beni culturali
   - Edilizia residenziale pubblica

4. INFRASTRUTTURE - Include:
   - Manutenzione strade e marciapiedi
   - Costruzione/riparazione ponti e viadotti
   - Gallerie e tunnel
   - Rotatorie e svincoli
   - Piste ciclabili
   - Opere di urbanizzazione

5. TRASPORTI - Include:
   - Trasporto pubblico locale
   - Metropolitane e tram
   - Stazioni ferroviarie e autostazioni
   - Parcheggi pubblici e interscambi
   - Mobilità elettrica e colonnine ricarica
   - Servizi di bike/car sharing

6. DIGITALE - Include:
   - Smart city e IoT urbano
   - Videosorveglianza e sicurezza
   - Reti wifi pubbliche
   - Fibra ottica e banda ultra larga
   - Digitalizzazione PA
   - Piattaforme e servizi cloud

7. AMBIENTE - Include:
   - Gestione rifiuti e raccolta differenziata
   - Impianti depurazione acque
   - Reti idriche e fognarie
   - Verde pubblico e parchi
   - Bonifiche ambientali
   - Economia circolare

CRITERI DI ESCLUSIONE:
- Forniture di cancelleria o materiale d'ufficio
- Servizi di pulizia ordinaria
- Servizi di ristorazione/mensa
- Forniture sanitarie di consumo
- Servizi professionali (legali, contabili, etc.)
- Eventi e manifestazioni temporanee

OUTPUT RICHIESTO:
Devi rispondere SEMPRE e SOLO con un oggetto JSON valido con questa struttura esatta:
{
  "is_relevant": true/false (booleano),
  "confidence": 0.0-1.0 (float, livello di certezza),
  "category": "CATEGORIA" (una delle 7 categorie sopra o "ALTRO"),
  "reason": "motivo breve max 50 caratteri",
  "estimated_lots": numero intero (stima numero lotti nel testo)
}

ESEMPI DI CLASSIFICAZIONE:
- "Affidamento servizio manutenzione illuminazione pubblica" → is_relevant: true, category: "ILLUMINAZIONE"
- "Fornitura carta e cancelleria per uffici comunali" → is_relevant: false, category: "ALTRO"
- "Lavori di efficientamento energetico scuola elementare" → is_relevant: true, category: "ENERGIA"
- "Servizio di pulizia locali comunali" → is_relevant: false, category: "ALTRO"
- "Realizzazione pista ciclabile lungo fiume" → is_relevant: true, category: "INFRASTRUTTURE"

Analizza il testo fornito e classifica secondo questi criteri."""

    # User prompt con solo il testo variabile
    user_prompt = f"Testo da analizzare:\n\n{text[:1500]}"

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        # Nuovo approccio robusto: fai generare testo codificato a pipe e parse manuale
        instruction = (
            "Rispondi SOLO in una riga nel formato: IS_RELEVANT|CONFIDENCE|CATEGORY|ESTIMATED_LOTS|REASON. "
            "Esempio: true|0.82|ILLUMINAZIONE|1|testo breve"
        )
        txt = CLIENT_MINI.chat_text(
            system_prompt=system_prompt + "\n" + instruction,
            user_prompt=user_prompt,
            max_tokens=80,
        )
        parts = [p.strip() for p in txt.split("|")]
        if len(parts) >= 4:
            is_rel = str(parts[0]).lower() in ("true", "1", "yes", "y")
            try:
                conf = float(parts[1])
            except Exception:
                conf = 0.5
            cat = parts[2] or "ALTRO"
            try:
                est = int(parts[3])
            except Exception:
                est = 1
            reason = parts[4] if len(parts) > 4 else ""
            return FilterResult(
                is_relevant=is_rel,
                confidence=conf,
                category=cat,
                reason=reason,
                estimated_lots=est,
            )
        # Secondo tentativo più semplice
        simple_instr = (
            "Rispondi SOLO in: true|0.8|ALTRO|1|ok oppure false|0.9|ALTRO|0|no"
        )
        txt2 = CLIENT_MINI.chat_text(
            system_prompt=system_prompt + "\n" + simple_instr,
            user_prompt=user_prompt,
            max_tokens=40,
        )
        parts2 = [p.strip() for p in txt2.split("|")]
        if len(parts2) >= 4:
            is_rel = str(parts2[0]).lower() in ("true", "1", "yes", "y")
            try:
                conf = float(parts2[1])
            except Exception:
                conf = 0.5
            cat = parts2[2] or "ALTRO"
            try:
                est = int(parts2[3])
            except Exception:
                est = 1
            reason = parts2[4] if len(parts2) > 4 else ""
            return FilterResult(
                is_relevant=is_rel,
                confidence=conf,
                category=cat,
                reason=reason,
                estimated_lots=est,
            )
        # Se anche il secondo fallisce: considera rilevante per non perdere record
        return FilterResult(
            is_relevant=True,
            confidence=0.8,
            category="ALTRO",
            reason="llm_parse_fail_proceed",
            estimated_lots=1,
        )

    except json.JSONDecodeError as e:
        print(f"Errore parsing JSON Stage 1: {e}")
        print("Response: N/A")
        # Conta errore e attiva circuit breaker se necessario
        STAGE1_LLM_ERRORS += 1
        if STAGE1_LLM_ERRORS >= CIRCUIT_BREAKER_THRESHOLD and ALLOW_LLM_STAGE1:
            ALLOW_LLM_STAGE1 = False
            print(
                f"⚠️ Circuit breaker attivato per Stage 1 LLM (errori: {STAGE1_LLM_ERRORS}). Uso regex-only."
            )
        return FilterResult(
            is_relevant=True,
            confidence=0.8,
            category="ALTRO",
            reason="json_error_proceed",
            estimated_lots=1,
        )
    except Exception as e:
        print(f"Errore Stage 1: {e}")
        # Conta errore e attiva circuit breaker se necessario
        STAGE1_LLM_ERRORS += 1
        if STAGE1_LLM_ERRORS >= CIRCUIT_BREAKER_THRESHOLD and ALLOW_LLM_STAGE1:
            ALLOW_LLM_STAGE1 = False
            print(
                f"⚠️ Circuit breaker attivato per Stage 1 LLM (errori: {STAGE1_LLM_ERRORS}). Uso regex-only."
            )
        return FilterResult(
            is_relevant=True,
            confidence=0.8,
            category="ALTRO",
            reason="llm_error_proceed",
            estimated_lots=1,
        )


# ==================== STAGE 2: ESTRAZIONE DETTAGLIATA ====================


def stage2_detailed_extraction(text: str, estimated_lots: int) -> List[Lotto]:
    """
    Stage 2: Usa GPT-5 solo per record rilevanti
    ~2000-3000 token per record
    System prompt > 1024 token per attivare caching automatico OpenAI
    """

    # System prompt lungo per caching automatico (>1024 token)
    system_prompt_cached = """Sei un esperto analista specializzato nell'estrazione di dati strutturati da testi di gare d'appalto italiane.
Il tuo compito è estrarre TUTTE le informazioni disponibili dal testo e mapparle nei campi strutturati secondo il modello Pydantic fornito.

REGOLE GENERALI DI ESTRAZIONE:
1. Ogni campo mancante o non trovato deve essere una stringa vuota ""
2. MAI inventare informazioni non presenti nel testo
3. Estrarre il valore più specifico quando ci sono multiple opzioni
4. Mantenere la formattazione originale per nomi propri e denominazioni

REGOLE SPECIFICHE PER CAMPO:

OGGETTO:
- Estrarre la descrizione completa dell'appalto
- Include tipologia di servizio/lavoro/fornitura
- Mantenere riferimenti a normative se presenti

CATEGORIA (CategoriaLotto):
- ILLUMINAZIONE: impianti illuminazione pubblica, LED, lampioni
- VIDEOSORVEGLIANZA: sistemi di videosorveglianza, telecamere
- GALLERIE: lavori in gallerie
- TUNNEL: lavori in tunnel
- IMPIANTI: impianti tecnologici generici
- EDIFICI: lavori su edifici pubblici
- TERMICI: impianti termici, riscaldamento
- COLONNINE: colonnine di ricarica elettrica
- RICARICA: infrastrutture di ricarica
- PARCHEGGI: parcheggi pubblici
- STRUTTURE_SPORTIVE: impianti sportivi
- TRASPORTI_PUBBLICI: servizi di trasporto pubblico
- INFRASTRUTTURE_DIGITALI: reti, fibra, datacenter
- ACQUEDOTTI: reti idriche
- SCUOLE: edifici scolastici
- SANITARIO: strutture sanitarie
- RIFIUTI: gestione rifiuti
- ALTRO: se non rientra nelle categorie sopra

TIPO_ILLUMINAZIONE (TipoIlluminazione):
- PUBBLICA: illuminazione pubblica generica
- STRADALE: specifica per strade
- SPORTIVA: impianti sportivi
- ARCHITETTURALE: monumentale/decorativa
- URBANA: aree urbane
- ALTRO: altri tipi

TIPO_EFFICIENTAMENTO (TipoEfficientamento):
- ENERGETICO: risparmio energetico
- TECNOLOGICO: upgrade tecnologico
- MANUTENZIONE_ORDINARIA: manutenzione programmata
- MANUTENZIONE_STRAORDINARIA: interventi straordinari
- ILLUMINAZIONE_LED: conversione a LED
- ALTRO: altri tipi

TIPO_APPALTO (TipoAppalto):
- AFFIDAMENTO: affidamento diretto
- APPALTO: appalto pubblico
- CONCESSIONE: concessione di servizi
- PROJECT_FINANCING: project financing
- SERVIZIO: contratto di servizio
- FORNITURA: fornitura beni
- ACCORDO_QUADRO: accordo quadro
- ALTRO: altre forme

TIPO_INTERVENTO (TipoIntervento):
- EFFICIENTAMENTO_ENERGETICO: interventi di efficientamento
- RIQUALIFICAZIONE: riqualificazione generale
- MANUTENZIONE_ORDINARIA: manutenzione ordinaria
- MANUTENZIONE_STRAORDINARIA: manutenzione straordinaria
- GESTIONE_IMPIANTI: gestione continuativa
- ALTRO: altri interventi

TIPO_IMPIANTO (TipoImpianto):
- PUBBLICA_ILLUMINAZIONE: impianti IP
- ILLUMINAZIONE_STRADALE: illuminazione stradale
- IMPIANTI_ELETTRICI: impianti elettrici generici
- VIDEOSORVEGLIANZA: sistemi di videosorveglianza
- SMART_CITY: tecnologie smart city
- ALTRO: altri impianti

TIPO_ENERGIA (TipoEnergia):
- ENERGIA_ELETTRICA: fornitura/gestione energia elettrica
- FONTI_RENEWABLE: energie rinnovabili
- RISPARMIO_ENERGETICO: interventi di risparmio
- ALTRO: altri aspetti energetici

TIPO_OPERAZIONE (TipoOperazione):
- GESTIONE: gestione servizi
- MANUTENZIONE: manutenzione
- LAVORI: esecuzione lavori
- EFFICIENTAMENTO: efficientamento
- FORNITURE: fornitura beni
- ALTRO: altre operazioni

PROCEDURA:
- Estrarre tipo di procedura (aperta, ristretta, negoziata, etc.)
- Include riferimenti normativi se presenti

AMMINISTRAZIONE_AGGIUDICATRICE:
- Nome completo dell'ente appaltante
- Mantenere denominazione ufficiale

OFFERTE_RICEVUTE:
- Solo il numero (senza testo)
- Stringa vuota se non specificato

DURATA_APPALTO:
- Convertire SEMPRE in giorni
- 1 mese = 30 giorni
- 1 anno = 365 giorni
- Format: solo numero

SCADENZA:
- Formato: dd/mm/yyyy
- Estrarre da "entro il" o simili
- Stringa vuota se non presente

IMPORTO_AGGIUDICAZIONE:
- Formato: numero con 2 decimali (es: 150000.00)
- Rimuovere simboli valuta e separatori migliaia
- Prendere importo più alto se multipli

DATA_AGGIUDICAZIONE:
- Formato: dd/mm/yyyy
- Cercare "aggiudicato il", "determina del", etc.

SCONTO:
- Percentuale con simbolo % o valore assoluto
- Mantenere formato originale

COMUNE:
- Nome del comune/città
- Estrarre da indirizzo se necessario

AGGIUDICATARIO:
- Ragione sociale completa ditta vincitrice
- Include forma societaria (SpA, Srl, etc.)

CIG:
- Codice Identificativo Gara
- Formato: alfanumerico maiuscolo
- Stringa vuota se non presente

CUP:
- Codice Unico Progetto
- Formato: alfanumerico maiuscolo
- Stringa vuota se non presente

GESTIONE MULTI-LOTTO:
- Se il testo contiene più lotti, estrarli separatamente
- Ogni lotto deve avere i propri valori specifici
- Mantenere informazioni comuni (es. amministrazione) per tutti i lotti"""

    def _extract_lotto_regex(tx: str) -> Lotto:
        t = tx
        # CIG
        m_cig = re.search(r"\b[0-9A-Z]{10}\b", t)
        cig = m_cig.group(0) if m_cig else ""
        # Importo (prendi max)
        amounts = [
            float(m.replace(".", "").replace(",", "."))
            for m in re.findall(r"\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?", t)
            if m
        ]
        importo = f"{max(amounts):.2f}" if amounts else ""
        # Data (prima occorrenza dd/mm/yyyy)
        m_dt = re.search(r"\b\d{1,2}/\d{1,2}/\d{4}\b", t)
        data_aggiud = m_dt.group(0) if m_dt else ""
        # Aggiudicatario (heuristic: dopo 'aggiudicat' prendi 1-7 parole)
        m_ag = re.search(
            r"aggiudicat[oa].{0,40}?([A-ZÀ-Ú][\wÀ-ú&'\.-]+(?:\s+[A-ZÀ-Ú][\wÀ-ú&'\.-]+){0,6})",
            t,
            flags=re.IGNORECASE,
        )
        aggiud = m_ag.group(1).strip() if m_ag else ""
        return Lotto(
            Oggetto=t[:200],
            Categoria=CategoriaLotto.ALTRO,
            TipoIlluminazione=TipoIlluminazione.ALTRO,
            TipoEfficientamento=TipoEfficientamento.ALTRO,
            TipoAppalto=TipoAppalto.ALTRO,
            TipoIntervento=TipoIntervento.ALTRO,
            TipoImpianto=TipoImpianto.ALTRO,
            TipoEnergia=TipoEnergia.ALTRO,
            TipoOperazione=TipoOperazione.ALTRO,
            Procedura="",
            AmministrazioneAggiudicatrice="",
            OfferteRicevute="",
            DurataAppalto="",
            Scadenza="",
            ImportoAggiudicazione=importo,
            DataAggiudicazione=data_aggiud,
            Sconto="",
            Comune="",
            Aggiudicatario=aggiud,
            CIG=cig,
            CUP="",
            CriterioAggiudicazione="",
        )

    if estimated_lots <= 1:
        try:
            payload = CLIENT_MINI.chat_json(
                system_prompt=system_prompt_cached,
                user_prompt=f"Estrai i dati dal seguente testo di gara e restituisci un JSON compatibile con il modello Lotto pydantic:\n\n{text}",
                max_tokens=1200,
            )
            return [Lotto(**payload)]
        except Exception:
            return [_extract_lotto_regex(text)]

    else:
        # Multi lotto - istruzioni aggiuntive
        try:
            payload = CLIENT_MINI.chat_json(
                system_prompt=system_prompt_cached,
                user_prompt=(
                    f"Il testo contiene {estimated_lots} lotti. Estrai TUTTI i lotti separatamente e restituisci un JSON con chiave 'Lotti' \n"
                    f"dove ogni elemento è compatibile con il modello Lotto pydantic.\n\n{text}"
                ),
                max_tokens=2000,
            )
            lotti_data = payload.get("Lotti", []) if isinstance(payload, dict) else []
            return (
                [Lotto(**lot) for lot in lotti_data]
                if lotti_data
                else [_extract_lotto_regex(text)]
            )
        except Exception:
            # Estima numero lotti ma ritorna almeno uno
            return [_extract_lotto_regex(text)]


# ==================== PROCESSAMENTO PRINCIPALE ====================


def process_record(
    row: pd.Series, index: int, total: int, checkpoint: Dict
) -> List[Dict]:
    """Processa un singolo record con sistema a due stadi"""

    text = clean_text(row["testo"])
    text_hash = hash_text(text)

    # Check if already processed
    if text_hash in checkpoint["processed"]:
        print(f"[{index}/{total}] ⏭️  Già processato (checkpoint)")
        return []

    print(f"[{index}/{total}] 🔍 Stage 1: Filtro rapido...")

    # Stage 1: Quick filter with caching
    if text_hash in checkpoint["stage1_cache"]:
        filter_result = FilterResult(**checkpoint["stage1_cache"][text_hash])
        print(f"    ✓ Cache hit: {filter_result.category}")
    else:
        filter_result = stage1_quick_filter(text)
        checkpoint["stage1_cache"][text_hash] = filter_result.dict()
        print(
            f"    ✓ Rilevante: {filter_result.is_relevant} ({filter_result.confidence:.0%})"
        )

    # Gating Stage 2: procedi se rilevante (ignora confidenza per richiesta cliente)
    if not filter_result.is_relevant:
        print(
            f"    ⏭️  Saltato: {filter_result.reason} (conf={filter_result.confidence:.0%})"
        )
        checkpoint["processed"].append(text_hash)
        return []

    # Stage 2: Detailed extraction
    print(
        f"    🎯 Stage 2: Estrazione dettagliata ({filter_result.estimated_lots} lotti)..."
    )

    try:
        lots = stage2_detailed_extraction(text, filter_result.estimated_lots)

        results = []
        for i, lot in enumerate(lots):
            new_row = row.to_dict()
            new_row.update(
                {
                    "testo": text,
                    "CodiceGruppo": text_hash,
                    "Lotto": f"Lotto {i + 1}",
                    "NumeroLotti": len(lots),
                    "FilterConfidence": filter_result.confidence,
                    "QuickCategory": filter_result.category,
                    **lot.dict(),
                }
            )
            results.append(new_row)

        print(f"    ✅ Estratti {len(lots)} lotti")
        checkpoint["processed"].append(text_hash)

        return results

    except Exception as e:
        print(f"    ❌ Errore Stage 2: {e}")
        checkpoint["processed"].append(text_hash)
        return []


# ==================== ANALIZZATORE PRINCIPALE ====================


class GazzettaAnalyzerOptimized:
    """Analizzatore ottimizzato con sistema a due stadi"""

    @staticmethod
    def estimate_tokens(num_records: int) -> Dict[str, Any]:
        """Stima uso token con sistema ottimizzato e cached input"""

        # Assumendo 70% non rilevanti (filtrati in Stage 1)
        stage1_tokens = num_records * 250  # Tutti passano per Stage 1
        stage2_records = int(num_records * 0.3)  # Solo 30% va a Stage 2
        stage2_tokens = stage2_records * 2500

        # Con cached input, riduciamo ulteriormente i token
        # Primi 100 record pagano full, poi -50% per cache hits
        cache_reduction = 0.5 if num_records > 100 else 0
        stage1_tokens_cached = stage1_tokens * (
            1 - cache_reduction * 0.5
        )  # 25% reduction con cache
        stage2_tokens_cached = stage2_tokens * (
            1 - cache_reduction * 0.3
        )  # 15% reduction con cache

        total_tokens = stage1_tokens_cached + stage2_tokens_cached

        # Stima costi con modelli corretti
        # GPT-4.1-mini: $0.15/1M input, $0.60/1M output
        stage1_input_cost = (stage1_tokens_cached * 0.8 / 1_000_000) * 0.15
        stage1_output_cost = (stage1_tokens_cached * 0.2 / 1_000_000) * 0.60
        stage1_cost = stage1_input_cost + stage1_output_cost

        # o3: $15/1M input, $60/1M output
        stage2_input_cost = (stage2_tokens_cached * 0.8 / 1_000_000) * 15.00
        stage2_output_cost = (stage2_tokens_cached * 0.2 / 1_000_000) * 60.00
        stage2_cost = stage2_input_cost + stage2_output_cost

        # Confronto con sistema originale (solo o3)
        original_tokens = num_records * 2500
        original_cost = (original_tokens * 0.8 / 1_000_000) * 15.00  # input
        original_cost += (original_tokens * 0.2 / 1_000_000) * 60.00  # output

        return {
            "total_records": num_records,
            "stage1_records": num_records,
            "stage2_records": stage2_records,
            "stage1_tokens": int(stage1_tokens_cached),
            "stage2_tokens": int(stage2_tokens_cached),
            "total_tokens": int(total_tokens),
            "cache_savings": f"{cache_reduction * 100:.0f}%",
            "stage1_cost": stage1_cost,
            "stage2_cost": stage2_cost,
            "total_cost": stage1_cost + stage2_cost,
            "original_cost": original_cost,
            "savings_amount": original_cost - (stage1_cost + stage2_cost),
            "savings_percent": 100 * (1 - (stage1_cost + stage2_cost) / original_cost),
        }

    @staticmethod
    def run(
        input_file: str = None, output_file: str = None, non_interactive: bool = False
    ):
        """Esegue analisi ottimizzata.

        Args:
            input_file: nome file input (in `data/temp`)
            output_file: nome file output (in `data/temp`)
            non_interactive: se True, non chiede conferme via input()
        """

        # Paths
        input_path = Path(Config.TEMP_DIR) / (input_file or Config.LOTTI_RAW)
        output_path = Path(Config.TEMP_DIR) / (output_file or Config.LOTTI_GAZZETTA)

        if not input_path.exists():
            print(f"❌ File input non trovato: {input_path}")
            return

        # Load data
        print("\n" + "=" * 70)
        print("🚀 GAZZETTA ANALYZER OTTIMIZZATO (2-STAGE)")
        print("=" * 70)

        df_input = pd.read_excel(input_path)
        total_records = len(df_input)

        print(f"\n📊 Records da processare: {total_records:,}")

        # Token estimation
        if Config.USE_TWO_STAGE:
            estimate = GazzettaAnalyzerOptimized.estimate_tokens(total_records)
            print(f"\n📐 STIMA (Sistema 2-Stage con Cached Input):")
            print(f"   Stage 1 (GPT-5-mini): {estimate['stage1_records']:,} records")
            print(f"   Stage 2 (GPT-5): ~{estimate['stage2_records']:,} records")
            print(f"   Token totali: {estimate['total_tokens']:,}")
            print(f"   Cache reduction: {estimate['cache_savings']}")

        # Confirm (skip in non-interactive mode)
        if not non_interactive:
            response = input("\n🚦 Procedere? (s/n): ")
            if response.lower() != "s":
                print("Annullato.")
                return

        # Load checkpoint e policy di ripartenza
        checkpoint = load_checkpoint()
        checkpoint["source_file"] = str(input_path)
        # Se vuoi ripartire da capo sempre, forzare last_index=0 e cache vuote
        ALWAYS_RESTART = True
        if ALWAYS_RESTART:
            checkpoint["processed"] = []
            checkpoint["stage1_cache"] = {}
            checkpoint["last_index"] = 0
            checkpoint["partial_results"] = []
        start_index = checkpoint.get("last_index", 0)

        # Recupera risultati parziali se presenti
        if checkpoint.get("partial_results"):
            results = checkpoint["partial_results"]
            print(f"\n♻️  Recuperati {len(results)} risultati dal checkpoint")
        else:
            results = []

        if start_index > 0:
            print(f"♻️  Ripresa da record {start_index}/{total_records}")
            print(
                f"   Cache Stage 1: {len(checkpoint.get('stage1_cache', {}))} entries"
            )

        # Process records
        start_time = time.time()
        last_save_time = time.time()

        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            futures = []

            for idx, row in df_input.iterrows():
                if idx < start_index:
                    continue

                # Throttle opzionale per stabilità API
                if Config.THROTTLE_MS > 0:
                    time.sleep(Config.THROTTLE_MS / 1000.0)

                future = executor.submit(
                    process_record, row, idx + 1, total_records, checkpoint
                )
                futures.append((idx, future))

                # Save checkpoint periodically (ogni N record o ogni 5 minuti)
                current_time = time.time()
                if (idx % Config.CHECKPOINT_EVERY == 0 and idx > 0) or (
                    current_time - last_save_time > 300
                ):  # 5 minuti
                    checkpoint["last_index"] = idx
                    checkpoint[
                        "partial_results"
                    ] = results  # Salva anche risultati parziali
                    save_checkpoint(checkpoint)
                    last_save_time = current_time
                    print(
                        f"\n💾 Checkpoint: record {idx}, risultati salvati: {len(results)}"
                    )

            # Collect results
            for idx, future in futures:
                try:
                    record_results = future.result(timeout=60)
                    results.extend(record_results)
                except Exception as e:
                    print(f"❌ Errore record {idx}: {e}")

        # Save results
        if results:
            df_output = pd.DataFrame(results)
            df_output.to_excel(output_path, index=False)

            elapsed = time.time() - start_time

            print("\n" + "=" * 70)
            print("✅ ANALISI COMPLETATA")
            print(f"   Records processati: {len(checkpoint['processed']):,}")
            print(f"   Lotti estratti: {len(results):,}")
            print(f"   Tempo: {elapsed/60:.1f} minuti")
            print(f"   Output: {output_path}")

            # Final token count (approximate)
            stage1_actual = len(checkpoint["processed"])
            stage2_actual = len(set(r["CodiceGruppo"] for r in results))

            print(f"\n📊 TOKEN UTILIZZATI (stima):")
            print(f"   Stage 1: {stage1_actual * 250:,} token")
            print(f"   Stage 2: {stage2_actual * 2500:,} token")
            print(f"   Totale: {stage1_actual * 250 + stage2_actual * 2500:,} token")

            # Clean checkpoint (opzionale, skip in non-interactive)
            if not non_interactive:
                response = input("\n🗑️ Rimuovere checkpoint? (s/n): ")
                if response.lower() == "s":
                    checkpoint_path = Path(Config.TEMP_DIR) / Config.CHECKPOINT_FILE
                    if checkpoint_path.exists():
                        # Salva backup finale prima di rimuovere
                        final_backup = checkpoint_path.with_suffix(".json.completed")
                        checkpoint_path.rename(final_backup)
                        print(f"✅ Checkpoint spostato in: {final_backup}")
                        print("   (puoi eliminarlo manualmente se non serve)")

        else:
            print("\n⚠️ Nessun record rilevante trovato")

        print("=" * 70)


# ==================== MAIN ====================

if __name__ == "__main__":
    # Check API keys
    if not Config.OPENAI_API_KEY:
        print("❌ OPENAI_API_KEY non configurata in .env")
        exit(1)

    # Run analyzer
    GazzettaAnalyzerOptimized.run()
