#!/usr/bin/env python
"""
OCDS Analyzer Ottimizzato con Sistema Two-Stage
Stage 1: GPT-4.1-mini per filtro rapido e identificazione rilevanza
Stage 2: o3 solo per record rilevanti con estrazione dettagliata

Riduzione stimata token: 70-80% su dati OCDS
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
from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, field_validator

# Carica variabili ambiente
load_dotenv()

# ==================== CONFIGURAZIONE ====================


class Config:
    """Configurazione centralizzata con supporto .env"""

    # API Keys
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    OPENAI_API_KEY_MINI = os.getenv("OPENAI_API_KEY_MINI", OPENAI_API_KEY)
    OPENAI_API_KEY_O3 = os.getenv("OPENAI_API_KEY_O3", OPENAI_API_KEY)

    # Modelli
    MINI_MODEL = os.getenv("MINI_MODEL", "gpt-4.1-mini")
    O3_MODEL = os.getenv("O3_MODEL", "o3")

    # Processing
    MAX_WORKERS = int(
        os.getenv("MAX_WORKERS", "5")
    )  # Meno worker per OCDS (più pesante)
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))  # Batch più piccoli per OCDS
    USE_TWO_STAGE = os.getenv("USE_TWO_STAGE", "true").lower() == "true"
    CHECKPOINT_EVERY = int(os.getenv("CHECKPOINT_EVERY", "100"))

    # Paths
    DATA_DIR = "data"
    OCDS_DIR = "data/ocds"
    OUTPUT_DIR = "data/temp"
    CHECKPOINT_FILE = "checkpoint_ocds_optimized.json"
    OUTPUT_FILE = "Lotti_OCDS_Optimized.xlsx"


# Inizializza client OpenAI
client_mini = OpenAI(api_key=Config.OPENAI_API_KEY_MINI)
client_o3 = OpenAI(api_key=Config.OPENAI_API_KEY_O3)

# ==================== MODELLI PYDANTIC ====================


class OCDSFilterResult(BaseModel):
    """Risultato del filtro rapido per OCDS"""

    is_relevant: bool
    confidence: float
    tender_type: str
    category: str
    reason: str
    has_award: bool
    amount_range: str


class QuickCategoryOCDS(str, Enum):
    """Categorie semplificate per filtro OCDS"""

    WORKS = "Lavori"
    SERVICES = "Servizi"
    SUPPLIES = "Forniture"
    INFRASTRUCTURE = "Infrastrutture"
    ENERGY = "Energia"
    DIGITAL = "Digitale"
    MAINTENANCE = "Manutenzione"
    NOT_RELEVANT = "Non Rilevante"


# Enums per OCDS (più strutturati rispetto a Gazzetta)
class ProcurementMethod(str, Enum):
    OPEN = "open"
    SELECTIVE = "selective"
    LIMITED = "limited"
    DIRECT = "direct"


class TenderStatus(str, Enum):
    PLANNING = "planning"
    ACTIVE = "active"
    COMPLETE = "complete"
    CANCELLED = "cancelled"


class ContractStatus(str, Enum):
    PENDING = "pending"
    ACTIVE = "active"
    COMPLETE = "complete"
    TERMINATED = "terminated"


class OCDSLotto(BaseModel):
    """Modello OCDS strutturato per estrazione dettagliata"""

    # Identificativi
    ocid: str
    tender_id: str
    tender_title: str
    tender_description: str

    # Classificazione
    category: str
    procurement_method: str
    main_procurement_category: str
    cpv_code: str = ""

    # Date
    tender_date_published: str
    tender_period_start: str
    tender_period_end: str
    award_date: str = ""
    contract_start: str = ""
    contract_end: str = ""

    # Importi
    tender_value: str
    award_value: str = ""
    contract_value: str = ""

    # Soggetti
    buyer_name: str
    buyer_id: str
    supplier_name: str = ""
    supplier_id: str = ""

    # Altri dati
    number_of_tenderers: int = 0
    tender_status: str
    contract_status: str = ""
    documents_count: int = 0
    amendments_count: int = 0

    # Link e riferimenti
    cig: str = ""
    cup: str = ""
    tender_url: str = ""

    @field_validator("tender_value", "award_value", "contract_value", mode="before")
    def format_amount(cls, v):
        """Formatta importi in formato numerico"""
        if not v or v == "":
            return ""
        if isinstance(v, dict):
            amount = v.get("amount", 0)
            return f"{float(amount):.2f}"
        try:
            return f"{float(v):.2f}"
        except:
            return ""

    @field_validator(
        "tender_date_published",
        "award_date",
        "contract_start",
        "contract_end",
        mode="before",
    )
    def format_date(cls, v):
        """Formatta date in dd/mm/yyyy"""
        if not v:
            return ""
        try:
            if isinstance(v, str):
                # Parse ISO date
                if "T" in v:
                    dt = datetime.fromisoformat(v.split("T")[0])
                else:
                    dt = datetime.fromisoformat(v)
                return dt.strftime("%d/%m/%Y")
        except:
            return ""
        return str(v)


class OCDSGrouppoLotti(BaseModel):
    Lotti: List[OCDSLotto]


# ==================== FUNZIONI UTILITÀ ====================


def hash_text(text: str) -> str:
    """Genera hash SHA256 del testo"""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def extract_text_from_ocds(release: dict) -> str:
    """Estrae testo rilevante da release OCDS"""
    texts = []

    # Tender info
    if "tender" in release:
        tender = release["tender"]
        texts.append(tender.get("title", ""))
        texts.append(tender.get("description", ""))

        # Items
        for item in tender.get("items", []):
            texts.append(item.get("description", ""))

        # Documents
        for doc in tender.get("documents", []):
            texts.append(doc.get("title", ""))

    # Awards info
    for award in release.get("awards", []):
        texts.append(award.get("title", ""))
        texts.append(award.get("description", ""))

    # Contracts info
    for contract in release.get("contracts", []):
        texts.append(contract.get("title", ""))
        texts.append(contract.get("description", ""))

    return " ".join(filter(None, texts))


def load_checkpoint() -> Dict[str, Any]:
    """Carica checkpoint se esiste"""
    checkpoint_path = Path(Config.OUTPUT_DIR) / Config.CHECKPOINT_FILE
    if checkpoint_path.exists():
        with open(checkpoint_path, "r") as f:
            return json.load(f)
    return {"processed": [], "stage1_cache": {}, "last_file": "", "last_index": 0}


def save_checkpoint(checkpoint: Dict[str, Any]):
    """Salva checkpoint"""
    checkpoint_path = Path(Config.OUTPUT_DIR) / Config.CHECKPOINT_FILE
    checkpoint_path.parent.mkdir(exist_ok=True)
    with open(checkpoint_path, "w") as f:
        json.dump(checkpoint, f, indent=2)


# ==================== STAGE 1: FILTRO RAPIDO OCDS ====================


def stage1_ocds_filter(release: dict) -> OCDSFilterResult:
    """
    Stage 1: Usa GPT-4.1-mini per filtro rapido su dati OCDS
    ~300-400 token per record
    """

    # System prompt lungo per caching automatico (>1024 token)
    system_prompt = """Sei un analista specializzato in dati OCDS (Open Contracting Data Standard) per appalti pubblici italiani.
Devi identificare rapidamente se un record OCDS riguarda infrastrutture pubbliche o servizi rilevanti.

CRITERI DI RILEVANZA PER OCDS:

RILEVANTE se riguarda:
1. LAVORI (works):
   - Costruzione, ristrutturazione edifici pubblici
   - Manutenzione straordinaria infrastrutture
   - Opere pubbliche (strade, ponti, gallerie)
   - Impianti tecnologici (illuminazione, energia)
   - Riqualificazione urbana
   - Edilizia scolastica e sanitaria

2. SERVIZI TECNICI (services):
   - Gestione illuminazione pubblica
   - Manutenzione impianti e infrastrutture
   - Servizi energia e efficientamento
   - Gestione rifiuti e ambiente
   - Trasporto pubblico
   - Servizi smart city e digitalizzazione

3. FORNITURE STRATEGICHE (supplies):
   - LED e apparecchi illuminazione
   - Impianti fotovoltaici
   - Colonnine ricarica elettrica
   - Tecnologie smart city
   - Veicoli elettrici/ibridi
   - Attrezzature per manutenzione

NON RILEVANTE se:
- Servizi professionali (legali, contabili, consulenza)
- Forniture di cancelleria o materiali consumo
- Servizi pulizia ordinaria
- Catering e ristorazione
- Eventi e manifestazioni
- Servizi sociali/assistenziali generici
- Forniture sanitarie di base

ANALISI CAMPI OCDS:
- tender.mainProcurementCategory: "works", "services", "supplies"
- tender.procurementMethodDetails: tipo procedura
- tender.value: importo base gara
- awards[].value: importo aggiudicazione
- tender.items[].classification: codici CPV
- parties[].roles: "buyer", "supplier", "tenderer"

CODICI CPV RILEVANTI:
- 45: Lavori di costruzione
- 50: Servizi manutenzione e riparazione  
- 71: Servizi architettura e ingegneria
- 09: Prodotti petroliferi e energia
- 31: Macchine e apparecchi elettrici
- 34: Attrezzature trasporto
- 65: Servizi pubblici

OUTPUT RICHIESTO (JSON):
{
  "is_relevant": boolean,
  "confidence": 0.0-1.0,
  "tender_type": "works/services/supplies",
  "category": "categoria principale",
  "reason": "motivo max 50 char",
  "has_award": boolean,
  "amount_range": "< 40K / 40K-200K / 200K-1M / > 1M"
}"""

    # Estrai informazioni chiave da OCDS
    tender = release.get("tender", {})
    awards = release.get("awards", [])

    # Prepara riassunto per analisi
    summary = {
        "id": release.get("id", ""),
        "title": tender.get("title", ""),
        "description": tender.get("description", "")[:500],
        "mainCategory": tender.get("mainProcurementCategory", ""),
        "method": tender.get("procurementMethod", ""),
        "value": tender.get("value", {}).get("amount", 0),
        "hasAwards": len(awards) > 0,
        "awardValue": awards[0].get("value", {}).get("amount", 0) if awards else 0,
        "items": len(tender.get("items", [])),
        "cpv": tender.get("items", [{}])[0].get("classification", {}).get("id", "")
        if tender.get("items")
        else "",
    }

    user_prompt = f"Analizza questo record OCDS:\n{json.dumps(summary, ensure_ascii=False, indent=2)}"

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        response = client_mini.chat.completions.create(
            model=Config.MINI_MODEL, messages=messages, max_tokens=200, seed=42
        )

        content = response.choices[0].message.content
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0]
        elif "```" in content:
            content = content.split("```")[1].split("```")[0]

        result = json.loads(content.strip())
        return OCDSFilterResult(**result)

    except Exception as e:
        print(f"Errore Stage 1 OCDS: {e}")
        return OCDSFilterResult(
            is_relevant=True,
            confidence=0.5,
            tender_type="unknown",
            category="ALTRO",
            reason="Errore analisi",
            has_award=False,
            amount_range="unknown",
        )


# ==================== STAGE 2: ESTRAZIONE DETTAGLIATA OCDS ====================


def stage2_ocds_extraction(release: dict) -> OCDSLotto:
    """
    Stage 2: Usa o3 per estrazione dettagliata da OCDS
    ~3000-4000 token per record
    """

    # System prompt lungo per caching (>1024 token)
    system_prompt = """Sei un esperto di dati OCDS (Open Contracting Data Standard) per appalti pubblici.
Estrai TUTTI i dati strutturati dal record OCDS seguendo esattamente il modello fornito.

MAPPATURA CAMPI OCDS -> MODELLO:

IDENTIFICATIVI:
- ocid: release.ocid
- tender_id: release.tender.id o release.id
- tender_title: release.tender.title
- tender_description: release.tender.description

CLASSIFICAZIONE:
- category: basato su mainProcurementCategory e descrizione
- procurement_method: release.tender.procurementMethod
- main_procurement_category: release.tender.mainProcurementCategory
- cpv_code: release.tender.items[0].classification.id (codice CPV principale)

DATE (formato dd/mm/yyyy):
- tender_date_published: release.tender.datePublished
- tender_period_start: release.tender.tenderPeriod.startDate
- tender_period_end: release.tender.tenderPeriod.endDate
- award_date: release.awards[0].date
- contract_start: release.contracts[0].period.startDate
- contract_end: release.contracts[0].period.endDate

IMPORTI (formato numerico con 2 decimali):
- tender_value: release.tender.value.amount
- award_value: release.awards[0].value.amount
- contract_value: release.contracts[0].value.amount

SOGGETTI:
- buyer_name: cerca in parties dove roles include "buyer"
- buyer_id: parties[buyer].id
- supplier_name: cerca in parties dove roles include "supplier" o da awards[0].suppliers
- supplier_id: parties[supplier].id

ALTRI DATI:
- number_of_tenderers: release.tender.numberOfTenderers o conta tenderers
- tender_status: release.tender.status
- contract_status: release.contracts[0].status
- documents_count: conta tutti i documents in tender + awards + contracts
- amendments_count: conta release.tender.amendments + contracts amendments

LINK E RIFERIMENTI:
- cig: cerca in additionalIdentifiers o documents o description
- cup: cerca in additionalIdentifiers o documents o description  
- tender_url: release.tender.documents[0].url dove documentType = "tenderNotice"

REGOLE ESTRAZIONE:
1. Se un campo non esiste, usa stringa vuota ""
2. Per date, converti da ISO a dd/mm/yyyy
3. Per importi, estrai solo il numero con 2 decimali
4. Per array, prendi il primo elemento se disponibile
5. Per status, usa i valori standard OCDS
6. CIG/CUP possono essere in vari campi, cerca ovunque

CATEGORIE DA ASSEGNARE:
- "Illuminazione": se riguarda illuminazione pubblica, LED
- "Energia": efficientamento energetico, fotovoltaico
- "Edifici": costruzione/ristrutturazione edifici
- "Infrastrutture": strade, ponti, reti
- "Servizi": gestione e manutenzione
- "Forniture": acquisto beni e attrezzature
- "Digitale": smart city, IT, telecomunicazioni
- "Ambiente": rifiuti, verde, depurazione
- "Trasporti": mobilità e trasporto pubblico"""

    # Prepara il record completo per l'analisi
    messages = [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": f"Estrai tutti i dati da questo record OCDS:\n\n{json.dumps(release, ensure_ascii=False, indent=2)[:8000]}",
        },
    ]

    try:
        response = client_o3.beta.chat.completions.parse(
            model=Config.O3_MODEL, messages=messages, response_format=OCDSLotto, seed=42
        )
        return response.choices[0].message.parsed

    except Exception as e:
        print(f"Errore Stage 2 OCDS: {e}")
        # Ritorna record con dati minimi
        return OCDSLotto(
            ocid=release.get("ocid", ""),
            tender_id=release.get("id", ""),
            tender_title=release.get("tender", {}).get("title", "Errore estrazione"),
            tender_description=release.get("tender", {}).get("description", ""),
            category="ALTRO",
            procurement_method=release.get("tender", {}).get("procurementMethod", ""),
            main_procurement_category=release.get("tender", {}).get(
                "mainProcurementCategory", ""
            ),
            tender_date_published="",
            tender_period_start="",
            tender_period_end="",
            tender_value="0.00",
            buyer_name="",
            buyer_id="",
            tender_status=release.get("tender", {}).get("status", ""),
            number_of_tenderers=0,
        )


# ==================== PROCESSAMENTO PRINCIPALE ====================


def process_ocds_file(filepath: Path, checkpoint: Dict) -> List[Dict]:
    """Processa un singolo file OCDS"""

    print(f"\n📂 Processing: {filepath.name}")
    results = []

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        releases = data.get("releases", [])
        print(f"   📊 Releases nel file: {len(releases):,}")

        # Filtra solo releases con tender
        valid_releases = [r for r in releases if "tender" in r]
        print(f"   ✅ Releases validi: {len(valid_releases):,}")

        # Process each release
        for idx, release in enumerate(valid_releases):
            if idx % 100 == 0:
                print(f"   ⚙️  Processati: {idx}/{len(valid_releases)}", end="\r")

            # Generate hash for deduplication
            release_hash = hash_text(json.dumps(release, sort_keys=True))

            if release_hash in checkpoint["processed"]:
                continue

            # Stage 1: Quick filter
            if release_hash in checkpoint["stage1_cache"]:
                filter_result = OCDSFilterResult(
                    **checkpoint["stage1_cache"][release_hash]
                )
            else:
                filter_result = stage1_ocds_filter(release)
                checkpoint["stage1_cache"][release_hash] = filter_result.dict()

            # Skip if not relevant with high confidence
            if not filter_result.is_relevant and filter_result.confidence > 0.7:
                checkpoint["processed"].append(release_hash)
                continue

            # Stage 2: Detailed extraction for relevant records
            if filter_result.is_relevant or filter_result.confidence < 0.7:
                try:
                    lotto = stage2_ocds_extraction(release)

                    # Add metadata
                    result = lotto.dict()
                    result["source_file"] = filepath.name
                    result["filter_confidence"] = filter_result.confidence
                    result["filter_category"] = filter_result.category
                    result["has_award"] = filter_result.has_award
                    result["amount_range"] = filter_result.amount_range

                    results.append(result)
                    checkpoint["processed"].append(release_hash)

                except Exception as e:
                    print(f"\n   ❌ Errore estrazione release {idx}: {e}")
                    checkpoint["processed"].append(release_hash)

            # Save checkpoint periodically
            if idx % Config.CHECKPOINT_EVERY == 0 and idx > 0:
                checkpoint["last_file"] = filepath.name
                checkpoint["last_index"] = idx
                save_checkpoint(checkpoint)

        print(f"\n   ✅ Completato: {len(results)} record rilevanti estratti")

    except Exception as e:
        print(f"\n   ❌ Errore file {filepath.name}: {e}")

    return results


# ==================== ANALYZER PRINCIPALE ====================


class OCDSAnalyzerOptimized:
    """Analyzer OCDS ottimizzato con two-stage processing"""

    @staticmethod
    def estimate_tokens(num_releases: int) -> Dict[str, Any]:
        """Stima uso token per OCDS"""

        # OCDS ha più dati strutturati, ~30% rilevanti dopo filtro
        stage1_tokens = num_releases * 350  # Più token per struttura OCDS
        stage2_records = int(num_releases * 0.3)
        stage2_tokens = stage2_records * 3500  # Più complesso di Gazzetta

        # Con caching
        cache_reduction = 0.5 if num_releases > 100 else 0
        stage1_tokens_cached = stage1_tokens * (1 - cache_reduction * 0.5)
        stage2_tokens_cached = stage2_tokens * (1 - cache_reduction * 0.3)

        total_tokens = stage1_tokens_cached + stage2_tokens_cached

        # Costi
        stage1_cost = (stage1_tokens_cached * 0.8 / 1_000_000) * 0.15  # input
        stage1_cost += (stage1_tokens_cached * 0.2 / 1_000_000) * 0.60  # output

        stage2_cost = (stage2_tokens_cached * 0.8 / 1_000_000) * 15.00  # input
        stage2_cost += (stage2_tokens_cached * 0.2 / 1_000_000) * 60.00  # output

        # Confronto con sistema originale
        original_tokens = num_releases * 3500
        original_cost = (original_tokens * 0.8 / 1_000_000) * 15.00
        original_cost += (original_tokens * 0.2 / 1_000_000) * 60.00

        return {
            "total_releases": num_releases,
            "stage1_releases": num_releases,
            "stage2_releases": stage2_records,
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
    def count_total_releases() -> int:
        """Conta releases totali nei file OCDS"""
        ocds_dir = Path(Config.OCDS_DIR)
        total = 0

        for filepath in ocds_dir.glob("*.json"):
            if filepath.name == "ocds_example.json":
                continue
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    total += len(data.get("releases", []))
            except:
                pass

        return total

    @staticmethod
    def run():
        """Esegue analisi OCDS ottimizzata"""

        print("\n" + "=" * 70)
        print("🚀 OCDS ANALYZER OTTIMIZZATO (TWO-STAGE)")
        print("=" * 70)

        # Check OCDS files
        ocds_dir = Path(Config.OCDS_DIR)
        ocds_files = list(ocds_dir.glob("*.json"))
        ocds_files = [f for f in ocds_files if f.name != "ocds_example.json"]

        if not ocds_files:
            print("❌ Nessun file OCDS trovato in data/ocds/")
            return

        print(f"\n📁 File OCDS trovati: {len(ocds_files)}")
        for f in ocds_files[:5]:
            size_mb = f.stat().st_size / (1024 * 1024)
            print(f"   - {f.name}: {size_mb:.1f} MB")

        # Count total releases
        print("\n⏳ Conteggio releases totali...")
        total_releases = OCDSAnalyzerOptimized.count_total_releases()
        print(f"📊 Releases totali: {total_releases:,}")

        if Config.USE_TWO_STAGE:
            # Token estimation
            estimate = OCDSAnalyzerOptimized.estimate_tokens(total_releases)
            print(f"\n💰 STIMA COSTI (Two-Stage + Cache):")
            print(
                f"   Stage 1 (GPT-4.1-mini): {estimate['stage1_releases']:,} releases"
            )
            print(f"   Stage 2 (o3): ~{estimate['stage2_releases']:,} releases")
            print(f"   Token totali: {estimate['total_tokens']:,}")
            print(f"   Cache reduction: {estimate['cache_savings']}")
            print(f"   Costo stimato: ${estimate['total_cost']:.2f}")
            print(f"   Costo originale (solo o3): ${estimate['original_cost']:.2f}")
            print(
                f"   Risparmio: ${estimate['savings_amount']:.2f} ({estimate['savings_percent']:.1f}%)"
            )

        # Confirm
        response = input("\n🚦 Procedere? (s/n): ")
        if response.lower() != "s":
            print("Annullato.")
            return

        # Load checkpoint
        checkpoint = load_checkpoint()

        if checkpoint.get("last_file"):
            print(f"\n♻️  Ripresa da checkpoint: {checkpoint['last_file']}")

        # Process files
        all_results = []
        start_time = time.time()

        print("\n" + "-" * 70)

        with ThreadPoolExecutor(max_workers=1) as executor:  # Processo file sequenziale
            futures = []

            for filepath in ocds_files:
                # Skip if already processed completely
                if checkpoint.get("last_file") == filepath.name and checkpoint.get(
                    "completed"
                ):
                    continue

                future = executor.submit(process_ocds_file, filepath, checkpoint)
                futures.append(future)

            # Collect results
            for future in as_completed(futures):
                try:
                    results = future.result()
                    all_results.extend(results)
                except Exception as e:
                    print(f"❌ Errore processamento: {e}")

        # Save results
        if all_results:
            output_path = Path(Config.OUTPUT_DIR) / Config.OUTPUT_FILE
            output_path.parent.mkdir(exist_ok=True)

            df = pd.DataFrame(all_results)
            df.to_excel(output_path, index=False)

            elapsed = time.time() - start_time

            print("\n" + "=" * 70)
            print("✅ ANALISI OCDS COMPLETATA")
            print(f"   Releases processati: {len(checkpoint['processed']):,}")
            print(f"   Record rilevanti: {len(all_results):,}")
            print(f"   Tempo: {elapsed/60:.1f} minuti")
            print(f"   Output: {output_path}")

            # Token usage estimate
            stage1_actual = len(checkpoint["processed"])
            stage2_actual = len(all_results)

            print(f"\n📊 TOKEN UTILIZZATI (stima):")
            print(f"   Stage 1: {stage1_actual * 350:,} token")
            print(f"   Stage 2: {stage2_actual * 3500:,} token")
            print(f"   Totale: {stage1_actual * 350 + stage2_actual * 3500:,} token")

            # Clean checkpoint
            checkpoint_path = Path(Config.OUTPUT_DIR) / Config.CHECKPOINT_FILE
            if checkpoint_path.exists():
                checkpoint_path.unlink()
                print("\n🧹 Checkpoint rimosso")

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
    OCDSAnalyzerOptimized.run()
