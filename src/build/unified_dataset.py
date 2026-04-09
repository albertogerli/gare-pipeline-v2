#!/usr/bin/env python3
"""
UNIFIED DATASET BUILDER
=======================
Module for merging 3 data sources into a unified CSV for the dashboard.

Evolution of scripts/build_dashboard_data.py with CRITICAL fixes for OCDS field mappings:
- tender_description (97.8% coverage) instead of tender_title (always empty)
- contract_end_date (31.7%) for data_scadenza (fine lavori/contratto)
- tender_period_end (97.3%) for scadenza_gara (scadenza presentazione offerte)
- calculated durata_appalto from contract dates instead of None
- lot_ids (CIG codes in ANAC) instead of None

Data sources:
- Gazzetta (Lotti_Gazzetta_Optimized.xlsx)
- OCDS (gare_filtrate_tutte.csv)
- CONSIP/ServizioLuce (ServizioLuce.xlsx)

Usage:
    python src/build/unified_dataset.py
    python src/build/unified_dataset.py --output /path/to/output.csv.gz
"""

from __future__ import annotations

import argparse
import gc
import gzip
import json
import logging
import re
import shutil
from pathlib import Path
from typing import Optional

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = DATA_DIR / "output"
CATEGORIE_DIR = OUTPUT_DIR / "categorie"

# Default input files
DEFAULT_GAZZETTA = OUTPUT_DIR / "Lotti_Gazzetta_Optimized.xlsx"
DEFAULT_OCDS = CATEGORIE_DIR / "gare_filtrate_tutte.csv"
DEFAULT_CONSIP = OUTPUT_DIR / "ServizioLuce.xlsx"
DEFAULT_CIG_JSON = CATEGORIE_DIR / "gare_cig_json.csv"
DEFAULT_OUTPUT = CATEGORIE_DIR / "gare_unificate.csv.gz"


# =============================================================================
# EXTRACTION AND NORMALIZATION FUNCTIONS
# =============================================================================

def extract_sconto_from_string(val) -> Optional[float]:
    """
    Estrae il valore numerico di sconto da una stringa.

    Esempi:
        "26,300%" -> 26.3
        "Ribasso economico 19,678%..." -> 19.678
        "- 32,919 %" -> 32.919
        43.0 -> 43.0
    """
    if pd.isna(val):
        return None

    if isinstance(val, (int, float)):
        return float(val) if 0 <= val <= 100 else None

    val_str = str(val)
    patterns = [
        r'(\d+[.,]\d+)\s*%',       # 26,300% o 26.300%
        r'[-–]\s*(\d+[.,]\d+)',    # - 32,919
        r'^(\d+[.,]\d+)$',          # Solo numero con decimali
        r'^(\d+)$',                 # Solo numero intero
    ]

    for p in patterns:
        match = re.search(p, val_str)
        if match:
            num_str = match.group(1).replace(',', '.')
            try:
                num = float(num_str)
                if 0 <= num <= 100:
                    return num
            except ValueError:
                continue
    return None


def normalize_quick_category(x) -> Optional[str]:
    """Normalizza quick_category."""
    if pd.isna(x):
        return None
    quick_map = {
        'illuminazione': 'Illuminazione',
        'energia edifici': 'Energia Edifici',
        'altro': 'Altro',
        'ambiente': 'Ambiente',
        'edifici': 'Edifici',
        'infrastrutture': 'Infrastrutture',
        'energia': 'Energia',
        'trasporti': 'Trasporti',
        'digitale': 'Digitale',
    }
    return quick_map.get(str(x).lower().strip(), str(x).strip().title())


def normalize_tipo_appalto(x) -> Optional[str]:
    """Normalizza tipo_appalto (unifica italiano/inglese)."""
    if pd.isna(x):
        return None
    appalto_map = {
        'works': 'Lavori',
        'services': 'Servizi',
        'goods': 'Forniture',
        'appalto': 'Lavori',
        'servizio': 'Servizi',
        'fornitura': 'Forniture',
        'accordo_quadro': 'Accordo Quadro',
        'concessione': 'Concessione',
        'affidamento': 'Affidamento',
        'project_financing': 'Project Financing',
    }
    key = str(x).lower().strip()
    return appalto_map.get(key, str(x).strip().replace('_', ' ').title())


def normalize_tipo_impianto(x) -> Optional[str]:
    """Normalizza tipo_impianto."""
    if pd.isna(x):
        return None
    impianto_map = {
        'impianti_elettrici': 'Impianti Elettrici',
        'pubblica_illuminazione': 'Pubblica Illuminazione',
        'illuminazione pubblica': 'Pubblica Illuminazione',
        'illuminazione_stradale': 'Illuminazione Stradale',
        'smart_city': 'Smart City',
        'videosorveglianza': 'Videosorveglianza',
        'edifici pubblici': 'Edifici Pubblici',
        'edifici_pubblici': 'Edifici Pubblici',
    }
    key = str(x).lower().strip()
    return impianto_map.get(key, str(x).strip().replace('_', ' ').title())


def normalize_tipo_energia(x) -> Optional[str]:
    """Normalizza tipo_energia."""
    if pd.isna(x):
        return None
    energia_map = {
        'risparmio_energetico': 'Risparmio Energetico',
        'energia_elettrica': 'Energia Elettrica',
        'fonti_renewable': 'Fonti Rinnovabili',
    }
    key = str(x).lower().strip()
    return energia_map.get(key, str(x).strip().replace('_', ' ').title())


def normalize_underscore_field(x) -> Optional[str]:
    """Normalizza campi con underscore (tipo_intervento, tipo_illuminazione, etc.)."""
    if pd.isna(x):
        return None
    return str(x).strip().replace('_', ' ').title()


def normalize_comune(x) -> Optional[str]:
    """
    Normalizza i nomi dei comuni italiani.
    - Converte a Title Case (Prima Lettera Maiuscola)
    - Gestisce apostrofi e accenti
    - Normalizza spazi multipli
    """
    if pd.isna(x):
        return None

    s = str(x).strip()
    if not s or s.lower() in ('nan', 'none', ''):
        return None

    # Normalizza spazi multipli
    s = ' '.join(s.split())

    # Converte a title case
    s = s.title()

    # Fix per preposizioni/articoli che devono essere minuscoli (tranne inizio)
    # Es: "Acquaviva Delle Fonti" -> "Acquaviva delle Fonti"
    preposizioni = ['Del', 'Della', 'Delle', 'Dei', 'Degli', 'Di', 'Da', 'Dal', 'Dalla',
                    'Dalle', 'Dai', 'Dagli', 'Nel', 'Nella', 'Nelle', 'Nei', 'Negli',
                    'Sul', 'Sulla', 'Sulle', 'Sui', 'Sugli', 'Al', 'Alla', 'Alle',
                    'Ai', 'Agli', 'Con', 'Per', 'Tra', 'Fra', 'In', 'Su', 'A', 'E']

    words = s.split()
    for i, word in enumerate(words):
        if i > 0 and word in preposizioni:
            words[i] = word.lower()

    # Fix per "D'" e "L'" che devono mantenere maiuscola dopo
    # Es: "L'aquila" -> "L'Aquila"
    result = ' '.join(words)
    for prefix in ["D'", "L'", "D'", "L'"]:  # Include sia apostrofo normale che fancy
        if prefix.lower() in result.lower():
            idx = result.lower().find(prefix.lower())
            if idx >= 0 and idx + len(prefix) < len(result):
                next_char = result[idx + len(prefix)]
                if next_char.islower():
                    result = result[:idx + len(prefix)] + next_char.upper() + result[idx + len(prefix) + 1:]

    return result


def fuzzy_deduplicate_entities(series: pd.Series, threshold: int = 90, min_occurrences: int = 2) -> tuple[pd.Series, dict]:
    """
    Deduplica entità simili usando fuzzy matching.

    Raggruppa nomi con similarità >= threshold e li mappa al nome più frequente (canonico).

    Args:
        series: Serie pandas con i nomi da deduplicare
        threshold: Soglia di similarità (0-100), default 90
        min_occurrences: Minimo occorrenze per considerare un nome come potenziale canonico

    Returns:
        tuple: (serie normalizzata, dizionario mapping originale -> canonico)
    """
    from rapidfuzz import fuzz, process
    from collections import Counter

    # Rimuovi NaN e ottieni valori unici
    valid_values = series.dropna().astype(str)
    valid_values = valid_values[~valid_values.isin(['nan', 'None', ''])]

    if len(valid_values) == 0:
        return series, {}

    # Conta frequenze
    value_counts = Counter(valid_values)
    unique_values = list(value_counts.keys())

    logger.info(f"      Valori unici iniziali: {len(unique_values):,}")

    # Per efficienza, lavora solo con valori che appaiono almeno min_occurrences volte
    # come potenziali "canonici"
    frequent_values = [v for v, c in value_counts.items() if c >= min_occurrences]

    if len(frequent_values) == 0:
        # Se nessun valore è frequente, usa tutti
        frequent_values = unique_values[:1000]  # Limita per performance

    # Costruisci mapping: per ogni valore, trova il match migliore tra i frequenti
    mapping = {}

    # Pre-normalizza per confronto (lowercase, rimuovi punteggiatura extra)
    def normalize_for_compare(s):
        s = str(s).lower().strip()
        # Rimuovi forme societarie comuni per confronto
        for suffix in [' s.p.a.', ' spa', ' s.r.l.', ' srl', ' s.a.s.', ' sas',
                       ' s.n.c.', ' snc', ' s.c.a.r.l.', ' scarl', ' ltd', ' inc',
                       ' società cooperativa', ' coop', ' onlus']:
            s = s.replace(suffix, '')
        # Rimuovi punteggiatura
        s = re.sub(r'[.,;:\-_\'\"()[\]{}]', ' ', s)
        s = ' '.join(s.split())  # Normalizza spazi
        return s

    # Cache delle forme normalizzate
    normalized_cache = {v: normalize_for_compare(v) for v in unique_values}

    # Limit candidate set for performance: use top-N most frequent
    MAX_CANDIDATES = 5000
    if len(frequent_values) > MAX_CANDIDATES:
        frequent_values = sorted(frequent_values, key=lambda v: -value_counts[v])[:MAX_CANDIDATES]
        logger.info(f"      Candidati limitati a top {MAX_CANDIDATES} per performance")

    # Build normalized candidate list
    freq_norms = [normalize_for_compare(v) for v in frequent_values]
    freq_set = set(frequent_values)

    # ---- Phase 1: deduplicate AMONG frequent values themselves ----
    # Sort by frequency descending so the most common form becomes canonical
    frequent_values = sorted(frequent_values, key=lambda v: -value_counts[v])
    freq_norms = [normalize_for_compare(v) for v in frequent_values]

    # Track which frequent values are already absorbed into a canonical
    freq_canonical = {}  # norm -> canonical original value
    absorbed = set()     # indices of absorbed frequent values

    # Minimum normalized length to be a canonical that can absorb others.
    # Very short normalized names (e.g. "re", "ab") cause WRatio false positives
    # because partial matching finds them inside longer strings.
    MIN_NORM_LEN = 4

    for i, (fv, fn) in enumerate(zip(frequent_values, freq_norms)):
        if i in absorbed:
            continue
        freq_canonical[fn] = fv
        mapping[fv] = fv  # self-map the canonical

        # Short canonical names cannot absorb others (WRatio partial match problem)
        if len(fn) < MIN_NORM_LEN:
            continue

        # Check remaining frequent values against this canonical
        for j in range(i + 1, len(frequent_values)):
            if j in absorbed:
                continue
            fn_j = freq_norms[j]
            # Skip if lengths are too different (avoids short-string false positives)
            if len(fn_j) < MIN_NORM_LEN or min(len(fn), len(fn_j)) / max(len(fn), len(fn_j)) < 0.3:
                continue
            score = fuzz.WRatio(fn, fn_j)
            if score >= threshold:
                mapping[frequent_values[j]] = fv
                absorbed.add(j)

    logger.info(f"      Frequenti assorbiti: {len(absorbed):,} (di {len(frequent_values):,})")

    # Rebuild candidate lists from surviving canonicals only
    canonical_values = [fv for i, fv in enumerate(frequent_values) if i not in absorbed]
    canonical_norms = [freq_norms[i] for i in range(len(frequent_values)) if i not in absorbed]

    # ---- Phase 2: match non-frequent values against canonicals ----
    non_frequent = [v for v in unique_values if v not in freq_set]
    logger.info(f"      Non-frequenti da matchare: {len(non_frequent):,}")

    processed = 0
    for original_value in non_frequent:
        original_norm = normalized_cache[original_value]

        # Skip very short names — they can't reliably match
        if len(original_norm) < MIN_NORM_LEN:
            mapping[original_value] = original_value
            processed += 1
            continue

        result = process.extractOne(
            original_norm,
            canonical_norms,
            scorer=fuzz.WRatio,
            score_cutoff=threshold
        )

        if result is not None:
            _match_text, _score, idx = result
            # Validate: length ratio must be reasonable to avoid short-string false positives
            if min(len(original_norm), len(_match_text)) / max(len(original_norm), len(_match_text)) >= 0.3:
                mapping[original_value] = canonical_values[idx]
            else:
                mapping[original_value] = original_value
        else:
            mapping[original_value] = original_value

        processed += 1
        if processed % 10000 == 0:
            logger.info(f"      Processati {processed:,}/{len(non_frequent):,}...")

    # Conta quanti sono stati mappati
    changed = sum(1 for k, v in mapping.items() if k != v)
    final_unique = len(set(mapping.values()))

    logger.info(f"      Mappati {changed:,} duplicati")
    logger.info(f"      Valori unici finali: {final_unique:,}")

    # Applica mapping alla serie
    result = series.map(lambda x: mapping.get(x, x) if pd.notna(x) else x)

    return result, mapping


def normalize_entity_name(x) -> Optional[str]:
    """
    Normalizza base di nomi entità (enti appaltanti, fornitori).
    - Title case
    - Normalizza forme societarie (S.P.A. -> S.p.A.)
    - Rimuovi spazi multipli
    """
    if pd.isna(x):
        return None

    s = str(x).strip()
    if not s or s.lower() in ('nan', 'none', ''):
        return None

    # Normalizza spazi
    s = ' '.join(s.split())

    # Se tutto maiuscolo o tutto minuscolo, converti a title
    if s.isupper() or s.islower():
        s = s.title()

    # --- Strip address / P.IVA / sede legale noise ---
    # Remove P.IVA / P.I. / C.F. + number
    s = re.sub(r'[,\s\-]*\b[PpCc]\.?\s*[IiFf]\.?[VvAa]?\.?\s*:?\s*\d{5,16}\b', '', s)
    # Remove "Con Sede In..." / "Con Sede Legale In..."
    s = re.sub(r'\s*[,\-]?\s*[Cc]on [Ss]ede\b.*$', '', s)
    # Remove " - Via..." / " - Viale..." / " - Piazza..." (address after dash)
    s = re.sub(r'\s*-\s*[Vv]i[ac]\b.*$', '', s)
    s = re.sub(r'\s*-\s*[Vv]iale\b.*$', '', s)
    s = re.sub(r'\s*-\s*[Pp]iazza\b.*$', '', s)
    s = re.sub(r'\s*-\s*[Ll]argo\b.*$', '', s)
    s = re.sub(r'\s*-\s*[Cc]orso\b.*$', '', s)
    # Remove standalone CAP (5 digits not preceded by word chars)
    s = re.sub(r'\s+\d{5}\s+\w+$', '', s)
    # Remove trailing " - CITY" patterns (but not RTI/ATI components)
    s = re.sub(r'\s*-\s*[A-Z]{2}\s*$', '', s)  # e.g. " - VI"

    # Normalizza forme societarie comuni
    replacements = [
        (r'\bS\.?P\.?A\.?\b', 'S.p.A.'),
        (r'\bS\.?R\.?L\.?\b', 'S.r.l.'),
        (r'\bS\.?A\.?S\.?\b', 'S.a.s.'),
        (r'\bS\.?N\.?C\.?\b', 'S.n.c.'),
        (r'\bS\.?C\.?A\.?R\.?L\.?\b', 'S.c.a.r.l.'),
        (r'\bCOOP\.?\b', 'Coop.'),
    ]

    for pattern, replacement in replacements:
        s = re.sub(pattern, replacement, s, flags=re.IGNORECASE)

    # Fix punteggiatura ripetuta (e.g. "S.r.l.." -> "S.r.l.")
    s = re.sub(r'\.{2,}', '.', s)
    # Trailing dots after closing form (e.g. "Nome S.r.l. ." -> "Nome S.r.l.")
    s = re.sub(r'\.\s+\.', '.', s)
    s = s.strip().rstrip(',;:-')

    return s


def normalize_procedura(x) -> Optional[str]:
    """
    Normalizza le procedure di gara da ~4,700 varianti a ~22 categorie.
    """
    if pd.isna(x) or str(x) == 'nan':
        return None

    x_lower = str(x).lower().strip()

    # Mapping esatto per valori OCDS
    exact_map = {
        'open': 'Procedura Aperta',
        'selective': 'Procedura Ristretta',
        'limited': 'Procedura Negoziata',
        'direct': 'Affidamento Diretto',
    }
    if x_lower in exact_map:
        return exact_map[x_lower]

    # Affidamento diretto
    if 'affidamento diretto' in x_lower:
        if 'accordo quadro' in x_lower or 'convenzione' in x_lower:
            return 'Affidamento Diretto (Accordo Quadro)'
        return 'Affidamento Diretto'

    # Confronto competitivo / Accordo Quadro
    if 'confronto competitivo' in x_lower or 'accordo quadro' in x_lower:
        return 'Accordo Quadro'

    # Sistema di qualificazione
    if 'qualificazione' in x_lower:
        return 'Sistema di Qualificazione'

    # Dialogo competitivo
    if 'dialogo' in x_lower:
        return 'Dialogo Competitivo'

    # Partenariato per innovazione
    if 'partenariato' in x_lower:
        return 'Partenariato per Innovazione'

    # Concessione
    if 'concessione' in x_lower:
        return 'Concessione'

    # Procedura negoziata
    if 'negoziata' in x_lower:
        if 'senza' in x_lower and ('pubblic' in x_lower or 'previa' in x_lower or 'bando' in x_lower):
            return 'Procedura Negoziata senza Bando'
        if 'con' in x_lower and 'pubblic' in x_lower:
            return 'Procedura Negoziata con Bando'
        return 'Procedura Negoziata'

    # Procedura ristretta
    if 'ristretta' in x_lower:
        return 'Procedura Ristretta'

    # Procedura aperta
    if 'aperta' in x_lower or 'open' in x_lower:
        return 'Procedura Aperta'

    # MePA
    if 'mepa' in x_lower or 'mercato elettronico' in x_lower:
        return 'MePA'

    # RdO
    if 'rdo' in x_lower or 'richiesta di offerta' in x_lower:
        return 'RdO'

    # Indagine di mercato
    if 'manifestazione' in x_lower or 'indagine di mercato' in x_lower:
        return 'Indagine di Mercato'

    # Competitiva con negoziazione
    if 'competitiva' in x_lower and 'negoziazione' in x_lower:
        return 'Procedura Competitiva con Negoziazione'

    # Procedure selettive
    if 'selettiva' in x_lower:
        return 'Procedura Aperta'

    # Pubblico incanto / Licitazione
    if 'pubblico incanto' in x_lower or 'licitazione' in x_lower:
        return 'Procedura Aperta'

    # Aggiudicazione senza pubblicazione
    if 'senza' in x_lower and ('pubblic' in x_lower or 'previa' in x_lower):
        return 'Procedura Negoziata senza Bando'

    if x_lower == 'aperto':
        return 'Procedura Aperta'

    # Varianti / Modifiche
    if 'variante' in x_lower or ('modifica' in x_lower and 'contratt' in x_lower):
        return 'Variante/Modifica Contrattuale'

    # Project Financing
    if 'project financ' in x_lower or 'finanza di progetto' in x_lower:
        return 'Project Financing'

    # Sistema dinamico
    if 'sistema dinamico' in x_lower or 'sdapa' in x_lower:
        return 'Sistema Dinamico di Acquisizione'

    # Concorso
    if 'concorso' in x_lower:
        return 'Concorso di Progettazione'

    # Cottimo / Economia
    if 'cottimo' in x_lower or 'economia' in x_lower:
        return 'Affidamento Diretto'

    # Asta
    if 'asta' in x_lower or 'incanto' in x_lower:
        return 'Procedura Aperta'

    # Criterio OEPV / prezzo
    if 'offerta economicamente' in x_lower or 'o.e.p.v' in x_lower or 'oepv' in x_lower:
        return 'Procedura Aperta'
    if 'prezzo' in x_lower or 'ribasso' in x_lower:
        return 'Procedura Aperta'

    # Avvisi
    if 'avviso' in x_lower or 'preinformazione' in x_lower:
        return 'Indagine di Mercato'

    # Bando di gara
    if 'bando' in x_lower and 'gara' in x_lower:
        return 'Procedura Aperta'

    # Settori speciali
    if 'settori speciali' in x_lower:
        return 'Procedura Aperta'

    # Articoli specifici
    if 'ai sensi' in x_lower or 'ex art' in x_lower or 'art.' in x_lower:
        if '36' in x_lower:
            return 'Affidamento Diretto'
        if '60' in x_lower:
            return 'Procedura Aperta'
        if '61' in x_lower:
            return 'Procedura Ristretta'
        if '62' in x_lower or '63' in x_lower:
            return 'Procedura Negoziata'
        if '54' in x_lower or '55' in x_lower:
            return 'Procedura Aperta'
        if '57' in x_lower:
            return 'Procedura Negoziata'
        if '122' in x_lower or '125' in x_lower:
            return 'Affidamento Diretto'
        if '153' in x_lower or '183' in x_lower:
            return 'Project Financing'
        if '106' in x_lower:
            return 'Variante/Modifica Contrattuale'
        if '112' in x_lower:
            return 'Procedura Riservata'

    # Gara europea/telematica
    if 'gara' in x_lower and ('europea' in x_lower or 'telematica' in x_lower):
        return 'Procedura Aperta'

    # Sintel
    if 'sintel' in x_lower:
        return 'MePA'

    # Riservata
    if 'riservat' in x_lower:
        return 'Procedura Riservata'

    # Catch-all
    return 'Altra Procedura'


# =============================================================================
# DATA LOADING FUNCTIONS
# =============================================================================

def load_gazzetta(gazzetta_path: Path) -> pd.DataFrame:
    """Carica e standardizza dati Gazzetta da Lotti_Gazzetta_Optimized.xlsx."""
    logger.info("📰 Caricamento Gazzetta...")

    df = pd.read_excel(gazzetta_path)
    logger.info(f"   Righe originali: {len(df):,}")

    df_std = pd.DataFrame({
        'cig': df['CIG'].astype(str).str.strip(),
        'ocid': None,
        'oggetto': df['Oggetto'],
        'testo_completo': df['testo'],
        'importo_aggiudicazione': pd.to_numeric(df['ImportoAggiudicazione'], errors='coerce'),
        'sconto': df['Sconto'].apply(extract_sconto_from_string),
        'data_aggiudicazione': pd.to_datetime(df['DataAggiudicazione'], errors='coerce'),
        'data_scadenza': None,  # Gazzetta non ha fine contratto separata
        'scadenza_gara': pd.to_datetime(df['Scadenza'], errors='coerce'),
        'ente_appaltante': df['AmministrazioneAggiudicatrice'],
        'comune': df['Comune'],
        'aggiudicatario': df['Aggiudicatario'],
        'categoria': df['Categoria'],
        'quick_category': df['QuickCategory'],
        'tipo_intervento': df['TipoIntervento'],
        'tipo_appalto': df['TipoAppalto'],
        'tipo_impianto': df['TipoImpianto'],
        'tipo_illuminazione': df['TipoIlluminazione'],
        'tipo_energia': df['TipoEnergia'],
        'tipo_efficientamento': df['TipoEfficientamento'],
        'tipo_operazione': df['TipoOperazione'],
        'procedura': df['Procedura'],
        'criterio_aggiudicazione': df['CriterioAggiudicazione'],
        'cup': df['CUP'],
        'durata_appalto': df['DurataAppalto'],
        'offerte_ricevute': pd.to_numeric(df['OfferteRicevute'], errors='coerce'),
        'num_lotti': pd.to_numeric(df['NumeroLotti'], errors='coerce'),
        'lotto': df['Lotto'],
        'codice_gruppo': df['CodiceGruppo'],
        'filter_confidence': pd.to_numeric(df['FilterConfidence'], errors='coerce'),
        'cpv_code': None,
        'cpv_description': None,
        'categorie_regex': None,
        'buyer_locality': None,
        'supplier_name': None,
        'procurement_method': None,
        'procurement_method_details': None,
        'tipo_accordo': None,
        'edizione': None,
        'regione': None,
        'fonte': 'Gazzetta'
    })

    logger.info(f"   ✅ Gazzetta: {len(df_std):,} righe, {df_std['sconto'].notna().sum():,} con sconto")
    return df_std


def _calculate_durata(df: pd.DataFrame) -> pd.Series:
    """Calculate contract duration in days from start/end dates."""
    if 'contract_start' not in df.columns or 'data_scadenza' not in df.columns:
        return pd.Series(index=df.index, dtype=float)
    start = pd.to_datetime(df.get('contract_start'), errors='coerce')
    end = pd.to_datetime(df.get('data_scadenza'), errors='coerce')
    duration_days = (end - start).dt.days
    # Only keep reasonable durations (1 day to 30 years)
    mask = (duration_days > 0) & (duration_days < 10950)
    result = pd.Series(index=df.index, dtype=float)
    result[mask] = duration_days[mask]
    return result


def load_ocds(ocds_path: Path) -> pd.DataFrame:
    """Carica e standardizza dati OCDS con mapping colonne refactored."""
    logger.info("📊 Caricamento OCDS...")

    df = pd.read_csv(ocds_path, low_memory=False)
    logger.info(f"   Righe originali: {len(df):,}")

    # Calcola sconto da importo_base/importo_aggiudicazione
    tender_amt = pd.to_numeric(df.get('importo_base'), errors='coerce')
    award_amt = pd.to_numeric(df.get('importo_aggiudicazione'), errors='coerce')
    mask = (tender_amt > 0) & (award_amt > 0) & (award_amt <= tender_amt)
    sconto_calc = pd.Series(index=df.index, dtype=float)
    sconto_calc[mask] = ((tender_amt[mask] - award_amt[mask]) / tender_amt[mask] * 100)
    sconto_calc[(sconto_calc <= 0) | (sconto_calc > 100)] = None

    logger.info(f"   Sconti calcolati: {sconto_calc.notna().sum():,}")

    df_std = pd.DataFrame({
        'cig': df.get('cig'),
        'ocid': df['ocid'].astype(str) if 'ocid' in df.columns else None,
        'oggetto': df.get('oggetto'),
        'testo_completo': df.get('oggetto'),
        'importo_aggiudicazione': award_amt,
        'sconto': sconto_calc,
        'data_aggiudicazione': pd.to_datetime(df.get('data_pubblicazione'), errors='coerce'),
        'data_scadenza': pd.to_datetime(df.get('data_scadenza'), errors='coerce'),
        'scadenza_gara': pd.to_datetime(df.get('scadenza_gara'), errors='coerce'),
        'ente_appaltante': df.get('ente_appaltante'),
        'comune': df.get('comune'),
        'aggiudicatario': df.get('aggiudicatario'),
        'categoria': df.get('categoria'),
        'quick_category': None,
        'tipo_intervento': None,
        'tipo_appalto': df.get('tipo_appalto'),
        'tipo_impianto': None,
        'tipo_illuminazione': None,
        'tipo_energia': None,
        'tipo_efficientamento': None,
        'tipo_operazione': None,
        'procedura': df.get('procedura'),
        'criterio_aggiudicazione': None,
        'cup': None,
        'durata_appalto': _calculate_durata(df),
        'offerte_ricevute': pd.to_numeric(df.get('n_offerte'), errors='coerce'),
        'num_lotti': pd.to_numeric(df.get('n_lotti'), errors='coerce'),
        'lotto': None,
        'codice_gruppo': None,
        'filter_confidence': None,
        'cpv_code': df.get('cpv'),
        'cpv_description': None,
        'categorie_regex': None,
        'buyer_locality': df.get('comune'),
        'supplier_name': df.get('aggiudicatario'),
        'procurement_method': df.get('procedura'),
        'procurement_method_details': None,
        'tipo_accordo': None,
        'edizione': None,
        'regione': df.get('regione'),
        'fonte': 'OCDS'
    })

    logger.info(f"   ✅ OCDS: {len(df_std):,} righe")
    return df_std


def load_servizio_luce(consip_path: Path) -> pd.DataFrame:
    """Carica e standardizza dati ServizioLuce/CONSIP."""
    logger.info("💡 Caricamento ServizioLuce/CONSIP...")

    df = pd.read_excel(consip_path)
    logger.info(f"   Righe originali: {len(df):,}")

    tipo_accordo = df.get('TipoAccordo', pd.Series(['SL'] * len(df)))
    categoria = tipo_accordo.apply(lambda x: 'Edifici' if str(x).upper() == 'SIE' else 'Illuminazione')
    quick_category = tipo_accordo.apply(lambda x: 'Energia Edifici' if str(x).upper() == 'SIE' else 'Illuminazione')
    tipo_impianto = tipo_accordo.apply(lambda x: 'Edifici Pubblici' if str(x).upper() == 'SIE' else 'Illuminazione Pubblica')

    df_std = pd.DataFrame({
        'cig': df['CIG'].astype(str).str.strip(),
        'ocid': None,
        'oggetto': df.get('OggettoGara', df.get('Oggetto')),
        'testo_completo': df.get('OggettoContratto'),
        'importo_aggiudicazione': pd.to_numeric(df.get('ImportoAggiudicazione'), errors='coerce'),
        'sconto': df['Sconto'].apply(extract_sconto_from_string) if 'Sconto' in df.columns else None,
        'data_aggiudicazione': pd.to_datetime(df.get('DataAggiudicazione'), format='%d/%m/%Y', errors='coerce'),
        'data_scadenza': pd.to_datetime(df.get('Scadenza'), errors='coerce', dayfirst=True),
        'scadenza_gara': None,
        'ente_appaltante': df.get('denominazione_centro_costo', df.get('cf_amministrazione_appaltante')),
        'comune': df['Comune'],
        'aggiudicatario': df.get('Aggiudicatario'),
        'categoria': categoria,
        'quick_category': quick_category,
        'tipo_intervento': None,
        'tipo_appalto': None,
        'tipo_impianto': tipo_impianto,
        'tipo_illuminazione': None,
        'tipo_energia': None,
        'tipo_efficientamento': None,
        'tipo_operazione': None,
        'procedura': df.get('TipoSceltaContraente'),
        'criterio_aggiudicazione': df.get('CriterioAggiudicazione'),
        'cup': None,
        'durata_appalto': df.get('DURATA_PREVISTA'),
        'offerte_ricevute': pd.to_numeric(df.get('numero_offerte_ammesse'), errors='coerce'),
        'num_lotti': None,
        'lotto': None,
        'codice_gruppo': None,
        'filter_confidence': None,
        'cpv_code': df.get('cod_cpv'),
        'cpv_description': df.get('descrizione_cpv'),
        'categorie_regex': None,
        'buyer_locality': df['Comune'],
        'supplier_name': df.get('Aggiudicatario'),
        'procurement_method': None,
        'procurement_method_details': None,
        'tipo_accordo': df.get('TipoAccordo'),
        'edizione': df.get('Edizione'),
        'regione': df['Regione'],
        'fonte': 'CONSIP'
    })

    logger.info(f"   ✅ ServizioLuce/CONSIP: {len(df_std):,} righe")
    return df_std


# =============================================================================
# MERGE AND NORMALIZATION
# =============================================================================

def merge_and_normalize(
    gazzetta_path: Path,
    ocds_path: Path,
    consip_path: Path,
    cig_json_path: Path = None
) -> pd.DataFrame:
    """Esegue merge e tutte le normalizzazioni."""
    logger.info("=" * 60)
    logger.info("🔗 BUILD UNIFIED DATASET")
    logger.info("=" * 60)

    # Carica tutti i dataset
    df_gazzetta = load_gazzetta(gazzetta_path)
    gc.collect()

    df_ocds = load_ocds(ocds_path)
    gc.collect()

    df_servizio_luce = load_servizio_luce(consip_path)
    gc.collect()

    # Carica CIG JSON se disponibile (mesi non in OCDS bulk)
    df_cig = None
    if cig_json_path and Path(cig_json_path).exists():
        df_cig = load_ocds(cig_json_path)  # Same CSV format as OCDS
        df_cig['fonte'] = 'CIG_JSON'
        logger.info(f"   ✅ CIG JSON: {len(df_cig):,} righe")
        gc.collect()

    # Concatena
    logger.info("\n📦 Concatenazione...")
    frames = [df_gazzetta, df_ocds, df_servizio_luce]
    if df_cig is not None:
        frames.append(df_cig)
    df = pd.concat(frames, ignore_index=True)
    del df_gazzetta, df_ocds, df_servizio_luce, df_cig
    gc.collect()

    logger.info(f"   Righe totali prima di dedup: {len(df):,}")

    # =================================
    # DEDUP CIG CROSS-SOURCE (Fase 2)
    # =================================
    logger.info("\n🔑 Deduplicazione CIG cross-source...")
    _dq = {}  # data quality report

    # Normalize CIG: extract first CIG from semicolon-separated, strip whitespace
    df['_cig_norm'] = df['cig'].apply(
        lambda x: str(x).split(';')[0].strip().upper()
        if pd.notna(x) and str(x).strip() not in ('', 'nan', 'None')
        else None
    )

    has_cig = df['_cig_norm'].notna()
    logger.info(f"   Record con CIG: {has_cig.sum():,} / {len(df):,}")

    # Source priority: Gazzetta (most enriched) > CONSIP > OCDS > CIG_JSON
    SOURCE_PRIORITY = {'Gazzetta': 0, 'CONSIP': 1, 'OCDS': 2, 'CIG_JSON': 3}
    df['_source_priority'] = df['fonte'].map(SOURCE_PRIORITY).fillna(99)

    # Completeness score: count non-null critical fields
    critical_fields = ['oggetto', 'importo_aggiudicazione', 'ente_appaltante',
                       'aggiudicatario', 'data_aggiudicazione', 'comune', 'regione']
    df['_completeness'] = df[critical_fields].notna().sum(axis=1)

    # Sort: best source first, then most complete
    df = df.sort_values(['_source_priority', '_completeness'],
                        ascending=[True, False])

    # Count duplicates before dedup
    dup_cigs = df[has_cig].groupby('_cig_norm').size()
    n_dup_cigs = (dup_cigs > 1).sum()
    n_dup_rows = dup_cigs[dup_cigs > 1].sum() - n_dup_cigs
    logger.info(f"   CIG duplicati cross-source: {n_dup_cigs:,} CIG ({n_dup_rows:,} righe extra)")
    _dq['cig_duplicates_found'] = int(n_dup_cigs)
    _dq['cig_duplicate_rows_removed'] = int(n_dup_rows)

    # Log which sources overlap
    if n_dup_cigs > 0:
        dup_mask = df['_cig_norm'].isin(dup_cigs[dup_cigs > 1].index) & has_cig
        source_overlap = df[dup_mask].groupby('_cig_norm')['fonte'].apply(
            lambda x: '+'.join(sorted(x.unique()))
        ).value_counts()
        for combo, count in source_overlap.head(10).items():
            logger.info(f"      {combo}: {count:,} CIG")

    # Keep first (best) row per CIG; keep all rows without CIG
    df_with_cig = df[has_cig].drop_duplicates(subset='_cig_norm', keep='first')
    df_no_cig = df[~has_cig]
    df = pd.concat([df_with_cig, df_no_cig], ignore_index=True)
    df = df.drop(columns=['_cig_norm', '_source_priority', '_completeness'])

    logger.info(f"   Righe dopo dedup CIG: {len(df):,}")

    # =================================
    # VALIDAZIONE IMPORTI (Fase 3)
    # =================================
    logger.info("\n💰 Validazione importi...")

    if 'importo_aggiudicazione' in df.columns:
        importi = pd.to_numeric(df['importo_aggiudicazione'], errors='coerce')

        # Flag anomalies
        n_zero = (importi == 0).sum()
        n_negative = (importi < 0).sum()
        n_huge = (importi > 1e9).sum()  # > 1 miliardo
        n_tiny = ((importi > 0) & (importi < 1)).sum()  # < 1 euro

        logger.info(f"   Importi zero: {n_zero:,}")
        logger.info(f"   Importi negativi: {n_negative:,}")
        logger.info(f"   Importi > 1 miliardo: {n_huge:,}")
        logger.info(f"   Importi < 1 euro: {n_tiny:,}")

        # Set invalid amounts to NaN
        invalid_mask = (importi < 0) | (importi > 1e10)
        n_invalidated = invalid_mask.sum()
        df.loc[invalid_mask, 'importo_aggiudicazione'] = None
        logger.info(f"   Importi invalidati (< 0 o > 10 miliardi): {n_invalidated:,}")

        _dq['amounts_zero'] = int(n_zero)
        _dq['amounts_negative'] = int(n_negative)
        _dq['amounts_over_1B'] = int(n_huge)
        _dq['amounts_invalidated'] = int(n_invalidated)

    # =================================
    # FIX DATE (Fase 4)
    # =================================
    logger.info("\n📅 Validazione date...")

    date_cols = ['data_aggiudicazione', 'data_scadenza', 'scadenza_gara']
    for col in date_cols:
        if col not in df.columns:
            continue
        dates = pd.to_datetime(df[col], errors='coerce')
        # Invalidate dates outside 2000-2030 range
        invalid_dates = (dates.dt.year < 2000) | (dates.dt.year > 2030)
        n_invalid = invalid_dates.sum()
        if n_invalid > 0:
            df.loc[invalid_dates, col] = None
            logger.info(f"   {col}: {n_invalid:,} date fuori range (< 2000 o > 2030) invalidate")
        # Count NaT
        n_nat = df[col].isna().sum()
        n_valid = len(df) - n_nat
        pct = n_valid / len(df) * 100
        logger.info(f"   {col}: {n_valid:,} valide ({pct:.1f}%), {n_nat:,} mancanti")
        _dq[f'{col}_valid'] = int(n_valid)
        _dq[f'{col}_missing'] = int(n_nat)

    # Crea chiave unificata
    df['chiave'] = df['cig'].fillna(df['ocid'])

    # Normalizza date e anno
    df['data_aggiudicazione'] = pd.to_datetime(df['data_aggiudicazione'], errors='coerce', utc=True)
    if df['data_aggiudicazione'].dt.tz is not None:
        df['data_aggiudicazione'] = df['data_aggiudicazione'].dt.tz_localize(None)
    df['anno'] = df['data_aggiudicazione'].dt.year

    # =================================
    # NORMALIZZA CATEGORIE
    # =================================
    logger.info("\n🏷️ Normalizzazione categorie...")

    # Usa quick_category come fallback
    mask_no_cat = df['categoria'].isna() | df['categoria'].astype(str).isin(['nan', 'None', ''])
    mask_has_quick = df['quick_category'].notna() & ~df['quick_category'].astype(str).isin(['nan', 'None', ''])
    fallback_mask = mask_no_cat & mask_has_quick
    df.loc[fallback_mask, 'categoria'] = df.loc[fallback_mask, 'quick_category']
    logger.info(f"   Categorie da quick_category: {fallback_mask.sum():,}")

    # Normalizza formato
    mask_valid = df['categoria'].notna() & ~df['categoria'].astype(str).isin(['nan', 'None', ''])
    df.loc[mask_valid, 'categoria'] = df.loc[mask_valid, 'categoria'].astype(str).str.strip().str.title()
    df.loc[~mask_valid, 'categoria'] = None
    logger.info(f"   Categorie uniche: {df['categoria'].nunique()}")

    # =================================
    # NORMALIZZA SOTTOCATEGORIE
    # =================================
    logger.info("\n📋 Normalizzazione sottocategorie...")

    df['quick_category'] = df['quick_category'].apply(normalize_quick_category)
    logger.info(f"   quick_category: {df['quick_category'].nunique()} unici")

    df['tipo_appalto'] = df['tipo_appalto'].apply(normalize_tipo_appalto)
    logger.info(f"   tipo_appalto: {df['tipo_appalto'].nunique()} unici")

    df['tipo_impianto'] = df['tipo_impianto'].apply(normalize_tipo_impianto)
    logger.info(f"   tipo_impianto: {df['tipo_impianto'].nunique()} unici")

    df['tipo_intervento'] = df['tipo_intervento'].apply(normalize_underscore_field)
    df['tipo_illuminazione'] = df['tipo_illuminazione'].apply(normalize_underscore_field)
    df['tipo_efficientamento'] = df['tipo_efficientamento'].apply(normalize_underscore_field)
    df['tipo_operazione'] = df['tipo_operazione'].apply(normalize_underscore_field)
    df['tipo_energia'] = df['tipo_energia'].apply(normalize_tipo_energia)

    # =================================
    # NORMALIZZA PROCEDURE
    # =================================
    logger.info("\n⚖️ Normalizzazione procedure...")
    df['procedura'] = df['procedura'].apply(normalize_procedura)
    logger.info(f"   Procedure uniche: {df['procedura'].nunique()}")

    # =================================
    # NORMALIZZA COMUNI
    # =================================
    logger.info("\n🏙️ Normalizzazione comuni...")
    comuni_before = df['comune'].nunique()
    df['comune'] = df['comune'].apply(normalize_comune)
    comuni_after = df['comune'].nunique()
    logger.info(f"   Comuni: {comuni_before:,} → {comuni_after:,}")

    # Normalizza anche buyer_locality se presente
    if 'buyer_locality' in df.columns:
        df['buyer_locality'] = df['buyer_locality'].apply(normalize_comune)

    # =================================
    # INFERISCI REGIONE DA COMUNE
    # =================================
    logger.info("\n🗺️ Inferenza regione da comune...")
    regioni_before = df['regione'].notna().sum()

    # Mapping capoluoghi e comuni principali -> regione
    COMUNE_REGIONE = {
        # Piemonte
        'Torino': 'Piemonte', 'Alessandria': 'Piemonte', 'Asti': 'Piemonte', 'Biella': 'Piemonte',
        'Cuneo': 'Piemonte', 'Novara': 'Piemonte', 'Verbania': 'Piemonte', 'Vercelli': 'Piemonte',
        # Valle d'Aosta
        'Aosta': "Valle d'Aosta",
        # Lombardia
        'Milano': 'Lombardia', 'Bergamo': 'Lombardia', 'Brescia': 'Lombardia', 'Como': 'Lombardia',
        'Cremona': 'Lombardia', 'Lecco': 'Lombardia', 'Lodi': 'Lombardia', 'Mantova': 'Lombardia',
        'Monza': 'Lombardia', 'Pavia': 'Lombardia', 'Sondrio': 'Lombardia', 'Varese': 'Lombardia',
        # Trentino-Alto Adige
        'Trento': 'Trentino-Alto Adige', 'Bolzano': 'Trentino-Alto Adige',
        # Veneto
        'Venezia': 'Veneto', 'Belluno': 'Veneto', 'Padova': 'Veneto', 'Rovigo': 'Veneto',
        'Treviso': 'Veneto', 'Verona': 'Veneto', 'Vicenza': 'Veneto',
        # Friuli-Venezia Giulia
        'Trieste': 'Friuli-Venezia Giulia', 'Gorizia': 'Friuli-Venezia Giulia',
        'Pordenone': 'Friuli-Venezia Giulia', 'Udine': 'Friuli-Venezia Giulia',
        # Liguria
        'Genova': 'Liguria', 'Imperia': 'Liguria', 'La Spezia': 'Liguria', 'Savona': 'Liguria',
        # Emilia-Romagna
        'Bologna': 'Emilia-Romagna', 'Ferrara': 'Emilia-Romagna', 'Forlì': 'Emilia-Romagna',
        'Modena': 'Emilia-Romagna', 'Parma': 'Emilia-Romagna', 'Piacenza': 'Emilia-Romagna',
        'Ravenna': 'Emilia-Romagna', 'Reggio Emilia': 'Emilia-Romagna', 'Rimini': 'Emilia-Romagna',
        'Cesena': 'Emilia-Romagna',
        # Toscana
        'Firenze': 'Toscana', 'Arezzo': 'Toscana', 'Grosseto': 'Toscana', 'Livorno': 'Toscana',
        'Lucca': 'Toscana', 'Massa': 'Toscana', 'Pisa': 'Toscana', 'Pistoia': 'Toscana',
        'Prato': 'Toscana', 'Siena': 'Toscana',
        # Umbria
        'Perugia': 'Umbria', 'Terni': 'Umbria',
        # Marche
        'Ancona': 'Marche', 'Ascoli Piceno': 'Marche', 'Fermo': 'Marche',
        'Macerata': 'Marche', 'Pesaro': 'Marche', 'Urbino': 'Marche',
        # Lazio
        'Roma': 'Lazio', 'Frosinone': 'Lazio', 'Latina': 'Lazio', 'Rieti': 'Lazio', 'Viterbo': 'Lazio',
        # Abruzzo
        "L'Aquila": 'Abruzzo', 'Chieti': 'Abruzzo', 'Pescara': 'Abruzzo', 'Teramo': 'Abruzzo',
        # Molise
        'Campobasso': 'Molise', 'Isernia': 'Molise',
        # Campania
        'Napoli': 'Campania', 'Avellino': 'Campania', 'Benevento': 'Campania',
        'Caserta': 'Campania', 'Salerno': 'Campania',
        # Puglia
        'Bari': 'Puglia', 'Barletta': 'Puglia', 'Brindisi': 'Puglia', 'Foggia': 'Puglia',
        'Lecce': 'Puglia', 'Taranto': 'Puglia', 'Andria': 'Puglia', 'Trani': 'Puglia',
        # Basilicata
        'Potenza': 'Basilicata', 'Matera': 'Basilicata',
        # Calabria
        'Catanzaro': 'Calabria', 'Cosenza': 'Calabria', 'Crotone': 'Calabria',
        'Reggio Calabria': 'Calabria', 'Vibo Valentia': 'Calabria',
        # Sicilia
        'Palermo': 'Sicilia', 'Agrigento': 'Sicilia', 'Caltanissetta': 'Sicilia',
        'Catania': 'Sicilia', 'Enna': 'Sicilia', 'Messina': 'Sicilia',
        'Ragusa': 'Sicilia', 'Siracusa': 'Sicilia', 'Trapani': 'Sicilia',
        # Sardegna
        'Cagliari': 'Sardegna', 'Carbonia': 'Sardegna', 'Nuoro': 'Sardegna',
        'Oristano': 'Sardegna', 'Sassari': 'Sardegna', 'Olbia': 'Sardegna',
    }

    # Applica mapping solo dove regione è NaN
    mask_no_regione = df['regione'].isna()
    df.loc[mask_no_regione, 'regione'] = df.loc[mask_no_regione, 'comune'].map(COMUNE_REGIONE)

    regioni_after = df['regione'].notna().sum()
    logger.info(f"   Regioni: {regioni_before:,} → {regioni_after:,} (+{regioni_after - regioni_before:,})")

    # =================================
    # FUZZY DEDUPLICATION ENTITÀ
    # =================================
    logger.info("\n🔗 Fuzzy deduplication entità...")

    # Prima normalizza base i nomi (case, forme societarie)
    logger.info("   Pre-normalizzazione nomi...")

    # Ente appaltante
    if 'ente_appaltante' in df.columns:
        df['ente_appaltante'] = df['ente_appaltante'].apply(normalize_entity_name)

    # Buyer name (OCDS)
    if 'buyer_name' in df.columns:
        df['buyer_name'] = df['buyer_name'].apply(normalize_entity_name)

    # Aggiudicatario
    if 'aggiudicatario' in df.columns:
        df['aggiudicatario'] = df['aggiudicatario'].apply(normalize_entity_name)

    # Supplier name (OCDS)
    if 'supplier_name' in df.columns:
        df['supplier_name'] = df['supplier_name'].apply(normalize_entity_name)

    # Ora applica fuzzy deduplication
    logger.info("\n   📍 Fuzzy matching ENTI APPALTANTI...")
    if 'ente_appaltante' in df.columns:
        enti_before = df['ente_appaltante'].nunique()
        df['ente_appaltante'], enti_mapping = fuzzy_deduplicate_entities(
            df['ente_appaltante'], threshold=92, min_occurrences=3
        )
        enti_after = df['ente_appaltante'].nunique()
        logger.info(f"      Enti: {enti_before:,} → {enti_after:,} (riduzione {(1 - enti_after/enti_before)*100:.1f}%)")

    if 'buyer_name' in df.columns:
        buyer_before = df['buyer_name'].nunique()
        df['buyer_name'], buyer_mapping = fuzzy_deduplicate_entities(
            df['buyer_name'], threshold=92, min_occurrences=3
        )
        buyer_after = df['buyer_name'].nunique()
        logger.info(f"      Buyer: {buyer_before:,} → {buyer_after:,} (riduzione {(1 - buyer_after/buyer_before)*100:.1f}%)")

    logger.info("\n   🏢 Fuzzy matching AGGIUDICATARI/FORNITORI...")
    if 'aggiudicatario' in df.columns:
        agg_before = df['aggiudicatario'].nunique()
        df['aggiudicatario'], agg_mapping = fuzzy_deduplicate_entities(
            df['aggiudicatario'], threshold=90, min_occurrences=2
        )
        agg_after = df['aggiudicatario'].nunique()
        logger.info(f"      Aggiudicatari: {agg_before:,} → {agg_after:,} (riduzione {(1 - agg_after/agg_before)*100:.1f}%)")

    if 'supplier_name' in df.columns:
        sup_before = df['supplier_name'].nunique()
        df['supplier_name'], sup_mapping = fuzzy_deduplicate_entities(
            df['supplier_name'], threshold=90, min_occurrences=2
        )
        sup_after = df['supplier_name'].nunique()
        logger.info(f"      Supplier: {sup_before:,} → {sup_after:,} (riduzione {(1 - sup_after/sup_before)*100:.1f}%)")

    logger.info("\n   🏙️ Fuzzy matching COMUNI...")
    comuni_before_fuzzy = df['comune'].nunique()
    df['comune'], comuni_mapping = fuzzy_deduplicate_entities(
        df['comune'], threshold=95, min_occurrences=5
    )
    comuni_after_fuzzy = df['comune'].nunique()
    logger.info(f"      Comuni: {comuni_before_fuzzy:,} → {comuni_after_fuzzy:,} (riduzione {(1 - comuni_after_fuzzy/max(1,comuni_before_fuzzy))*100:.1f}%)")

    # =================================
    # ENRICHMENT DURATA (da Gemini cache)
    # =================================
    enrichment_path = CATEGORIE_DIR / "enrichment_durata.json"
    if enrichment_path.exists():
        logger.info("\n🤖 Applicazione enrichment durata Gemini...")
        try:
            with open(enrichment_path) as f:
                enrichment = json.load(f)
            items = enrichment.get("items", {})
            logger.info(f"   Cache enrichment: {len(items):,} CIG")

            # Normalize CIG for lookup (first CIG, uppercase)
            df['_cig_lookup'] = df['cig'].apply(
                lambda x: str(x).split(';')[0].strip().upper()
                if pd.notna(x) and str(x).strip() not in ('', 'nan', 'None')
                else None
            )

            # Build enrichment series
            durata_enriched = df['_cig_lookup'].map(
                lambda c: items.get(c, items.get(str(c).lower(), {})).get('durata_giorni')
                if c else None
            )
            durata_max_enriched = df['_cig_lookup'].map(
                lambda c: items.get(c, items.get(str(c).lower(), {})).get('durata_max_giorni')
                if c else None
            )
            confidence = df['_cig_lookup'].map(
                lambda c: items.get(c, items.get(str(c).lower(), {})).get('confidence', 0)
                if c else 0
            )

            # Only apply high-confidence results (>= 0.7) where durata_appalto is missing
            high_conf = confidence >= 0.7
            missing_durata = df['durata_appalto'].isna() | (df['durata_appalto'] == 0)
            mask = high_conf & missing_durata & durata_enriched.notna()

            before = df['durata_appalto'].notna().sum()
            df.loc[mask, 'durata_appalto'] = durata_enriched[mask]

            # Also compute data_scadenza from durata where missing
            missing_scadenza = df['data_scadenza'].isna()
            has_award_date = df['data_aggiudicazione'].notna()
            scad_mask = mask & missing_scadenza & has_award_date
            if scad_mask.any():
                df.loc[scad_mask, 'data_scadenza'] = (
                    df.loc[scad_mask, 'data_aggiudicazione']
                    + pd.to_timedelta(durata_enriched[scad_mask], unit='D')
                )

            after = df['durata_appalto'].notna().sum()
            scad_after = df['data_scadenza'].notna().sum()
            logger.info(f"   Durata arricchita: {before:,} → {after:,} (+{after - before:,})")
            logger.info(f"   Data scadenza dopo enrichment: {scad_after:,}")

            df = df.drop(columns=['_cig_lookup'])
        except Exception as e:
            logger.warning(f"   Errore enrichment: {e}")
    else:
        logger.info("\n🤖 Nessun enrichment durata trovato (esegui scripts/enrich_durata_gemini.py)")

    # =================================
    # CALCOLA data_scadenza DA durata_appalto (per tutte le fonti)
    # =================================
    logger.info("\n📅 Calcolo data_scadenza da durata_appalto...")
    scad_before = df['data_scadenza'].notna().sum()

    missing_scadenza = df['data_scadenza'].isna()
    has_durata = df['durata_appalto'].notna() & (df['durata_appalto'] > 0)
    has_award = df['data_aggiudicazione'].notna()
    can_compute = missing_scadenza & has_durata & has_award

    if can_compute.any():
        df.loc[can_compute, 'data_scadenza'] = (
            df.loc[can_compute, 'data_aggiudicazione']
            + pd.to_timedelta(df.loc[can_compute, 'durata_appalto'], unit='D')
        )

    scad_after = df['data_scadenza'].notna().sum()
    logger.info(f"   data_scadenza: {scad_before:,} → {scad_after:,} (+{scad_after - scad_before:,})")
    if can_compute.any():
        by_fonte = df.loc[can_compute].groupby('fonte').size()
        for fonte, count in by_fonte.items():
            logger.info(f"     {fonte}: +{count:,}")

    # =================================
    # RIORDINA COLONNE
    # =================================
    cols_order = [
        'chiave', 'cig', 'ocid',
        'oggetto', 'testo_completo',
        'importo_aggiudicazione', 'sconto',
        'data_aggiudicazione', 'anno', 'data_scadenza', 'scadenza_gara',
        'ente_appaltante', 'comune', 'regione', 'aggiudicatario',
        'categoria', 'quick_category', 'tipo_intervento', 'tipo_appalto',
        'tipo_impianto', 'tipo_illuminazione', 'tipo_energia',
        'tipo_efficientamento', 'tipo_operazione',
        'procedura', 'criterio_aggiudicazione',
        'procurement_method', 'procurement_method_details',
        'cpv_code', 'cpv_description', 'categorie_regex',
        'cup', 'durata_appalto', 'offerte_ricevute', 'num_lotti', 'lotto',
        'codice_gruppo', 'filter_confidence',
        'tipo_accordo', 'edizione',
        'buyer_locality', 'supplier_name',
        'fonte'
    ]

    existing_cols = [c for c in cols_order if c in df.columns]
    other_cols = [c for c in df.columns if c not in cols_order]
    df = df[existing_cols + other_cols]

    # =================================
    # STATISTICHE
    # =================================
    logger.info("\n" + "=" * 60)
    logger.info("📊 STATISTICHE FINALI")
    logger.info("=" * 60)
    logger.info(f"   Righe totali: {len(df):,}")
    logger.info(f"   Chiavi uniche: {df['chiave'].nunique():,}")
    logger.info(f"   Con sconto: {df['sconto'].notna().sum():,}")
    logger.info(f"   Con data: {df['data_aggiudicazione'].notna().sum():,}")
    logger.info(f"   Categorie: {df['categoria'].nunique()}")
    logger.info(f"   Procedure: {df['procedura'].nunique()}")

    by_fonte = df.groupby('fonte').size()
    logger.info("\n   Per fonte:")
    for fonte, count in by_fonte.items():
        logger.info(f"     - {fonte}: {count:,}")

    by_anno = df.groupby('anno').size().sort_index()
    logger.info("\n   Per anno (top 5):")
    for anno, count in by_anno.tail(5).items():
        if pd.notna(anno):
            logger.info(f"     - {int(anno)}: {count:,}")

    # =================================
    # DATA QUALITY REPORT (Fase 3 cont.)
    # =================================
    logger.info("\n" + "=" * 60)
    logger.info("🔍 DATA QUALITY REPORT")
    logger.info("=" * 60)

    # Field coverage
    key_fields = [
        'cig', 'oggetto', 'importo_aggiudicazione', 'data_aggiudicazione',
        'ente_appaltante', 'comune', 'regione', 'aggiudicatario',
        'categoria', 'procedura', 'sconto', 'data_scadenza', 'durata_appalto'
    ]
    for field in key_fields:
        if field in df.columns:
            filled = df[field].fillna('')
            n_valid = (filled.astype(str).str.strip() != '').sum()
            pct = n_valid / len(df) * 100
            bar = '█' * int(pct / 5) + '░' * (20 - int(pct / 5))
            logger.info(f"   {field:30s} {bar} {pct:5.1f}% ({n_valid:,})")

    # Dedup summary
    if _dq:
        logger.info("\n   Dedup & Validazione:")
        for k, v in _dq.items():
            logger.info(f"     {k}: {v:,}")

    return df


def save_output(df: pd.DataFrame, output_path: Path, compress: bool = True):
    """Salva il DataFrame in CSV (opzionalmente compresso)."""
    # Gestisci correttamente i suffissi .csv.gz
    path_str = str(output_path)
    if path_str.endswith('.csv.gz'):
        csv_path = Path(path_str[:-3])  # Rimuovi .gz
        gz_path = output_path
    elif path_str.endswith('.gz'):
        csv_path = output_path.with_suffix('.csv')
        gz_path = output_path
    else:
        csv_path = output_path if output_path.suffix == '.csv' else output_path.with_suffix('.csv')
        gz_path = Path(str(csv_path) + '.gz')

    logger.info(f"\n💾 Salvataggio...")
    df.to_csv(csv_path, index=False)
    size_mb = csv_path.stat().st_size / 1e6
    logger.info(f"   CSV: {csv_path} ({size_mb:.1f} MB)")

    if compress:
        with open(csv_path, 'rb') as f_in:
            with gzip.open(gz_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        gz_size_mb = gz_path.stat().st_size / 1e6
        logger.info(f"   GZ: {gz_path} ({gz_size_mb:.1f} MB)")

    return gz_path if compress else csv_path


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def build_unified_dataset(
    gazzetta_path: Path = DEFAULT_GAZZETTA,
    ocds_path: Path = DEFAULT_OCDS,
    consip_path: Path = DEFAULT_CONSIP,
    output_path: Path = DEFAULT_OUTPUT,
    cig_json_path: Path = DEFAULT_CIG_JSON,
    compress: bool = True
) -> Path:
    """
    Build unified dataset from 3+ data sources.

    Args:
        gazzetta_path: Path to Gazzetta Excel file
        ocds_path: Path to OCDS CSV file
        consip_path: Path to CONSIP/ServizioLuce Excel file
        output_path: Path to output file
        cig_json_path: Path to CIG JSON CSV (months not in OCDS bulk)
        compress: Whether to compress output

    Returns:
        Path to output file
    """
    df = merge_and_normalize(gazzetta_path, ocds_path, consip_path, cig_json_path)
    output_file = save_output(df, output_path, compress=compress)
    return output_file


def main():
    parser = argparse.ArgumentParser(
        description='Build unified dataset from source databases',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use default paths
  python src/build/unified_dataset.py

  # Custom output
  python src/build/unified_dataset.py --output /path/to/output.csv.gz

  # Uncompressed output
  python src/build/unified_dataset.py --no-compress

  # Custom input files
  python src/build/unified_dataset.py --gazzetta /path/to/gazzetta.xlsx --ocds /path/to/ocds.csv
        """
    )
    parser.add_argument('--gazzetta', type=str, default=str(DEFAULT_GAZZETTA),
                        help='Gazzetta Excel file path')
    parser.add_argument('--ocds', type=str, default=str(DEFAULT_OCDS),
                        help='OCDS CSV file path')
    parser.add_argument('--consip', type=str, default=str(DEFAULT_CONSIP),
                        help='CONSIP/ServizioLuce Excel file path')
    parser.add_argument('--output', '-o', type=str, default=str(DEFAULT_OUTPUT),
                        help='Output file path')
    parser.add_argument('--no-compress', action='store_true',
                        help='Do not compress output')
    args = parser.parse_args()

    gazzetta_path = Path(args.gazzetta)
    ocds_path = Path(args.ocds)
    consip_path = Path(args.consip)
    output_path = Path(args.output)

    # Create output directory
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Build dataset
    output_file = build_unified_dataset(
        gazzetta_path=gazzetta_path,
        ocds_path=ocds_path,
        consip_path=consip_path,
        output_path=output_path,
        compress=not args.no_compress
    )

    logger.info("\n" + "=" * 60)
    logger.info("✅ BUILD COMPLETATO!")
    logger.info(f"   Output: {output_file}")
    logger.info("=" * 60)

    return output_file


if __name__ == "__main__":
    main()
