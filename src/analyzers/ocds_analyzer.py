"""
Analizzatore OCDS con filtro regex + sistema AI a due stadi (GPT-5-mini → GPT-5).
"""

import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from pydantic import BaseModel, Field

from config.settings import config
from src.llm.client import get_llm_client
from src.utils.checkpoint import CheckpointManager
from src.utils.performance import timer

logger = logging.getLogger(__name__)

# Configurazione OpenAI GPT-5
_llm_mini = get_llm_client(use_full_model=False)
_llm_full = get_llm_client(use_full_model=True)


def _estrai_criterio_aggiudicazione(tender: Dict[str, Any]) -> str:
    """Prova ad estrarre il criterio di aggiudicazione da campi OCDS comuni."""
    candidates = []
    # Possibili campi OCDS/varianti
    for key in [
        "awardCriteria",
        "awardCriteriaDetails",
        "procurementMethodDetails",
        "awardCriteriaDescription",
        "awardCriteriaDetailsText",
    ]:
        val = tender.get(key)
        if isinstance(val, str) and val.strip():
            candidates.append(val)
    # Se c'è un oggetto strutturato (alcuni dataset OCDS)
    if isinstance(tender.get("awardCriteria"), dict):
        for v in tender["awardCriteria"].values():
            if isinstance(v, str) and v.strip():
                candidates.append(v)
    text = " ".join(candidates).strip().upper()
    if not text and isinstance(tender.get("procurementMethod"), str):
        text = tender["procurementMethod"].upper()

    if not text:
        return ""

    # Heuristics mapping
    if any(
        k in text
        for k in [
            "MINOR",
            "PREZZO",
            "LOWEST",
            "PRICE ONLY",
            "LOWEST COST",
            "PREZZO PIÙ BASSO",
        ]
    ):
        return "MINOR PREZZO"
    if any(
        k in text
        for k in [
            "OFFERTA ECONOMICAMENTE",
            "OEPV",
            "MOST ECONOMICALLY ADVANTAGEOUS",
            "MEAT",
            "QUALITY/PRICE",
            "QUALITÀ/",
        ]
    ):
        return "OFFERTA ECONOMICAMENTE PIÙ VANTAGGIOSA"
    if any(k in text for k in ["QUALIT", "QUALITY", "TECHNICAL"]):
        return "QUALITATIVO/TECNICO"
    return text


def _load_ocds_with_fallback(file_path: Path) -> Dict[str, Any]:
    """
    Carica un file OCDS con fallback tollerante:
    - Primo tentativo: JSON completo
    - Fallback: parse riga-per-riga; accetta oggetti con chiave 'releases' o
      singoli 'release' plausibili (contengono 'tender' o 'buyer' o 'awards').
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict) and "releases" in data:
                return data
            # Se non è nel formato atteso, continua col fallback
    except Exception:
        pass

    releases: List[Dict[str, Any]] = []
    bad_lines = 0
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    bad_lines += 1
                    continue
                if isinstance(obj, dict):
                    if "releases" in obj and isinstance(obj["releases"], list):
                        releases.extend(obj["releases"])
                    elif any(k in obj for k in ("tender", "buyer", "awards")):
                        releases.append(obj)
        if releases:
            logger.warning(
                f"OCDS fallback parziale: {file_path.name} (releases valide: {len(releases)}, righe scartate: {bad_lines})"
            )
            return {"releases": releases}
    except Exception as e:
        logger.error(f"Errore fallback OCDS {file_path}: {e}")
    # Se tutto fallisce, ritorna struttura vuota per non bloccare
    return {"releases": []}


# MODELLI PYDANTIC PER CATEGORIZZAZIONE


class CategoriaAppalto(str, Enum):
    """Categorie principali degli appalti."""

    ILLUMINAZIONE = "Illuminazione"
    VIDEOSORVEGLIANZA = "Videosorveglianza"
    ENERGIA = "Energia"
    EDIFICI = "Edifici"
    MOBILITA = "Mobilità"
    SMART_CITY = "Smart City"
    VERDE_PUBBLICO = "Verde Pubblico"
    STRADE = "Strade"
    IMPIANTI_SPORTIVI = "Impianti Sportivi"
    SERVIZI_PUBBLICI = "Servizi Pubblici"
    EMERGENZA = "Emergenza"
    ACQUA_FOGNATURE = "Acqua e Fognature"
    RIFIUTI = "Rifiuti"
    ALTRO = "Altro"


class TipoIntervento(str, Enum):
    """Tipo di intervento."""

    FORNITURA = "Fornitura"
    LAVORI = "Lavori"
    SERVIZI = "Servizi"
    MANUTENZIONE_ORDINARIA = "Manutenzione Ordinaria"
    MANUTENZIONE_STRAORDINARIA = "Manutenzione Straordinaria"
    RIQUALIFICAZIONE = "Riqualificazione"
    EFFICIENTAMENTO = "Efficientamento"
    NUOVA_COSTRUZIONE = "Nuova Costruzione"
    GESTIONE = "Gestione"
    ALTRO = "Altro"


class TipoEnergia(str, Enum):
    """Tipo di energia/impianto."""

    ELETTRICO = "Elettrico"
    TERMICO = "Termico"
    FOTOVOLTAICO = "Fotovoltaico"
    SOLARE_TERMICO = "Solare Termico"
    EOLICO = "Eolico"
    GEOTERMICO = "Geotermico"
    LED = "LED"
    TRADIZIONALE = "Tradizionale"
    MISTO = "Misto"
    NON_APPLICABILE = "Non Applicabile"


class AppaltoOCDS(BaseModel):
    """Modello per un appalto OCDS categorizzato."""

    oggetto: str = Field(default="", description="Oggetto dell'appalto")
    categoria: CategoriaAppalto = Field(
        default=CategoriaAppalto.ALTRO, description="Categoria principale"
    )
    tipo_intervento: TipoIntervento = Field(
        default=TipoIntervento.ALTRO, description="Tipo di intervento"
    )
    tipo_energia: TipoEnergia = Field(
        default=TipoEnergia.NON_APPLICABILE, description="Tipo energia se applicabile"
    )
    comune: str = Field(default="", description="Comune/Località")
    ente: str = Field(default="", description="Ente appaltante")
    importo: float = Field(default=0.0, description="Importo in euro")
    aggiudicatario: str = Field(default="", description="Aggiudicatario")
    cig: str = Field(default="", description="Codice CIG")
    smart_city: bool = Field(default=False, description="Contiene elementi smart city")
    sostenibilita: bool = Field(
        default=False, description="Contiene elementi di sostenibilità"
    )


class AppaltiOCDS(BaseModel):
    """Collezione di appalti OCDS."""

    appalti: List[AppaltoOCDS]


class OCDSFilterResult(BaseModel):
    """Risultato filtro rapido (Stage 1) per OCDS."""

    is_relevant: bool
    confidence: float
    category: str
    reason: str


def applica_filtro_categoria(testo: str) -> bool:
    """
    Applica filtri regex (allineati a Gazzetta) per catturare gare rilevanti.
    """
    if not testo:
        return False

    testo_lower = str(testo).lower()

    # Regex allineati a Gazzetta (macro-ambiti)
    patterns = [
        # Illuminazione
        r"illumin|lampion|lampade|pubblica illuminazione|led",
        # Videosorveglianza / digitale
        r"videosorveglian|telecamer|tvcc|sicurezza urbana|controllo accessi",
        r"smart city|\biot\b|iot|sensori|monitoraggio|telecontrollo|fibra|banda ultra|digitalizzazion|wifi|datacenter",
        # Energia / efficientamento
        r"efficientament|fotovoltaic|pannelli solari|energia|centrale termic|cogenerazion|trigenerazion|gestione calore|servizio energia|climatizzazion|termoidraul",
        # Edifici / scuole / sanità
        r"edific|manutenzion|ristrutturazion|riqualificazion|scuol|ospedal|asl|universit|palazz|plesso",
        # Strade / infrastrutture
        r"strad|marciapied|asfalto|segnaletic|rotond|rotatori|ponti|viadott|galleri|tunnel|svincol|ciclabil",
        # Trasporti / parcheggi
        r"parchegg|sosta|metro|tram|stazione|autostazion|tpl|trasporto pubblic",
        # Ambiente / idrico / rifiuti / verde
        r"acquedott|idric|fognar|depurator|bonifiche",
        r"rifiut|igiene urbana|raccolta differenziata|isola ecologic",
        r"verde pubblic|parchi|giardin|arredo urbano",
        # E-mobility
        r"colonnin|ricaric|e-mobility|mobilità elettrica|veicoli elettrici",
        # Impianti sportivi
        r"impianti sportiv|campo sportiv|palestra|piscina|stadio",
    ]

    for pattern in patterns:
        if re.search(pattern, testo_lower):
            return True

    return False


def stage1_quick_filter_ocds(testo: str) -> OCDSFilterResult:
    """
    Stage 1: filtro rapido con GPT-5-mini per decidere pertinenza e categoria macro.
    """
    system = (
        "Sei un assistente che filtra rapidamente appalti pubblici italiani. "
        "Rispondi SOLO con JSON."
    )
    user = (
        "Classifica se il seguente testo è rilevante per appalti di infrastrutture/servizi pubblici.\n"
        "Restituisci JSON con chiavi: is_relevant (bool), confidence (0-1), "
        "category (string), reason (string concisa).\n\n"
        f"TESTO: {str(testo)[:1500]}"
    )
    try:
        result = _llm_mini.chat_json(
            system_prompt=system, user_prompt=user, max_tokens=200
        )
        return OCDSFilterResult(
            is_relevant=bool(result.get("is_relevant", True)),
            confidence=float(result.get("confidence", 0.6)),
            category=str(result.get("category", "Altro")),
            reason=str(result.get("reason", "")),
        )
    except Exception:
        # fallback conservativo: NON passare allo Stage 2 per evitare costi
        return OCDSFilterResult(
            is_relevant=False, confidence=0.99, category="Altro", reason="fallback"
        )


def categorizza_con_gpt5(
    testo: str, importo: float = 0, ente: str = "", use_full: bool = False
) -> AppaltoOCDS:
    """
    Usa GPT-5 per categorizzare un appalto.

    Args:
        testo: Testo da categorizzare (titolo + descrizione)
        importo: Importo dell'appalto
        ente: Ente appaltante

    Returns:
        AppaltoOCDS categorizzato
    """
    try:
        prompt = f"""Analizza questo appalto pubblico e categorizzalo:

TESTO: {testo[:2000]}  # Limita per token
ENTE: {ente}
IMPORTO: €{importo:,.2f}

Restituisci un JSON con i seguenti campi:
- oggetto: breve descrizione dell'oggetto (max 200 caratteri)
- categoria: una tra {[e.value for e in CategoriaAppalto]}
- tipo_intervento: uno tra {[e.value for e in TipoIntervento]}
- tipo_energia: uno tra {[e.value for e in TipoEnergia]}
- comune: estrai il comune se presente
- smart_city: true se contiene elementi smart/IoT/sensori
- sostenibilita: true se contiene elementi di sostenibilità/efficientamento

Rispondi SOLO con il JSON, nessun altro testo."""

        system = "Sei un esperto di appalti pubblici. Restituisci solo JSON valido."
        llm = _llm_full if use_full else _llm_mini
        result = llm.chat_json(system_prompt=system, user_prompt=prompt, max_tokens=600)

        # Crea oggetto AppaltoOCDS
        return AppaltoOCDS(
            oggetto=result.get("oggetto", testo[:200]),
            categoria=CategoriaAppalto(result.get("categoria", "Altro")),
            tipo_intervento=TipoIntervento(result.get("tipo_intervento", "Altro")),
            tipo_energia=TipoEnergia(result.get("tipo_energia", "Non Applicabile")),
            comune=result.get("comune", ""),
            ente=ente,
            importo=importo,
            smart_city=result.get("smart_city", False),
            sostenibilita=result.get("sostenibilita", False),
        )

    except Exception as e:
        logger.warning(f"Errore categorizzazione GPT-5: {e}")
        # Fallback a categorizzazione base
        return AppaltoOCDS(
            oggetto=testo[:200] if testo else "",
            categoria=CategoriaAppalto.ALTRO,
            tipo_intervento=TipoIntervento.ALTRO,
            tipo_energia=TipoEnergia.NON_APPLICABILE,
            ente=ente,
            importo=importo,
        )


class OCDSAnalyzer:
    """
    Analizza file OCDS con: filtro regex → Stage 1 (GPT-5-mini) → Stage 2 (GPT-5).
    """

    def __init__(
        self,
        use_filter: bool = True,
        use_ai: bool = True,
        use_full_model: bool = False,
        max_workers: int = 4,
    ):
        """
        Inizializza l'analizzatore.

        Args:
            use_filter: Se True, applica il filtro categorie
            use_ai: Se True, usa o3 per categorizzazione
        """
        self.use_filter = use_filter
        self.use_ai = use_ai
        self.use_full_model = use_full_model
        self.max_workers = max_workers
        self.checkpoint_manager = CheckpointManager()
        self.checkpoint_manager.create_session("ocds_analyzer")

    def process_ocds_file(self, file_path: Path) -> pd.DataFrame:
        """
        Processa un file OCDS con categorizzazione AI.

        Args:
            file_path: Path del file OCDS

        Returns:
            DataFrame con dati categorizzati
        """
        try:
            data = _load_ocds_with_fallback(file_path)

            records = []
            records_filtered = 0
            records_total = 0
            records_categorized = 0

            # Estrai releases
            releases = data.get("releases", [])

            for release in releases:
                records_total += 1

                # Estrai campi principali
                tender = release.get("tender", {})
                tender_title = tender.get("title", "")
                tender_description = tender.get("description", "")

                # Combina testi
                testo_completo = f"{tender_title} {tender_description}"

                # Applica filtro se richiesto
                if self.use_filter and not applica_filtro_categoria(testo_completo):
                    continue

                records_filtered += 1

                # Stage 1 (LLM quick filter)
                if self.use_ai and testo_completo.strip():
                    fr = stage1_quick_filter_ocds(testo_completo)
                    # Se non rilevante con confidenza alta, salta
                    if (not fr.is_relevant) and (
                        fr.confidence > config.CONFIDENCE_NOT_RELEVANT_THRESHOLD
                    ):
                        continue

                # Estrai dati base
                tender_value = tender.get("value", {}).get("amount", 0)
                buyer_name = release.get("buyer", {}).get("name", "")

                # Criterio aggiudicazione (heuristic)
                criterio = _estrai_criterio_aggiudicazione(tender)

                # Categorizza con AI se richiesto
                if self.use_ai and testo_completo.strip():
                    appalto = categorizza_con_gpt5(
                        testo=testo_completo,
                        importo=tender_value or 0,
                        ente=buyer_name,
                        use_full=False,  # Forza GPT-5-mini anche per Stage 2
                    )
                    records_categorized += 1

                    record = {
                        "id": release.get("id"),
                        "date": release.get("date"),
                        "oggetto": appalto.oggetto,
                        "categoria": appalto.categoria.value,
                        "tipo_intervento": appalto.tipo_intervento.value,
                        "tipo_energia": appalto.tipo_energia.value,
                        "comune": appalto.comune,
                        "ente": buyer_name,
                        "importo": tender_value,
                        "aggiudicatario": appalto.aggiudicatario,
                        "cig": appalto.cig,
                        "smart_city": appalto.smart_city,
                        "sostenibilita": appalto.sostenibilita,
                        "CriterioAggiudicazione": criterio,
                        "FilterConfidence": fr.confidence if "fr" in locals() else None,
                        "QuickCategory": fr.category if "fr" in locals() else None,
                        "tender_status": tender.get("status"),
                        "tender_currency": tender.get("value", {}).get("currency"),
                    }
                else:
                    # Senza AI, usa dati base
                    record = {
                        "id": release.get("id"),
                        "date": release.get("date"),
                        "oggetto": tender_title[:200],
                        "categoria": "Non Categorizzato",
                        "tipo_intervento": "Non Categorizzato",
                        "tipo_energia": "Non Applicabile",
                        "comune": "",
                        "ente": buyer_name,
                        "importo": tender_value,
                        "CriterioAggiudicazione": criterio,
                        "tender_status": tender.get("status"),
                        "tender_currency": tender.get("value", {}).get("currency"),
                    }

                # Gestisci lotti se presenti
                lots = tender.get("lots", [])
                if lots:
                    for lot in lots:
                        lot_title = lot.get("title", "")
                        lot_description = lot.get("description", "")
                        lot_text = f"{lot_title} {lot_description}"

                        # Filtra lotto se necessario
                        if self.use_filter and not applica_filtro_categoria(lot_text):
                            continue

                        lot_record = record.copy()
                        lot_record.update(
                            {
                                "lot_id": lot.get("id"),
                                "lot_title": lot_title,
                                "lot_value": lot.get("value", {}).get("amount"),
                            }
                        )
                        records.append(lot_record)
                else:
                    records.append(record)

            logger.info(f"OCDS {file_path.name}:")
            logger.info(f"  - Totale: {records_total}")
            logger.info(f"  - Filtrati: {records_filtered}")
            logger.info(f"  - Categorizzati con AI: {records_categorized}")
            logger.info(f"  - Record finali: {len(records)}")

            return pd.DataFrame(records)

        except Exception as e:
            logger.error(f"Errore processamento OCDS {file_path}: {e}")
            return pd.DataFrame()

    @timer
    def run(self) -> None:
        """
        Esegue l'analisi di tutti i file OCDS con categorizzazione GPT-5.
        """
        logger.info("=== ANALISI OCDS CON CATEGORIZZAZIONE GPT-5 ===")
        logger.info(f"Filtro: {'ATTIVO' if self.use_filter else 'DISATTIVO'}")
        logger.info(f"AI (GPT-5): {'ATTIVO' if self.use_ai else 'DISATTIVO'}")

        all_data = []
        total_filtered = 0
        total_categorized = 0

        # Trova file OCDS
        ocds_files = list(config.OCDS_DIR.glob("*.json"))
        logger.info(f"File OCDS trovati: {len(ocds_files)}")

        # Processa con ThreadPoolExecutor per velocizzare
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}

            for ocds_file in ocds_files:
                task_id = f"analyze_ocds_{ocds_file.name}"

                if self.checkpoint_manager.should_skip(task_id):
                    logger.info(f"⏭️ Già analizzato: {ocds_file.name}")
                    continue

                future = executor.submit(self.process_ocds_file, ocds_file)
                futures[future] = (ocds_file, task_id)

            # Raccogli risultati
            for future in as_completed(futures):
                ocds_file, task_id = futures[future]
                try:
                    df = future.result()
                    if not df.empty:
                        all_data.append(df)
                        total_filtered += len(df)

                        # Conta categorizzati
                        if "categoria" in df.columns:
                            total_categorized += (
                                df["categoria"] != "Non Categorizzato"
                            ).sum()

                        self.checkpoint_manager.mark_completed(
                            task_id,
                            {"records": len(df), "categorized": total_categorized},
                        )
                    else:
                        self.checkpoint_manager.mark_completed(task_id, {"records": 0})

                except Exception as e:
                    logger.error(f"Errore processamento {ocds_file}: {e}")
                    self.checkpoint_manager.mark_failed(task_id, str(e))

        # Combina tutti i dati
        if all_data:
            df_combined = pd.concat(all_data, ignore_index=True)

            # Rimuovi duplicati
            if "id" in df_combined.columns:
                df_combined.drop_duplicates(subset=["id"], keep="first", inplace=True)

            # Statistiche finali
            logger.info("=== STATISTICHE ANALISI OCDS ===")
            logger.info(f"Record totali: {len(df_combined)}")
            logger.info(f"Record categorizzati con GPT-5: {total_categorized}")

            if "categoria" in df_combined.columns:
                logger.info("\nDistribuzione categorie:")
                for cat, count in (
                    df_combined["categoria"].value_counts().head(10).items()
                ):
                    logger.info(f"  - {cat}: {count}")

            if "smart_city" in df_combined.columns:
                smart_count = df_combined["smart_city"].sum()
                logger.info(f"\nAppalti Smart City: {smart_count}")

            if "sostenibilita" in df_combined.columns:
                sost_count = df_combined["sostenibilita"].sum()
                logger.info(f"Appalti Sostenibilità: {sost_count}")

            # Salva risultato
            output_file = config.get_file_path("OCDS_Analyzed.xlsx", "output")

            if len(df_combined) > 1000000:
                # CSV per file grandi
                csv_file = output_file.with_suffix(".csv")
                df_combined.to_csv(csv_file, index=False)
                logger.info(f"✅ Dati salvati in CSV: {csv_file}")
            else:
                df_combined.to_excel(output_file, index=False)
                logger.info(f"✅ Dati salvati: {output_file}")

            self.checkpoint_manager.mark_completed(
                "ocds_analysis_complete",
                {
                    "total_records": len(df_combined),
                    "categorized": total_categorized,
                    "output": str(output_file),
                },
            )
        else:
            logger.warning("Nessun dato OCDS processato")
