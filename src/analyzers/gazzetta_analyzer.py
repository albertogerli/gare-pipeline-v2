"""
Analizzatore per i dati della Gazzetta Ufficiale.
"""

import hashlib
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

from config.settings import config
from src.llm.client import get_llm_client
from src.utils.checkpoint import CheckpointManager
from src.utils.logging_config import ProgressLogger
from src.utils.performance import timer

logger = logging.getLogger(__name__)
progress_logger = ProgressLogger("gazzetta_analyzer")


class GazzettaAnalyzer:
    """
    Analizza e processa i dati estratti dalla Gazzetta Ufficiale.
    """

    def __init__(self, use_ai: bool = False, use_full_model: bool = False):
        """Inizializza l'analizzatore."""
        self.checkpoint_manager = CheckpointManager()
        self.checkpoint_manager.create_session("gazzetta_analyzer")
        self.use_ai = use_ai
        self.use_full_model = use_full_model
        self._llm = get_llm_client(use_full_model=use_full_model) if use_ai else None

    def clean_text(self, text: str) -> str:
        """
        Pulisce il testo rimuovendo caratteri speciali.

        Args:
            text: Testo da pulire

        Returns:
            Testo pulito
        """
        if pd.isna(text):
            return ""

        text = str(text)
        text = text.replace("\\n", " ")
        text = text.replace("\n", " ")
        text = text.replace("\xa0", " ")
        text = re.sub(r"\s{2,}", " ", text)
        return text.strip()

    def hash_text(self, text: str) -> str:
        """
        Genera hash SHA256 del testo.

        Args:
            text: Testo da hashare

        Returns:
            Hash esadecimale
        """
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def extract_cig(self, text: str) -> Optional[str]:
        """
        Estrae il codice CIG dal testo.

        Args:
            text: Testo da analizzare

        Returns:
            CIG se trovato, None altrimenti
        """
        # Pattern per CIG (10 caratteri alfanumerici)
        cig_pattern = r"\b[A-Z0-9]{10}\b"

        # Cerca prima con keyword CIG
        cig_with_label = re.search(r"CIG[:\s]+([A-Z0-9]{10})", text, re.IGNORECASE)
        if cig_with_label:
            return cig_with_label.group(1)

        # Cerca pattern generico
        matches = re.findall(cig_pattern, text)
        if matches:
            # Filtra codici che sembrano CIG validi
            for match in matches:
                if match[0].isdigit() or match[:2] in ["Z0", "Y0", "X0"]:
                    return match

        return None

    def extract_amount(self, text: str) -> Optional[float]:
        """
        Estrae importi monetari dal testo.

        Args:
            text: Testo da analizzare

        Returns:
            Importo se trovato, None altrimenti
        """
        # Pattern per importi in euro
        patterns = [
            r"€\s*([\d.,]+)",
            r"euro\s*([\d.,]+)",
            r"([\d.,]+)\s*€",
            r"([\d.,]+)\s*euro",
            r"importo[:\s]+([\d.,]+)",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    # Pulisci e converti
                    amount_str = match.group(1)
                    amount_str = amount_str.replace(".", "").replace(",", ".")
                    return float(amount_str)
                except ValueError:
                    continue

        return None

    def extract_date(self, text: str) -> Optional[str]:
        """
        Estrae date dal testo.

        Args:
            text: Testo da analizzare

        Returns:
            Data in formato ISO se trovata
        """
        # Pattern per date italiane
        patterns = [
            r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})",  # gg/mm/aaaa o gg-mm-aaaa
            r"(\d{1,2})\s+(gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|agosto|settembre|ottobre|novembre|dicembre)\s+(\d{4})",
        ]

        months = {
            "gennaio": 1,
            "febbraio": 2,
            "marzo": 3,
            "aprile": 4,
            "maggio": 5,
            "giugno": 6,
            "luglio": 7,
            "agosto": 8,
            "settembre": 9,
            "ottobre": 10,
            "novembre": 11,
            "dicembre": 12,
        }

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    if "/" in pattern or "-" in pattern:
                        day, month, year = match.groups()
                        return f"{year}-{month:02d}-{day:02d}"
                    else:
                        day, month_name, year = match.groups()
                        month = months.get(month_name.lower(), 0)
                        if month:
                            return f"{year}-{month:02d}-{int(day):02d}"
                except:
                    continue

        return None

    def extract_entity(self, text: str) -> Optional[str]:
        """
        Estrae nome dell'ente dal testo.

        Args:
            text: Testo da analizzare

        Returns:
            Nome ente se trovato
        """
        # Pattern per enti
        patterns = [
            r"comune\s+di\s+([^,\n]+)",
            r"provincia\s+di\s+([^,\n]+)",
            r"regione\s+([^,\n]+)",
            r"ministero\s+([^,\n]+)",
            r"azienda\s+([^,\n]+)",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return self.clean_text(match.group(1))

        return None

    def process_batch(self, texts: List[str]) -> pd.DataFrame:
        """
        Processa un batch di testi.

        Args:
            texts: Lista di testi da processare

        Returns:
            DataFrame con dati estratti
        """
        results = []

        for text in texts:
            if not text:
                continue

            # Estrai informazioni
            result = {
                "testo_originale": text[:500],  # Primi 500 caratteri
                "hash": self.hash_text(text),
                "cig": self.extract_cig(text),
                "importo": self.extract_amount(text),
                "data": self.extract_date(text),
                "ente": self.extract_entity(text),
                "lunghezza": len(text),
                "timestamp_analisi": datetime.now().isoformat(),
            }

            # Categorizzazione opzionale con GPT-5
            if self.use_ai and self._llm:
                try:
                    system = "Sei un esperto di appalti pubblici italiani. Restituisci solo JSON valido."
                    prompt = (
                        "Analizza il seguente testo di gara e restituisci un JSON con: "
                        "oggetto (max 200 char), categoria (Illuminazione/Videosorveglianza/Energia/Edifici/Mobilità/Smart City/Verde Pubblico/Strade/Impianti Sportivi/Servizi Pubblici/Emergenza/Acqua e Fognature/Rifiuti/Altro), "
                        "tipo_intervento (Fornitura/Lavori/Servizi/Manutenzione Ordinaria/Manutenzione Straordinaria/Riqualificazione/Efficientamento/Nuova Costruzione/Gestione/Altro), "
                        "tipo_energia (Elettrico/Termico/Fotovoltaico/Solare Termico/Eolico/Geotermico/LED/Tradizionale/Misto/Non Applicabile), "
                        "comune (se presente), smart_city (boolean), sostenibilita (boolean).\n\n"
                        f"TESTO: {text[:2000]}"
                    )
                    data_ai = self._llm.chat_json(
                        system_prompt=system, user_prompt=prompt, max_tokens=500
                    )
                    result.update(
                        {
                            "ai_oggetto": data_ai.get("oggetto", ""),
                            "ai_categoria": data_ai.get("categoria", ""),
                            "ai_tipo_intervento": data_ai.get("tipo_intervento", ""),
                            "ai_tipo_energia": data_ai.get("tipo_energia", ""),
                            "ai_comune": data_ai.get("comune", ""),
                            "ai_smart_city": data_ai.get("smart_city", False),
                            "ai_sostenibilita": data_ai.get("sostenibilita", False),
                        }
                    )
                except Exception as e:
                    logger.debug(f"Categorizzazione AI fallita: {e}")

            results.append(result)

        return pd.DataFrame(results)

    @timer
    def run(self) -> None:
        """
        Esegue l'analisi completa dei dati Gazzetta.
        """
        logger.info("=== ANALISI DATI GAZZETTA ===")
        logger.info(f"AI (GPT-5): {'ATTIVA' if self.use_ai else 'DISATTIVA'}")

        # Input file
        input_file = config.get_file_path(config.LOTTI_RAW, "temp")

        if not input_file.exists():
            logger.warning(f"File input non trovato: {input_file}")
            logger.info("Creando file di esempio...")

            # Crea DataFrame di esempio
            df = pd.DataFrame(
                {
                    "testo": [
                        "Gara per illuminazione pubblica CIG: 1234567890 importo € 100.000",
                        "Affidamento servizio videosorveglianza Comune di Milano",
                        "Manutenzione impianti termici CIG Z0A1234567",
                    ]
                }
            )
            df.to_excel(input_file, index=False)
            logger.info(f"File di esempio creato: {input_file}")

        # Leggi dati
        logger.info(f"Lettura file: {input_file}")
        df_input = pd.read_excel(input_file)

        if "testo" not in df_input.columns:
            logger.error("Colonna 'testo' non trovata nel file")
            return

        # Filtra testi validi
        texts = df_input["testo"].dropna().tolist()
        logger.info(f"Testi da analizzare: {len(texts)}")

        if not texts:
            logger.warning("Nessun testo da analizzare")
            return

        # Processa in batch con ThreadPoolExecutor
        batch_size = 100
        batches = [texts[i : i + batch_size] for i in range(0, len(texts), batch_size)]

        progress_logger.start_operation(
            "analyze_gazzetta", len(batches), "Analisi batch Gazzetta"
        )

        all_results = []

        with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
            futures = {
                executor.submit(self.process_batch, batch): i
                for i, batch in enumerate(batches)
            }

            for future in as_completed(futures):
                batch_idx = futures[future]
                try:
                    df_batch = future.result()
                    all_results.append(df_batch)
                    progress_logger.update("analyze_gazzetta")
                    logger.info(f"Batch {batch_idx+1}/{len(batches)} completato")
                except Exception as e:
                    logger.error(f"Errore batch {batch_idx}: {e}")
                    progress_logger.update("analyze_gazzetta", error=True)

        progress_logger.complete_operation("analyze_gazzetta")

        # Combina risultati
        if all_results:
            df_results = pd.concat(all_results, ignore_index=True)

            # Statistiche
            logger.info("=== STATISTICHE ANALISI ===")
            logger.info(f"Record totali: {len(df_results)}")
            logger.info(f"CIG trovati: {df_results['cig'].notna().sum()}")
            logger.info(f"Importi trovati: {df_results['importo'].notna().sum()}")
            logger.info(f"Date trovate: {df_results['data'].notna().sum()}")
            logger.info(f"Enti trovati: {df_results['ente'].notna().sum()}")

            # Salva risultati
            output_file = config.get_file_path(config.LOTTI_GAZZETTA, "output")
            df_results.to_excel(output_file, index=False)
            logger.info(f"✅ Risultati salvati: {output_file}")

            # Salva checkpoint
            self.checkpoint_manager.mark_completed(
                "gazzetta_analysis",
                {"records": len(df_results), "output_file": str(output_file)},
            )
        else:
            logger.warning("Nessun risultato prodotto")
