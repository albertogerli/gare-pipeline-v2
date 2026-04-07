"""
Processore per file JSON e OCDS.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from config.settings import config
from src.utils.checkpoint import CheckpointManager
from src.utils.performance import timer

logger = logging.getLogger(__name__)


class JsonProcessor:
    """
    Processa file JSON e OCDS per l'analisi gare.
    """

    def __init__(self):
        """Inizializza il processore."""
        self.checkpoint_manager = CheckpointManager()
        self.checkpoint_manager.create_session("json_processor")

    def process_ocds_file(self, file_path: Path) -> pd.DataFrame:
        """
        Processa un singolo file OCDS.

        Args:
            file_path: Path del file JSON

        Returns:
            DataFrame con dati estratti
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            records = []

            # Estrai releases (standard OCDS)
            releases = data.get("releases", [])

            for release in releases:
                record = {
                    "ocid": release.get("ocid"),
                    "id": release.get("id"),
                    "date": release.get("date"),
                    "tag": release.get("tag", []),
                    "initiationType": release.get("initiationType"),
                    "tender_id": release.get("tender", {}).get("id"),
                    "tender_title": release.get("tender", {}).get("title"),
                    "tender_description": release.get("tender", {}).get("description"),
                    "tender_status": release.get("tender", {}).get("status"),
                    "tender_value": release.get("tender", {})
                    .get("value", {})
                    .get("amount"),
                    "tender_currency": release.get("tender", {})
                    .get("value", {})
                    .get("currency"),
                    "buyer_name": release.get("buyer", {}).get("name"),
                    "buyer_id": release.get("buyer", {}).get("id"),
                }

                # Estrai informazioni sui lotti
                lots = release.get("tender", {}).get("lots", [])
                if lots:
                    for lot in lots:
                        lot_record = record.copy()
                        lot_record.update(
                            {
                                "lot_id": lot.get("id"),
                                "lot_title": lot.get("title"),
                                "lot_description": lot.get("description"),
                                "lot_value": lot.get("value", {}).get("amount"),
                            }
                        )
                        records.append(lot_record)
                else:
                    records.append(record)

            return pd.DataFrame(records)

        except Exception as e:
            logger.error(f"Errore processamento {file_path}: {e}")
            return pd.DataFrame()

    def process_cig_csv(self, file_path: Path) -> pd.DataFrame:
        """
        Processa file CSV CIG.

        Args:
            file_path: Path del file CSV

        Returns:
            DataFrame con dati CIG
        """
        try:
            # Leggi CSV con encoding italiano
            df = pd.read_csv(file_path, encoding="latin-1", sep=";", low_memory=False)

            # Rinomina colonne standard
            column_mapping = {
                "cig": "CIG",
                "oggetto_gara": "oggetto",
                "importo_aggiudicazione": "importo",
                "data_aggiudicazione": "data",
                "cf_amministrazione": "cf_ente",
                "denominazione_amministrazione": "ente",
            }

            df.rename(columns=column_mapping, inplace=True)

            # Converti tipi
            if "importo" in df.columns:
                df["importo"] = pd.to_numeric(df["importo"], errors="coerce")

            if "data" in df.columns:
                df["data"] = pd.to_datetime(df["data"], errors="coerce")

            return df

        except Exception as e:
            logger.error(f"Errore processamento CSV {file_path}: {e}")
            return pd.DataFrame()

    @timer
    def run(self) -> None:
        """
        Esegue il processamento di tutti i file JSON/OCDS.
        """
        logger.info("=== PROCESSAMENTO JSON/OCDS ===")

        all_data = []

        # Processa file OCDS
        ocds_files = list(config.OCDS_DIR.glob("*.json"))
        logger.info(f"File OCDS trovati: {len(ocds_files)}")

        for ocds_file in ocds_files:
            task_id = f"process_ocds_{ocds_file.name}"

            if self.checkpoint_manager.should_skip(task_id):
                logger.info(f"⏭️ Già processato: {ocds_file.name}")
                continue

            logger.info(f"Processamento: {ocds_file.name}")
            df = self.process_ocds_file(ocds_file)

            if not df.empty:
                all_data.append(df)
                self.checkpoint_manager.mark_completed(task_id, {"records": len(df)})
            else:
                self.checkpoint_manager.mark_failed(task_id, "Empty dataframe")

        # Processa file CSV CIG
        cig_files = list(config.CIG_DIR.glob("*.csv"))
        logger.info(f"File CIG CSV trovati: {len(cig_files)}")

        for cig_file in cig_files:
            task_id = f"process_cig_{cig_file.name}"

            if self.checkpoint_manager.should_skip(task_id):
                logger.info(f"⏭️ Già processato: {cig_file.name}")
                continue

            logger.info(f"Processamento: {cig_file.name}")
            df = self.process_cig_csv(cig_file)

            if not df.empty:
                all_data.append(df)
                self.checkpoint_manager.mark_completed(task_id, {"records": len(df)})

        # Combina tutti i dati
        if all_data:
            df_combined = pd.concat(all_data, ignore_index=True)

            # Rimuovi duplicati
            if "CIG" in df_combined.columns:
                df_combined.drop_duplicates(subset=["CIG"], keep="first", inplace=True)

            # Statistiche
            logger.info(f"Record totali: {len(df_combined)}")
            logger.info(f"Colonne: {list(df_combined.columns)}")

            # Salva risultato - usa CSV per file grandi, Excel per piccoli
            output_file = config.get_file_path(config.LOTTI_OCDS, "output")

            # Excel ha limite di 1,048,576 righe
            if len(df_combined) > 1000000:
                # Salva come CSV per file grandi
                csv_file = output_file.with_suffix(".csv")
                df_combined.to_csv(csv_file, index=False)
                logger.info(
                    f"✅ Dati salvati in CSV (troppo grande per Excel): {csv_file}"
                )
                logger.info(
                    f"📊 Dimensione: {len(df_combined):,} righe x {len(df_combined.columns)} colonne"
                )

                # Opzionale: salva un campione in Excel per preview
                sample_file = output_file.parent / f"{output_file.stem}_sample.xlsx"
                df_combined.head(100000).to_excel(sample_file, index=False)
                logger.info(f"✅ Campione (prime 100k righe) salvato: {sample_file}")
            else:
                # Usa Excel per file più piccoli
                df_combined.to_excel(output_file, index=False)
                logger.info(f"✅ Dati salvati: {output_file}")

            self.checkpoint_manager.mark_completed(
                "json_processing",
                {"total_records": len(df_combined), "output": str(output_file)},
            )
        else:
            logger.warning("Nessun dato processato")

            # Crea file vuoto di esempio
            df_empty = pd.DataFrame(
                {
                    "CIG": ["ESEMPIO001"],
                    "oggetto": ["Esempio gara"],
                    "importo": [10000.0],
                    "ente": ["Comune Esempio"],
                }
            )

            output_file = config.get_file_path(config.LOTTI_OCDS, "output")
            df_empty.to_excel(output_file, index=False)
            logger.info(f"📝 File esempio creato: {output_file}")
