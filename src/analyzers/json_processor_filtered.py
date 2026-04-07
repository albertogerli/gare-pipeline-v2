"""
Processore JSON/OCDS con filtro per categorie rilevanti.
"""

import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from config.settings import config
from src.utils.checkpoint import CheckpointManager
from src.utils.performance import timer

logger = logging.getLogger(__name__)


def applica_filtro_categoria(testo: str) -> bool:
    """
    Applica filtri bilanciati per gare rilevanti di infrastrutture pubbliche.

    Args:
        testo: Testo da verificare (titolo, descrizione, oggetto)

    Returns:
        True se passa il filtro, False altrimenti
    """
    if not testo:
        return False

    testo_lower = str(testo).lower()

    # 1. ILLUMINAZIONE PUBBLICA (più specifico)
    if re.search(
        r"illuminazion[ei] pubblic|lampioni|pubblica illuminazione|impianti di illuminazione|corpi illuminanti",
        testo_lower,
    ):
        return True

    # 2. VIDEOSORVEGLIANZA (specifico)
    if re.search(
        r"videosorveglian|telecamer[ae]|tvcc|sistema.{0,20}sorveglian", testo_lower
    ):
        return True

    # 3. EFFICIENTAMENTO ENERGETICO (più specifico)
    if re.search(
        r"efficientamento energetic|riqualificazione energetic|risparmio energetic",
        testo_lower,
    ) or (
        re.search(r"impiant[oi]", testo_lower)
        and re.search(r"fotovoltaic|solare|led|termic", testo_lower)
    ):
        return True

    # 4. EDIFICI PUBBLICI + ENERGIA/IMPIANTI (combinato)
    if re.search(
        r"scuol[ae]|municipio|palazzo comunale|biblioteca|ospedale", testo_lower
    ) and re.search(
        r"impiant[oi]|manutenzion|ristrutturazion|adeguament|climatizzazion|riscaldament",
        testo_lower,
    ):
        return True

    # 5. MOBILITÀ ELETTRICA (specifico)
    if re.search(
        r"colonnin[ae].{0,20}ricaric|ricarica.{0,20}elettric|stazion.{0,20}ricaric|e-mobility",
        testo_lower,
    ):
        return True

    # 6. PARCHEGGI CON GESTIONE/TECNOLOGIA
    if re.search(r"parchegg[io]", testo_lower) and re.search(
        r"gestion|parcometr|parchimetr|automat|smart|sensor", testo_lower
    ):
        return True

    # 7. SMART CITY (più specifico)
    if re.search(r"smart city|città intelligente", testo_lower) or (
        re.search(r"sensor[ei]|iot|telecontroll|telegestione", testo_lower)
        and re.search(r"pubblic|urban|città|comune", testo_lower)
    ):
        return True

    # 8. VERDE PUBBLICO CON IMPIANTI
    if re.search(r"verde pubblic|parchi|giardini", testo_lower) and re.search(
        r"irrigazion|impiant|illuminazion|manutenzion", testo_lower
    ):
        return True

    # 9. STRADE + ILLUMINAZIONE/SEGNALETICA
    if re.search(r"strad[ae]|viabilità", testo_lower) and re.search(
        r"illuminazion|segnaletic|semafori|asfalto|manutenzion", testo_lower
    ):
        return True

    # 10. IMPIANTI SPORTIVI (specifico)
    if re.search(
        r"impianti sportiv|palestra|piscina|campo sportiv|palazzetto", testo_lower
    ) and re.search(r"manutenzion|ristrutturazion|impiant|illuminazion", testo_lower):
        return True

    # 11. GLOBAL SERVICE/FACILITY (specifico per edifici pubblici)
    if re.search(
        r"global service|facility management|gestione integrata", testo_lower
    ) and re.search(r"edifici|immobili|pubblic|comunal", testo_lower):
        return True

    # 12. GALLERIE/TUNNEL CON IMPIANTI
    if (
        re.search(r"galleri[ae]|tunnel", testo_lower)
        and re.search(r"impiant[oi]|illuminazion|ventilazion|sicurezza", testo_lower)
        and not re.search(r"museo|arte|mostra", testo_lower)
    ):
        return True

    # 13. RETI IDRICHE/FOGNATURE (più specifico)
    if re.search(r"acquedott|rete idric|fognatur|depurator", testo_lower) and re.search(
        r"manutenzion|gestion|lavori|riparazion", testo_lower
    ):
        return True

    # 14. PUBBLICA ILLUMINAZIONE LED
    if re.search(r"\bled\b", testo_lower) and re.search(
        r"pubblic|strad|comunal|illuminazion", testo_lower
    ):
        return True

    # 15. IMPIANTI TERMICI/CLIMATIZZAZIONE EDIFICI PUBBLICI
    if re.search(
        r"termic|climatizzazion|condizionament|caldai", testo_lower
    ) and re.search(r"edifici pubblic|scuol|comunal|municipal", testo_lower):
        return True

    return False


class JsonProcessorFiltered:
    """
    Processa file JSON/OCDS applicando il filtro categorie.
    """

    def __init__(self):
        """Inizializza il processore."""
        self.checkpoint_manager = CheckpointManager()
        self.checkpoint_manager.create_session("json_processor_filtered")

    def process_ocds_file(self, file_path: Path) -> pd.DataFrame:
        """
        Processa un singolo file OCDS applicando il filtro.

        Args:
            file_path: Path del file OCDS

        Returns:
            DataFrame con i dati processati e filtrati
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            records = []
            records_filtered = 0
            records_total = 0

            # Estrai releases
            releases = data.get("releases", [])

            for release in releases:
                records_total += 1

                # Estrai campi principali
                tender = release.get("tender", {})
                tender_title = tender.get("title", "")
                tender_description = tender.get("description", "")

                # Combina testi per il filtro
                testo_completo = f"{tender_title} {tender_description}"

                # Applica filtro
                if not applica_filtro_categoria(testo_completo):
                    continue

                records_filtered += 1

                # Crea record base
                record = {
                    "id": release.get("id"),
                    "date": release.get("date"),
                    "tag": release.get("tag", []),
                    "initiationType": release.get("initiationType"),
                    "tender_id": tender.get("id"),
                    "tender_title": tender_title,
                    "tender_description": tender_description,
                    "tender_status": tender.get("status"),
                    "tender_value": tender.get("value", {}).get("amount"),
                    "tender_currency": tender.get("value", {}).get("currency"),
                    "buyer_name": release.get("buyer", {}).get("name"),
                    "buyer_id": release.get("buyer", {}).get("id"),
                }

                # Estrai informazioni sui lotti
                lots = tender.get("lots", [])
                if lots:
                    for lot in lots:
                        lot_title = lot.get("title", "")
                        lot_description = lot.get("description", "")
                        lot_text = f"{lot_title} {lot_description}"

                        # Applica filtro anche ai lotti
                        if applica_filtro_categoria(lot_text):
                            lot_record = record.copy()
                            lot_record.update(
                                {
                                    "lot_id": lot.get("id"),
                                    "lot_title": lot_title,
                                    "lot_description": lot_description,
                                    "lot_value": lot.get("value", {}).get("amount"),
                                }
                            )
                            records.append(lot_record)
                else:
                    records.append(record)

            logger.info(
                f"OCDS {file_path.name}: {records_filtered}/{records_total} passano il filtro"
            )

            return pd.DataFrame(records)

        except Exception as e:
            logger.error(f"Errore processamento OCDS {file_path}: {e}")
            return pd.DataFrame()

    def process_cig_csv(self, file_path: Path) -> pd.DataFrame:
        """
        Processa file CSV CIG applicando il filtro.

        Args:
            file_path: Path del file CSV

        Returns:
            DataFrame con i dati processati e filtrati
        """
        try:
            # Leggi CSV in chunk per file grandi
            chunks = []
            total_rows = 0
            filtered_rows = 0

            for chunk in pd.read_csv(
                file_path,
                sep=";",
                chunksize=10000,
                on_bad_lines="skip",
                low_memory=False,
            ):
                total_rows += len(chunk)

                # Applica filtro su colonne rilevanti
                mask = pd.Series([False] * len(chunk))

                # Controlla oggetto_gara
                if "oggetto_gara" in chunk.columns:
                    mask |= chunk["oggetto_gara"].apply(
                        lambda x: applica_filtro_categoria(str(x))
                    )

                # Controlla oggetto_lotto
                if "oggetto_lotto" in chunk.columns:
                    mask |= chunk["oggetto_lotto"].apply(
                        lambda x: applica_filtro_categoria(str(x))
                    )

                # Controlla oggetto_principale_contratto
                if "oggetto_principale_contratto" in chunk.columns:
                    mask |= chunk["oggetto_principale_contratto"].apply(
                        lambda x: applica_filtro_categoria(str(x))
                    )

                filtered_chunk = chunk[mask]
                filtered_rows += len(filtered_chunk)

                if not filtered_chunk.empty:
                    chunks.append(filtered_chunk)

            logger.info(
                f"CIG {file_path.name}: {filtered_rows}/{total_rows} passano il filtro"
            )

            if chunks:
                df = pd.concat(chunks, ignore_index=True)

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
            else:
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"Errore processamento CSV {file_path}: {e}")
            return pd.DataFrame()

    @timer
    def run(self) -> None:
        """
        Esegue il processamento di tutti i file JSON/OCDS con filtro.
        """
        logger.info("=== PROCESSAMENTO JSON/OCDS CON FILTRO AMPIO ===")
        logger.info("Filtri ampliati per catturare più tipologie di gare rilevanti:")
        logger.info("  - Illuminazione e LED")
        logger.info("  - Videosorveglianza e sicurezza")
        logger.info("  - Energia ed efficientamento")
        logger.info("  - Edifici e manutenzioni")
        logger.info("  - Mobilità elettrica e sostenibile")
        logger.info("  - Smart city e tecnologie")
        logger.info("  - Verde pubblico e ambiente")
        logger.info("  - Strade e infrastrutture")
        logger.info("  - Servizi pubblici e facility management")
        logger.info("  - E molti altri...")

        all_data = []
        total_filtered = 0
        total_processed = 0

        # Processa file OCDS
        ocds_files = list(config.OCDS_DIR.glob("*.json"))
        logger.info(f"File OCDS trovati: {len(ocds_files)}")

        for ocds_file in ocds_files:
            task_id = f"process_ocds_filtered_{ocds_file.name}"

            if self.checkpoint_manager.should_skip(task_id):
                logger.info(f"⏭️ Già processato: {ocds_file.name}")
                continue

            logger.info(f"Processamento con filtro: {ocds_file.name}")
            df = self.process_ocds_file(ocds_file)

            if not df.empty:
                all_data.append(df)
                total_filtered += len(df)
                self.checkpoint_manager.mark_completed(task_id, {"records": len(df)})
            else:
                self.checkpoint_manager.mark_completed(task_id, {"records": 0})

        # Processa file CSV CIG (solo se non c'è già il file aggregato)
        cig_aggregato = config.get_file_path(config.FILE_CIG, "temp")

        if cig_aggregato.exists():
            # Usa file aggregato
            logger.info(f"Uso file CIG aggregato: {cig_aggregato}")
            df = self.process_cig_csv(cig_aggregato)
            if not df.empty:
                all_data.append(df)
                total_filtered += len(df)
        else:
            # Processa file singoli
            cig_files = list(config.CIG_DIR.glob("*.csv"))[:5]  # Limita per test
            logger.info(
                f"File CIG CSV trovati: {len(cig_files)} (limitati a 5 per test)"
            )

            for cig_file in cig_files:
                task_id = f"process_cig_filtered_{cig_file.name}"

                if self.checkpoint_manager.should_skip(task_id):
                    logger.info(f"⏭️ Già processato: {cig_file.name}")
                    continue

                logger.info(f"Processamento con filtro: {cig_file.name}")
                df = self.process_cig_csv(cig_file)

                if not df.empty:
                    all_data.append(df)
                    total_filtered += len(df)
                    self.checkpoint_manager.mark_completed(
                        task_id, {"records": len(df)}
                    )

        # Combina tutti i dati
        if all_data:
            df_combined = pd.concat(all_data, ignore_index=True)

            # Rimuovi duplicati
            if "CIG" in df_combined.columns:
                df_combined.drop_duplicates(subset=["CIG"], keep="first", inplace=True)

            # Statistiche
            logger.info("=== STATISTICHE FINALI ===")
            logger.info(f"Record totali dopo filtro: {len(df_combined)}")
            logger.info(f"Colonne: {len(df_combined.columns)}")

            # Salva risultato
            output_file = config.get_file_path(config.LOTTI_OCDS, "output")

            if len(df_combined) > 1000000:
                # CSV per file grandi
                csv_file = output_file.with_suffix(".csv")
                df_combined.to_csv(csv_file, index=False)
                logger.info(f"✅ Dati salvati in CSV: {csv_file}")

                # Sample Excel
                sample_file = output_file.parent / f"{output_file.stem}_sample.xlsx"
                df_combined.head(100000).to_excel(sample_file, index=False)
                logger.info(f"✅ Campione salvato: {sample_file}")
            else:
                df_combined.to_excel(output_file, index=False)
                logger.info(f"✅ Dati salvati: {output_file}")

            self.checkpoint_manager.mark_completed(
                "json_processing_filtered",
                {"total_records": len(df_combined), "output": str(output_file)},
            )
        else:
            logger.warning("Nessun dato passa il filtro!")

            # Crea file esempio
            df_example = pd.DataFrame(
                [
                    {
                        "CIG": "EXAMPLE001",
                        "oggetto": "Illuminazione pubblica esempio",
                        "importo": 100000,
                        "data": pd.Timestamp.now(),
                        "ente": "Comune Esempio",
                    }
                ]
            )

            output_file = config.get_file_path(config.LOTTI_OCDS, "output")
            df_example.to_excel(output_file, index=False)
            logger.info(f"📁 File esempio creato: {output_file}")
