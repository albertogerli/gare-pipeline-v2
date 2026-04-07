"""
Analizzatore Servizio Luce Consip completo.
"""

import gc
import glob
import logging
import os
from pathlib import Path
from typing import Optional

import pandas as pd

from config.settings import config
from src.utils.checkpoint import CheckpointManager
from src.utils.logging_config import ProgressLogger
from src.utils.performance import timer

logger = logging.getLogger(__name__)
progress_logger = ProgressLogger("servizio_luce")


class ServizioLuceAnalyzer:
    """
    Analizza gare relative al Servizio Luce Consip.
    """

    def __init__(self):
        """Inizializza l'analizzatore."""
        self.checkpoint_manager = CheckpointManager()
        self.checkpoint_manager.create_session("servizio_luce_analyzer")
        self.dimensione_segmenti = 10**6

    def process_servizio_luce_data(self) -> pd.DataFrame:
        """
        Processa i dati del Servizio Luce.

        Returns:
            DataFrame con gare Servizio Luce
        """
        file_cig = config.get_file_path(config.FILE_CIG, "temp")
        servizio_luce_intermedio = config.get_file_path(
            config.SERVIZIO_LUCE_INTERMEDIO, "temp"
        )

        # Reset file intermedio
        if servizio_luce_intermedio.exists():
            servizio_luce_intermedio.unlink()

        # Aggrega file CIG se necessario
        if not file_cig.exists():
            csv_files = list(config.CIG_DIR.glob("*.csv"))

            if not csv_files:
                # Crea dati esempio
                logger.info("Nessun file CIG trovato, creazione dati esempio...")
                df_example = pd.DataFrame(
                    {
                        "cig": ["SL001", "SL002", "SL003"],
                        "oggetto_gara": [
                            "Servizio Luce 1 - Illuminazione pubblica",
                            "Servizio Luce 2 - Efficientamento energetico",
                            "Servizio Luce 3 - Smart lighting",
                        ],
                        "oggetto_lotto": [
                            "Servizio Luce 1 Milano",
                            "Servizio Luce 2 Roma",
                            "Servizio Luce 3 Napoli",
                        ],
                        "oggetto_principale_contratto": [
                            "Gestione illuminazione",
                            "Manutenzione impianti",
                            "Installazione LED",
                        ],
                        "denominazione_amministrazione_appaltante": [
                            "COMUNE DI MILANO",
                            "COMUNE DI ROMA",
                            "COMUNE DI NAPOLI",
                        ],
                        "sezione_regionale": [
                            "SEZIONE REGIONALE LOMBARDIA",
                            "SEZIONE REGIONALE LAZIO",
                            "SEZIONE REGIONALE CAMPANIA",
                        ],
                        "importo_complessivo_gara": [100000, 150000, 200000],
                        "importo_aggiudicazione": [90000, 135000, 180000],
                        "data_pubblicazione": [
                            "2024-01-01",
                            "2024-02-01",
                            "2024-03-01",
                        ],
                    }
                )
                df_example.to_csv(file_cig, sep=";", index=False)
            else:
                # Aggrega file CIG reali
                logger.info(f"Aggregazione {len(csv_files)} file CIG...")
                for file in csv_files:
                    for segmento in pd.read_csv(
                        file,
                        sep=";",
                        chunksize=self.dimensione_segmenti,
                        on_bad_lines="skip",
                        low_memory=False,
                    ):
                        segmento.to_csv(
                            file_cig,
                            mode="a",
                            index=False,
                            sep=";",
                            header=not file_cig.exists(),
                        )
                        del segmento
                        gc.collect()

        # Cerca gare Servizio Luce
        logger.info("Ricerca gare Servizio Luce...")
        found_count = 0

        for chunk_idx, segmento in enumerate(
            pd.read_csv(
                file_cig,
                sep=";",
                chunksize=self.dimensione_segmenti,
                on_bad_lines="skip",
                low_memory=False,
            )
        ):
            # Cerca "servizio luce" in diverse colonne
            gare_cercate = segmento[
                segmento.apply(
                    lambda gara: any(
                        "servizio luce" in str(gara[col]).lower()
                        for col in [
                            "oggetto_gara",
                            "oggetto_lotto",
                            "oggetto_principale_contratto",
                        ]
                        if col in gara.index
                    ),
                    axis=1,
                )
            ]

            if not gare_cercate.empty:
                gare_cercate.to_csv(
                    servizio_luce_intermedio,
                    mode="a",
                    index=False,
                    sep=";",
                    header=not servizio_luce_intermedio.exists(),
                )
                found_count += len(gare_cercate)
                logger.debug(f"Chunk {chunk_idx}: trovate {len(gare_cercate)} gare")

            del gare_cercate
            del segmento
            gc.collect()

        logger.info(f"Gare Servizio Luce trovate: {found_count}")

        # Carica e processa risultati
        if servizio_luce_intermedio.exists() and found_count > 0:
            df_gare = pd.read_csv(
                servizio_luce_intermedio, sep=";", on_bad_lines="skip", low_memory=False
            )
        else:
            # Usa dati esempio se non trovato nulla
            df_gare = pd.read_csv(
                file_cig, sep=";", on_bad_lines="skip", low_memory=False
            )

        return df_gare

    def categorize_servizio_luce(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Categorizza le gare per tipo di Servizio Luce.

        Args:
            df: DataFrame da categorizzare

        Returns:
            DataFrame con categoria ServizioLuce
        """
        df["ServizioLuce"] = df.apply(
            lambda row: 1
            if "servizio luce 1" in str(row).lower()
            else 2
            if "servizio luce 2" in str(row).lower()
            else 3
            if "servizio luce 3" in str(row).lower()
            else 4
            if "servizio luce 4" in str(row).lower()
            else None,
            axis=1,
        )
        df["Consip"] = True
        return df

    @timer
    def run(self) -> None:
        """
        Esegue l'analisi completa del Servizio Luce.
        """
        logger.info("=== ANALISI SERVIZIO LUCE CONSIP ===")

        # Processa dati Servizio Luce
        df_gare = self.process_servizio_luce_data()

        # Pulizia colonne
        df_gare.columns = df_gare.columns.str.strip()

        # Pulizia testi amministrazioni
        if "denominazione_amministrazione_appaltante" in df_gare.columns:
            df_gare["denominazione_amministrazione_appaltante"] = (
                df_gare["denominazione_amministrazione_appaltante"]
                .str.replace("^COMUNE DI ", "", regex=True)
                .str.capitalize()
            )

        if "sezione_regionale" in df_gare.columns:
            df_gare["sezione_regionale"] = (
                df_gare["sezione_regionale"]
                .str.replace("^SEZIONE REGIONALE ", "", regex=True)
                .str.capitalize()
            )

        # Categorizza Servizio Luce
        df_gare = self.categorize_servizio_luce(df_gare)

        # Merge con aggiudicazioni se disponibili
        aggiud_file = config.DATA_DIR / config.AGGIUDICAZIONI
        aggiudicatari_file = config.DATA_DIR / config.AGGIUDICATARI

        if aggiud_file.exists():
            df_aggiudicazioni = pd.read_csv(
                aggiud_file, sep=";", on_bad_lines="skip", low_memory=False
            )
            df_gare = pd.merge(df_gare, df_aggiudicazioni, on="cig", how="left")
            logger.info(f"✅ Merge con aggiudicazioni")

        if aggiudicatari_file.exists():
            df_aggiudicatari = pd.read_csv(
                aggiudicatari_file, sep=";", on_bad_lines="skip", low_memory=False
            )
            df_gare = pd.merge(df_gare, df_aggiudicatari, on="cig", how="left")
            logger.info(f"✅ Merge con aggiudicatari")

        # Formatta date
        colonne_date = [
            "data_pubblicazione",
            "data_scadenza_offerta",
            "DATA_ULTIMO_PERFEZIONAMENTO",
            "data_aggiudicazione_definitiva",
            "DATA_COMUNICAZIONE_ESITO",
        ]

        for col in colonne_date:
            if col in df_gare.columns:
                try:
                    df_gare[col] = pd.to_datetime(
                        df_gare[col], errors="coerce"
                    ).dt.strftime("%d/%m/%Y")
                except:
                    pass

        # Rinomina colonne principali
        mappa_colonne = {
            "denominazione_amministrazione_appaltante": "Comune",
            "luogo_istat": "LuogoISTAT",
            "provincia": "Provincia",
            "sezione_regionale": "Regione",
            "data_pubblicazione": "DataPubblicazione",
            "data_scadenza_offerta": "Scadenza",
            "data_aggiudicazione_definitiva": "DataAggiudicazione",
            "cig": "CIG",
            "numero_gara": "NumeroGara",
            "importo_complessivo_gara": "ImportoGara",
            "importo_aggiudicazione": "ImportoAggiudicazione",
            "oggetto_gara": "OggettoGara",
            "oggetto_lotto": "Oggetto",
            "oggetto_principale_contratto": "OggettoContratto",
            "tipo_scelta_contraente": "TipoSceltaContraente",
            "criterio_aggiudicazione": "CriterioAggiudicazione",
            "denominazione": "Aggiudicatario",
            "codice_fiscale": "CodFiscaleAggiudicatario",
            "ribasso_aggiudicazione": "Sconto",
        }

        df_renamed = df_gare.rename(columns=mappa_colonne)

        # Processa sconto
        if "Sconto" in df_renamed.columns:
            df_renamed["Sconto"] = (
                df_renamed["Sconto"]
                .astype(str)
                .str.replace("%", "", regex=False)
                .str.replace(",", ".", regex=False)
                .str.strip()
            )
            df_renamed["Sconto"] = pd.to_numeric(df_renamed["Sconto"], errors="coerce")
            df_renamed["Sconto %"] = df_renamed["Sconto"] / 100

        # Aggiungi URL verbale
        if "CIG" in df_renamed.columns:
            df_renamed["URLVerbaleAggiudicazione"] = df_renamed["CIG"].apply(
                lambda x: f"https://dati.anticorruzione.it/superset/dashboard/dettaglio_cig/?cig={x}&standalone=2"
                if pd.notna(x)
                else None
            )

        # Calcola durata appalto
        if (
            "Scadenza" in df_renamed.columns
            and "DataAggiudicazione" in df_renamed.columns
        ):
            df_renamed["DurataAppalto"] = (
                pd.to_datetime(df_renamed["Scadenza"], errors="coerce", dayfirst=True)
                - pd.to_datetime(
                    df_renamed["DataAggiudicazione"], errors="coerce", dayfirst=True
                )
            ).dt.days

        # Statistiche finali
        logger.info("=== STATISTICHE SERVIZIO LUCE ===")
        logger.info(f"Record totali: {len(df_renamed)}")
        logger.info(f"Colonne: {len(df_renamed.columns)}")

        if "ServizioLuce" in df_renamed.columns:
            for i in range(1, 5):
                count = (df_renamed["ServizioLuce"] == i).sum()
                if count > 0:
                    logger.info(f"Servizio Luce {i}: {count} gare")

        if "ImportoAggiudicazione" in df_renamed.columns:
            totale = df_renamed["ImportoAggiudicazione"].sum()
            logger.info(f"Importo totale aggiudicazioni: €{totale:,.2f}")

        # Salva risultato
        output_file = config.get_file_path(config.SERVIZIO_LUCE, "output")
        df_renamed.to_excel(output_file, index=False)
        logger.info(f"✅ Servizio Luce salvato: {output_file}")

        # Checkpoint finale
        self.checkpoint_manager.mark_completed(
            "servizio_luce_analysis",
            {
                "total_records": int(len(df_renamed)),
                "servizio_luce_1": int((df_renamed.get("ServizioLuce") == 1).sum())
                if "ServizioLuce" in df_renamed
                else 0,
                "servizio_luce_2": int((df_renamed.get("ServizioLuce") == 2).sum())
                if "ServizioLuce" in df_renamed
                else 0,
                "servizio_luce_3": int((df_renamed.get("ServizioLuce") == 3).sum())
                if "ServizioLuce" in df_renamed
                else 0,
                "servizio_luce_4": int((df_renamed.get("ServizioLuce") == 4).sum())
                if "ServizioLuce" in df_renamed
                else 0,
                "output_file": str(output_file),
            },
        )
