"""
Analizzatore verbali completo.
"""

import gc
import glob
import logging
import os
from pathlib import Path

import pandas as pd

from config.settings import config
from src.utils.checkpoint import CheckpointManager
from src.utils.logging_config import ProgressLogger
from src.utils.performance import timer

logger = logging.getLogger(__name__)
progress_logger = ProgressLogger("verbali")


class VerbaliAnalyzer:
    """
    Analizza e processa i verbali di gara unendo dati CIG.
    """

    def __init__(self):
        """Inizializza l'analizzatore."""
        self.checkpoint_manager = CheckpointManager()
        self.checkpoint_manager.create_session("verbali_analyzer")
        self.dimensione_segmenti = 10**6

    def aggregate_cig_files(self) -> Path:
        """
        Aggrega tutti i file CIG CSV in un unico file.

        Returns:
            Path del file CIG unificato
        """
        cig_unico = config.get_file_path(config.FILE_CIG, "temp")

        # Reset file if exists
        if cig_unico.exists():
            cig_unico.unlink()

        csv_files = list(config.CIG_DIR.glob("*.csv"))
        logger.info(f"File CSV CIG trovati: {len(csv_files)}")

        if not csv_files:
            # Crea file esempio se non ci sono CSV
            df_example = pd.DataFrame(
                {
                    "cig": ["ABC123", "DEF456"],
                    "oggetto_gara": ["Illuminazione", "Manutenzione"],
                    "denominazione_amministrazione_appaltante": [
                        "COMUNE DI MILANO",
                        "COMUNE DI ROMA",
                    ],
                    "sezione_regionale": [
                        "SEZIONE REGIONALE LOMBARDIA",
                        "SEZIONE REGIONALE LAZIO",
                    ],
                }
            )
            df_example.to_csv(cig_unico, sep=";", index=False)
            logger.info(f"📝 File CIG esempio creato: {cig_unico}")
            return cig_unico

        # Aggrega file CIG
        progress_logger.start_operation(
            "aggregate_cig", len(csv_files), "Aggregazione CIG"
        )

        for csv_path in csv_files:
            try:
                for chunk in pd.read_csv(
                    csv_path,
                    sep=";",
                    chunksize=self.dimensione_segmenti,
                    on_bad_lines="skip",
                    low_memory=False,
                ):
                    chunk.to_csv(
                        cig_unico,
                        mode="a",
                        index=False,
                        sep=";",
                        header=not cig_unico.exists(),
                    )
                    del chunk
                    gc.collect()

                progress_logger.update("aggregate_cig")
                logger.info(f"✅ Aggregato: {csv_path.name}")

            except Exception as e:
                logger.error(f"Errore aggregazione {csv_path}: {e}")
                progress_logger.update("aggregate_cig", error=True)

        progress_logger.complete_operation("aggregate_cig")
        return cig_unico

    @timer
    def run(self) -> None:
        """
        Esegue l'analisi completa dei verbali.
        """
        logger.info("=== ANALISI VERBALI ===")

        # Aggrega file CIG
        cig_unico = self.aggregate_cig_files()

        # Carica lotti merged
        lotti_file = config.get_file_path(config.LOTTI_MERGED, "output")
        if not lotti_file.exists():
            logger.warning(f"File lotti non trovato: {lotti_file}")
            # Crea file esempio
            df_lotti = pd.DataFrame(
                {
                    "CIG": ["ABC123", "DEF456", "GHI789", None],
                    "Oggetto": ["Gara 1", "Gara 2", "Gara 3", "Gara 4"],
                    "Importo": [10000, 20000, 30000, 40000],
                }
            )
        else:
            df_lotti = pd.read_excel(lotti_file)
            logger.info(f"Lotti caricati: {len(df_lotti)} record")

        # Processa CIG in chunk e fai merge
        logger.info("Merge con dati CIG...")
        matched_df = pd.DataFrame()
        cigs_matched = set()

        try:
            cigar_chunks = pd.read_csv(
                cig_unico,
                sep=";",
                chunksize=self.dimensione_segmenti,
                on_bad_lines="skip",
                low_memory=False,
            )

            for chunk_idx, chunk in enumerate(cigar_chunks):
                # Rinomina colonna cig
                chunk.rename(columns={"cig": "CIG"}, inplace=True)
                chunk.drop_duplicates(subset=["CIG"], inplace=True)

                # Inner join con lotti
                temp_merge = pd.merge(df_lotti, chunk, on="CIG", how="inner")
                cigs_matched.update(temp_merge["CIG"].dropna().unique())
                matched_df = pd.concat([matched_df, temp_merge], ignore_index=True)

                logger.debug(f"Chunk {chunk_idx+1} processato: {len(temp_merge)} match")

        except Exception as e:
            logger.error(f"Errore processing CIG chunks: {e}")

        # Gestisci record non matchati
        nan_df = df_lotti[df_lotti["CIG"].isna()]
        unmatched_df = df_lotti[
            df_lotti["CIG"].notna() & ~df_lotti["CIG"].isin(cigs_matched)
        ]

        logger.info(f"CIG matchati: {len(matched_df)}")
        logger.info(f"CIG mancanti: {len(nan_df)}")
        logger.info(f"CIG non trovati: {len(unmatched_df)}")

        # Combina tutto
        union_df = pd.concat([matched_df, nan_df, unmatched_df], ignore_index=True)

        # Merge con aggiudicazioni e aggiudicatari
        aggiud_file = config.DATA_DIR / config.AGGIUDICAZIONI
        aggiudicatari_file = config.DATA_DIR / config.AGGIUDICATARI

        if aggiud_file.exists():
            df_aggiud = pd.read_csv(aggiud_file, sep=";", on_bad_lines="skip")
            df_aggiud.rename(columns={"cig": "CIG"}, inplace=True)
            union_df = pd.merge(union_df, df_aggiud, on="CIG", how="left")
            logger.info(f"✅ Merge con aggiudicazioni: {len(df_aggiud)} record")

        if aggiudicatari_file.exists():
            df_aggiudicatari = pd.read_csv(
                aggiudicatari_file, sep=";", on_bad_lines="skip"
            )
            df_aggiudicatari.rename(columns={"cig": "CIG"}, inplace=True)
            union_df = pd.merge(union_df, df_aggiudicatari, on="CIG", how="left")
            logger.info(f"✅ Merge con aggiudicatari: {len(df_aggiudicatari)} record")

        # Pulizia testi
        if "denominazione_amministrazione_appaltante" in union_df.columns:
            union_df["denominazione_amministrazione_appaltante"] = (
                union_df["denominazione_amministrazione_appaltante"]
                .str.replace("^COMUNE DI ", "", regex=True)
                .str.capitalize()
            )

        if "sezione_regionale" in union_df.columns:
            union_df["sezione_regionale"] = (
                union_df["sezione_regionale"]
                .str.replace("^SEZIONE REGIONALE ", "", regex=True)
                .str.capitalize()
            )

        # Formattazione date
        date_columns = [col for col in union_df.columns if "data" in col.lower()]
        for col in date_columns:
            try:
                union_df[col] = pd.to_datetime(
                    union_df[col], errors="coerce"
                ).dt.strftime("%d/%m/%Y")
            except:
                pass

        # Statistiche finali
        logger.info("=== STATISTICHE VERBALI ===")
        logger.info(f"Record totali: {len(union_df)}")
        logger.info(f"Colonne: {len(union_df.columns)}")

        if "CIG" in union_df.columns:
            logger.info(f"CIG validi: {union_df['CIG'].notna().sum()}")

        # Salva risultato
        output_file = config.get_file_path(config.VERBALI, "output")
        union_df.to_excel(output_file, index=False)
        logger.info(f"✅ Verbali salvati: {output_file}")

        # Checkpoint finale
        self.checkpoint_manager.mark_completed(
            "verbali_analysis",
            {
                "total_records": len(union_df),
                "matched_cigs": len(cigs_matched),
                "output_file": str(output_file),
            },
        )
