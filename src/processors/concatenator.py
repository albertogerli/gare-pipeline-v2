"""
Concatenatore per unire i dati da diverse fonti.
"""

import logging
from pathlib import Path

import pandas as pd

from config.settings import config
from src.utils.performance import timer

logger = logging.getLogger(__name__)


class Concatenator:
    """
    Concatena e unisce dati da diverse fonti.
    """

    @timer
    def concat_lotti(self) -> None:
        """
        Concatena i file dei lotti da diverse fonti.
        """
        logger.info("=== CONCATENAZIONE LOTTI ===")

        all_data = []

        # File da concatenare
        files_to_concat = [
            config.get_file_path(config.LOTTI_GAZZETTA, "output"),
            config.get_file_path(config.LOTTI_OCDS, "output"),
        ]

        for file_path in files_to_concat:
            if file_path.exists():
                logger.info(f"Lettura: {file_path.name}")
                df = pd.read_excel(file_path)
                all_data.append(df)
                logger.info(f"  Record: {len(df)}")
            else:
                logger.warning(f"File non trovato: {file_path}")

        if all_data:
            # Concatena tutti i DataFrame
            df_merged = pd.concat(all_data, ignore_index=True)

            # Rimuovi duplicati se c'è colonna CIG
            if "CIG" in df_merged.columns:
                before = len(df_merged)
                df_merged.drop_duplicates(subset=["CIG"], keep="first", inplace=True)
                after = len(df_merged)
                logger.info(f"Duplicati rimossi: {before - after}")

            # Salva risultato
            output_file = config.get_file_path(config.LOTTI_MERGED, "output")
            df_merged.to_excel(output_file, index=False)
            logger.info(f"✅ Lotti concatenati: {output_file} ({len(df_merged)} record)")
        else:
            logger.warning("Nessun dato da concatenare")

    @timer
    def concat_all(self) -> None:
        """
        Concatena tutti i file finali per creare il dataset completo.
        """
        logger.info("=== CONCATENAZIONE FINALE ===")

        all_data = []

        # File da includere nella concatenazione finale
        final_files = [
            config.get_file_path(config.LOTTI_MERGED, "output"),
            config.get_file_path(config.VERBALI, "output"),
            config.get_file_path(config.SERVIZIO_LUCE, "output"),
        ]

        for file_path in final_files:
            if file_path.exists():
                logger.info(f"Lettura: {file_path.name}")
                df = pd.read_excel(file_path)
                all_data.append(df)
                logger.info(f"  Record: {len(df)}")
            else:
                logger.warning(f"File non trovato: {file_path}")

        if all_data:
            # Concatena con outer join per mantenere tutte le colonne
            df_final = pd.concat(all_data, ignore_index=True, sort=False)

            # Ordina per CIG se presente
            if "CIG" in df_final.columns:
                df_final.sort_values("CIG", inplace=True)

            # Salva risultato finale
            output_file = config.get_file_path(config.FINAL, "output")
            df_final.to_excel(output_file, index=False)
            logger.info(f"✅ Dataset finale: {output_file}")
            logger.info(f"  Record totali: {len(df_final)}")
            logger.info(f"  Colonne: {len(df_final.columns)}")
        else:
            logger.warning("Nessun dato per concatenazione finale")

            # Crea file esempio
            df_example = pd.DataFrame(
                {
                    "ID": [1, 2, 3],
                    "Descrizione": ["Esempio 1", "Esempio 2", "Esempio 3"],
                    "Valore": [1000, 2000, 3000],
                }
            )

            output_file = config.get_file_path(config.FINAL, "output")
            df_example.to_excel(output_file, index=False)
            logger.info(f"📝 File esempio creato: {output_file}")
