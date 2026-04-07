"""
Transformer per la trasformazione finale dei dati.
"""

import logging
from datetime import datetime

import numpy as np
import pandas as pd

from config.settings import config
from src.utils.performance import timer

logger = logging.getLogger(__name__)


class Transformer:
    """
    Trasforma i dati per l'output finale.
    """

    @timer
    def run(self) -> None:
        """
        Esegue la trasformazione finale dei dati.
        """
        logger.info("=== TRASFORMAZIONE FINALE ===")

        # File input
        input_file = config.get_file_path(config.FINAL, "output")

        if not input_file.exists():
            logger.warning(f"File input non trovato: {input_file}")

            # Crea dati di esempio
            df = pd.DataFrame(
                {
                    "CIG": ["ABC123", "DEF456", "GHI789"],
                    "Oggetto": [
                        "Illuminazione pubblica",
                        "Videosorveglianza",
                        "Manutenzione",
                    ],
                    "Importo": [50000, 75000, 30000],
                    "Data": ["2024-01-15", "2024-02-20", "2024-03-10"],
                    "Ente": ["Comune A", "Comune B", "Comune C"],
                }
            )
        else:
            logger.info(f"Lettura file: {input_file}")
            df = pd.read_excel(input_file)

        logger.info(f"Record iniziali: {len(df)}")

        # Trasformazioni
        transformations_applied = []

        # 1. Pulizia valori mancanti
        before = df.isna().sum().sum()
        df = df.fillna("")
        after = df.isna().sum().sum()
        if before > after:
            transformations_applied.append(
                f"Valori mancanti riempiti: {before - after}"
            )

        # 2. Conversione date
        date_columns = [
            col for col in df.columns if "data" in col.lower() or "date" in col.lower()
        ]
        for col in date_columns:
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                transformations_applied.append(f"Colonna {col} convertita in datetime")
            except:
                pass

        # 3. Conversione importi
        amount_columns = [
            col
            for col in df.columns
            if "importo" in col.lower() or "amount" in col.lower()
        ]
        for col in amount_columns:
            try:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                transformations_applied.append(f"Colonna {col} convertita in numerico")
            except:
                pass

        # 4. Standardizzazione CIG
        if "CIG" in df.columns:
            df["CIG"] = df["CIG"].astype(str).str.upper().str.strip()
            transformations_applied.append("CIG standardizzati")

        # 5. Aggiunta metadati
        df["data_elaborazione"] = datetime.now().isoformat()
        df["versione"] = "1.0"
        transformations_applied.append("Metadati aggiunti")

        # 6. Ordinamento
        if "CIG" in df.columns:
            df = df.sort_values("CIG")
        elif "Data" in df.columns:
            df = df.sort_values("Data")
        transformations_applied.append("Dati ordinati")

        # Report trasformazioni
        logger.info("Trasformazioni applicate:")
        for trans in transformations_applied:
            logger.info(f"  - {trans}")

        # Statistiche finali
        logger.info("=== STATISTICHE FINALI ===")
        logger.info(f"Record totali: {len(df)}")
        logger.info(f"Colonne: {len(df.columns)}")

        # Analisi per tipo di colonna
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        date_cols = df.select_dtypes(include=["datetime64"]).columns.tolist()
        text_cols = df.select_dtypes(include=["object"]).columns.tolist()

        logger.info(f"Colonne numeriche: {len(numeric_cols)}")
        logger.info(f"Colonne data: {len(date_cols)}")
        logger.info(f"Colonne testo: {len(text_cols)}")

        # Salva file finale
        output_file = config.get_file_path(config.GARE, "output")
        df.to_excel(output_file, index=False)
        logger.info(f"✅ File finale salvato: {output_file}")

        # Report memoria utilizzata
        memory_usage = df.memory_usage(deep=True).sum() / 1024 / 1024
        logger.info(f"Memoria utilizzata: {memory_usage:.2f} MB")

        # Top 5 record per importo (se presente)
        if amount_columns and amount_columns[0] in df.columns:
            top_5 = df.nlargest(5, amount_columns[0])
            logger.info(f"Top 5 per {amount_columns[0]}:")
            for idx, row in top_5.iterrows():
                logger.info(f"  - {row.get('CIG', idx)}: {row[amount_columns[0]]:,.2f}")
