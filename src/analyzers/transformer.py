"""
Trasformatore finale per pulizia e arricchimento dati.
"""

import logging
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from config.settings import config
from src.utils.checkpoint import CheckpointManager
from src.utils.performance import timer

logger = logging.getLogger(__name__)


def to_decimal(series: pd.Series, base: pd.Series) -> pd.Series:
    """
    Fill NaNs from base, cast to float, and convert any value >1 (i.e. percent)
    into decimal (0–1) by dividing by 100.
    """
    s = series.fillna(base).astype(float)
    return s.mask(s > 1, s / 100)


def clean_num(series: pd.Series) -> pd.Series:
    """Strip percent sign, commas; cast to float."""
    s = (
        series.astype(str)
        .str.replace("%", "", regex=False)
        .str.replace(",", ".", regex=False)
        .replace("", pd.NA)
    )
    return pd.to_numeric(s, errors="coerce")


def clean_text(series: pd.Series) -> pd.Series:
    """Trim, uppercase, collapse spaces."""
    return (
        series.astype(str).str.strip().str.upper().str.replace(r"\s+", " ", regex=True)
    )


class Transformer:
    """
    Trasforma e pulisce i dati finali.
    """

    def __init__(self):
        """Inizializza il trasformatore."""
        self.checkpoint_manager = CheckpointManager()
        self.checkpoint_manager.create_session("transformer")

    @timer
    def run(self):
        """Esegue la trasformazione finale."""
        logger.info("=== TRASFORMAZIONE FINALE ===")

        # Paths
        input_path = config.get_file_path(config.FINAL, "output")
        output_path = config.get_file_path(config.GARE, "output")

        # Check se esiste CSV invece di Excel
        if not input_path.exists() and input_path.with_suffix(".csv").exists():
            input_path = input_path.with_suffix(".csv")
            logger.info(f"Usando file CSV: {input_path}")

        if not input_path.exists():
            logger.error(f"File di input non trovato: {input_path}")
            return

        try:
            # Carica dati
            logger.info(f"Caricamento dati da: {input_path}")
            if input_path.suffix == ".csv":
                df = pd.read_csv(input_path)
            else:
                df = pd.read_excel(input_path)

            logger.info(f"Record caricati: {len(df)}")

            # ========================
            # 1. Date columns
            # ========================
            date_cols = [
                "DataPubblicazione",
                "DataAggiudicazione",
                "Scadenza",
                "DataEsito",
                "DataUltimoPerfezionamento",
            ]
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

            # ========================
            # 2. Discount percentages
            # ========================
            if "Sconto" in df.columns:
                df["Sconto"] = clean_num(df["Sconto"])
                df["ScontoPerc"] = to_decimal(df["Sconto"].copy(), df["Sconto"])

            if "MassimoSconto" in df.columns:
                df["MassimoSconto"] = clean_num(df["MassimoSconto"])
                df["MassimoScontoPerc"] = to_decimal(
                    df["MassimoSconto"].copy(), df["MassimoSconto"]
                )

            if "MinimoSconto" in df.columns:
                df["MinimoSconto"] = clean_num(df["MinimoSconto"])
                df["MinimoScontoPerc"] = to_decimal(
                    df["MinimoSconto"].copy(), df["MinimoSconto"]
                )

            # ========================
            # 3. Year/Quarter/Month
            # ========================
            if "DataAggiudicazione" in df.columns:
                df["Anno"] = df["DataAggiudicazione"].dt.year
                df["Trimestre"] = df["DataAggiudicazione"].dt.quarter
                df["Mese"] = df["DataAggiudicazione"].dt.month

            # ========================
            # 4. Text cleaning
            # ========================
            text_cols = [
                "Comune",
                "Provincia",
                "Regione",
                "Aggiudicatario",
                "CodFiscaleAggiudicatario",
                "OggettoGara",
                "Oggetto",
                "OggettoContratto",
                "CriterioAggiudicazione",
            ]
            for col in text_cols:
                if col in df.columns:
                    df[col] = clean_text(df[col])

            # ========================
            # 5. Import cleaning
            # ========================
            import_cols = ["ImportoGara", "ImportoAggiudicazione"]
            for col in import_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # ========================
            # 6. De-duplicate
            # ========================
            if "CIG" in df.columns:
                # Rimuovi duplicati basati su CIG (mantieni il primo)
                df_no_dup = df[df["CIG"].notna()].drop_duplicates(
                    subset=["CIG"], keep="first"
                )
                df_null_cig = df[df["CIG"].isna()]
                df = pd.concat([df_no_dup, df_null_cig], ignore_index=True)
                logger.info(f"Record dopo deduplicazione: {len(df)}")

            # ========================
            # 6b. Arricchimento Consip Servizio Luce (join by CIG)
            # ========================
            try:
                sl_path = config.get_file_path(config.SERVIZIO_LUCE, "output")
                if sl_path.exists() and "CIG" in df.columns:
                    df_sl = pd.read_excel(sl_path)
                    cols_keep = [
                        c
                        for c in ["CIG", "ServizioLuce", "Consip"]
                        if c in df_sl.columns
                    ]
                    if cols_keep:
                        df = df.merge(
                            df_sl[cols_keep].drop_duplicates("CIG"),
                            on="CIG",
                            how="left",
                        )
                        logger.info("Arricchimento Servizio Luce applicato")
            except Exception as e:
                logger.warning(f"Arricchimento Servizio Luce saltato: {e}")

            # ========================
            # 7. Sort
            # ========================
            if "DataAggiudicazione" in df.columns:
                df = df.sort_values("DataAggiudicazione", ascending=False)

            # ========================
            # 7b. Normalizza criterio aggiudicazione, flag Minor Prezzo
            # ========================
            if "CriterioAggiudicazione" in df.columns:
                df["CriterioAggiudicazione"] = (
                    df["CriterioAggiudicazione"].fillna("").str.upper()
                )
                df["MinorPrezzo"] = df["CriterioAggiudicazione"].str.contains(
                    "MINOR|PREZZO", regex=True
                )

            # ========================
            # 8. Incremental update (append-safe su SQLite) e Save
            # ========================
            if config.ENABLE_SQLITE:
                db_path = config.SQLITE_PATH
                db_path.parent.mkdir(parents=True, exist_ok=True)
                with sqlite3.connect(db_path) as conn:
                    # Scrivi in tabella 'gare' con dedup su CIG
                    df.to_sql("gare_stage", conn, if_exists="replace", index=False)
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS gare (
                            CIG TEXT,
                            Oggetto TEXT,
                            ImportoAggiudicazione REAL,
                            DataAggiudicazione TEXT,
                            Comune TEXT,
                            Regione TEXT,
                            Aggiudicatario TEXT,
                            Categoria TEXT,
                            TipoIntervento TEXT
                        )
                    """
                    )
                    # Inserisci/aggiorna per CIG
                    conn.execute(
                        "DELETE FROM gare WHERE CIG IN (SELECT CIG FROM gare_stage WHERE CIG IS NOT NULL)"
                    )
                    cols = [
                        c
                        for c in [
                            "CIG",
                            "Oggetto",
                            "ImportoAggiudicazione",
                            "DataAggiudicazione",
                            "Comune",
                            "Regione",
                            "Aggiudicatario",
                            "Categoria",
                            "TipoIntervento",
                        ]
                        if c in df.columns
                    ]
                    sel_cols = ",".join(cols)
                    ins_cols = ",".join(cols)
                    conn.execute(
                        f"INSERT INTO gare ({ins_cols}) SELECT {sel_cols} FROM gare_stage"
                    )
                    conn.commit()
                logger.info(f"✅ SQLite aggiornato: {db_path}")

            # Salvataggio file tabellare
            if len(df) > 1000000:
                # Usa CSV per file grandi
                csv_path = output_path.with_suffix(".csv")
                df.to_csv(csv_path, index=False)
                logger.info(f"✅ File finale trasformato salvato (CSV): {csv_path}")

                # Crea anche sample Excel
                sample_path = output_path.parent / f"{output_path.stem}_sample.xlsx"
                df.head(100000).to_excel(sample_path, index=False)
                logger.info(f"✅ Sample Excel salvato: {sample_path}")
            else:
                df.to_excel(output_path, index=False)
                logger.info(f"✅ File finale trasformato salvato: {output_path}")

            # Parquet opzionale
            if config.ENABLE_PARQUET:
                parquet_path = output_path.with_suffix(".parquet")
                try:
                    df.to_parquet(parquet_path, index=False)
                    logger.info(f"✅ Salvato anche Parquet: {parquet_path}")
                except Exception as _:
                    logger.warning(
                        "Parquet non disponibile (installa pyarrow o fastparquet)"
                    )

            # Statistiche finali
            logger.info("=== STATISTICHE FINALI ===")
            logger.info(f"Record totali: {len(df)}")
            logger.info(f"Colonne: {len(df.columns)}")

            if "CIG" in df.columns:
                logger.info(f"CIG validi: {df['CIG'].notna().sum()}")
                logger.info(f"CIG mancanti: {df['CIG'].isna().sum()}")

            if "ImportoAggiudicazione" in df.columns:
                totale = df["ImportoAggiudicazione"].sum()
                logger.info(f"Importo totale aggiudicazioni: €{totale:,.2f}")

            if "Anno" in df.columns:
                anni = df["Anno"].value_counts().head(5)
                logger.info(f"Top 5 anni:\n{anni}")

            # Checkpoint
            self.checkpoint_manager.mark_completed(
                "transformation",
                {
                    "total_records": len(df),
                    "columns": len(df.columns),
                    "output": str(output_path),
                },
            )

        except Exception as e:
            logger.error(f"Errore trasformazione: {e}")
            raise
