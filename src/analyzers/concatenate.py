"""
Concatenatore per unire i dataset.
"""

import logging
from pathlib import Path

import pandas as pd

from config.settings import config
from src.utils.checkpoint import CheckpointManager
from src.utils.performance import timer

logger = logging.getLogger(__name__)


class Concatenator:
    """
    Concatena i vari dataset in file unificati.
    """

    def __init__(self):
        """Inizializza il concatenatore."""
        self.checkpoint_manager = CheckpointManager()
        self.checkpoint_manager.create_session("concatenator")

    @timer
    def concat_lotti(self):
        """Concatena i lotti da Gazzetta e OCDS."""
        logger.info("=== CONCATENAZIONE LOTTI ===")

        output_path = config.get_file_path(config.LOTTI_MERGED, "output")
        standard_columns = {
            # Standard -> possibili sorgenti
            "CIG": ["CIG", "cig"],
            "Oggetto": [
                "Oggetto",
                "oggetto",
                "OggettoGara",
                "tender_title",
                "oggetto_gara",
                "tender_description",
            ],
            "ImportoAggiudicazione": [
                "ImportoAggiudicazione",
                "importo",
                "tender_value",
                "award_value",
                "importo_aggiudicazione",
            ],
            "DataAggiudicazione": [
                "DataAggiudicazione",
                "data",
                "date",
                "award_date",
                "data_aggiudicazione",
            ],
            "Comune": [
                "Comune",
                "comune",
                "buyer_name",
                "denominazione_amministrazione_appaltante",
            ],
            "Regione": ["Regione", "sezione_regionale"],
            "Aggiudicatario": [
                "Aggiudicatario",
                "aggiudicatario",
                "supplier_name",
                "denominazione",
            ],
            "Categoria": ["Categoria", "categoria"],
            "TipoIntervento": ["TipoIntervento", "tipo_intervento"],
            "CriterioAggiudicazione": [
                "CriterioAggiudicazione",
                "criterio_aggiudicazione",
            ],
        }

        def standardize_df(df: pd.DataFrame) -> pd.DataFrame:
            df = df.copy()
            # Mappa colonne
            for std_col, candidates in standard_columns.items():
                for c in candidates:
                    if c in df.columns:
                        df[std_col] = df.get(std_col, df[c])
                        break
            # Tipi e formattazioni
            if "ImportoAggiudicazione" in df.columns:
                df["ImportoAggiudicazione"] = pd.to_numeric(
                    df["ImportoAggiudicazione"], errors="coerce"
                )
            if "DataAggiudicazione" in df.columns:
                df["DataAggiudicazione"] = pd.to_datetime(
                    df["DataAggiudicazione"], errors="coerce"
                )
            # Crea hash se mancante
            if "hash" not in df.columns:
                key_cols = [
                    col
                    for col in ["CIG", "Oggetto", "Comune", "DataAggiudicazione"]
                    if col in df.columns
                ]
                if key_cols:
                    df["hash"] = (
                        df[key_cols]
                        .astype(str)
                        .apply(lambda r: "|".join(r.values), axis=1)
                        .str.encode("utf-8")
                        .map(__import__("hashlib").sha256)
                        .map(lambda h: h.hexdigest())
                    )
            return df

        try:
            # File da concatenare (ricerca robusta)
            gazzetta_candidates = [
                config.get_file_path(config.LOTTI_GAZZETTA, "output"),
                config.get_file_path("Lotti_Gazzetta_Optimized.xlsx", "temp"),
                config.get_file_path("Lotti_Gazzetta.xlsx", "temp"),
            ]
            ocds_candidates = [
                config.get_file_path("OCDS_Analyzed.xlsx", "output"),
                config.get_file_path(config.LOTTI_OCDS, "output"),
                config.get_file_path("OCDS_Analyzed.csv", "output"),
            ]

            dfs = []

            # Carica Gazzetta (prima esistente tra i candidati)
            gazzetta_file = next((p for p in gazzetta_candidates if p.exists()), None)
            if gazzetta_file is not None:
                df1 = (
                    pd.read_excel(gazzetta_file)
                    if gazzetta_file.suffix == ".xlsx"
                    else pd.read_csv(gazzetta_file)
                )
                df1 = standardize_df(df1)
                df1["Fonte"] = "Gazzetta"
                dfs.append(df1)
                logger.info(
                    f"✅ Caricato Gazzetta: {len(df1)} record da {gazzetta_file}"
                )
            else:
                logger.warning("File Gazzetta non trovato in output/temp")

            # Carica OCDS (prima esistente tra i candidati)
            ocds_file = next((p for p in ocds_candidates if p.exists()), None)
            if ocds_file is not None:
                if ocds_file.suffix == ".xlsx":
                    df2 = pd.read_excel(ocds_file)
                else:
                    df2 = pd.read_csv(ocds_file)
                df2 = standardize_df(df2)
                df2["Fonte"] = "OCDS"
                dfs.append(df2)
                logger.info(f"✅ Caricato OCDS: {len(df2)} record da {ocds_file}")
            else:
                logger.warning("File OCDS non trovato in output")

            if dfs:
                # Concatena
                combined_df = pd.concat(dfs, ignore_index=True, sort=False)
                # Se possibile, riempi CIG mancanti usando valori presenti su righe con stesso hash/Oggetto+Comune+Data
                if "CIG" in combined_df.columns:
                    if "hash" in combined_df.columns:
                        combined_df["CIG"] = combined_df.groupby("hash")[
                            "CIG"
                        ].transform(lambda s: s.ffill().bfill())
                    key_cols = [
                        c
                        for c in ["Oggetto", "Comune", "DataAggiudicazione"]
                        if c in combined_df.columns
                    ]
                    if key_cols:
                        combined_df.sort_values(key_cols, inplace=True)
                        combined_df["CIG"] = combined_df.groupby(key_cols)[
                            "CIG"
                        ].transform(lambda s: s.ffill().bfill())
                # Dedup: preferisci CIG, altrimenti hash
                if "CIG" in combined_df.columns:
                    combined_df = combined_df.drop_duplicates(
                        subset=["CIG"], keep="first"
                    )
                elif "hash" in combined_df.columns:
                    combined_df = combined_df.drop_duplicates(
                        subset=["hash"], keep="first"
                    )

                # Enrichment: unisci info base da CIG aggregato se disponibile
                cig_path = config.get_file_path(config.FILE_CIG, "temp")
                if cig_path.exists() and "CIG" in combined_df.columns:
                    try:
                        df_cig = pd.read_csv(cig_path, sep=";", low_memory=False)
                        # Rinomina colonne chiave per standard
                        rename_map = {
                            "denominazione_amministrazione_appaltante": "Comune",
                            "sezione_regionale": "Regione",
                            "denominazione": "Aggiudicatario",
                            "importo_aggiudicazione": "ImportoAggiudicazione",
                            "data_aggiudicazione_definitiva": "DataAggiudicazione",
                            "cig": "CIG",
                        }
                        for c_old, c_new in rename_map.items():
                            if c_old in df_cig.columns and c_new not in df_cig.columns:
                                df_cig[c_new] = df_cig[c_old]
                        cols_keep = [
                            c
                            for c in [
                                "CIG",
                                "Comune",
                                "Regione",
                                "Aggiudicatario",
                                "ImportoAggiudicazione",
                                "DataAggiudicazione",
                            ]
                            if c in df_cig.columns
                        ]
                        df_cig_small = df_cig[cols_keep].copy()
                        # Dedup per CIG lato CIG
                        df_cig_small = df_cig_small.drop_duplicates(
                            subset=["CIG"], keep="first"
                        )
                        before_cols = set(combined_df.columns)
                        combined_df = combined_df.merge(
                            df_cig_small, on="CIG", how="left", suffixes=("", "_CIG")
                        )
                        # Riempie i campi standard solo dove mancanti
                        for col in [
                            "Comune",
                            "Regione",
                            "Aggiudicatario",
                            "ImportoAggiudicazione",
                            "DataAggiudicazione",
                        ]:
                            if (
                                col in before_cols
                                and f"{col}_CIG" in combined_df.columns
                            ):
                                combined_df[col] = combined_df[col].fillna(
                                    combined_df[f"{col}_CIG"]
                                )
                                combined_df.drop(columns=[f"{col}_CIG"], inplace=True)
                    except Exception as e:
                        logger.warning(f"Arricchimento da CIG saltato: {e}")

                # Salva
                if len(combined_df) > 1000000:
                    # Usa CSV per file grandi
                    csv_path = output_path.with_suffix(".csv")
                    combined_df.to_csv(csv_path, index=False)
                    logger.info(f"✅ Lotti merged salvati (CSV): {csv_path}")
                else:
                    combined_df.to_excel(output_path, index=False, engine="openpyxl")
                    logger.info(f"✅ Lotti merged salvati: {output_path}")

                logger.info(f"📊 Record totali: {len(combined_df)}")

                self.checkpoint_manager.mark_completed(
                    "concat_lotti",
                    {"total_records": len(combined_df), "output": str(output_path)},
                )
            else:
                logger.error("Nessun file da concatenare trovato")

        except Exception as e:
            logger.error(f"Errore concatenazione lotti: {e}")
            raise

    @timer
    def concat_all(self):
        """Concatena tutti i file finali."""
        logger.info("=== CONCATENAZIONE FINALE ===")

        output_path = config.get_file_path(config.FINAL, "output")

        try:
            # File da concatenare
            servizio_luce_file = config.get_file_path(config.SERVIZIO_LUCE, "output")
            verbali_file = config.get_file_path(config.VERBALI, "output")

            dfs = []

            # Carica Servizio Luce
            if servizio_luce_file.exists():
                df1 = pd.read_excel(servizio_luce_file)
                dfs.append(df1)
                logger.info(f"✅ Caricato Servizio Luce: {len(df1)} record")
            else:
                logger.warning(f"File Servizio Luce non trovato: {servizio_luce_file}")

            # Carica Verbali
            if verbali_file.exists():
                df2 = pd.read_excel(verbali_file)
                dfs.append(df2)
                logger.info(f"✅ Caricato Verbali: {len(df2)} record")
            else:
                logger.warning(f"File Verbali non trovato: {verbali_file}")

            if dfs:
                # Concatena
                combined_df = pd.concat(dfs, ignore_index=True, sort=False)

                # Salva
                if len(combined_df) > 1000000:
                    # Usa CSV per file grandi
                    csv_path = output_path.with_suffix(".csv")
                    combined_df.to_csv(csv_path, index=False)
                    logger.info(f"✅ File finale salvato (CSV): {csv_path}")
                else:
                    combined_df.to_excel(output_path, index=False, engine="openpyxl")
                    logger.info(f"✅ File finale salvato: {output_path}")

                logger.info(f"📊 Record totali finali: {len(combined_df)}")

                self.checkpoint_manager.mark_completed(
                    "concat_all",
                    {"total_records": len(combined_df), "output": str(output_path)},
                )
            else:
                logger.error("Nessun file finale da concatenare")

        except Exception as e:
            logger.error(f"Errore concatenazione finale: {e}")
            raise

    def run(self):
        """Esegue tutte le concatenazioni."""
        self.concat_lotti()
        self.concat_all()
