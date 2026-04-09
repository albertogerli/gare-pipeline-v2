"""
Document enrichment module.

Enriches procurement records with additional data from ANAC document APIs.
This is an optional step (Phase 4) that can be run after the main pipeline.
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def enrich_with_documents(input_path: Path, output_path: Path) -> pd.DataFrame:
    """
    Enrich unified dataset with document links from ANAC.

    Adds URL links to official ANAC documents for each CIG.

    Args:
        input_path: Path to unified dataset (CSV/CSV.GZ).
        output_path: Where to save enriched output.

    Returns:
        Enriched DataFrame.
    """
    logger.info(f"Loading dataset: {input_path}")

    if str(input_path).endswith(".gz"):
        df = pd.read_csv(input_path, compression="gzip", low_memory=False)
    else:
        df = pd.read_csv(input_path, low_memory=False)

    logger.info(f"Records: {len(df)}")

    # Add ANAC detail URL for records with CIG
    if "cig" in df.columns:
        df["url_anac"] = df["cig"].apply(
            lambda x: (
                f"https://dati.anticorruzione.it/superset/dashboard/"
                f"dettaglio_cig/?cig={str(x).split(';')[0]}&standalone=2"
                if pd.notna(x) and str(x).strip()
                else None
            )
        )
        n_urls = df["url_anac"].notna().sum()
        logger.info(f"Added ANAC URLs: {n_urls}")

    # Save
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if str(output_path).endswith(".gz"):
        df.to_csv(output_path, index=False, compression="gzip")
    else:
        df.to_csv(output_path, index=False)

    logger.info(f"Enriched dataset saved: {output_path}")
    return df
