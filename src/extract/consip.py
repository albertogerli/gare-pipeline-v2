"""
CONSIP/ServizioLuce classification from OCDS and CIG JSON data.

Identifies CONSIP framework agreements:
- SL (Servizio Luce) editions 1-4
- SIE (Servizio Integrato Energia) editions 1-4
- GEIP (Gestione Efficiente Illuminazione Pubblica)
- AQ_SL (Accordo Quadro Servizio Luce)

Uses two-stage matching:
1. PREFILTER: Quick regex to identify potential matches
2. CONSIP_PATTERNS: Detailed patterns with confidence scoring

Keeps historical records for CIGs not found in new data.
"""

import logging
import re
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Stage 1: Quick prefilter — must mention at least one of these
PREFILTER = re.compile(
    r"consip|sigef|servizio\s+(?:integrato\s+)?(?:luce|energia)|"
    r"SIE\s*\d|SL\s*\d|GEIP|AQ[\s_]SL|"
    r"1615|1178|1614|1270|1879|2634",
    re.IGNORECASE,
)

# False positive exclusions
FALSE_POSITIVES = re.compile(
    r"siemens|sielco|diesel|schleswig|consilium",
    re.IGNORECASE,
)

# Stage 2: Detailed patterns with (tipo_accordo, edizione, confidence)
CONSIP_PATTERNS: list[tuple[re.Pattern, str, str, float]] = [
    # SIGEF IDs (highest confidence)
    (re.compile(r"\b1615\b"), "SIE", "4", 0.95),
    (re.compile(r"\b1178\b"), "SIE", "3", 0.95),
    (re.compile(r"\b1614\b"), "SL", "4", 0.95),
    (re.compile(r"\b1270\b"), "SL", "3", 0.95),
    (re.compile(r"\b1879\b"), "GEIP", "", 0.95),
    (re.compile(r"\b2634\b"), "AQ_SL", "", 0.95),
    # Edition patterns
    (re.compile(r"SIE\s*4|servizio\s+integrato\s+energia\s*4", re.I), "SIE", "4", 0.90),
    (re.compile(r"SIE\s*3|servizio\s+integrato\s+energia\s*3", re.I), "SIE", "3", 0.90),
    (re.compile(r"SL\s*4|servizio\s+luce\s*4", re.I), "SL", "4", 0.90),
    (re.compile(r"SL\s*3|servizio\s+luce\s*3", re.I), "SL", "3", 0.90),
    (re.compile(r"GEIP|gestione\s+efficiente\s+illuminazione", re.I), "GEIP", "", 0.85),
    (re.compile(r"AQ[\s_]SL|accordo\s+quadro\s+servizio\s+luce", re.I), "AQ_SL", "", 0.85),
    # Generic patterns (lower confidence)
    (re.compile(r"servizio\s+integrato\s+(?:di\s+)?energia", re.I), "SIE", "", 0.70),
    (re.compile(r"servizio\s+luce\b", re.I), "SL", "", 0.70),
    (re.compile(r"consip.*(?:illumin|luce|energia)", re.I), "SL", "", 0.60),
    (re.compile(r"(?:illumin|luce|energia).*consip", re.I), "SL", "", 0.60),
]


def classify_consip(text: str) -> Optional[dict]:
    """
    Classify a text as CONSIP framework agreement.

    Args:
        text: Combined text from tender description and lot descriptions.

    Returns:
        dict with tipo_accordo, edizione, confidenza or None.
    """
    if not text or not isinstance(text, str):
        return None

    # Stage 1: Quick prefilter
    if not PREFILTER.search(text):
        return None

    # Check false positives
    if FALSE_POSITIVES.search(text):
        return None

    # Stage 2: Match detailed patterns (return first/best match)
    for pattern, tipo, edizione, confidence in CONSIP_PATTERNS:
        if pattern.search(text):
            return {
                "tipo_accordo": tipo,
                "edizione": edizione,
                "confidenza": confidence,
            }

    return None


def build_servizio_luce(
    ocds_csv: Path,
    cig_csv: Path | None,
    old_file: Path | None,
    output_path: Path,
) -> pd.DataFrame:
    """
    Build ServizioLuce.xlsx from OCDS, CIG JSON and historical data.

    Args:
        ocds_csv: Path to extracted OCDS CSV.
        cig_csv: Path to extracted CIG JSON CSV (optional).
        old_file: Path to old ServizioLuce.xlsx for historical records.
        output_path: Where to save the result.

    Returns:
        DataFrame with classified CONSIP records.
    """
    all_records = []

    # Process OCDS
    if ocds_csv.exists():
        df_ocds = pd.read_csv(ocds_csv, low_memory=False)
        logger.info(f"OCDS: {len(df_ocds)} records to scan for CONSIP")

        for _, row in df_ocds.iterrows():
            text = str(row.get("oggetto", ""))
            result = classify_consip(text)
            if result:
                record = row.to_dict()
                record.update(result)
                record["consip_fonte"] = "OCDS"
                all_records.append(record)

        logger.info(f"OCDS: {len(all_records)} CONSIP matches")

    # Process CIG JSON
    cig_count = 0
    if cig_csv and cig_csv.exists():
        df_cig = pd.read_csv(cig_csv, low_memory=False)
        logger.info(f"CIG JSON: {len(df_cig)} records to scan")

        for _, row in df_cig.iterrows():
            text = str(row.get("oggetto", ""))
            result = classify_consip(text)
            if result:
                record = row.to_dict()
                record.update(result)
                record["consip_fonte"] = "CIG_JSON"
                all_records.append(record)
                cig_count += 1

        logger.info(f"CIG JSON: {cig_count} CONSIP matches")

    # Deduplicate by CIG (prefer OCDS > CIG_JSON)
    df_new = pd.DataFrame(all_records)

    if not df_new.empty and "cig" in df_new.columns:
        # Extract first CIG from semicolon-separated list
        df_new["_first_cig"] = df_new["cig"].astype(str).str.split(";").str[0]
        df_new = df_new.drop_duplicates(subset=["_first_cig"], keep="first")
        df_new = df_new.drop(columns=["_first_cig"])

    # Merge with historical records
    if old_file and old_file.exists():
        try:
            df_old = pd.read_excel(old_file)
            logger.info(f"Historical: {len(df_old)} records")

            # Find CIGs already in new data
            new_cigs = set()
            if not df_new.empty and "cig" in df_new.columns:
                for cig_val in df_new["cig"].dropna():
                    for c in str(cig_val).split(";"):
                        new_cigs.add(c.strip())

            # Keep old records whose CIG is not in new data
            if "cig" in df_old.columns or "CIG" in df_old.columns:
                cig_col = "CIG" if "CIG" in df_old.columns else "cig"
                old_only = df_old[~df_old[cig_col].isin(new_cigs)]
                old_only = old_only.copy()
                old_only["consip_fonte"] = "STORICO"

                if not old_only.empty:
                    df_new = pd.concat([df_new, old_only], ignore_index=True)
                    logger.info(f"Added {len(old_only)} historical records")

        except Exception as e:
            logger.error(f"Cannot read historical file: {e}")

    if df_new.empty:
        logger.warning("No CONSIP records found")
        return pd.DataFrame()

    # Save
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_new.to_excel(output_path, index=False)
    logger.info(
        f"ServizioLuce saved: {len(df_new)} records "
        f"({len(all_records)} new + historical) -> {output_path}"
    )
    return df_new
