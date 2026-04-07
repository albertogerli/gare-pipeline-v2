"""
Extract structured data from CIG JSON (NDJSON) ZIP files.

CIG JSON is the ANAC format for months not yet in OCDS bulk.
Format: ZIP containing NDJSON (one JSON object per line).
Note: CIG JSON does NOT contain supplier info.

Sezione regionale mapping included for region inference.
"""

import json
import logging
import zipfile
from pathlib import Path

import pandas as pd

from .filters import get_primary_category, passes_filter

logger = logging.getLogger(__name__)

# ANAC sezione_regionale -> standard region name
SEZIONE_REGIONE = {
    "SEZIONE REGIONALE ABRUZZO": "Abruzzo",
    "SEZIONE REGIONALE BASILICATA": "Basilicata",
    "SEZIONE REGIONALE CALABRIA": "Calabria",
    "SEZIONE REGIONALE CAMPANIA": "Campania",
    "SEZIONE REGIONALE EMILIA ROMAGNA": "Emilia-Romagna",
    "SEZIONE REGIONALE FRIULI VENEZIA GIULIA": "Friuli Venezia Giulia",
    "SEZIONE REGIONALE LAZIO": "Lazio",
    "SEZIONE REGIONALE LIGURIA": "Liguria",
    "SEZIONE REGIONALE LOMBARDIA": "Lombardia",
    "SEZIONE REGIONALE MARCHE": "Marche",
    "SEZIONE REGIONALE MOLISE": "Molise",
    "SEZIONE REGIONALE PIEMONTE": "Piemonte",
    "SEZIONE REGIONALE PUGLIA": "Puglia",
    "SEZIONE REGIONALE SARDEGNA": "Sardegna",
    "SEZIONE REGIONALE SICILIA": "Sicilia",
    "SEZIONE REGIONALE TOSCANA": "Toscana",
    "SEZIONE REGIONALE TRENTINO ALTO ADIGE": "Trentino-Alto Adige",
    "SEZIONE REGIONALE UMBRIA": "Umbria",
    "SEZIONE REGIONALE VALLE D'AOSTA": "Valle d'Aosta",
    "SEZIONE REGIONALE VENETO": "Veneto",
}

# Procurement method codes
METODO_SCELTA = {
    "01": "Procedura Aperta",
    "02": "Procedura Ristretta",
    "03": "Procedura Negoziata Previa Pubblicazione",
    "04": "Procedura Negoziata Senza Previa Pubblicazione",
    "05": "Dialogo Competitivo",
    "06": "Procedura Negoziata Senza Previa Pubblicazione (Urgenza)",
    "07": "Sistema Dinamico di Acquisizione",
    "08": "Affidamento Diretto",
    "14": "Procedura Selettiva (Concessioni)",
    "17": "Affidamento Diretto in Adesione ad AQ",
    "23": "Affidamento Diretto (Sotto Soglia)",
    "26": "Affidamento Diretto a Società In House",
    "27": "Confronto Competitivo in Adesione ad AQ",
}


def extract_cig_zip(filepath: Path) -> pd.DataFrame:
    """
    Extract and filter a single CIG JSON ZIP file.

    Args:
        filepath: Path to ZIP file containing NDJSON.

    Returns:
        DataFrame with filtered and mapped records.
    """
    try:
        with zipfile.ZipFile(filepath) as zf:
            ndjson_files = [n for n in zf.namelist() if n.endswith(".json")]
            if not ndjson_files:
                logger.warning(f"{filepath.name}: no JSON in ZIP")
                return pd.DataFrame()

            records = []
            total = 0
            filtered = 0

            for ndjson_name in ndjson_files:
                with zf.open(ndjson_name) as f:
                    for line in f:
                        total += 1
                        try:
                            obj = json.loads(line.decode("utf-8"))
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            continue

                        # Build searchable text — combine ALL fields for filtering
                        oggetto_gara = obj.get("oggetto_gara", "") or ""
                        oggetto_lotto = obj.get("oggetto_lotto", "") or ""
                        desc_cpv = obj.get("descrizione_cpv", "") or ""
                        combined = f"{oggetto_gara} {oggetto_lotto} {desc_cpv}".strip()

                        if not passes_filter(combined):
                            filtered += 1
                            continue

                        # Region mapping
                        sezione = (obj.get("sezione_regionale", "") or "").upper().strip()
                        regione = SEZIONE_REGIONE.get(sezione, "")

                        # Procurement method
                        cod_metodo = obj.get("cod_tipo_scelta_contraente", "")
                        procedura = METODO_SCELTA.get(str(cod_metodo), str(cod_metodo))

                        categoria = get_primary_category(combined)

                        record = {
                            "fonte": "CIG_JSON",
                            "cig": obj.get("cig", ""),
                            "oggetto": oggetto_lotto or oggetto_gara,
                            "categoria": categoria,
                            "importo_base": obj.get("importo_complessivo_gara"),
                            "importo_aggiudicazione": obj.get("importo_aggiudicazione"),
                            "data_pubblicazione": obj.get("data_pubblicazione"),
                            "scadenza_gara": obj.get("data_scadenza_offerta"),
                            "data_scadenza": None,  # Not available in CIG JSON
                            "ente_appaltante": obj.get(
                                "denominazione_amministrazione_appaltante", ""
                            ),
                            "comune": "",
                            "regione": regione,
                            "aggiudicatario": "",  # NOT available in CIG JSON
                            "procedura": procedura,
                            "tipo_appalto": obj.get("oggetto_principale_contratto", ""),
                            "cpv": obj.get("cpv", ""),
                            "n_lotti": 0,
                            "source_file": filepath.name,
                        }

                        records.append(record)

            logger.info(
                f"{filepath.name}: {len(records)} matched, "
                f"{filtered} filtered out of {total}"
            )
            return pd.DataFrame(records)

    except (zipfile.BadZipFile, OSError) as e:
        logger.error(f"Cannot read {filepath}: {e}")
        return pd.DataFrame()


def extract_all_cig_json(
    cig_dir: Path, output_path: Path | None = None
) -> pd.DataFrame:
    """
    Extract and filter all CIG JSON ZIP files in a directory.

    Args:
        cig_dir: Directory containing CIG JSON ZIP files.
        output_path: Optional path to save CSV output.

    Returns:
        Combined DataFrame from all files.
    """
    files = sorted(cig_dir.glob("*.zip"))
    if not files:
        logger.warning(f"No CIG JSON files in {cig_dir}")
        return pd.DataFrame()

    logger.info(f"Processing {len(files)} CIG JSON files...")
    frames = []

    for f in files:
        df = extract_cig_zip(f)
        if not df.empty:
            frames.append(df)

    if not frames:
        logger.warning("No records extracted from CIG JSON")
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    logger.info(f"CIG JSON total: {len(result)} records from {len(frames)} files")

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result.to_csv(output_path, index=False)
        logger.info(f"Saved: {output_path}")

    return result
