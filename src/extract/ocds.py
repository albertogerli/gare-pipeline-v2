"""
Extract structured data from OCDS bulk JSON files.

Reads ANAC OCDS JSON files, filters by category, and extracts
standardized fields. No LLM required — uses direct JSON field mapping
and regex-based category filters.

CRITICAL field mappings (from analysis):
- tender_description (97.8%) NOT tender_title (always empty in ANAC data)
- release_date (97.8%) for data_pubblicazione
- contract_end_date for data_scadenza (fine lavori/contratto)
- tender_period_end for scadenza_gara
- lot.id = CIG in ANAC system
"""

import json
import logging
from pathlib import Path

import pandas as pd

from .filters import get_primary_category, passes_filter

logger = logging.getLogger(__name__)

# CAP (postal code) -> Region mapping (first 2 digits)
CAP_REGIONE = {
    "00": "Lazio", "01": "Lazio", "02": "Lazio", "03": "Lazio", "04": "Lazio",
    "05": "Umbria",
    "06": "Umbria",
    "07": "Sardegna",
    "08": "Sardegna", "09": "Sardegna",
    "10": "Piemonte", "11": "Valle d'Aosta", "12": "Piemonte",
    "13": "Piemonte", "14": "Piemonte", "15": "Piemonte",
    "16": "Liguria", "17": "Liguria", "18": "Liguria", "19": "Liguria",
    "20": "Lombardia", "21": "Lombardia", "22": "Lombardia", "23": "Lombardia",
    "24": "Lombardia", "25": "Lombardia", "26": "Lombardia", "27": "Lombardia",
    "28": "Piemonte", "29": "Emilia-Romagna",
    "30": "Veneto", "31": "Veneto", "32": "Veneto", "33": "Friuli Venezia Giulia",
    "34": "Friuli Venezia Giulia", "35": "Veneto", "36": "Veneto", "37": "Veneto",
    "38": "Trentino-Alto Adige", "39": "Trentino-Alto Adige",
    "40": "Emilia-Romagna", "41": "Emilia-Romagna", "42": "Emilia-Romagna",
    "43": "Emilia-Romagna", "44": "Emilia-Romagna", "45": "Veneto",
    "46": "Lombardia", "47": "Emilia-Romagna", "48": "Emilia-Romagna",
    "50": "Toscana", "51": "Toscana", "52": "Toscana", "53": "Toscana",
    "54": "Toscana", "55": "Toscana", "56": "Toscana", "57": "Toscana",
    "58": "Toscana", "59": "Toscana",
    "60": "Marche", "61": "Marche", "62": "Marche", "63": "Marche",
    "64": "Abruzzo", "65": "Abruzzo", "66": "Abruzzo", "67": "Abruzzo",
    "70": "Puglia", "71": "Puglia", "72": "Puglia", "73": "Puglia",
    "74": "Puglia", "75": "Basilicata", "76": "Puglia",
    "80": "Campania", "81": "Campania", "82": "Campania", "83": "Campania",
    "84": "Campania", "85": "Basilicata",
    "86": "Molise", "87": "Calabria", "88": "Calabria", "89": "Calabria",
    "90": "Sicilia", "91": "Sicilia", "92": "Sicilia", "93": "Sicilia",
    "94": "Sicilia", "95": "Sicilia", "96": "Sicilia", "97": "Sicilia",
    "98": "Sicilia",
}


def _safe_date(val: str | None) -> str | None:
    """Extract date from potentially malformed ANAC timestamps.

    ANAC format is sometimes: "2024-02-10 17:35:30.228T12:00:00Z"
    We take only the first 10 chars (YYYY-MM-DD).
    """
    if not val or not isinstance(val, str):
        return None
    return val[:10] if len(val) >= 10 else None


def _get_buyer(parties: list[dict]) -> tuple[str, str, str, str]:
    """Extract buyer info from OCDS parties array.

    Returns: (name, id, address, cap)
    """
    for p in parties:
        if "buyer" in p.get("roles", []):
            addr = p.get("address", {})
            cap = addr.get("postalCode", "")
            locality = addr.get("locality", "")
            return (
                p.get("name", ""),
                p.get("id", ""),
                locality,
                str(cap) if cap else "",
            )
    return ("", "", "", "")


def _get_supplier(parties: list[dict], awards: list[dict]) -> tuple[str, str]:
    """Extract first supplier from parties or awards."""
    for p in parties:
        if "supplier" in p.get("roles", []):
            return (p.get("name", ""), p.get("id", ""))

    for award in awards:
        suppliers = award.get("suppliers", [])
        if suppliers:
            s = suppliers[0]
            return (s.get("name", ""), s.get("id", ""))

    return ("", "")


def _get_lot_ids(tender: dict) -> str:
    """Extract CIG codes from lots (lot.id = CIG in ANAC)."""
    lots = tender.get("lots", [])
    if lots:
        ids = [lot.get("id", "") for lot in lots if lot.get("id")]
        return ";".join(ids)
    return ""


def extract_ocds_file(filepath: Path) -> pd.DataFrame:
    """
    Extract and filter a single OCDS JSON file.

    Args:
        filepath: Path to OCDS JSON file.

    Returns:
        DataFrame with filtered and mapped records.
    """
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.error(f"Cannot read {filepath}: {e}")
        return pd.DataFrame()

    releases = data.get("releases", [])
    if not releases:
        logger.warning(f"{filepath.name}: no releases")
        return pd.DataFrame()

    records = []
    filtered_count = 0

    for release in releases:
        tender = release.get("tender", {})
        if not tender:
            continue

        # Use description (97.8% coverage), NOT title (always empty)
        description = tender.get("description", "") or ""
        title = tender.get("title", "") or ""
        combined_text = f"{title} {description}".strip()

        # Also check lot descriptions
        lot_texts = []
        for lot in tender.get("lots", []):
            lot_desc = lot.get("description", "") or ""
            lot_title = lot.get("title", "") or ""
            lot_texts.append(f"{lot_title} {lot_desc}")

        full_text = f"{combined_text} {' '.join(lot_texts)}".strip()

        if not passes_filter(full_text):
            filtered_count += 1
            continue

        parties = release.get("parties", [])
        awards = release.get("awards", [])
        contracts = release.get("contracts", [])

        buyer_name, buyer_id, buyer_city, buyer_cap = _get_buyer(parties)
        supplier_name, supplier_id = _get_supplier(parties, awards)

        # Contract dates
        contract_start = None
        contract_end = None
        if contracts:
            period = contracts[0].get("period", {})
            contract_start = _safe_date(period.get("startDate"))
            contract_end = _safe_date(period.get("endDate"))

        # Award value
        award_value = None
        if awards:
            award_value = awards[0].get("value", {}).get("amount")

        # CPV code
        cpv = ""
        items = tender.get("items", [])
        if items:
            cpv = items[0].get("classification", {}).get("id", "")

        # Region from CAP
        regione = CAP_REGIONE.get(buyer_cap[:2], "") if len(buyer_cap) >= 2 else ""

        # Category
        categoria = get_primary_category(full_text)

        record = {
            "fonte": "OCDS",
            "ocid": release.get("ocid", ""),
            "cig": _get_lot_ids(tender),
            "oggetto": description or title,
            "categoria": categoria,
            "importo_base": tender.get("value", {}).get("amount"),
            "importo_aggiudicazione": award_value,
            "data_pubblicazione": _safe_date(release.get("date")),
            "scadenza_gara": _safe_date(
                tender.get("tenderPeriod", {}).get("endDate")
            ),
            "data_scadenza": contract_end,  # fine lavori/contratto
            "contract_start": contract_start,
            "ente_appaltante": buyer_name,
            "ente_id": buyer_id,
            "comune": buyer_city,
            "cap": buyer_cap,
            "regione": regione,
            "aggiudicatario": supplier_name,
            "aggiudicatario_id": supplier_id,
            "procedura": tender.get("procurementMethod", ""),
            "tipo_appalto": tender.get("mainProcurementCategory", ""),
            "cpv": cpv,
            "stato_gara": tender.get("status", ""),
            "n_offerte": tender.get("numberOfTenderers", 0),
            "n_lotti": len(tender.get("lots", [])),
            "source_file": filepath.name,
        }

        records.append(record)

    logger.info(
        f"{filepath.name}: {len(records)} matched, "
        f"{filtered_count} filtered out of {len(releases)} releases"
    )
    return pd.DataFrame(records)


def extract_all_ocds(ocds_dir: Path, output_path: Path | None = None) -> pd.DataFrame:
    """
    Extract and filter all OCDS files in a directory.

    Args:
        ocds_dir: Directory containing OCDS JSON files.
        output_path: Optional path to save CSV output.

    Returns:
        Combined DataFrame from all files.
    """
    files = sorted(ocds_dir.glob("*.json"))
    if not files:
        logger.warning(f"No OCDS files in {ocds_dir}")
        return pd.DataFrame()

    logger.info(f"Processing {len(files)} OCDS files...")
    frames = []

    for f in files:
        df = extract_ocds_file(f)
        if not df.empty:
            frames.append(df)

    if not frames:
        logger.warning("No records extracted from OCDS")
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    logger.info(f"OCDS total: {len(result)} records from {len(frames)} files")

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result.to_csv(output_path, index=False)
        logger.info(f"Saved: {output_path}")

    return result
