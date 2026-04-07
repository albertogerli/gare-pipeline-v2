"""
Incremental pipeline update.

Only downloads and processes new data since last run.
Tracks state in data/output/.pipeline_state.json.

Usage:
    python -m src.update          # Incremental update
    python -m src.update --force  # Force full re-extraction
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("update")

# Paths (same as pipeline.py)
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
OCDS_DIR = DATA_DIR / "ocds"
CIG_JSON_DIR = DATA_DIR / "cig_json"
OUTPUT_DIR = DATA_DIR / "output"
CATEGORIE_DIR = OUTPUT_DIR / "categorie"

STATE_FILE = OUTPUT_DIR / ".pipeline_state.json"

OCDS_CSV = CATEGORIE_DIR / "gare_filtrate_tutte.csv"
CIG_CSV = CATEGORIE_DIR / "gare_cig_json.csv"
CONSIP_FILE = OUTPUT_DIR / "ServizioLuce.xlsx"
CONSIP_OLD = OUTPUT_DIR / "ServizioLuce_OLD_backup.xlsx"
UNIFIED_FILE = CATEGORIE_DIR / "gare_unificate.csv.gz"

# OCDS coverage boundary
OCDS_END_YEAR = 2025
OCDS_END_MONTH = 8


def _load_state() -> dict:
    """Load pipeline state from disk, or return defaults."""
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Could not load state file: {e}")
    return {}


def _save_state(state: dict) -> None:
    """Persist pipeline state to disk."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, default=str)
    logger.info(f"State saved to {STATE_FILE}")


def _month_key(year: int, month: int) -> str:
    """Return a sortable month key like '2025_08'."""
    return f"{year}_{month:02d}"


def _list_ocds_files() -> list[Path]:
    """List all OCDS JSON files on disk."""
    if not OCDS_DIR.exists():
        return []
    return sorted(OCDS_DIR.glob("*.json"))


def _list_cig_files() -> list[Path]:
    """List all CIG JSON ZIP files on disk."""
    if not CIG_JSON_DIR.exists():
        return []
    return sorted(CIG_JSON_DIR.glob("*.zip"))


def _file_month_key(filepath: Path) -> str | None:
    """
    Extract month key from filename.

    Expected formats: '2025_08.json' or 'cig_json_2025_09.zip'
    """
    stem = filepath.stem
    # Try OCDS format: YYYY_MM
    if len(stem) == 7 and stem[4] == "_":
        return stem
    # Try CIG JSON format: cig_json_YYYY_MM
    parts = stem.split("_")
    if len(parts) >= 4 and parts[0] == "cig" and parts[1] == "json":
        return f"{parts[2]}_{parts[3]}"
    return None


def _get_new_months_to_download() -> tuple[list[tuple[int, int]], list[tuple[int, int]]]:
    """
    Determine which OCDS and CIG JSON months need downloading.

    Returns:
        (ocds_months, cig_months) where each is a list of (year, month)
        tuples representing files not yet on disk.
    """
    now = datetime.now()

    # OCDS: check from START to OCDS_END boundary
    existing_ocds = {_file_month_key(f) for f in _list_ocds_files()}
    new_ocds: list[tuple[int, int]] = []
    for year in range(2021, OCDS_END_YEAR + 1):
        m_start = 5 if year == 2021 else 1
        m_end = OCDS_END_MONTH if year == OCDS_END_YEAR else 12
        for month in range(m_start, m_end + 1):
            key = _month_key(year, month)
            if key not in existing_ocds:
                new_ocds.append((year, month))

    # CIG JSON: months after OCDS boundary up to current
    existing_cig = {_file_month_key(f) for f in _list_cig_files()}
    new_cig: list[tuple[int, int]] = []
    for year in range(OCDS_END_YEAR, now.year + 1):
        m_start = OCDS_END_MONTH + 1 if year == OCDS_END_YEAR else 1
        m_end = now.month if year == now.year else 12
        for month in range(m_start, m_end + 1):
            key = _month_key(year, month)
            if key not in existing_cig:
                new_cig.append((year, month))

    return new_ocds, new_cig


def step_download_new() -> dict:
    """
    Download only OCDS/CIG JSON files not yet on disk.

    Returns:
        Summary dict with counts of new downloads.
    """
    new_ocds, new_cig = _get_new_months_to_download()
    summary = {"ocds_downloaded": 0, "cig_downloaded": 0}

    if new_ocds:
        from src.download.ocds import download_ocds

        logger.info(f"Downloading {len(new_ocds)} new OCDS files...")
        # download_ocds handles skip logic internally; we just call it
        # and it will only fetch what's missing
        stats = download_ocds(
            OCDS_DIR, end_year=OCDS_END_YEAR, end_month=OCDS_END_MONTH
        )
        summary["ocds_downloaded"] = stats.get("downloaded", 0)
    else:
        logger.info("All OCDS files already on disk.")

    if new_cig:
        from src.download.ocds import download_cig_json

        logger.info(f"Downloading {len(new_cig)} new CIG JSON files...")
        # Group by year
        by_year: dict[int, list[int]] = {}
        for year, month in new_cig:
            by_year.setdefault(year, []).append(month)

        for year, months in sorted(by_year.items()):
            stats = download_cig_json(CIG_JSON_DIR, year, months)
            summary["cig_downloaded"] += stats.get("downloaded", 0)
    else:
        logger.info("All CIG JSON files already on disk.")

    return summary


def step_extract_new(state: dict, force: bool = False) -> dict:
    """
    Extract only files newer than last processed state.

    If force=True, re-extracts everything.

    Returns:
        Summary dict with extraction counts.
    """
    summary = {"ocds_files_processed": 0, "cig_files_processed": 0}
    last_ocds = state.get("last_ocds_month", "")
    last_cig = state.get("last_cig_month", "")

    # --- OCDS extraction ---
    ocds_files = _list_ocds_files()
    if force:
        new_ocds = ocds_files
    else:
        new_ocds = [
            f for f in ocds_files
            if (_file_month_key(f) or "") > last_ocds
        ]

    if new_ocds:
        from src.extract.ocds import extract_ocds_file

        import pandas as pd

        logger.info(f"Extracting {len(new_ocds)} OCDS file(s)...")
        frames = []
        for f in new_ocds:
            df = extract_ocds_file(f)
            if not df.empty:
                frames.append(df)
            summary["ocds_files_processed"] += 1

        if frames:
            new_df = pd.concat(frames, ignore_index=True)
            logger.info(f"New OCDS records: {len(new_df)}")

            # Merge with existing CSV if not force
            CATEGORIE_DIR.mkdir(parents=True, exist_ok=True)
            if not force and OCDS_CSV.exists():
                existing_df = pd.read_csv(OCDS_CSV, low_memory=False)
                combined = pd.concat([existing_df, new_df], ignore_index=True)
                # Deduplicate by ocid (OCDS identifier)
                before = len(combined)
                if "ocid" in combined.columns:
                    combined.drop_duplicates(subset=["ocid"], keep="last", inplace=True)
                after = len(combined)
                if before != after:
                    logger.info(f"OCDS dedup: {before} -> {after} ({before - after} removed)")
                combined.to_csv(OCDS_CSV, index=False)
            else:
                # Force or no existing: also include all existing files
                if force:
                    from src.extract.ocds import extract_all_ocds
                    extract_all_ocds(OCDS_DIR, OCDS_CSV)
                else:
                    new_df.to_csv(OCDS_CSV, index=False)

        # Update state with latest processed month
        all_keys = sorted(
            _file_month_key(f) for f in ocds_files if _file_month_key(f)
        )
        if all_keys:
            state["last_ocds_month"] = all_keys[-1]
    else:
        logger.info("No new OCDS files to extract.")

    # --- CIG JSON extraction ---
    cig_files = _list_cig_files()
    if force:
        new_cig = cig_files
    else:
        new_cig = [
            f for f in cig_files
            if (_file_month_key(f) or "") > last_cig
        ]

    if new_cig:
        from src.extract.cig_json import extract_cig_zip

        import pandas as pd

        logger.info(f"Extracting {len(new_cig)} CIG JSON file(s)...")
        frames = []
        for f in new_cig:
            df = extract_cig_zip(f)
            if not df.empty:
                frames.append(df)
            summary["cig_files_processed"] += 1

        if frames:
            new_df = pd.concat(frames, ignore_index=True)
            logger.info(f"New CIG JSON records: {len(new_df)}")

            CATEGORIE_DIR.mkdir(parents=True, exist_ok=True)
            if not force and CIG_CSV.exists():
                existing_df = pd.read_csv(CIG_CSV, low_memory=False)
                combined = pd.concat([existing_df, new_df], ignore_index=True)
                before = len(combined)
                if "cig" in combined.columns:
                    combined.drop_duplicates(subset=["cig"], keep="last", inplace=True)
                after = len(combined)
                if before != after:
                    logger.info(f"CIG dedup: {before} -> {after} ({before - after} removed)")
                combined.to_csv(CIG_CSV, index=False)
            else:
                if force:
                    from src.extract.cig_json import extract_all_cig_json
                    extract_all_cig_json(CIG_JSON_DIR, CIG_CSV)
                else:
                    new_df.to_csv(CIG_CSV, index=False)

        all_keys = sorted(
            _file_month_key(f) for f in cig_files if _file_month_key(f)
        )
        if all_keys:
            state["last_cig_month"] = all_keys[-1]
    else:
        logger.info("No new CIG JSON files to extract.")

    return summary


def step_rebuild_unified(state: dict) -> dict:
    """
    Rebuild CONSIP and unified dataset.

    Returns:
        Summary dict.
    """
    summary = {"unified_records": 0}

    # Build CONSIP
    from src.extract.consip import build_servizio_luce

    logger.info("Rebuilding CONSIP/ServizioLuce...")
    consip_df = build_servizio_luce(
        ocds_csv=OCDS_CSV,
        cig_csv=CIG_CSV if CIG_CSV.exists() else None,
        old_file=CONSIP_OLD if CONSIP_OLD.exists() else None,
        output_path=CONSIP_FILE,
    )
    logger.info(f"CONSIP: {len(consip_df)} records")

    # Build unified
    from src.build.unified_dataset import build_unified_dataset

    logger.info("Rebuilding unified dataset...")
    build_unified_dataset()

    # Count records in output
    if UNIFIED_FILE.exists():
        import pandas as pd
        try:
            df = pd.read_csv(UNIFIED_FILE, compression="gzip", usecols=[0])
            summary["unified_records"] = len(df)
        except Exception:
            pass

    return summary


def run_incremental_update(force: bool = False) -> dict:
    """
    Run the full incremental update pipeline.

    Steps:
        1. Load state
        2. Download new OCDS/CIG JSON files
        3. Extract only new files (or all if force=True)
        4. Rebuild CONSIP + unified dataset
        5. Save updated state

    Args:
        force: If True, re-extract all files regardless of state.

    Returns:
        Combined summary dict of all steps.
    """
    start = time.time()
    state = _load_state()

    logger.info("=" * 60)
    logger.info("INCREMENTAL UPDATE" + (" (FORCE)" if force else ""))
    logger.info(f"Previous state: {state}")
    logger.info("=" * 60)

    # Step 1: Download
    logger.info("--- Step 1: Download new data ---")
    dl_summary = step_download_new()

    # Step 2: Extract
    logger.info("--- Step 2: Extract new data ---")
    ext_summary = step_extract_new(state, force=force)

    # Step 3: Rebuild unified
    needs_rebuild = (
        force
        or dl_summary["ocds_downloaded"] > 0
        or dl_summary["cig_downloaded"] > 0
        or ext_summary["ocds_files_processed"] > 0
        or ext_summary["cig_files_processed"] > 0
    )

    uni_summary = {"unified_records": 0}
    if needs_rebuild:
        logger.info("--- Step 3: Rebuild unified dataset ---")
        uni_summary = step_rebuild_unified(state)
    else:
        logger.info("--- Step 3: No changes detected, skipping rebuild ---")

    # Save state
    state["last_update"] = datetime.now().isoformat()
    state["last_update_forced"] = force
    _save_state(state)

    # Summary
    elapsed = time.time() - start
    combined = {**dl_summary, **ext_summary, **uni_summary, "elapsed_seconds": round(elapsed, 1)}

    logger.info("=" * 60)
    logger.info("UPDATE COMPLETE")
    for k, v in combined.items():
        logger.info(f"  {k}: {v}")
    logger.info(f"  Total time: {elapsed / 60:.1f} minutes")
    logger.info("=" * 60)

    return combined


def main():
    parser = argparse.ArgumentParser(
        description="Incremental pipeline update",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Force full re-extraction (ignore state)",
    )

    args = parser.parse_args()
    run_incremental_update(force=args.force)


if __name__ == "__main__":
    main()
