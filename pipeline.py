#!/usr/bin/env python3
"""
GARE PIPELINE v2
================
Unified pipeline for Italian public procurement data.

Steps:
  1. Download OCDS bulk JSON from ANAC
  2. Download CIG JSON for months not in OCDS
  3. Extract OCDS -> CSV (regex filter, no LLM)
  4. Extract CIG JSON -> CSV
  5. Build CONSIP/ServizioLuce classification
  6. Build unified dataset (merge all sources + fuzzy dedup)
  7. Deploy to dashboard directory

Usage:
  python pipeline.py --full           # Run everything
  python pipeline.py --rebuild-only   # Steps 3-6 only (no download)
  python pipeline.py --extract        # Steps 3-5 only
  python pipeline.py --merge          # Step 6 only
  python pipeline.py --deploy         # Step 7 only
  python pipeline.py --stats          # Show data statistics
  python pipeline.py --enrich         # Add document URLs (Phase 4)
  python pipeline.py --update         # Incremental update (only new data)
  python pipeline.py --update --force # Force full re-extraction
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("pipeline")

# Paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
OCDS_DIR = DATA_DIR / "ocds"
CIG_JSON_DIR = DATA_DIR / "cig_json"
OUTPUT_DIR = DATA_DIR / "output"
CATEGORIE_DIR = OUTPUT_DIR / "categorie"
DASHBOARD_DIR = BASE_DIR.parent / "dashboard_gare" / "data"

# File paths
OCDS_CSV = CATEGORIE_DIR / "gare_filtrate_tutte.csv"
CIG_CSV = CATEGORIE_DIR / "gare_cig_json.csv"
CONSIP_FILE = OUTPUT_DIR / "ServizioLuce.xlsx"
CONSIP_OLD = OUTPUT_DIR / "ServizioLuce_OLD_backup.xlsx"
GAZZETTA_FILE = OUTPUT_DIR / "Lotti_Gazzetta_Optimized.xlsx"
UNIFIED_FILE = CATEGORIE_DIR / "gare_unificate.csv.gz"

# OCDS covers up to this month; CIG JSON for later months
OCDS_END_YEAR = 2025
OCDS_END_MONTH = 8


def step_download_ocds():
    """Step 1: Download OCDS bulk."""
    from src.download.ocds import download_ocds

    logger.info("=" * 60)
    logger.info("STEP 1: Download OCDS bulk")
    stats = download_ocds(OCDS_DIR, end_year=OCDS_END_YEAR, end_month=OCDS_END_MONTH)
    logger.info(f"OCDS: {stats}")


def step_download_cig_json():
    """Step 2: Download CIG JSON for months after OCDS."""
    from src.download.ocds import download_cig_json

    logger.info("=" * 60)
    logger.info("STEP 2: Download CIG JSON")

    now = datetime.now()
    # CIG JSON for months after OCDS ends
    if now.year == OCDS_END_YEAR:
        months = list(range(OCDS_END_MONTH + 1, now.month + 1))
        if months:
            download_cig_json(CIG_JSON_DIR, OCDS_END_YEAR, months)
    else:
        # Rest of OCDS end year
        months_end_year = list(range(OCDS_END_MONTH + 1, 13))
        if months_end_year:
            download_cig_json(CIG_JSON_DIR, OCDS_END_YEAR, months_end_year)
        # Full current year
        for y in range(OCDS_END_YEAR + 1, now.year + 1):
            m_end = now.month if y == now.year else 12
            download_cig_json(CIG_JSON_DIR, y, list(range(1, m_end + 1)))


def step_extract_ocds():
    """Step 3: Extract OCDS -> CSV."""
    from src.extract.ocds import extract_all_ocds

    logger.info("=" * 60)
    logger.info("STEP 3: Extract OCDS")
    CATEGORIE_DIR.mkdir(parents=True, exist_ok=True)
    df = extract_all_ocds(OCDS_DIR, OCDS_CSV)
    logger.info(f"OCDS extracted: {len(df)} records")
    return df


def step_extract_cig_json():
    """Step 4: Extract CIG JSON -> CSV."""
    from src.extract.cig_json import extract_all_cig_json

    logger.info("=" * 60)
    logger.info("STEP 4: Extract CIG JSON")
    df = extract_all_cig_json(CIG_JSON_DIR, CIG_CSV)
    logger.info(f"CIG JSON extracted: {len(df)} records")
    return df


def step_build_consip():
    """Step 5: Build CONSIP/ServizioLuce."""
    from src.extract.consip import build_servizio_luce

    logger.info("=" * 60)
    logger.info("STEP 5: Build CONSIP/ServizioLuce")
    df = build_servizio_luce(
        ocds_csv=OCDS_CSV,
        cig_csv=CIG_CSV if CIG_CSV.exists() else None,
        old_file=CONSIP_OLD if CONSIP_OLD.exists() else None,
        output_path=CONSIP_FILE,
    )
    logger.info(f"CONSIP: {len(df)} records")
    return df


def step_build_unified():
    """Step 6: Build unified dataset."""
    from src.build.unified_dataset import build_unified_dataset

    logger.info("=" * 60)
    logger.info("STEP 6: Build unified dataset")
    df = build_unified_dataset()
    logger.info(f"Unified: {len(df)} records")
    return df


def step_deploy():
    """Step 7: Deploy to dashboard."""
    import shutil

    logger.info("=" * 60)
    logger.info("STEP 7: Deploy to dashboard")

    if not UNIFIED_FILE.exists():
        logger.error(f"Unified file not found: {UNIFIED_FILE}")
        return

    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)

    # Copy unified dataset
    dest = DASHBOARD_DIR / UNIFIED_FILE.name
    shutil.copy2(UNIFIED_FILE, dest)
    logger.info(f"Deployed: {dest}")

    # Copy ServizioLuce if exists
    if CONSIP_FILE.exists():
        shutil.copy2(CONSIP_FILE, DASHBOARD_DIR / CONSIP_FILE.name)
        logger.info(f"Deployed: {DASHBOARD_DIR / CONSIP_FILE.name}")


def step_enrich():
    """Optional: Enrich with document URLs."""
    from src.enrich.documents import enrich_with_documents

    logger.info("=" * 60)
    logger.info("ENRICH: Adding document URLs")
    enriched_path = CATEGORIE_DIR / "gare_unificate_enriched.csv.gz"
    enrich_with_documents(UNIFIED_FILE, enriched_path)


def show_stats():
    """Show data statistics."""
    logger.info("=" * 60)
    logger.info("DATA STATISTICS")

    # OCDS files
    ocds_files = list(OCDS_DIR.glob("*.json")) if OCDS_DIR.exists() else []
    total_mb = sum(f.stat().st_size for f in ocds_files) / (1024 * 1024)
    logger.info(f"OCDS files: {len(ocds_files)} ({total_mb:.0f} MB)")

    # CIG JSON files
    cig_files = list(CIG_JSON_DIR.glob("*.zip")) if CIG_JSON_DIR.exists() else []
    logger.info(f"CIG JSON files: {len(cig_files)}")

    # Extracted CSVs
    for name, path in [
        ("OCDS CSV", OCDS_CSV),
        ("CIG JSON CSV", CIG_CSV),
        ("Gazzetta", GAZZETTA_FILE),
        ("CONSIP", CONSIP_FILE),
        ("Unified", UNIFIED_FILE),
    ]:
        if path.exists():
            try:
                if str(path).endswith(".gz"):
                    df = pd.read_csv(path, compression="gzip", nrows=0)
                elif str(path).endswith(".xlsx"):
                    df = pd.read_excel(path, nrows=0)
                else:
                    df = pd.read_csv(path, nrows=0)
                # Count rows
                import subprocess
                if str(path).endswith(".gz"):
                    result = subprocess.run(
                        ["zcat", str(path)], capture_output=True
                    )
                    n = result.stdout.count(b"\n") - 1
                elif str(path).endswith(".xlsx"):
                    n = len(pd.read_excel(path))
                else:
                    with open(path) as f:
                        n = sum(1 for _ in f) - 1
                size_mb = path.stat().st_size / (1024 * 1024)
                logger.info(f"{name}: {n:,} records ({size_mb:.1f} MB)")
            except Exception as e:
                logger.info(f"{name}: exists but error reading: {e}")
        else:
            logger.info(f"{name}: not found")


def main():
    parser = argparse.ArgumentParser(description="Gare Pipeline v2")
    parser.add_argument("--full", action="store_true", help="Run full pipeline")
    parser.add_argument("--rebuild-only", action="store_true", help="Extract + merge (no download)")
    parser.add_argument("--extract", action="store_true", help="Extract only (steps 3-5)")
    parser.add_argument("--merge", action="store_true", help="Merge only (step 6)")
    parser.add_argument("--deploy", action="store_true", help="Deploy to dashboard")
    parser.add_argument("--enrich", action="store_true", help="Enrich with document URLs")
    parser.add_argument("--update", action="store_true", help="Incremental update (only new data)")
    parser.add_argument("--force", action="store_true", help="Force re-extraction (with --update)")
    parser.add_argument("--stats", action="store_true", help="Show statistics")

    args = parser.parse_args()

    if not any(vars(args).values()):
        parser.print_help()
        sys.exit(1)

    start = time.time()

    if args.stats:
        import pandas as pd
        show_stats()
        return

    if args.full:
        step_download_ocds()
        step_download_cig_json()
        step_extract_ocds()
        step_extract_cig_json()
        step_build_consip()
        step_build_unified()
        step_deploy()

    elif args.rebuild_only:
        step_extract_ocds()
        step_extract_cig_json()
        step_build_consip()
        step_build_unified()

    elif args.extract:
        step_extract_ocds()
        step_extract_cig_json()
        step_build_consip()

    elif args.merge:
        step_build_unified()

    elif args.deploy:
        step_deploy()

    elif args.enrich:
        step_enrich()

    elif args.update:
        from src.update import run_incremental_update
        run_incremental_update(force=args.force)

    elapsed = time.time() - start
    logger.info(f"Pipeline completed in {elapsed / 60:.1f} minutes")


if __name__ == "__main__":
    main()
