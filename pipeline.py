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
  6. Enrich durata via Gemini (optional, uses cache)
  7. Build unified dataset (merge all sources + fuzzy dedup + enrichment)
  8. Deploy to dashboard directory

Usage:
  python pipeline.py --full              # Run everything (1-8)
  python pipeline.py --rebuild           # Steps 3-8 (no download)
  python pipeline.py --extract           # Steps 3-5 only
  python pipeline.py --merge             # Step 7 only (uses enrichment cache)
  python pipeline.py --deploy            # Step 8 only
  python pipeline.py --enrich-durata     # Step 6: run Gemini enrichment
  python pipeline.py --update            # Incremental: download new + rebuild
  python pipeline.py --update --force    # Force full re-extraction
  python pipeline.py --stats             # Show data statistics
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("pipeline")

# ============================================================
# PATHS
# ============================================================
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
ENRICHMENT_CACHE = DATA_DIR / "output" / "enrichment_durata.json"

# OCDS covers up to this month; CIG JSON for later months
OCDS_END_YEAR = 2025
OCDS_END_MONTH = 8


# ============================================================
# PIPELINE STEPS
# ============================================================

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
    if now.year == OCDS_END_YEAR:
        months = list(range(OCDS_END_MONTH + 1, now.month + 1))
        if months:
            download_cig_json(CIG_JSON_DIR, OCDS_END_YEAR, months)
    else:
        # Rest of OCDS end year
        months_end_year = list(range(OCDS_END_MONTH + 1, 13))
        if months_end_year:
            download_cig_json(CIG_JSON_DIR, OCDS_END_YEAR, months_end_year)
        # Full subsequent years
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
    logger.info(f"OCDS extracted: {len(df):,} records")
    return df


def step_extract_cig_json():
    """Step 4: Extract CIG JSON -> CSV."""
    from src.extract.cig_json import extract_all_cig_json

    logger.info("=" * 60)
    logger.info("STEP 4: Extract CIG JSON")
    df = extract_all_cig_json(CIG_JSON_DIR, CIG_CSV)
    logger.info(f"CIG JSON extracted: {len(df):,} records")
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
    logger.info(f"CONSIP: {len(df):,} records")
    return df


def step_enrich_durata():
    """Step 6: Run Gemini enrichment for contract duration."""
    import subprocess

    logger.info("=" * 60)
    logger.info("STEP 6: Enrich durata via Gemini")

    script = BASE_DIR / "scripts" / "enrich_durata_gemini.py"
    if not script.exists():
        logger.error(f"Enrichment script not found: {script}")
        return

    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=str(BASE_DIR),
    )
    if result.returncode != 0:
        logger.error(f"Enrichment failed with code {result.returncode}")
    else:
        logger.info("Enrichment completed")


def step_build_unified():
    """Step 7: Build unified dataset (merge + fuzzy dedup + enrichment)."""
    from src.build.unified_dataset import build_unified_dataset

    logger.info("=" * 60)
    logger.info("STEP 7: Build unified dataset")
    output_path = build_unified_dataset()
    logger.info(f"Unified: {output_path}")
    return output_path


def step_deploy():
    """Step 8: Deploy to dashboard directory."""
    logger.info("=" * 60)
    logger.info("STEP 8: Deploy to dashboard")

    if not UNIFIED_FILE.exists():
        logger.error(f"Unified file not found: {UNIFIED_FILE}")
        return

    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)

    # Copy unified dataset
    dest = DASHBOARD_DIR / UNIFIED_FILE.name
    shutil.copy2(UNIFIED_FILE, dest)
    logger.info(f"Deployed: {dest} ({dest.stat().st_size / 1e6:.1f} MB)")

    # Copy ServizioLuce if exists
    if CONSIP_FILE.exists():
        shutil.copy2(CONSIP_FILE, DASHBOARD_DIR / CONSIP_FILE.name)
        logger.info(f"Deployed: {DASHBOARD_DIR / CONSIP_FILE.name}")

    # Generate dashboard KPI data.json
    _generate_dashboard_json()


def _generate_dashboard_json():
    """Generate data.json with KPIs for dashboard homepage."""
    logger.info("   Generating data.json...")
    try:
        df = pd.read_csv(UNIFIED_FILE, compression="gzip", low_memory=False)

        kpi = {
            "totale_gare": int(len(df)),
            "valore_totale": float(df["importo_aggiudicazione"].sum()),
            "aggiudicatari_unici": int(df["aggiudicatario"].dropna().nunique()),
            "comuni_coinvolti": int(df["comune"].dropna().nunique()),
            "gare_consip": int(len(df[df["fonte"] == "CONSIP"])) if "fonte" in df.columns else 0,
            "sconto_medio": float(df["sconto"].dropna().mean()) if df["sconto"].notna().any() else 0,
            "partecipanti_medi": float(df["offerte_ricevute"].dropna().mean()) if df["offerte_ricevute"].notna().any() else 0,
        }

        # By category
        by_cat = []
        if "categoria" in df.columns:
            for cat, group in df.groupby("categoria"):
                by_cat.append({
                    "categoria": str(cat),
                    "num_gare": int(len(group)),
                    "valore": float(group["importo_aggiudicazione"].sum()),
                })

        # By region
        by_reg = []
        if "regione" in df.columns:
            for reg, group in df[df["regione"].notna()].groupby("regione"):
                by_reg.append({
                    "regione": str(reg),
                    "num_gare": int(len(group)),
                    "valore": float(group["importo_aggiudicazione"].sum()),
                })

        # By year
        by_anno = []
        if "anno" in df.columns:
            for anno, group in df[df["anno"].notna()].groupby("anno"):
                by_anno.append({
                    "anno": int(anno),
                    "num_gare": int(len(group)),
                    "valore": float(group["importo_aggiudicazione"].sum()),
                })

        # CONSIP breakdown
        consip_data = []
        if "tipo_accordo" in df.columns and "fonte" in df.columns:
            consip = df[df["fonte"] == "CONSIP"]
            if len(consip) > 0:
                for ta, group in consip.groupby("tipo_accordo"):
                    consip_data.append({
                        "TipoAccordo": str(ta),
                        "num_gare": int(len(group)),
                        "valore": float(group["importo_aggiudicazione"].sum()),
                        "sconto_medio": float(group["sconto"].dropna().mean()) if group["sconto"].notna().any() else 0,
                    })

        data = {
            "kpi": kpi,
            "consip": {"by_tipo": consip_data},
            "by_categoria": by_cat,
            "by_regione": by_reg,
            "by_anno": by_anno,
        }

        out = DASHBOARD_DIR / "data.json"
        with open(out, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"   data.json: {kpi['totale_gare']:,} gare, €{kpi['valore_totale']/1e9:.1f}B")

    except Exception as e:
        logger.error(f"   Error generating data.json: {e}")


def show_stats():
    """Show data statistics."""
    logger.info("=" * 60)
    logger.info("DATA STATISTICS")

    # Source files
    for name, path in [
        ("OCDS JSON", OCDS_DIR),
        ("CIG JSON", CIG_JSON_DIR),
    ]:
        if path.exists():
            files = list(path.glob("*.json")) + list(path.glob("*.zip"))
            total_mb = sum(f.stat().st_size for f in files) / (1024 * 1024)
            logger.info(f"  {name}: {len(files)} files ({total_mb:.0f} MB)")

    # Processed files
    for name, path in [
        ("OCDS CSV", OCDS_CSV),
        ("CIG JSON CSV", CIG_CSV),
        ("Gazzetta", GAZZETTA_FILE),
        ("CONSIP", CONSIP_FILE),
        ("Unified", UNIFIED_FILE),
    ]:
        if path.exists():
            size_mb = path.stat().st_size / (1024 * 1024)
            try:
                if str(path).endswith(".gz"):
                    n = len(pd.read_csv(path, compression="gzip", usecols=[0]))
                elif str(path).endswith(".xlsx"):
                    n = len(pd.read_excel(path, usecols=[0]))
                else:
                    n = len(pd.read_csv(path, usecols=[0]))
                logger.info(f"  {name}: {n:,} records ({size_mb:.1f} MB)")
            except Exception as e:
                logger.info(f"  {name}: {size_mb:.1f} MB (error counting: {e})")
        else:
            logger.info(f"  {name}: not found")

    # Enrichment cache
    if ENRICHMENT_CACHE.exists():
        try:
            with open(ENRICHMENT_CACHE) as f:
                cache = json.load(f)
            items = cache.get("items", {})
            with_durata = sum(1 for v in items.values() if v.get("durata_giorni") is not None)
            logger.info(f"  Enrichment cache: {len(items):,} CIG ({with_durata:,} with durata)")
        except Exception:
            logger.info("  Enrichment cache: exists but unreadable")


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Gare Pipeline v2",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--full", action="store_true", help="Run full pipeline (steps 1-8)")
    parser.add_argument("--rebuild", "--rebuild-only", action="store_true", help="Extract + merge + deploy (steps 3-8)")
    parser.add_argument("--extract", action="store_true", help="Extract only (steps 3-5)")
    parser.add_argument("--merge", action="store_true", help="Merge only (step 7)")
    parser.add_argument("--deploy", action="store_true", help="Deploy to dashboard (step 8)")
    parser.add_argument("--enrich-durata", action="store_true", help="Run Gemini duration enrichment (step 6)")
    parser.add_argument("--update", action="store_true", help="Incremental update (download new + rebuild)")
    parser.add_argument("--force", action="store_true", help="Force re-extraction (with --update)")
    parser.add_argument("--stats", action="store_true", help="Show statistics")

    args = parser.parse_args()

    if not any(vars(args).values()):
        parser.print_help()
        sys.exit(1)

    start = time.time()

    if args.stats:
        show_stats()
        return

    if args.full:
        step_download_ocds()
        step_download_cig_json()
        step_extract_ocds()
        step_extract_cig_json()
        step_build_consip()
        # Skip enrich-durata in full pipeline (takes hours, run separately)
        step_build_unified()
        step_deploy()

    elif args.rebuild:
        step_extract_ocds()
        step_extract_cig_json()
        step_build_consip()
        step_build_unified()
        step_deploy()

    elif args.extract:
        step_extract_ocds()
        step_extract_cig_json()
        step_build_consip()

    elif args.merge:
        step_build_unified()

    elif args.deploy:
        step_deploy()

    elif args.enrich_durata:
        step_enrich_durata()

    elif args.update:
        from src.update import run_incremental_update
        run_incremental_update(force=args.force)

    elapsed = time.time() - start
    logger.info(f"Pipeline completed in {elapsed / 60:.1f} minutes")


if __name__ == "__main__":
    main()
