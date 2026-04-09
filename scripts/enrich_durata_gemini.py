#!/usr/bin/env python3
"""Enrich contract duration using Gemini 3.1 Flash Lite Preview.

Phase 1: Extract duration from oggetto text (fast, cheap)
Phase 2: Google search for disciplinare, then extract (slower)

Results saved incrementally to data/output/enrichment_durata.json
"""

import json
import os
import sys
import time
import hashlib
import logging
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

MODEL = "gemini-3.1-flash-lite-preview"
CACHE_PATH = Path("data/output/enrichment_durata.json")
BATCH_SAVE_EVERY = 50
MAX_WORKERS = 5  # Parallel requests

# API
api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
if not api_key:
    print("ERROR: Set GEMINI_API_KEY or GOOGLE_API_KEY")
    sys.exit(1)

import google.generativeai as genai
genai.configure(api_key=api_key)
model = genai.GenerativeModel(MODEL)

SYSTEM_PROMPT = """Sei un analista di contratti pubblici italiani. Dato il testo di una gara d'appalto, estrai la durata del contratto.

Regole:
- "triennale"=1095gg, "biennale"=730, "quinquennale"=1825, "annuale"=365, "semestrale"=180
- "36 mesi"=1080, "24 mesi"=720, "18 mesi"=540, "12 mesi"=360, "6 mesi"=180
- Se c'è rinnovo/proroga quantificato: durata_max = base + rinnovo
- "proroga tecnica" senza durata: durata_max = null
- NON inventare. Se non c'è info sulla durata, restituisci null per entrambi.
- Cerca pattern: "durata X mesi/anni", "per il periodo", "dal...al...", "per X anni"

Rispondi SOLO con JSON valido:
{"durata_giorni": number|null, "durata_max_giorni": number|null, "confidence": number}"""


def load_cache() -> dict:
    if CACHE_PATH.exists():
        with open(CACHE_PATH) as f:
            return json.load(f)
    return {"items": {}, "stats": {}}


def save_cache(cache: dict):
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE_PATH, "w") as f:
        json.dump(cache, f, ensure_ascii=False)


def extract_duration(cig: str, oggetto: str, categoria: str = "") -> dict:
    """Call Gemini to extract duration from text."""
    user_msg = f"CIG: {cig}\nCategoria: {categoria}\nTesto: {oggetto[:600]}"

    for attempt in range(3):
        try:
            response = model.generate_content(
                [{"role": "user", "parts": [{"text": SYSTEM_PROMPT + "\n\n" + user_msg}]}],
                generation_config=genai.GenerationConfig(
                    temperature=0.0,
                    max_output_tokens=100,
                    response_mime_type="application/json",
                ),
            )
            parsed = json.loads(response.text.strip())

            tokens_in = getattr(response.usage_metadata, 'prompt_token_count', 0)
            tokens_out = getattr(response.usage_metadata, 'candidates_token_count', 0)

            return {
                "durata_giorni": parsed.get("durata_giorni"),
                "durata_max_giorni": parsed.get("durata_max_giorni"),
                "confidence": parsed.get("confidence", 0),
                "tokens_in": tokens_in,
                "tokens_out": tokens_out,
                "status": "ok",
            }
        except Exception as e:
            if attempt == 2:
                return {"durata_giorni": None, "durata_max_giorni": None, "confidence": 0,
                        "tokens_in": 0, "tokens_out": 0, "status": f"error: {str(e)[:80]}"}
            time.sleep(1 * (attempt + 1))

    return {"durata_giorni": None, "durata_max_giorni": None, "confidence": 0,
            "tokens_in": 0, "tokens_out": 0, "status": "error"}


def process_cig(row: dict, cache_items: dict) -> tuple[str, dict]:
    """Process a single CIG. Returns (cig, result)."""
    cig = row["cig"]

    # Check cache
    if cig in cache_items:
        existing = cache_items[cig]
        if existing.get("durata_giorni") is not None or existing.get("status") == "no_text":
            return cig, None  # Already done

    oggetto = row.get("oggetto", "")
    if not oggetto or len(str(oggetto)) < 10 or str(oggetto) == "nan":
        return cig, {"durata_giorni": None, "durata_max_giorni": None, "confidence": 0,
                      "tokens_in": 0, "tokens_out": 0, "status": "no_text"}

    result = extract_duration(cig, str(oggetto), str(row.get("categoria", "")))
    return cig, result


def main():
    logger.info("Loading dataset...")
    df = pd.read_csv(
        "data/output/categorie/gare_unificate.csv.gz",
        compression="gzip",
        usecols=["cig", "oggetto", "categoria", "data_scadenza", "durata_appalto"],
        low_memory=False,
    )
    logger.info(f"Total records: {len(df):,}")

    # Filter: no scadenza, has CIG, has oggetto
    mask = (
        df["data_scadenza"].isna()
        & df["cig"].notna()
        & df["oggetto"].notna()
        & (df["oggetto"].str.len() > 10)
    )
    candidates = df[mask].copy()
    # Normalize CIG (take first if multiple)
    candidates["cig"] = candidates["cig"].astype(str).str.split(";").str[0].str.strip()
    # Deduplicate by CIG
    candidates = candidates.drop_duplicates(subset="cig")
    logger.info(f"Candidates without scadenza: {len(candidates):,}")

    # Load cache
    cache = load_cache()
    items = cache.setdefault("items", {})
    stats = cache.setdefault("stats", {})

    # Filter out already-processed CIGs
    to_process = candidates[~candidates["cig"].isin(items)].to_dict("records")
    logger.info(f"Already cached: {len(candidates) - len(to_process):,}")
    logger.info(f"To process: {len(to_process):,}")

    if not to_process:
        logger.info("Nothing to process!")
        return

    # Process with thread pool
    total_tokens_in = 0
    total_tokens_out = 0
    found = 0
    errors = 0
    processed = 0
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_cig, row, items): row["cig"]
            for row in to_process
        }

        for future in as_completed(futures):
            try:
                cig, result = future.result()
            except Exception as e:
                cig = futures[future]
                result = {"durata_giorni": None, "status": f"exception: {str(e)[:60]}"}

            if result is None:
                continue  # Was cached

            items[cig] = result
            processed += 1
            total_tokens_in += result.get("tokens_in", 0)
            total_tokens_out += result.get("tokens_out", 0)

            if result.get("durata_giorni") is not None:
                found += 1
            if "error" in result.get("status", ""):
                errors += 1

            # Progress
            if processed % 100 == 0:
                elapsed = time.time() - t0
                rate = processed / elapsed
                eta = (len(to_process) - processed) / rate / 60 if rate > 0 else 0
                logger.info(
                    f"  {processed:,}/{len(to_process):,} "
                    f"({found} found, {errors} errors, "
                    f"{rate:.1f}/s, ETA {eta:.0f}min)"
                )

            # Save periodically
            if processed % BATCH_SAVE_EVERY == 0:
                cache["items"] = items
                cache["stats"] = {
                    "total_processed": len(items),
                    "total_tokens_in": total_tokens_in,
                    "total_tokens_out": total_tokens_out,
                    "found_durata": found,
                    "errors": errors,
                    "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
                save_cache(cache)

    # Final save
    elapsed = time.time() - t0
    cache["items"] = items
    cache["stats"] = {
        "total_processed": len(items),
        "total_tokens_in": total_tokens_in,
        "total_tokens_out": total_tokens_out,
        "found_durata": found,
        "errors": errors,
        "elapsed_seconds": round(elapsed, 1),
        "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    save_cache(cache)

    # Summary
    total_tokens = total_tokens_in + total_tokens_out
    cost = total_tokens * 0.00001 / 1000  # Approximate
    logger.info(f"\n{'='*60}")
    logger.info(f"DONE: {processed:,} CIG in {elapsed/60:.1f} min")
    logger.info(f"Found duration: {found:,} ({found/max(1,processed)*100:.1f}%)")
    logger.info(f"Errors: {errors:,}")
    logger.info(f"Tokens: {total_tokens:,} (~${cost:.4f})")
    logger.info(f"Cache total: {len(items):,} CIG")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()
