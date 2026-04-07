"""
Download OCDS bulk data from ANAC.

Downloads monthly JSON files from the Italian anticorruption authority.
Files are validated after download (JSON parse + min size check).
Already-downloaded valid files are skipped.
"""

import json
import logging
from datetime import datetime
from pathlib import Path

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

OCDS_URL = (
    "https://dati.anticorruzione.it/opendata/download/dataset/ocds"
    "/filesystem/bulk/{year}/{month:02d}.json"
)

START_YEAR = 2021
START_MONTH = 5
MIN_FILE_SIZE = 1000  # bytes


def download_ocds(
    output_dir: Path,
    end_year: int | None = None,
    end_month: int | None = None,
    timeout: int = 120,
) -> dict:
    """
    Download OCDS bulk JSON files from ANAC.

    Args:
        output_dir: Directory to save files.
        end_year: Last year to download (default: current).
        end_month: Last month to download (default: current).
        timeout: Request timeout in seconds.

    Returns:
        dict with keys: downloaded, skipped, errors, total_size_mb
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now()
    end_year = end_year or now.year
    end_month = end_month or now.month

    stats = {"downloaded": 0, "skipped": 0, "errors": 0, "total_size_mb": 0.0}

    for year in range(START_YEAR, end_year + 1):
        m_start = START_MONTH if year == START_YEAR else 1
        m_end = end_month if year == end_year else 12

        for month in range(m_start, m_end + 1):
            fname = f"{year}_{month:02d}.json"
            local = output_dir / fname

            # Skip if already valid
            if local.exists() and local.stat().st_size > MIN_FILE_SIZE:
                try:
                    with open(local, "r", encoding="utf-8") as f:
                        json.load(f)
                    size_mb = local.stat().st_size / (1024 * 1024)
                    stats["skipped"] += 1
                    stats["total_size_mb"] += size_mb
                    logger.info(f"Skip {fname} ({size_mb:.1f} MB)")
                    continue
                except json.JSONDecodeError:
                    logger.warning(f"{fname} corrupted, re-downloading")
                    local.unlink(missing_ok=True)

            url = OCDS_URL.format(year=year, month=month)
            logger.info(f"Downloading {fname}...")

            try:
                resp = requests.get(
                    url,
                    stream=True,
                    verify=False,
                    timeout=timeout,
                    headers={"User-Agent": "gare-pipeline/2.0"},
                )

                if resp.status_code == 200:
                    with open(local, "wb") as f:
                        for chunk in resp.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)

                    # Validate
                    try:
                        with open(local, "r", encoding="utf-8") as f:
                            data = json.load(f)
                        n = len(data.get("releases", []))
                        size_mb = local.stat().st_size / (1024 * 1024)
                        stats["downloaded"] += 1
                        stats["total_size_mb"] += size_mb
                        logger.info(f"OK {fname} ({size_mb:.1f} MB, {n} releases)")
                    except json.JSONDecodeError:
                        local.unlink(missing_ok=True)
                        logger.error(f"{fname}: invalid JSON")
                        stats["errors"] += 1
                elif resp.status_code == 404:
                    logger.warning(f"{fname}: not found (404)")
                    stats["errors"] += 1
                else:
                    logger.error(f"{fname}: HTTP {resp.status_code}")
                    stats["errors"] += 1

            except requests.exceptions.Timeout:
                logger.error(f"{fname}: timeout")
                stats["errors"] += 1
            except Exception as e:
                logger.error(f"{fname}: {type(e).__name__}: {e}")
                stats["errors"] += 1

    logger.info(
        f"OCDS download complete: {stats['downloaded']} new, "
        f"{stats['skipped']} cached, {stats['errors']} errors, "
        f"{stats['total_size_mb']:.1f} MB total"
    )
    return stats


def download_cig_json(
    output_dir: Path,
    year: int,
    months: list[int],
    timeout: int = 120,
) -> dict:
    """
    Download CIG JSON (NDJSON) ZIP files from ANAC.

    These cover months not yet available in OCDS bulk.

    Args:
        output_dir: Directory to save ZIP files.
        year: Year to download.
        months: List of months to download.
        timeout: Request timeout in seconds.

    Returns:
        dict with keys: downloaded, skipped, errors
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    url_tpl = (
        "https://dati.anticorruzione.it/opendata/download/dataset/"
        "cig-{year}/filesystem/cig_json_{year}_{month:02d}.zip"
    )

    stats = {"downloaded": 0, "skipped": 0, "errors": 0}

    for month in months:
        fname = f"cig_json_{year}_{month:02d}.zip"
        local = output_dir / fname

        if local.exists() and local.stat().st_size > 1000:
            logger.info(f"Skip {fname}")
            stats["skipped"] += 1
            continue

        url = url_tpl.format(year=year, month=month)
        logger.info(f"Downloading {fname}...")

        try:
            resp = requests.get(url, verify=False, timeout=timeout,
                                headers={"User-Agent": "gare-pipeline/2.0"})
            if resp.status_code == 200:
                with open(local, "wb") as f:
                    f.write(resp.content)
                stats["downloaded"] += 1
                logger.info(f"OK {fname} ({local.stat().st_size / 1024:.0f} KB)")
            else:
                logger.error(f"{fname}: HTTP {resp.status_code}")
                stats["errors"] += 1
        except Exception as e:
            logger.error(f"{fname}: {e}")
            stats["errors"] += 1

    return stats
