#!/usr/bin/env python3
"""
Unified OCDS Downloader with Async Support and Flexible Configuration.
Consolidates all download_ocds_* variants into a single modular implementation.
"""

import asyncio
import json
import logging
import ssl
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import AsyncIterator, Dict, List, Optional, Tuple

import aiofiles
import aiohttp
import certifi
import urllib3

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Disable SSL warnings for public datasets
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class DownloadStrategy(str, Enum):
    """Available download strategies"""

    SIMPLE = "simple"
    PROGRESS = "progress"
    RESUME = "resume"
    BULK = "bulk"


class SSLMode(str, Enum):
    """SSL verification modes"""

    VERIFY = "verify"
    IGNORE = "ignore"
    CUSTOM_CA = "custom_ca"


@dataclass
class OCDSConfig:
    """Configuration for OCDS downloader"""

    base_url: str = (
        "https://dati.anticorruzione.it/opendata/download/dataset/ocds/filesystem/bulk"
    )
    download_dir: Path = Path("data/ocds")
    max_concurrent: int = 3
    timeout: int = 120
    chunk_size: int = 8192
    ssl_mode: SSLMode = SSLMode.IGNORE
    retry_attempts: int = 3
    verify_json: bool = True
    start_year: int = 2021
    start_month: int = 5
    end_year: Optional[int] = None
    end_month: Optional[int] = None
    headers: Dict[str, str] = None

    def __post_init__(self):
        if self.headers is None:
            self.headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }

        if self.end_year is None:
            self.end_year = datetime.now().year

        if self.end_month is None:
            self.end_month = datetime.now().month

        # Ensure download directory exists
        self.download_dir.mkdir(parents=True, exist_ok=True)


@dataclass
class DownloadResult:
    """Result of a download operation"""

    year: int
    month: int
    success: bool
    file_path: Optional[Path] = None
    file_size: Optional[int] = None
    error: Optional[str] = None
    skipped: bool = False


class BaseOCDSDownloader(ABC):
    """Base class for OCDS downloading strategies"""

    def __init__(self, config: OCDSConfig):
        self.config = config

    @abstractmethod
    async def download_file(
        self, session: aiohttp.ClientSession, year: int, month: int
    ) -> DownloadResult:
        """Download a single OCDS file"""
        pass

    def _get_file_url(self, year: int, month: int) -> str:
        """Generate URL for a specific year/month"""
        return f"{self.config.base_url}/{year}/{month:02d}.json"

    def _get_local_path(self, year: int, month: int) -> Path:
        """Generate local file path for a specific year/month"""
        return self.config.download_dir / f"{year}_{month:02d}.json"

    def _create_ssl_context(self) -> Optional[ssl.SSLContext]:
        """Create SSL context based on configuration"""
        if self.config.ssl_mode == SSLMode.VERIFY:
            context = ssl.create_default_context(cafile=certifi.where())
            return context
        elif self.config.ssl_mode == SSLMode.IGNORE:
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            return context
        elif self.config.ssl_mode == SSLMode.CUSTOM_CA:
            return ssl.create_default_context(cafile=certifi.where())

        return None

    async def _verify_json_file(self, file_path: Path) -> bool:
        """Verify that downloaded file is valid JSON"""
        if not self.config.verify_json:
            return True

        try:
            async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
                content = await f.read()
                json.loads(content)
            return True
        except (json.JSONDecodeError, Exception) as e:
            logger.warning(f"JSON verification failed for {file_path}: {e}")
            return False


class SimpleOCDSDownloader(BaseOCDSDownloader):
    """Simple downloader without progress tracking"""

    async def download_file(
        self, session: aiohttp.ClientSession, year: int, month: int
    ) -> DownloadResult:
        url = self._get_file_url(year, month)
        local_path = self._get_local_path(year, month)

        # Check if file already exists and is valid
        if local_path.exists() and local_path.stat().st_size > 1000:
            if await self._verify_json_file(local_path):
                logger.info(
                    f"⏭️  {local_path.name} already exists ({local_path.stat().st_size / (1024 * 1024):.1f} MB)"
                )
                return DownloadResult(
                    year,
                    month,
                    True,
                    local_path,
                    local_path.stat().st_size,
                    skipped=True,
                )

        try:
            async with session.get(url) as response:
                if response.status == 200:
                    async with aiofiles.open(local_path, "wb") as f:
                        async for chunk in response.content.iter_chunked(
                            self.config.chunk_size
                        ):
                            await f.write(chunk)

                    # Verify JSON if required
                    if await self._verify_json_file(local_path):
                        file_size = local_path.stat().st_size
                        logger.info(
                            f"✅ Downloaded {local_path.name} ({file_size / (1024 * 1024):.1f} MB)"
                        )
                        return DownloadResult(year, month, True, local_path, file_size)
                    else:
                        # Remove corrupted file
                        local_path.unlink(missing_ok=True)
                        return DownloadResult(
                            year, month, False, error="JSON verification failed"
                        )
                else:
                    logger.error(f"HTTP {response.status} for {url}")
                    return DownloadResult(
                        year, month, False, error=f"HTTP {response.status}"
                    )

        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            return DownloadResult(year, month, False, error=str(e))


class ProgressOCDSDownloader(BaseOCDSDownloader):
    """Downloader with detailed progress tracking"""

    async def download_file(
        self, session: aiohttp.ClientSession, year: int, month: int
    ) -> DownloadResult:
        url = self._get_file_url(year, month)
        local_path = self._get_local_path(year, month)

        # Check if file already exists and is valid
        if local_path.exists() and local_path.stat().st_size > 1000:
            if await self._verify_json_file(local_path):
                file_size = local_path.stat().st_size
                logger.info(
                    f"⏭️  {local_path.name} already exists ({file_size / (1024 * 1024):.1f} MB)"
                )
                return DownloadResult(
                    year, month, True, local_path, file_size, skipped=True
                )

        logger.info(f"📥 Downloading {local_path.name}...", end="")

        try:
            async with session.get(url) as response:
                if response.status == 200:
                    total_size = int(response.headers.get("Content-Length", 0))
                    downloaded = 0

                    async with aiofiles.open(local_path, "wb") as f:
                        async for chunk in response.content.iter_chunked(
                            self.config.chunk_size
                        ):
                            await f.write(chunk)
                            downloaded += len(chunk)

                            if total_size > 0:
                                percent = (downloaded / total_size) * 100
                                print(
                                    f"\r📥 Downloading {local_path.name}... {percent:.1f}%",
                                    end="",
                                )

                    print()  # New line after progress

                    # Verify JSON if required
                    if await self._verify_json_file(local_path):
                        file_size = local_path.stat().st_size
                        logger.info(
                            f"✅ Downloaded {local_path.name} ({file_size / (1024 * 1024):.1f} MB)"
                        )
                        return DownloadResult(year, month, True, local_path, file_size)
                    else:
                        local_path.unlink(missing_ok=True)
                        return DownloadResult(
                            year, month, False, error="JSON verification failed"
                        )
                else:
                    print()  # New line after progress indicator
                    logger.error(f"HTTP {response.status} for {url}")
                    return DownloadResult(
                        year, month, False, error=f"HTTP {response.status}"
                    )

        except Exception as e:
            print()  # New line after progress indicator
            logger.error(f"Error downloading {url}: {e}")
            return DownloadResult(year, month, False, error=str(e))


class ResumeOCDSDownloader(BaseOCDSDownloader):
    """Downloader with resume capability for partial downloads"""

    async def download_file(
        self, session: aiohttp.ClientSession, year: int, month: int
    ) -> DownloadResult:
        url = self._get_file_url(year, month)
        local_path = self._get_local_path(year, month)
        temp_path = local_path.with_suffix(".tmp")

        # Check if file already exists and is valid
        if local_path.exists() and local_path.stat().st_size > 1000:
            if await self._verify_json_file(local_path):
                file_size = local_path.stat().st_size
                logger.info(
                    f"⏭️  {local_path.name} already exists ({file_size / (1024 * 1024):.1f} MB)"
                )
                return DownloadResult(
                    year, month, True, local_path, file_size, skipped=True
                )

        # Check for partial download
        resume_pos = 0
        if temp_path.exists():
            resume_pos = temp_path.stat().st_size
            logger.info(f"🔄 Resuming download from {resume_pos} bytes")

        headers = self.config.headers.copy()
        if resume_pos > 0:
            headers["Range"] = f"bytes={resume_pos}-"

        try:
            async with session.get(url, headers=headers) as response:
                if response.status in (200, 206):  # 206 = Partial Content
                    mode = "ab" if resume_pos > 0 else "wb"

                    async with aiofiles.open(temp_path, mode) as f:
                        async for chunk in response.content.iter_chunked(
                            self.config.chunk_size
                        ):
                            await f.write(chunk)

                    # Move temp file to final location
                    temp_path.rename(local_path)

                    # Verify JSON if required
                    if await self._verify_json_file(local_path):
                        file_size = local_path.stat().st_size
                        logger.info(
                            f"✅ Downloaded {local_path.name} ({file_size / (1024 * 1024):.1f} MB)"
                        )
                        return DownloadResult(year, month, True, local_path, file_size)
                    else:
                        local_path.unlink(missing_ok=True)
                        return DownloadResult(
                            year, month, False, error="JSON verification failed"
                        )
                else:
                    logger.error(f"HTTP {response.status} for {url}")
                    return DownloadResult(
                        year, month, False, error=f"HTTP {response.status}"
                    )

        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            return DownloadResult(year, month, False, error=str(e))


class BulkOCDSDownloader(BaseOCDSDownloader):
    """Bulk downloader optimized for downloading multiple files"""

    async def download_file(
        self, session: aiohttp.ClientSession, year: int, month: int
    ) -> DownloadResult:
        # Use simple strategy for individual files in bulk mode
        simple_downloader = SimpleOCDSDownloader(self.config)
        return await simple_downloader.download_file(session, year, month)


class DownloaderFactory:
    """Factory for creating OCDS downloaders with different strategies"""

    _strategies = {
        DownloadStrategy.SIMPLE: SimpleOCDSDownloader,
        DownloadStrategy.PROGRESS: ProgressOCDSDownloader,
        DownloadStrategy.RESUME: ResumeOCDSDownloader,
        DownloadStrategy.BULK: BulkOCDSDownloader,
    }

    @classmethod
    def create_downloader(
        cls, strategy: DownloadStrategy, config: OCDSConfig
    ) -> BaseOCDSDownloader:
        """Create a downloader based on strategy"""
        downloader_class = cls._strategies.get(strategy)
        if not downloader_class:
            raise ValueError(f"Unknown download strategy: {strategy}")
        return downloader_class(config)


class AsyncOCDSDownloader:
    """Main async OCDS downloader with configurable strategies"""

    def __init__(
        self, config: OCDSConfig, strategy: DownloadStrategy = DownloadStrategy.SIMPLE
    ):
        self.config = config
        self.downloader = DownloaderFactory.create_downloader(strategy, config)
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        """Async context manager entry"""
        ssl_context = self.downloader._create_ssl_context()
        connector = aiohttp.TCPConnector(
            limit=self.config.max_concurrent, ssl_context=ssl_context
        )
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)

        self.session = aiohttp.ClientSession(
            connector=connector, timeout=timeout, headers=self.config.headers
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()

    def _generate_download_periods(self) -> List[Tuple[int, int]]:
        """Generate list of (year, month) tuples to download"""
        periods = []

        for year in range(self.config.start_year, self.config.end_year + 1):
            start_month = (
                self.config.start_month if year == self.config.start_year else 1
            )
            end_month = self.config.end_month if year == self.config.end_year else 12

            for month in range(start_month, end_month + 1):
                periods.append((year, month))

        return periods

    async def download_single(self, year: int, month: int) -> DownloadResult:
        """Download a single OCDS file"""
        return await self.downloader.download_file(self.session, year, month)

    async def download_all(self) -> AsyncIterator[DownloadResult]:
        """Download all OCDS files for the configured period"""
        periods = self._generate_download_periods()

        logger.info(
            f"📅 Period: {self.config.start_year}/{self.config.start_month:02d} - "
            f"{self.config.end_year}/{self.config.end_month:02d}"
        )
        logger.info(f"📂 Download directory: {self.config.download_dir}")
        logger.info("-" * 70)

        semaphore = asyncio.Semaphore(self.config.max_concurrent)

        async def bounded_download(year_month):
            async with semaphore:
                year, month = year_month
                return await self.download_single(year, month)

        tasks = [bounded_download(period) for period in periods]

        for coro in asyncio.as_completed(tasks):
            result = await coro
            yield result

    async def download_summary(self) -> Dict[str, int]:
        """Download all files and return summary statistics"""
        downloaded = 0
        skipped = 0
        errors = 0
        total_size = 0

        async for result in self.download_all():
            if result.success:
                if result.skipped:
                    skipped += 1
                else:
                    downloaded += 1

                if result.file_size:
                    total_size += result.file_size
            else:
                errors += 1
                logger.error(
                    f"❌ Failed to download {result.year}_{result.month:02d}.json: {result.error}"
                )

        return {
            "downloaded": downloaded,
            "skipped": skipped,
            "errors": errors,
            "total_size_mb": total_size / (1024 * 1024),
        }


# Convenience factory functions
def create_simple_downloader(download_dir: str = "data/ocds") -> AsyncOCDSDownloader:
    """Create a simple OCDS downloader"""
    config = OCDSConfig(download_dir=Path(download_dir))
    return AsyncOCDSDownloader(config, DownloadStrategy.SIMPLE)


def create_progress_downloader(download_dir: str = "data/ocds") -> AsyncOCDSDownloader:
    """Create an OCDS downloader with progress tracking"""
    config = OCDSConfig(download_dir=Path(download_dir))
    return AsyncOCDSDownloader(config, DownloadStrategy.PROGRESS)


def create_resume_downloader(download_dir: str = "data/ocds") -> AsyncOCDSDownloader:
    """Create an OCDS downloader with resume capability"""
    config = OCDSConfig(download_dir=Path(download_dir))
    return AsyncOCDSDownloader(config, DownloadStrategy.RESUME)


def create_bulk_downloader(
    download_dir: str = "data/ocds", max_concurrent: int = 5
) -> AsyncOCDSDownloader:
    """Create a bulk OCDS downloader for high-throughput downloads"""
    config = OCDSConfig(download_dir=Path(download_dir), max_concurrent=max_concurrent)
    return AsyncOCDSDownloader(config, DownloadStrategy.BULK)


# Legacy synchronous interface for backward compatibility
class OCDSDownloader:
    """Legacy synchronous interface"""

    def __init__(self, download_dir: str = "data/ocds"):
        self.config = OCDSConfig(download_dir=Path(download_dir))
        self.async_downloader = AsyncOCDSDownloader(self.config)

    def download_ocds(self) -> Dict[str, int]:
        """Legacy synchronous download method"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:

            async def run_download():
                async with self.async_downloader as downloader:
                    return await downloader.download_summary()

            return loop.run_until_complete(run_download())
        finally:
            loop.close()


async def main():
    """Example usage of the unified OCDS downloader"""
    logger.info("Starting OCDS downloader example")

    # Create downloader with progress tracking
    config = OCDSConfig(
        start_year=2023, start_month=1, end_year=2023, end_month=3, max_concurrent=2
    )

    async with AsyncOCDSDownloader(config, DownloadStrategy.PROGRESS) as downloader:
        summary = await downloader.download_summary()

        logger.info("=" * 50)
        logger.info(f"✅ Download completed")
        logger.info(f"📁 Files downloaded: {summary['downloaded']}")
        logger.info(f"⏭️  Files skipped: {summary['skipped']}")
        logger.info(f"❌ Errors: {summary['errors']}")
        logger.info(f"💾 Total size: {summary['total_size_mb']:.1f} MB")
        logger.info("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
