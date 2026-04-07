#!/usr/bin/env python3
"""
Unified Gazzetta Scraper with Strategy Pattern and Async Support.
Consolidates all gazzetta_scraper_* variants into a single modular implementation.
"""

import asyncio
import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import AsyncIterator, Dict, List, Optional, Protocol
from urllib.parse import urljoin

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FilterStrategy(str, Enum):
    """Available filtering strategies"""

    ORIGINAL = "original"
    BALANCED = "balanced"
    COMPREHENSIVE = "comprehensive"
    STRICT = "strict"


@dataclass
class ScraperConfig:
    """Configuration for Gazzetta scraper"""

    base_url: str = "http://www.gazzettaufficiale.it"
    max_concurrent: int = 20
    timeout: int = 15
    retry_attempts: int = 3
    min_text_length: int = 500
    filter_strategy: FilterStrategy = FilterStrategy.BALANCED
    headers: Dict[str, str] = None

    def __post_init__(self):
        if self.headers is None:
            self.headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }


class TextFilterProtocol(Protocol):
    """Protocol for text filtering strategies"""

    async def filter_text(self, text: str) -> Optional[str]:
        """Filter text according to strategy"""
        ...


class BaseTextFilter(ABC):
    """Base class for text filtering strategies"""

    @abstractmethod
    async def filter_text(self, text: str) -> Optional[str]:
        """Filter text according to strategy"""
        pass

    def _clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""

        # Remove multiple whitespace and clean
        text = " ".join(text.split())
        text = text.replace("\xa0", " ")
        return text.strip()


class OriginalTextFilter(BaseTextFilter):
    """Original filtering logic - finds thousands of records when working correctly"""

    async def filter_text(self, text: str) -> Optional[str]:
        if not text:
            return None

        text_clean = self._clean_text(text)

        # Original illuminazione filter
        if re.search(r"illuminazion(?:e|i)?", text_clean, re.IGNORECASE):
            return text_clean

        # Videosorveglianza filter
        elif re.search(r"videosorveglianz(?:a|e)?", text_clean, re.IGNORECASE):
            return text_clean

        # Gallery/tunnel filter with exclusions
        elif (
            re.search(r"galleri(?:a|e|i)|tunnel(?:i)?", text_clean, re.IGNORECASE)
            and re.search(r"impiant(?:o|i|istica|iche)", text_clean, re.IGNORECASE)
            and not re.search(r"muse(?:o|ale|i|ali)?", text_clean, re.IGNORECASE)
            and not re.search(r"via Galler(?:ia|ie)?", text_clean, re.IGNORECASE)
            and not re.search(r"MINISTERO DELLA CULTURA", text_clean, re.IGNORECASE)
            and not re.search(r"ferrovia(?:r|ria|rio|rie)", text_clean, re.IGNORECASE)
        ):
            return text_clean

        # Buildings and thermal systems
        elif re.search(
            r"edific(?:io|i|ia|azione|azioni)|termic(?:o|i|a|he)",
            text_clean,
            re.IGNORECASE,
        ):
            return text_clean

        # Electric charging stations
        elif (
            re.search(r"colonnin(?:a|e|i)", text_clean, re.IGNORECASE)
            and re.search(r"elettr(?:ico|ici|ica|iche)", text_clean, re.IGNORECASE)
        ) or (
            re.search(r"ricaric(?:a|he|he)", text_clean, re.IGNORECASE)
            and re.search(r"elettr(?:ico|ici|ica|iche)", text_clean, re.IGNORECASE)
        ):
            return text_clean

        # Parking management
        elif re.search(r"parchegg(?:io|i|e)", text_clean, re.IGNORECASE) and re.search(
            r"gestion(?:e|i)|parcom(?:etro|etri)|parchim(?:etri)",
            text_clean,
            re.IGNORECASE,
        ):
            return text_clean

        return None


class BalancedTextFilter(BaseTextFilter):
    """Balanced filter for infrastructure and public services"""

    async def filter_text(self, text: str) -> Optional[str]:
        if not text:
            return None

        text_clean = self._clean_text(text)
        text_lower = text_clean.lower()

        # Public lighting (more specific)
        if re.search(
            r"illuminazion[ei] pubblic|lampioni|pubblica illuminazione|impianti di illuminazione|corpi illuminanti",
            text_lower,
        ):
            return text_clean

        # Video surveillance (specific)
        elif re.search(
            r"videosorveglian|telecamer[ae]|tvcc|sistema.{0,20}sorveglian", text_lower
        ):
            return text_clean

        # Energy efficiency (more specific)
        elif re.search(
            r"efficientamento energetic|riqualificazione energetic|risparmio energetic",
            text_lower,
        ) or (
            re.search(r"impiant[oi]", text_lower)
            and re.search(r"fotovoltaic|solare|led|termic", text_lower)
        ):
            return text_clean

        # Public buildings + energy/systems (combined)
        elif re.search(
            r"scuol[ae]|municipio|palazzo comunale|biblioteca|ospedale", text_lower
        ) and re.search(
            r"impiant[oi]|manutenzion|ristrutturazion|adeguament|climatizzazion|riscaldament",
            text_lower,
        ):
            return text_clean

        # Electric mobility (specific)
        elif re.search(
            r"colonnin[ae].{0,20}ricaric|ricarica.{0,20}elettric|stazion.{0,20}ricaric|e-mobility",
            text_lower,
        ):
            return text_clean

        # Parking with management/technology
        elif re.search(r"parchegg[io]", text_lower) and re.search(
            r"gestion|parcometr|parchimetr|automat|smart|sensor", text_lower
        ):
            return text_clean

        # Smart city (more specific)
        elif re.search(r"smart city|città intelligente", text_lower) or (
            re.search(r"sensor[ei]|iot|telecontroll|telegestione", text_lower)
            and re.search(r"pubblic|comunale|urban", text_lower)
        ):
            return text_clean

        return None


class ComprehensiveTextFilter(BaseTextFilter):
    """Comprehensive filter that includes broader infrastructure categories"""

    async def filter_text(self, text: str) -> Optional[str]:
        if not text:
            return None

        text_clean = self._clean_text(text)
        text_lower = text_clean.lower()

        # First try balanced filter
        balanced_filter = BalancedTextFilter()
        if await balanced_filter.filter_text(text):
            return text_clean

        # Additional comprehensive patterns
        comprehensive_patterns = [
            r"impianti elettrici|sistemi elettrici",
            r"reti idriche|acquedotto|fognatura",
            r"trasporti pubblici|autobus|metro",
            r"rifiuti|raccolta differenziata|isola ecologica",
            r"verde pubblico|parchi|giardini pubblici",
            r"sicurezza urbana|protezione civile",
            r"digitalizzazione|fibra ottica|banda larga",
        ]

        for pattern in comprehensive_patterns:
            if re.search(pattern, text_lower):
                return text_clean

        return None


class StrictTextFilter(BaseTextFilter):
    """Strict filter with very specific criteria"""

    async def filter_text(self, text: str) -> Optional[str]:
        if not text:
            return None

        text_clean = self._clean_text(text)
        text_lower = text_clean.lower()

        # Only very specific matches
        strict_patterns = [
            r"pubblica illuminazione.{0,50}led",
            r"videosorveglianza.{0,50}urbana",
            r"efficientamento energetico.{0,50}edifici pubblici",
            r"colonnine di ricarica.{0,50}elettrica",
            r"smart city.{0,50}iot",
        ]

        for pattern in strict_patterns:
            if re.search(pattern, text_lower):
                return text_clean

        return None


class FilterFactory:
    """Factory for creating text filter strategies"""

    _strategies = {
        FilterStrategy.ORIGINAL: OriginalTextFilter,
        FilterStrategy.BALANCED: BalancedTextFilter,
        FilterStrategy.COMPREHENSIVE: ComprehensiveTextFilter,
        FilterStrategy.STRICT: StrictTextFilter,
    }

    @classmethod
    def create_filter(cls, strategy: FilterStrategy) -> TextFilterProtocol:
        """Create a text filter based on strategy"""
        filter_class = cls._strategies.get(strategy)
        if not filter_class:
            raise ValueError(f"Unknown filter strategy: {strategy}")
        return filter_class()


@dataclass
class ScrapingResult:
    """Result of scraping operation"""

    url: str
    text: Optional[str]
    success: bool
    error: Optional[str] = None


class AsyncGazzettaScraper:
    """Async scraper for Gazzetta Ufficiale with configurable strategies"""

    def __init__(self, config: ScraperConfig):
        self.config = config
        self.text_filter = FilterFactory.create_filter(config.filter_strategy)
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        """Async context manager entry"""
        connector = aiohttp.TCPConnector(limit=self.config.max_concurrent)
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)

        self.session = aiohttp.ClientSession(
            connector=connector, timeout=timeout, headers=self.config.headers
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()

    async def _fetch_with_retry(self, url: str) -> Optional[str]:
        """Fetch URL with retry logic"""
        for attempt in range(self.config.retry_attempts):
            try:
                async with self.session.get(url) as response:
                    if response.status == 200:
                        return await response.text()
                    else:
                        logger.warning(f"HTTP {response.status} for {url}")
            except Exception as e:
                if attempt == self.config.retry_attempts - 1:
                    logger.error(
                        f"Failed to fetch {url} after {self.config.retry_attempts} attempts: {e}"
                    )
                    return None
                else:
                    await asyncio.sleep(2**attempt)  # Exponential backoff

        return None

    async def _process_detail_link(self, link: str) -> ScrapingResult:
        """Process a single detail link"""
        full_url = urljoin(self.config.base_url, link)

        try:
            html_content = await self._fetch_with_retry(full_url)
            if not html_content:
                return ScrapingResult(full_url, None, False, "Failed to fetch HTML")

            soup = BeautifulSoup(html_content, "html.parser")
            text = soup.get_text()

            # Clean the text
            clean_text = " ".join(text.split())

            # Validate text length and content
            if (
                len(clean_text) < self.config.min_text_length
                or "Gazzetta Ufficiale Home" not in clean_text
            ):
                return ScrapingResult(full_url, None, False, "Invalid content")

            # Apply filter strategy
            filtered_text = await self.text_filter.filter_text(clean_text)

            return ScrapingResult(full_url, filtered_text, True)

        except Exception as e:
            return ScrapingResult(full_url, None, False, str(e))

    async def scrape_links(self, links: List[str]) -> AsyncIterator[ScrapingResult]:
        """Scrape multiple links concurrently"""
        semaphore = asyncio.Semaphore(self.config.max_concurrent)

        async def bounded_process(link):
            async with semaphore:
                return await self._process_detail_link(link)

        tasks = [bounded_process(link) for link in links]

        # Process in batches to avoid overwhelming the server
        batch_size = min(self.config.max_concurrent, len(tasks))

        for i in range(0, len(tasks), batch_size):
            batch = tasks[i : i + batch_size]
            results = await asyncio.gather(*batch, return_exceptions=True)

            for result in results:
                if isinstance(result, ScrapingResult):
                    yield result
                elif isinstance(result, Exception):
                    yield ScrapingResult("", None, False, str(result))

    async def scrape_search_results(self, search_url: str) -> List[str]:
        """Scrape search results page to extract detail links"""
        html_content = await self._fetch_with_retry(search_url)
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, "html.parser")
        links = []

        # Extract links from AVVISI ESITI DI GARA section
        for link_elem in soup.find_all("a", href=True):
            href = link_elem.get("href")
            if href and "/atto/serie_generale/" in href:
                links.append(href)

        return links


class GazzettaScraperFactory:
    """Factory for creating Gazzetta scrapers with different configurations"""

    @staticmethod
    def create_default_scraper() -> AsyncGazzettaScraper:
        """Create scraper with default configuration"""
        config = ScraperConfig()
        return AsyncGazzettaScraper(config)

    @staticmethod
    def create_high_performance_scraper() -> AsyncGazzettaScraper:
        """Create scraper optimized for high performance"""
        config = ScraperConfig(
            max_concurrent=50,
            timeout=10,
            retry_attempts=2,
            filter_strategy=FilterStrategy.BALANCED,
        )
        return AsyncGazzettaScraper(config)

    @staticmethod
    def create_comprehensive_scraper() -> AsyncGazzettaScraper:
        """Create scraper with comprehensive filtering"""
        config = ScraperConfig(
            filter_strategy=FilterStrategy.COMPREHENSIVE, max_concurrent=30
        )
        return AsyncGazzettaScraper(config)

    @staticmethod
    def create_strict_scraper() -> AsyncGazzettaScraper:
        """Create scraper with strict filtering"""
        config = ScraperConfig(filter_strategy=FilterStrategy.STRICT, max_concurrent=40)
        return AsyncGazzettaScraper(config)


# Legacy compatibility class maintaining the original interface
class GazzettaScraper:
    """Legacy synchronous interface for backward compatibility"""

    def __init__(self, resume_session: Optional[str] = None):
        self.config = ScraperConfig()
        self.async_scraper = None

    async def run_async(self) -> pd.DataFrame:
        """Run scraper asynchronously and return results"""
        async with AsyncGazzettaScraper(self.config) as scraper:
            # Implementation would go here
            # This is a placeholder for async execution
            return pd.DataFrame()

    def run(self) -> pd.DataFrame:
        """Legacy synchronous run method"""
        # Run async method in event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self.run_async())
        finally:
            loop.close()


async def main():
    """Example usage of the unified Gazzetta scraper"""
    logger.info("Starting Gazzetta scraper example")

    # Create scraper with default configuration
    config = ScraperConfig(filter_strategy=FilterStrategy.BALANCED)

    async with AsyncGazzettaScraper(config) as scraper:
        # Example: scrape some test links
        test_links = [
            "/atto/serie_generale/caricaDettaglio?caricaDettaglio.codice=example1",
            "/atto/serie_generale/caricaDettaglio?caricaDettaglio.codice=example2",
        ]

        results = []
        async for result in scraper.scrape_links(test_links):
            results.append(result)
            logger.info(f"Processed: {result.url} - Success: {result.success}")

        logger.info(f"Total results: {len(results)}")
        successful = sum(1 for r in results if r.success and r.text)
        logger.info(f"Successful extractions: {successful}")


if __name__ == "__main__":
    asyncio.run(main())
