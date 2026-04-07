"""Unified Gazzetta Ufficiale scraper module."""

import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, Timeout
from urllib3.util.retry import Retry

from ..config.config import Config


def filter_text(text: str) -> Optional[str]:
    """Filter text for relevant procurement content.

    Args:
        text: Text to filter

    Returns:
        Original text if matches criteria, None otherwise
    """
    # Illumination keywords
    if re.search(r"illuminazion(?:e|i)?", text, re.IGNORECASE):
        return text

    # Video surveillance
    elif re.search(r"videosorveglianz(?:a|e)?", text, re.IGNORECASE):
        return text

    # Galleries and tunnels with infrastructure
    elif (
        re.search(r"galleri(?:a|e|i)|tunnel(?:i)?", text, re.IGNORECASE)
        and re.search(r"impiant(?:o|i|istica|iche)", text, re.IGNORECASE)
        and not re.search(r"muse(?:o|ale|i|ali)?", text, re.IGNORECASE)
        and not re.search(r"via Galler(?:ia|ie)?", text, re.IGNORECASE)
        and not re.search(r"MINISTERO DELLA CULTURA", text, re.IGNORECASE)
        and not re.search(r"ferrovia(?:r|ria|rio|rie)", text, re.IGNORECASE)
    ):
        return text

    # Buildings and thermal systems
    elif re.search(
        r"edific(?:io|i|ia|azione|azioni)|termic(?:o|i|a|he)", text, re.IGNORECASE
    ):
        return text

    # Electric charging stations
    elif (
        re.search(r"colonnin(?:a|e|i)", text, re.IGNORECASE)
        and re.search(r"elettr(?:ico|ici|ica|iche)", text, re.IGNORECASE)
    ) or (
        re.search(r"ricaric(?:a|he|he)", text, re.IGNORECASE)
        and re.search(r"elettr(?:ico|ici|ica|iche)", text, re.IGNORECASE)
    ):
        return text

    # Parking with management systems
    elif re.search(r"parchegg(?:io|i|e)", text, re.IGNORECASE) and re.search(
        r"gestion(?:e|i)|parcom(?:etro|etri)|parchim(?:etro|etri)", text, re.IGNORECASE
    ):
        return text

    return None


def create_session() -> requests.Session:
    """Create a requests session with retry strategy.

    Returns:
        Configured requests session
    """
    session = requests.Session()
    retries = Retry(
        total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def read_html_with_timeout(url: str, timeout_sec: int = 560) -> Optional[str]:
    """Read HTML content with timeout and error handling.

    Args:
        url: URL to fetch
        timeout_sec: Timeout in seconds

    Returns:
        HTML content or None if failed
    """
    try:
        response = requests.get(url, timeout=timeout_sec)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Error reading {url}: {e}")
        return None


class GazzettaScraper:
    """Unified Gazzetta Ufficiale scraper."""

    def __init__(self, config: Optional[Config] = None):
        """Initialize scraper with configuration.

        Args:
            config: Configuration object, defaults to global config
        """
        self.config = config or Config()
        self.session = create_session()

    def scrape_year_page(self, year: int) -> List[dict]:
        """Scrape a specific year page.

        Args:
            year: Year to scrape

        Returns:
            List of procurement records
        """
        url = f"https://www.gazzettaufficiale.it/atti/serie_generale/caricaDettaglioAtto/originario?atto.dataPubblicazioneGazzetta=01/01/{year}&atto.codiceRedazionale=&atto.numeroAtto=&atto.tipoAtto=&atto.dataAtto=&atto.numeroGU=&atto.annoGU={year}&atto.categoriaBando=&atto.sottoCategoria1=&atto.sottoCategoria2=&atto.regione=&atto.provincia=&atto.comune=&atto.denominazioneAmministrazione=&atto.tipologiaAppalto=&atto.oggetto=&atto.importo=&atto.scadenza=&atto.codiceIdentificativoGara=&paginazione.paginaCorrente=0&paginazione.pagineTotali=&paginazione.elementiTotali=&paginazione.elementiPagina=&azione=carica"

        html_content = read_html_with_timeout(url)
        if not html_content:
            return []

        # Parse and extract data
        results = self._parse_html_content(html_content, year)
        print(f"Scraped {len(results)} records for year {year}")

        return results

    def _parse_html_content(self, html_content: str, year: int) -> List[dict]:
        """Parse HTML content to extract procurement data.

        Args:
            html_content: HTML content to parse
            year: Year being processed

        Returns:
            List of extracted records
        """
        soup = BeautifulSoup(html_content, "html.parser")
        results = []

        # Find all tender entries (implement specific parsing logic based on HTML structure)
        # This is a simplified version - actual implementation would depend on the HTML structure

        tender_elements = soup.find_all("div", class_="tender-item")  # Adjust selector

        for element in tender_elements:
            # Extract data fields
            text_content = element.get_text(strip=True)

            # Apply filtering
            if filter_text(text_content):
                record = {
                    "testo": text_content,
                    "anno": year,
                    "url": element.find("a")["href"] if element.find("a") else "",
                    # Add other fields as needed
                }
                results.append(record)

        return results

    def scrape_date_range(
        self, start_year: Optional[int] = None, end_year: Optional[int] = None
    ) -> pd.DataFrame:
        """Scrape data for a date range.

        Args:
            start_year: Start year (defaults to config)
            end_year: End year (defaults to config)

        Returns:
            DataFrame with scraped data
        """
        start_year = start_year or self.config.GAZZETTA_START_YEAR
        end_year = end_year or self.config.GAZZETTA_LAST_YEAR

        all_results = []

        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=self.config.MAX_WORKERS) as executor:
            futures = {
                executor.submit(self.scrape_year_page, year): year
                for year in range(start_year, end_year + 1)
            }

            for future in as_completed(futures):
                year = futures[future]
                try:
                    results = future.result()
                    all_results.extend(results)
                    print(f"Completed year {year}: {len(results)} records")
                except Exception as e:
                    print(f"Error processing year {year}: {e}")

        return pd.DataFrame(all_results)

    @staticmethod
    def run():
        """Run the scraper with default configuration."""
        scraper = GazzettaScraper()
        config = Config()

        # Ensure output directory exists
        os.makedirs(config.TEMP_DIR, exist_ok=True)

        output_path = os.path.join(config.TEMP_DIR, config.LOTTI_RAW)

        # Check if file already exists
        if os.path.exists(output_path):
            print(f"File {output_path} already exists. Skipping scrape.")
            return

        print("Starting Gazzetta scraping...")
        df = scraper.scrape_date_range()

        # Save results
        df.to_excel(output_path, index=False)
        print(f"Scraping complete. Saved {len(df)} records to {output_path}")


if __name__ == "__main__":
    GazzettaScraper.run()
