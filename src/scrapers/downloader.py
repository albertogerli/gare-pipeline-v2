"""
Download manager per CIG e OCDS con supporto checkpoint.
"""

import logging
import os
import time
import zipfile
from pathlib import Path
from typing import List, Optional, Set

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from config.settings import config
from src.utils.checkpoint import CheckpointManager, TaskStatus
from src.utils.logging_config import ProgressLogger
from src.utils.resilient import ResilientBrowser, retry_on_exception

logger = logging.getLogger(__name__)
progress_logger = ProgressLogger("downloader")


class DownloadManager:
    """
    Gestisce i download di CIG e OCDS con checkpoint e resume.
    """

    # URL patterns
    CIG_BASE_URL = (
        "https://dati.anticorruzione.it/opendata/download/dataset/cig-{year}/"
        "filesystem/cig_csv_{year}_{month:02d}.zip"
    )

    EXTRA_URLS = [
        "https://dati.anticorruzione.it/opendata/download/dataset/aggiudicatari/filesystem/aggiudicatari_csv.zip",
        "https://dati.anticorruzione.it/opendata/download/dataset/aggiudicazioni/filesystem/aggiudicazioni_csv.zip",
    ]

    OCDS_BASE_URL = (
        "https://dati.anticorruzione.it/opendata/download/dataset/bandi-cig-{year}/"
        "filesystem/ocds-{year}-{month:02d}.zip"
    )

    def __init__(self, resume_session: Optional[str] = None):
        """
        Inizializza il download manager.

        Args:
            resume_session: ID sessione da riprendere
        """
        self.checkpoint_manager = CheckpointManager()

        # Setup directories
        self.download_dir = config.CIG_DOWNLOAD_DIR
        self.extract_dir = config.CIG_DIR
        self.ocds_dir = config.OCDS_DIR

        for dir_path in [self.download_dir, self.extract_dir, self.ocds_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)

        # Resume session if provided
        if resume_session:
            if self.checkpoint_manager.resume_session(resume_session):
                logger.info(f"✅ Sessione ripresa: {resume_session}")
            else:
                self.checkpoint_manager.create_session("download_manager")
        else:
            session_id = self.checkpoint_manager.create_session("download_manager")
            logger.info(f"📝 Nuova sessione download: {session_id}")

    def setup_driver(self, download_dir: Path) -> webdriver.Chrome:
        """
        Configura Chrome WebDriver con impostazioni di download.

        Args:
            download_dir: Directory per i download

        Returns:
            WebDriver configurato
        """
        options = webdriver.ChromeOptions()

        # Download preferences
        options.add_experimental_option(
            "prefs",
            {
                "download.default_directory": str(download_dir),
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "plugins.always_open_pdf_externally": True,
            },
        )

        # Headless mode
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")

        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=options
        )

        driver.set_page_load_timeout(300)
        driver.set_script_timeout(300)

        return driver

    def generate_cig_urls(self) -> List[str]:
        """
        Genera lista di URL per download CIG.

        Returns:
            Lista di URL CIG
        """
        urls = []
        for year in range(config.CIG_START_YEAR, config.CIG_LAST_YEAR + 1):
            last_month = 12 if year < config.CIG_LAST_YEAR else config.CIG_LAST_MONTH
            for month in range(1, last_month + 1):
                urls.append(self.CIG_BASE_URL.format(year=year, month=month))
        return urls

    def generate_ocds_urls(self) -> List[str]:
        """
        Genera lista di URL per download OCDS.

        Returns:
            Lista di URL OCDS
        """
        urls = []
        for year in range(config.OCDS_START_YEAR, config.OCDS_LAST_YEAR + 1):
            if year == config.OCDS_START_YEAR:
                start_month = config.OCDS_START_MONTH
            else:
                start_month = 1

            if year == config.OCDS_LAST_YEAR:
                end_month = config.OCDS_LAST_MONTH
            else:
                end_month = 12

            for month in range(start_month, end_month + 1):
                urls.append(self.OCDS_BASE_URL.format(year=year, month=month))

        return urls

    def wait_for_download(
        self, download_dir: Path, before_files: Set[str], timeout: int = 200
    ) -> Optional[Path]:
        """
        Attende il completamento di un download.

        Args:
            download_dir: Directory dei download
            before_files: File presenti prima del download
            timeout: Timeout in secondi

        Returns:
            Path del file scaricato o None
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            current_files = set(os.listdir(download_dir))
            new_files = current_files - before_files

            for file in new_files:
                if not file.endswith((".crdownload", ".tmp", ".part")):
                    file_path = download_dir / file
                    # Verifica che il file sia stabile
                    size1 = file_path.stat().st_size
                    time.sleep(0.5)
                    if file_path.exists():
                        size2 = file_path.stat().st_size
                        if size1 == size2 and size1 > 0:
                            return file_path

            time.sleep(0.5)

        return None

    @retry_on_exception(exceptions=(TimeoutException,))
    def download_file(
        self, driver: webdriver.Chrome, url: str, download_dir: Path
    ) -> bool:
        """
        Scarica un file con browser automatizzato.

        Args:
            driver: WebDriver
            url: URL da scaricare
            download_dir: Directory di destinazione

        Returns:
            True se download completato
        """
        # Check if already downloaded
        expected_filename = os.path.basename(url)
        file_path = download_dir / expected_filename

        task_id = f"download_{expected_filename}"

        # Check checkpoint
        if self.checkpoint_manager.should_skip(task_id):
            logger.info(f"⏭️ Già scaricato: {expected_filename}")
            return True

        if file_path.exists():
            logger.info(f"📁 File esistente: {expected_filename}")
            self.checkpoint_manager.mark_completed(task_id, {"file": str(file_path)})
            return True

        # Get files before download
        before_files = set(os.listdir(download_dir))

        try:
            # Navigate to URL
            logger.info(f"📥 Download: {expected_filename}")
            driver.get(url)

            # Wait for download
            downloaded_file = self.wait_for_download(download_dir, before_files)

            if downloaded_file:
                logger.info(f"✅ Completato: {downloaded_file.name}")
                self.checkpoint_manager.mark_completed(
                    task_id,
                    {
                        "file": str(downloaded_file),
                        "size": downloaded_file.stat().st_size,
                    },
                )
                return True
            else:
                logger.error(f"❌ Download fallito: {url}")
                self.checkpoint_manager.mark_failed(task_id, "Timeout")
                return False

        except Exception as e:
            logger.error(f"❌ Errore download {url}: {e}")
            self.checkpoint_manager.mark_failed(task_id, str(e))
            return False

    def extract_zip_files(self, source_dir: Path, target_dir: Path) -> None:
        """
        Estrae tutti i file ZIP.

        Args:
            source_dir: Directory sorgente
            target_dir: Directory destinazione
        """
        zip_files = list(source_dir.glob("*.zip"))

        if not zip_files:
            logger.info("Nessun file ZIP da estrarre")
            return

        progress_logger.start_operation("extract_zip", len(zip_files), "Estrazione ZIP")

        for zip_path in zip_files:
            task_id = f"extract_{zip_path.name}"

            if self.checkpoint_manager.should_skip(task_id):
                progress_logger.update("extract_zip")
                continue

            try:
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(target_dir)
                logger.info(f"📦 Estratto: {zip_path.name}")
                self.checkpoint_manager.mark_completed(task_id)
                progress_logger.update("extract_zip")

            except zipfile.BadZipFile:
                logger.error(f"❌ ZIP corrotto: {zip_path.name}")
                self.checkpoint_manager.mark_failed(task_id, "BadZipFile")
                progress_logger.update("extract_zip", error=True)

        progress_logger.complete_operation("extract_zip")

    def download_cig_data(self) -> None:
        """
        Scarica tutti i dati CIG con checkpoint.
        """
        logger.info("=== DOWNLOAD DATI CIG ===")

        # Generate URLs
        cig_urls = self.generate_cig_urls()
        logger.info(f"📋 URL CIG da scaricare: {len(cig_urls)}")

        # Setup driver
        driver = self.setup_driver(self.download_dir)

        try:
            # Download CIG files
            progress_logger.start_operation(
                "download_cig", len(cig_urls) + len(self.EXTRA_URLS), "Download CIG"
            )

            # Download monthly CIG files
            for url in cig_urls:
                success = self.download_file(driver, url, self.download_dir)
                progress_logger.update("download_cig", error=not success)

            # Download extra files
            for url in self.EXTRA_URLS:
                success = self.download_file(driver, url, config.DATA_DIR)
                progress_logger.update("download_cig", error=not success)

            progress_logger.complete_operation("download_cig")

        finally:
            driver.quit()

        # Extract files
        self.extract_zip_files(self.download_dir, self.extract_dir)
        self.extract_zip_files(config.DATA_DIR, config.DATA_DIR)

        logger.info("✅ Download CIG completato")

    def download_ocds_data(self) -> None:
        """
        Scarica tutti i dati OCDS con checkpoint.
        """
        logger.info("=== DOWNLOAD DATI OCDS ===")

        # Generate URLs
        ocds_urls = self.generate_ocds_urls()
        logger.info(f"📋 URL OCDS da scaricare: {len(ocds_urls)}")

        # Setup driver
        driver = self.setup_driver(self.ocds_dir)

        try:
            # Download OCDS files
            progress_logger.start_operation(
                "download_ocds", len(ocds_urls), "Download OCDS"
            )

            for url in ocds_urls:
                success = self.download_file(driver, url, self.ocds_dir)
                progress_logger.update("download_ocds", error=not success)

            progress_logger.complete_operation("download_ocds")

        finally:
            driver.quit()

        # Extract files
        self.extract_zip_files(self.ocds_dir, self.ocds_dir)

        logger.info("✅ Download OCDS completato")
