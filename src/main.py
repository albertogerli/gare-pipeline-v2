"""
Modulo principale per l'orchestrazione del sistema di analisi gare.

Questo modulo coordina l'esecuzione di tutti i componenti del sistema
per l'analisi dei bandi di gara pubblici con supporto per resume e logging avanzato.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import click

# Aggiungi src al path
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import config
from src.analyzers.gazzetta_analyzer import GazzettaAnalyzer
from src.analyzers.json_processor import JsonProcessor
from src.analyzers.servizio_luce import ServizioLuceAnalyzer
from src.analyzers.verbali import VerbaliAnalyzer
from src.processors.concatenator import Concatenator
from src.processors.transformer import Transformer
from src.scrapers.downloader import DownloadManager
from src.scrapers.gazzetta import GazzettaScraper
from src.utils.checkpoint import CheckpointManager
from src.utils.logging_config import get_logger, log_exception, setup_logging
from src.utils.performance import PerformanceMonitor, timer

logger = get_logger(__name__)


class Pipeline:
    """
    Pipeline principale per l'elaborazione dei dati di gara con supporto resume.

    Coordina l'esecuzione sequenziale di tutti i componenti
    del sistema di analisi con checkpoint e logging avanzato.
    """

    def __init__(
        self,
        skip_download: bool = False,
        skip_scraping: bool = False,
        resume_session: Optional[str] = None,
    ):
        """
        Inizializza la pipeline.

        Args:
            skip_download: Se True, salta la fase di download
            skip_scraping: Se True, salta la fase di scraping
            resume_session: ID sessione da riprendere (per resume)
        """
        self.skip_download = skip_download
        self.skip_scraping = skip_scraping
        self.resume_session = resume_session
        self.checkpoint_manager = CheckpointManager()

        # Crea o riprende sessione
        if resume_session:
            if self.checkpoint_manager.resume_session(resume_session):
                logger.info(f"✅ Sessione ripresa: {resume_session}")
                self.show_resume_status()
            else:
                logger.error(f"❌ Impossibile riprendere sessione: {resume_session}")
                sys.exit(1)
        else:
            session_id = self.checkpoint_manager.create_session("pipeline_main")
            logger.info(f"📝 Nuova sessione pipeline: {session_id}")

        # Valida configurazione
        self.validate_config()

    def validate_config(self) -> None:
        """Valida la configurazione del sistema."""
        if not config.validate():
            logger.error("Configurazione non valida. Verificare le API keys.")
            sys.exit(1)
        logger.info("✅ Configurazione validata con successo")

    def show_resume_status(self) -> None:
        """Mostra lo stato dei task da riprendere."""
        incomplete = self.checkpoint_manager.get_incomplete_tasks()
        if incomplete:
            logger.info("📊 Task da completare:")
            for task in incomplete:
                logger.info(
                    f"  - {task.task_id}: {task.status.value} "
                    f"({task.progress:.1f}%) - Tentativi: {task.attempts}"
                )

    @timer
    def run_downloads(self) -> None:
        """Esegue la fase di download dei dati con checkpoint."""
        task_id = "download_phase"

        if self.skip_download:
            logger.info("⏭️ Download saltato per configurazione")
            return

        if self.checkpoint_manager.should_skip(task_id):
            logger.info("⏭️ Download già completato (checkpoint)")
            return

        try:
            self.checkpoint_manager.save_checkpoint(
                task_id=task_id,
                progress=0.0,
                data={"start_time": datetime.now().isoformat()},
            )

            with PerformanceMonitor("Download CIG"):
                downloader = DownloadManager(resume_session=self.resume_session)
                downloader.download_cig_data()

            self.checkpoint_manager.save_checkpoint(
                task_id=task_id, progress=50.0, data={"cig_completed": True}
            )

            with PerformanceMonitor("Download OCDS"):
                downloader.download_ocds_data()

            self.checkpoint_manager.mark_completed(
                task_id=task_id, data={"completed_time": datetime.now().isoformat()}
            )

            logger.info("✅ Download completato con successo")

        except Exception as e:
            self.checkpoint_manager.mark_failed(task_id, str(e))
            log_exception(logger, e, {"phase": "download"})
            raise

    @timer
    def run_scraping(self) -> None:
        """Esegue la fase di scraping della Gazzetta Ufficiale con checkpoint."""
        task_id = "scraping_phase"

        if self.skip_scraping:
            logger.info("⏭️ Scraping saltato per configurazione")
            return

        if self.checkpoint_manager.should_skip(task_id):
            logger.info("⏭️ Scraping già completato (checkpoint)")
            return

        try:
            self.checkpoint_manager.save_checkpoint(
                task_id=task_id,
                progress=0.0,
                data={"start_time": datetime.now().isoformat()},
            )

            with PerformanceMonitor("Scraping Gazzetta"):
                scraper = GazzettaScraper(resume_session=self.resume_session)
                scraper.run()
                scraper.cleanup()

            self.checkpoint_manager.mark_completed(
                task_id=task_id, data={"completed_time": datetime.now().isoformat()}
            )

            logger.info("✅ Scraping completato con successo")

        except Exception as e:
            self.checkpoint_manager.mark_failed(task_id, str(e))
            log_exception(logger, e, {"phase": "scraping"})
            raise

    @timer
    def run_analysis(self) -> None:
        """Esegue la fase di analisi dei dati con checkpoint."""
        task_id = "analysis_phase"

        if self.checkpoint_manager.should_skip(task_id):
            logger.info("⏭️ Analisi già completata (checkpoint)")
            return

        try:
            self.checkpoint_manager.save_checkpoint(task_id=task_id, progress=0.0)

            with PerformanceMonitor("Analisi Gazzetta"):
                analyzer = GazzettaAnalyzer()
                analyzer.run()

            self.checkpoint_manager.save_checkpoint(task_id=task_id, progress=50.0)

            with PerformanceMonitor("Elaborazione JSON/OCDS"):
                processor = JsonProcessor()
                processor.run()

            self.checkpoint_manager.mark_completed(task_id)
            logger.info("✅ Analisi completata con successo")

        except Exception as e:
            self.checkpoint_manager.mark_failed(task_id, str(e))
            log_exception(logger, e, {"phase": "analysis"})
            raise

    @timer
    def run_concatenation(self) -> None:
        """Esegue la fase di concatenazione dei dati con checkpoint."""
        task_id = "concatenation_phase"

        if self.checkpoint_manager.should_skip(task_id):
            logger.info("⏭️ Concatenazione già completata (checkpoint)")
            return

        try:
            steps = [
                ("Concatenazione lotti", 20, lambda: Concatenator().concat_lotti()),
                ("Analisi verbali", 40, lambda: VerbaliAnalyzer().run()),
                ("Analisi Servizio Luce", 60, lambda: ServizioLuceAnalyzer().run()),
                ("Concatenazione finale", 80, lambda: Concatenator().concat_all()),
            ]

            for step_name, progress, step_func in steps:
                self.checkpoint_manager.save_checkpoint(
                    task_id=task_id, progress=progress, data={"current_step": step_name}
                )

                with PerformanceMonitor(step_name):
                    step_func()

            self.checkpoint_manager.mark_completed(task_id)
            logger.info("✅ Concatenazione completata con successo")

        except Exception as e:
            self.checkpoint_manager.mark_failed(task_id, str(e))
            log_exception(logger, e, {"phase": "concatenation"})
            raise

    @timer
    def run_transformation(self) -> None:
        """Esegue la fase di trasformazione finale dei dati."""
        task_id = "transformation_phase"

        if self.checkpoint_manager.should_skip(task_id):
            logger.info("⏭️ Trasformazione già completata (checkpoint)")
            return

        try:
            with PerformanceMonitor("Trasformazione finale"):
                transformer = Transformer()
                transformer.run()

            self.checkpoint_manager.mark_completed(task_id)
            logger.info("✅ Trasformazione completata con successo")

        except Exception as e:
            self.checkpoint_manager.mark_failed(task_id, str(e))
            log_exception(logger, e, {"phase": "transformation"})
            raise

    def run(self) -> None:
        """
        Esegue l'intera pipeline di elaborazione con gestione checkpoint.
        """
        logger.info("=" * 60)
        logger.info("🚀 AVVIO PIPELINE DI ELABORAZIONE GARE")
        logger.info(f"📅 Data/Ora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        if self.resume_session:
            logger.info(f"♻️ Resume sessione: {self.resume_session}")
        logger.info("=" * 60)

        try:
            # Esegui fasi in sequenza
            phases = [
                ("Download", self.run_downloads),
                ("Scraping", self.run_scraping),
                ("Analisi", self.run_analysis),
                ("Concatenazione", self.run_concatenation),
                ("Trasformazione", self.run_transformation),
            ]

            for phase_name, phase_func in phases:
                logger.info(f"\n{'='*40}")
                logger.info(f"📌 FASE: {phase_name}")
                logger.info(f"{'='*40}")
                phase_func()

            # Report finale
            summary = self.checkpoint_manager.get_session_summary()

            logger.info("=" * 60)
            logger.info("✅ PIPELINE COMPLETATA CON SUCCESSO")
            logger.info(f"📁 File finale: {config.OUTPUT_DIR / config.GARE}")
            logger.info(f"📊 Riepilogo sessione:")
            logger.info(f"   - Task totali: {summary.get('total', 0)}")
            logger.info(f"   - Completati: {summary.get('completed', 0)}")
            logger.info(f"   - Falliti: {summary.get('failed', 0)}")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"❌ Errore durante l'esecuzione della pipeline: {e}")
            logger.error(
                "💡 Suggerimento: Usa --resume per riprendere dalla sessione corrente"
            )
            log_exception(logger, e, {"pipeline": "main"})
            sys.exit(1)


@click.command()
@click.option(
    "--skip-download", is_flag=True, help="Salta la fase di download dei dati"
)
@click.option(
    "--skip-scraping", is_flag=True, help="Salta la fase di scraping della Gazzetta"
)
@click.option(
    "--only-transform", is_flag=True, help="Esegue solo la trasformazione finale"
)
@click.option(
    "--resume",
    type=str,
    help="Riprende da una sessione precedente (fornire session ID)",
)
@click.option(
    "--list-sessions", is_flag=True, help="Elenca le sessioni disponibili per resume"
)
@click.option("--verbose", is_flag=True, help="Abilita output verboso (DEBUG level)")
@click.option("--log-file", is_flag=True, help="Salva log su file oltre che su console")
def main(
    skip_download: bool,
    skip_scraping: bool,
    only_transform: bool,
    resume: Optional[str],
    list_sessions: bool,
    verbose: bool,
    log_file: bool,
) -> None:
    """
    Sistema di analisi bandi di gara pubblici con supporto resume.

    Elabora dati da multiple fonti (Gazzetta Ufficiale, ANAC, OCDS)
    per creare un dataset unificato dei bandi di gara.

    Supporta checkpoint e resume per operazioni lunghe interrotte.
    """
    # Configura logging
    log_level = "DEBUG" if verbose else "INFO"
    setup_logging(
        log_level=log_level,
        console=True,
        file=log_file,
        rotation=True,
        session_id=resume if resume else datetime.now().strftime("%Y%m%d_%H%M%S"),
    )

    # Lista sessioni disponibili
    if list_sessions:
        checkpoint_manager = CheckpointManager()
        sessions_dir = checkpoint_manager.checkpoint_dir

        logger.info("📋 Sessioni disponibili per resume:")
        for session_dir in sorted(sessions_dir.iterdir()):
            if session_dir.is_dir():
                metadata_file = session_dir / "metadata.json"
                if metadata_file.exists():
                    import json

                    with open(metadata_file) as f:
                        metadata = json.load(f)
                        created = metadata.get("created_at", "N/A")
                        status = metadata.get("status", "unknown")
                        logger.info(
                            f"  - {session_dir.name} " f"[{status}] creata: {created}"
                        )
        return

    # Modalità solo trasformazione
    if only_transform:
        logger.info("🔄 Modalità solo trasformazione")
        with PerformanceMonitor("Trasformazione"):
            transformer = Transformer()
            transformer.run()
        return

    # Esegui pipeline completa
    pipeline = Pipeline(
        skip_download=skip_download, skip_scraping=skip_scraping, resume_session=resume
    )
    pipeline.run()


if __name__ == "__main__":
    main()
