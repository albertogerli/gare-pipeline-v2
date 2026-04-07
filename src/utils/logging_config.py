"""
Configurazione avanzata del sistema di logging.

Questo modulo configura un sistema di logging completo con:
- File di log separati per livello
- Rotazione automatica dei log
- Formato dettagliato con context
- Integrazione con checkpoint system
"""

import json
import logging
import logging.handlers
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from config.settings import config


class ColoredFormatter(logging.Formatter):
    """Formatter con colori per output console."""

    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"

    def format(self, record):
        """Formatta il record con colori."""
        if record.levelname in self.COLORS:
            record.levelname = (
                f"{self.COLORS[record.levelname]}{record.levelname}{self.RESET}"
            )
        return super().format(record)


class ContextFilter(logging.Filter):
    """Aggiunge context addizionale ai log."""

    def __init__(self, context: dict = None):
        """
        Inizializza il filtro.

        Args:
            context: Dizionario di context da aggiungere
        """
        super().__init__()
        self.context = context or {}

    def filter(self, record):
        """Aggiunge context al record."""
        for key, value in self.context.items():
            setattr(record, key, value)
        return True


class ProgressLogger:
    """Logger specializzato per tracciare progresso operazioni."""

    def __init__(self, logger_name: str = "progress"):
        """
        Inizializza progress logger.

        Args:
            logger_name: Nome del logger
        """
        self.logger = logging.getLogger(logger_name)
        self.operations = {}

    def start_operation(self, operation_id: str, total: int, description: str = ""):
        """
        Inizia tracking di un'operazione.

        Args:
            operation_id: ID univoco operazione
            total: Numero totale di elementi
            description: Descrizione operazione
        """
        self.operations[operation_id] = {
            "total": total,
            "current": 0,
            "description": description,
            "start_time": datetime.now(),
            "errors": 0,
        }

        self.logger.info(
            f"🚀 Avvio operazione: {description or operation_id} " f"({total} elementi)"
        )

    def update(self, operation_id: str, increment: int = 1, error: bool = False):
        """
        Aggiorna progresso operazione.

        Args:
            operation_id: ID operazione
            increment: Incremento progresso
            error: Se True, incrementa contatore errori
        """
        if operation_id not in self.operations:
            return

        op = self.operations[operation_id]
        op["current"] += increment

        if error:
            op["errors"] += 1

        # Calcola statistiche
        progress = (op["current"] / op["total"]) * 100
        elapsed = (datetime.now() - op["start_time"]).total_seconds()
        rate = op["current"] / elapsed if elapsed > 0 else 0
        eta = (op["total"] - op["current"]) / rate if rate > 0 else 0

        # Log ogni 10% o su errore
        if error or op["current"] % max(1, op["total"] // 10) == 0:
            self.logger.info(
                f"📊 {op['description'] or operation_id}: "
                f"{op['current']}/{op['total']} ({progress:.1f}%) "
                f"[{rate:.1f} items/s, ETA: {eta:.0f}s] "
                f"{'❌ Errori: ' + str(op['errors']) if op['errors'] else ''}"
            )

    def complete_operation(self, operation_id: str):
        """
        Completa tracking operazione.

        Args:
            operation_id: ID operazione
        """
        if operation_id not in self.operations:
            return

        op = self.operations[operation_id]
        elapsed = (datetime.now() - op["start_time"]).total_seconds()

        status = (
            "✅ Completata"
            if op["errors"] == 0
            else f"⚠️ Completata con {op['errors']} errori"
        )

        self.logger.info(
            f"{status} operazione: {op['description'] or operation_id} "
            f"({op['current']}/{op['total']}) in {elapsed:.1f}s "
            f"[{op['current']/elapsed:.1f} items/s]"
        )

        del self.operations[operation_id]


def setup_logging(
    log_level: str = "INFO",
    log_dir: Optional[Path] = None,
    console: bool = True,
    file: bool = True,
    rotation: bool = True,
    session_id: Optional[str] = None,
) -> None:
    """
    Configura il sistema di logging completo.

    Args:
        log_level: Livello di logging (DEBUG, INFO, WARNING, ERROR)
        log_dir: Directory per i file di log
        console: Se abilitare output su console
        file: Se abilitare output su file
        rotation: Se abilitare rotazione dei log
        session_id: ID sessione per contestualizzare i log
    """
    # Directory log
    if log_dir is None:
        log_dir = config.DATA_DIR / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))

    # Rimuovi handler esistenti
    root_logger.handlers.clear()

    # Formato log
    if console:
        console_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    else:
        console_format = None

    file_format = (
        "%(asctime)s - %(name)s - %(levelname)s - "
        "%(filename)s:%(lineno)d - %(funcName)s() - %(message)s"
    )

    # Console handler
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(ColoredFormatter(console_format))
        root_logger.addHandler(console_handler)

    # File handlers
    if file:
        # Log principale con rotazione
        if rotation:
            # Rotazione per dimensione (10MB)
            main_handler = logging.handlers.RotatingFileHandler(
                log_dir / "gare.log",
                maxBytes=10 * 1024 * 1024,
                backupCount=5,
                encoding="utf-8",
            )
        else:
            main_handler = logging.FileHandler(log_dir / "gare.log", encoding="utf-8")

        main_handler.setLevel(logging.DEBUG)
        main_handler.setFormatter(logging.Formatter(file_format))
        root_logger.addHandler(main_handler)

        # Log errori separato
        error_handler = logging.FileHandler(log_dir / "errors.log", encoding="utf-8")
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(logging.Formatter(file_format))
        root_logger.addHandler(error_handler)

        # Log per sessione specifica
        if session_id:
            session_handler = logging.FileHandler(
                log_dir / f"session_{session_id}.log", encoding="utf-8"
            )
            session_handler.setLevel(logging.DEBUG)
            session_handler.setFormatter(logging.Formatter(file_format))

            # Aggiungi context filter
            session_handler.addFilter(ContextFilter({"session_id": session_id}))
            root_logger.addHandler(session_handler)

    # Log di avvio
    logger = logging.getLogger(__name__)
    logger.info("=" * 60)
    logger.info("SISTEMA DI LOGGING INIZIALIZZATO")
    logger.info(f"Livello: {log_level}")
    logger.info(f"Directory log: {log_dir}")
    logger.info(f"Console: {'ATTIVO' if console else 'DISATTIVO'}")
    logger.info(f"File: {'ATTIVO' if file else 'DISATTIVO'}")
    logger.info(f"Rotazione: {'ATTIVA' if rotation else 'DISATTIVA'}")
    if session_id:
        logger.info(f"Session ID: {session_id}")
    logger.info("=" * 60)


def get_logger(name: str) -> logging.Logger:
    """
    Ottiene un logger configurato.

    Args:
        name: Nome del logger (tipicamente __name__)

    Returns:
        Logger configurato
    """
    return logging.getLogger(name)


def log_exception(logger: logging.Logger, e: Exception, context: dict = None):
    """
    Logga un'eccezione con context dettagliato.

    Args:
        logger: Logger da utilizzare
        e: Eccezione da loggare
        context: Context addizionale
    """
    import traceback

    error_info = {
        "exception_type": type(e).__name__,
        "exception_message": str(e),
        "traceback": traceback.format_exc(),
    }

    if context:
        error_info.update(context)

    logger.error(
        f"❌ ECCEZIONE: {type(e).__name__}: {e}\n"
        f"Context: {json.dumps(context, indent=2) if context else 'N/A'}\n"
        f"Traceback:\n{error_info['traceback']}"
    )

    # Salva anche su file dedicato
    error_log_file = config.DATA_DIR / "logs" / "exceptions.jsonl"
    error_log_file.parent.mkdir(exist_ok=True)

    with open(error_log_file, "a") as f:
        error_info["timestamp"] = datetime.now().isoformat()
        f.write(json.dumps(error_info) + "\n")


# Inizializzazione di default
def init_default_logging():
    """Inizializza logging con configurazione di default."""
    setup_logging(
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
        console=True,
        file=True,
        rotation=True,
    )


# Auto-inizializzazione se importato
import os

if os.environ.get("AUTO_INIT_LOGGING", "true").lower() == "true":
    init_default_logging()
