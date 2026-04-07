"""
Sistema di checkpoint e resume per operazioni lunghe.

Questo modulo gestisce il salvataggio dello stato e il recupero
da interruzioni durante download e scraping.
"""

import json
import logging
import pickle
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class TaskStatus(str, Enum):
    """Stati possibili per un task."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class TaskCheckpoint:
    """
    Checkpoint per un singolo task.

    Attributes:
        task_id: Identificativo univoco del task
        status: Stato corrente del task
        progress: Progresso percentuale (0-100)
        data: Dati specifici del task
        error: Eventuale messaggio di errore
        created_at: Timestamp creazione
        updated_at: Timestamp ultimo aggiornamento
        attempts: Numero di tentativi
    """

    task_id: str
    status: TaskStatus = TaskStatus.PENDING
    progress: float = 0.0
    data: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    attempts: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Converte in dizionario serializzabile."""
        result = asdict(self)
        result["status"] = self.status.value
        result["created_at"] = self.created_at.isoformat()
        result["updated_at"] = self.updated_at.isoformat()
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskCheckpoint":
        """Crea istanza da dizionario."""
        data["status"] = TaskStatus(data["status"])
        data["created_at"] = datetime.fromisoformat(data["created_at"])
        data["updated_at"] = datetime.fromisoformat(data["updated_at"])
        return cls(**data)


class CheckpointManager:
    """
    Gestisce checkpoint e resume per operazioni lunghe.
    """

    def __init__(self, checkpoint_dir: Path = None, ttl_hours: int = 24):
        """
        Inizializza il checkpoint manager.

        Args:
            checkpoint_dir: Directory per salvare i checkpoint
            ttl_hours: Ore di validità dei checkpoint
        """
        from config.settings import config

        self.checkpoint_dir = checkpoint_dir or config.DATA_DIR / "checkpoints"
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.ttl = timedelta(hours=ttl_hours)
        self.current_session = None
        # Tentativo di resume automatico via variabile d'ambiente
        import os

        env_session = os.environ.get("CHECKPOINT_SESSION_ID")
        if env_session:
            if self.resume_session(env_session):
                logger.info(f"♻️  Sessione ripristinata da env: {env_session}")
            else:
                logger.warning(
                    f"Impossibile ripristinare sessione da env: {env_session}"
                )

    def create_session(self, session_name: str) -> str:
        """
        Crea una nuova sessione di checkpoint.

        Args:
            session_name: Nome della sessione

        Returns:
            ID della sessione creata
        """
        # Se esiste già una sessione attiva (es. ripristinata), riusa quella
        if self.current_session:
            logger.info(f"↩️  Riutilizzo sessione attiva: {self.current_session}")
            return self.current_session

        session_id = f"{session_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        session_dir = self.checkpoint_dir / session_id
        session_dir.mkdir(exist_ok=True)

        # Salva metadata sessione
        metadata = {
            "session_id": session_id,
            "session_name": session_name,
            "created_at": datetime.now().isoformat(),
            "status": "active",
        }

        with open(session_dir / "metadata.json", "w") as f:
            json.dump(metadata, f, indent=2)

        self.current_session = session_id
        logger.info(f"📝 Sessione checkpoint creata: {session_id}")

        return session_id

    def save_checkpoint(
        self,
        task_id: str,
        status: TaskStatus = TaskStatus.IN_PROGRESS,
        progress: float = 0.0,
        data: Dict[str, Any] = None,
    ) -> None:
        """
        Salva un checkpoint per un task.

        Args:
            task_id: ID del task
            status: Stato del task
            progress: Progresso percentuale
            data: Dati da salvare
        """
        if not self.current_session:
            logger.warning("Nessuna sessione attiva per checkpoint")
            return

        session_dir = self.checkpoint_dir / self.current_session
        checkpoint_file = session_dir / f"{task_id}.json"

        # Carica checkpoint esistente o crea nuovo
        if checkpoint_file.exists():
            with open(checkpoint_file, "r") as f:
                checkpoint_data = json.load(f)
                checkpoint = TaskCheckpoint.from_dict(checkpoint_data)
                checkpoint.attempts += 1
        else:
            checkpoint = TaskCheckpoint(task_id=task_id)

        # Aggiorna checkpoint
        checkpoint.status = status
        checkpoint.progress = progress
        checkpoint.data = data or {}
        checkpoint.updated_at = datetime.now()

        # Salva su file
        with open(checkpoint_file, "w") as f:
            json.dump(checkpoint.to_dict(), f, indent=2)

        logger.debug(f"💾 Checkpoint salvato: {task_id} ({progress:.1f}%)")

    def load_checkpoint(self, task_id: str) -> Optional[TaskCheckpoint]:
        """
        Carica un checkpoint per un task.

        Args:
            task_id: ID del task

        Returns:
            Checkpoint se esiste e valido, None altrimenti
        """
        if not self.current_session:
            return None

        session_dir = self.checkpoint_dir / self.current_session
        checkpoint_file = session_dir / f"{task_id}.json"

        if not checkpoint_file.exists():
            return None

        try:
            with open(checkpoint_file, "r") as f:
                data = json.load(f)
                checkpoint = TaskCheckpoint.from_dict(data)

                # Verifica TTL
                if datetime.now() - checkpoint.updated_at > self.ttl:
                    logger.warning(f"Checkpoint scaduto per {task_id}")
                    return None

                return checkpoint

        except Exception as e:
            logger.error(f"Errore caricamento checkpoint {task_id}: {e}")
            return None

    def get_incomplete_tasks(self) -> List[TaskCheckpoint]:
        """
        Restituisce lista dei task non completati.

        Returns:
            Lista di checkpoint per task incompleti
        """
        if not self.current_session:
            return []

        session_dir = self.checkpoint_dir / self.current_session
        incomplete = []

        for checkpoint_file in session_dir.glob("*.json"):
            if checkpoint_file.name == "metadata.json":
                continue

            try:
                with open(checkpoint_file, "r") as f:
                    data = json.load(f)
                    checkpoint = TaskCheckpoint.from_dict(data)

                    if checkpoint.status != TaskStatus.COMPLETED:
                        incomplete.append(checkpoint)

            except Exception as e:
                logger.error(f"Errore lettura checkpoint {checkpoint_file}: {e}")

        return incomplete

    def resume_session(self, session_id: str) -> bool:
        """
        Riprende una sessione esistente.

        Args:
            session_id: ID della sessione da riprendere

        Returns:
            True se sessione ripresa con successo
        """
        session_dir = self.checkpoint_dir / session_id

        if not session_dir.exists():
            logger.error(f"Sessione non trovata: {session_id}")
            return False

        metadata_file = session_dir / "metadata.json"
        if not metadata_file.exists():
            logger.error(f"Metadata sessione non trovati: {session_id}")
            return False

        try:
            with open(metadata_file, "r") as f:
                metadata = json.load(f)

            # Verifica validità
            created_at = datetime.fromisoformat(metadata["created_at"])
            if datetime.now() - created_at > timedelta(days=7):
                logger.warning(f"Sessione troppo vecchia: {session_id}")
                return False

            self.current_session = session_id

            # Report task incompleti
            incomplete = self.get_incomplete_tasks()
            if incomplete:
                logger.info(f"📊 Sessione ripresa con {len(incomplete)} task incompleti")
                for task in incomplete:
                    logger.info(
                        f"  - {task.task_id}: {task.status.value} ({task.progress:.1f}%)"
                    )

            return True

        except Exception as e:
            logger.error(f"Errore ripresa sessione {session_id}: {e}")
            return False

    def mark_completed(self, task_id: str, data: Dict[str, Any] = None) -> None:
        """
        Marca un task come completato.

        Args:
            task_id: ID del task
            data: Dati finali del task
        """
        self.save_checkpoint(
            task_id=task_id, status=TaskStatus.COMPLETED, progress=100.0, data=data
        )
        logger.info(f"✅ Task completato: {task_id}")

    def mark_failed(self, task_id: str, error: str) -> None:
        """
        Marca un task come fallito.

        Args:
            task_id: ID del task
            error: Messaggio di errore
        """
        if not self.current_session:
            return

        session_dir = self.checkpoint_dir / self.current_session
        checkpoint_file = session_dir / f"{task_id}.json"

        # Carica o crea checkpoint
        if checkpoint_file.exists():
            with open(checkpoint_file, "r") as f:
                checkpoint_data = json.load(f)
                checkpoint = TaskCheckpoint.from_dict(checkpoint_data)
        else:
            checkpoint = TaskCheckpoint(task_id=task_id)

        # Aggiorna con errore
        checkpoint.status = TaskStatus.FAILED
        checkpoint.error = error
        checkpoint.updated_at = datetime.now()

        # Salva
        with open(checkpoint_file, "w") as f:
            json.dump(checkpoint.to_dict(), f, indent=2)

        logger.error(f"❌ Task fallito: {task_id} - {error}")

    def should_skip(self, task_id: str) -> bool:
        """
        Verifica se un task deve essere saltato (già completato).

        Args:
            task_id: ID del task

        Returns:
            True se il task è già completato
        """
        checkpoint = self.load_checkpoint(task_id)
        return checkpoint is not None and checkpoint.status == TaskStatus.COMPLETED

    def get_session_summary(self) -> Dict[str, Any]:
        """
        Restituisce un riepilogo della sessione corrente.

        Returns:
            Dizionario con statistiche della sessione
        """
        if not self.current_session:
            return {}

        session_dir = self.checkpoint_dir / self.current_session

        stats = {
            "session_id": self.current_session,
            "total": 0,
            "completed": 0,
            "failed": 0,
            "in_progress": 0,
            "pending": 0,
        }

        for checkpoint_file in session_dir.glob("*.json"):
            if checkpoint_file.name == "metadata.json":
                continue

            try:
                with open(checkpoint_file, "r") as f:
                    data = json.load(f)
                    status = data.get("status", "pending")

                    stats["total"] += 1
                    if status == "completed":
                        stats["completed"] += 1
                    elif status == "failed":
                        stats["failed"] += 1
                    elif status == "in_progress":
                        stats["in_progress"] += 1
                    else:
                        stats["pending"] += 1

            except Exception:
                pass

        return stats

    def cleanup_old_sessions(self, days: int = 7) -> int:
        """
        Pulisce sessioni vecchie.

        Args:
            days: Giorni di retention

        Returns:
            Numero di sessioni rimosse
        """
        cutoff = datetime.now() - timedelta(days=days)
        removed = 0

        for session_dir in self.checkpoint_dir.iterdir():
            if not session_dir.is_dir():
                continue

            metadata_file = session_dir / "metadata.json"
            if not metadata_file.exists():
                continue

            try:
                with open(metadata_file, "r") as f:
                    metadata = json.load(f)
                    created_at = datetime.fromisoformat(metadata["created_at"])

                    if created_at < cutoff:
                        import shutil

                        shutil.rmtree(session_dir)
                        removed += 1
                        logger.info(f"🗑️ Rimossa sessione vecchia: {session_dir.name}")

            except Exception as e:
                logger.error(f"Errore pulizia sessione {session_dir}: {e}")

        return removed
