"""
Utilità per il monitoraggio delle performance.

Questo modulo fornisce decoratori e funzioni per monitorare
le performance del codice.
"""

import functools
import logging
import time
from typing import Any, Callable

try:
    from memory_profiler import profile as memory_profile
except ImportError:
    memory_profile = None

logger = logging.getLogger(__name__)


def timer(func: Callable) -> Callable:
    """
    Decoratore per misurare il tempo di esecuzione di una funzione.

    Args:
        func: Funzione da decorare

    Returns:
        Funzione decorata che logga il tempo di esecuzione

    Examples:
        >>> @timer
        ... def slow_function():
        ...     time.sleep(1)
        ...     return "done"
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.perf_counter()

        try:
            result = func(*args, **kwargs)
            return result
        finally:
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time

            logger.info(f"{func.__name__} completato in {elapsed_time:.2f} secondi")

    return wrapper


def profile_memory(func: Callable) -> Callable:
    """
    Decoratore per profilare l'utilizzo di memoria di una funzione.

    Args:
        func: Funzione da profilare

    Returns:
        Funzione decorata con profiling della memoria

    Examples:
        >>> @profile_memory
        ... def memory_intensive():
        ...     data = [i for i in range(1000000)]
        ...     return len(data)
    """
    if memory_profile:
        return memory_profile(func)
    else:
        # Return the function unchanged if memory_profiler is not available
        return func


class PerformanceMonitor:
    """
    Context manager per monitorare le performance di un blocco di codice.

    Examples:
        >>> with PerformanceMonitor("operazione complessa"):
        ...     # codice da monitorare
        ...     time.sleep(1)
    """

    def __init__(self, operation_name: str = "Operation"):
        """
        Inizializza il monitor.

        Args:
            operation_name: Nome dell'operazione da monitorare
        """
        self.operation_name = operation_name
        self.start_time = None
        self.end_time = None

    def __enter__(self):
        """Avvia il monitoraggio."""
        self.start_time = time.perf_counter()
        logger.info(f"Inizio {self.operation_name}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Termina il monitoraggio e logga i risultati.

        Args:
            exc_type: Tipo di eccezione (se presente)
            exc_val: Valore dell'eccezione
            exc_tb: Traceback dell'eccezione
        """
        self.end_time = time.perf_counter()
        elapsed = self.end_time - self.start_time

        if exc_type is None:
            logger.info(f"{self.operation_name} completato in {elapsed:.2f} secondi")
        else:
            logger.error(
                f"{self.operation_name} fallito dopo {elapsed:.2f} secondi: {exc_val}"
            )

    @property
    def elapsed_time(self) -> float:
        """
        Restituisce il tempo trascorso.

        Returns:
            Tempo trascorso in secondi
        """
        if self.start_time is None:
            return 0.0

        end = self.end_time if self.end_time else time.perf_counter()
        return end - self.start_time


def batch_processor(items: list, batch_size: int = 100) -> list:
    """
    Processa una lista di elementi in batch per ottimizzare la memoria.

    Args:
        items: Lista di elementi da processare
        batch_size: Dimensione del batch

    Yields:
        Batch di elementi

    Examples:
        >>> items = list(range(1000))
        >>> for batch in batch_processor(items, 100):
        ...     # processa batch
        ...     pass
    """
    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]


def log_performance_metrics(
    operation: str, items_processed: int, elapsed_time: float
) -> None:
    """
    Logga metriche di performance standardizzate.

    Args:
        operation: Nome dell'operazione
        items_processed: Numero di elementi processati
        elapsed_time: Tempo trascorso in secondi
    """
    if items_processed > 0 and elapsed_time > 0:
        rate = items_processed / elapsed_time
        logger.info(
            f"{operation}: {items_processed} elementi in {elapsed_time:.2f}s "
            f"({rate:.1f} elementi/s)"
        )
    else:
        logger.info(f"{operation}: {items_processed} elementi in {elapsed_time:.2f}s")
