"""
Utilità per operazioni resilienti con retry e recovery.

Questo modulo fornisce decoratori e funzioni per rendere
le operazioni più robuste contro errori temporanei.
"""

import logging
import random
import time
from functools import wraps
from typing import Any, Callable, Optional, Tuple, Type

from requests.exceptions import ConnectionError, RequestException, Timeout
from selenium.common.exceptions import TimeoutException, WebDriverException

logger = logging.getLogger(__name__)


class RetryConfig:
    """Configurazione per retry policy."""

    def __init__(
        self,
        max_attempts: int = 3,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
    ):
        """
        Inizializza configurazione retry.

        Args:
            max_attempts: Numero massimo di tentativi
            initial_delay: Delay iniziale in secondi
            max_delay: Delay massimo in secondi
            exponential_base: Base per backoff esponenziale
            jitter: Se aggiungere jitter random al delay
        """
        self.max_attempts = max_attempts
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter

    def get_delay(self, attempt: int) -> float:
        """
        Calcola il delay per un tentativo.

        Args:
            attempt: Numero del tentativo (1-based)

        Returns:
            Delay in secondi
        """
        delay = min(
            self.initial_delay * (self.exponential_base ** (attempt - 1)),
            self.max_delay,
        )

        if self.jitter:
            delay *= 0.5 + random.random()

        return delay


# Configurazioni predefinite
DEFAULT_RETRY = RetryConfig()
AGGRESSIVE_RETRY = RetryConfig(max_attempts=5, initial_delay=0.5)
PATIENT_RETRY = RetryConfig(max_attempts=10, initial_delay=2.0, max_delay=120.0)


def retry_on_exception(
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    config: RetryConfig = None,
    on_retry: Optional[Callable] = None,
) -> Callable:
    """
    Decoratore per retry automatico su eccezioni specifiche.

    Args:
        exceptions: Tuple di eccezioni da catturare
        config: Configurazione retry (default: DEFAULT_RETRY)
        on_retry: Callback chiamata ad ogni retry

    Returns:
        Funzione decorata con retry

    Examples:
        >>> @retry_on_exception((RequestException,), config=AGGRESSIVE_RETRY)
        ... def download_file(url):
        ...     return requests.get(url)
    """
    if config is None:
        config = DEFAULT_RETRY

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for attempt in range(1, config.max_attempts + 1):
                try:
                    return func(*args, **kwargs)

                except exceptions as e:
                    last_exception = e

                    if attempt == config.max_attempts:
                        logger.error(
                            f"❌ {func.__name__} fallito dopo {attempt} tentativi: {e}"
                        )
                        raise

                    delay = config.get_delay(attempt)
                    logger.warning(
                        f"⚠️ {func.__name__} tentativo {attempt}/{config.max_attempts} "
                        f"fallito: {e}. Retry tra {delay:.1f}s"
                    )

                    if on_retry:
                        on_retry(attempt, e)

                    time.sleep(delay)

            # Non dovrebbe mai arrivare qui
            if last_exception:
                raise last_exception

        return wrapper

    return decorator


def resilient_download(
    download_func: Callable,
    url: str,
    *args,
    max_attempts: int = 3,
    checkpoint_manager=None,
    task_id: Optional[str] = None,
    **kwargs,
) -> Any:
    """
    Wrapper resiliente per funzioni di download.

    Args:
        download_func: Funzione di download da wrappare
        url: URL da scaricare
        *args: Argomenti aggiuntivi per download_func
        max_attempts: Numero massimo di tentativi
        checkpoint_manager: Manager per checkpoint
        task_id: ID del task per checkpoint
        **kwargs: Keyword arguments per download_func

    Returns:
        Risultato del download

    Raises:
        Exception: Se tutti i tentativi falliscono
    """
    from .checkpoint import TaskStatus

    # Genera task_id se non fornito
    if not task_id:
        task_id = f"download_{url.split('/')[-1]}"

    # Controlla checkpoint
    if checkpoint_manager:
        checkpoint = checkpoint_manager.load_checkpoint(task_id)
        if checkpoint and checkpoint.status == TaskStatus.COMPLETED:
            logger.info(f"⏭️ Download già completato (checkpoint): {task_id}")
            return checkpoint.data.get("result")

    last_error = None

    for attempt in range(1, max_attempts + 1):
        try:
            # Salva checkpoint in progress
            if checkpoint_manager:
                checkpoint_manager.save_checkpoint(
                    task_id=task_id,
                    status=TaskStatus.IN_PROGRESS,
                    progress=(attempt - 1) / max_attempts * 100,
                    data={"url": url, "attempt": attempt},
                )

            # Esegui download
            result = download_func(url, *args, **kwargs)

            # Salva checkpoint completato
            if checkpoint_manager:
                checkpoint_manager.mark_completed(
                    task_id=task_id, data={"url": url, "result": str(result)[:100]}
                )

            logger.info(f"✅ Download completato: {url}")
            return result

        except (RequestException, Timeout, ConnectionError) as e:
            last_error = e

            if attempt < max_attempts:
                delay = 2**attempt + random.random()
                logger.warning(
                    f"⚠️ Download fallito ({attempt}/{max_attempts}): {url}\n"
                    f"   Errore: {e}\n"
                    f"   Retry tra {delay:.1f}s"
                )
                time.sleep(delay)
            else:
                logger.error(f"❌ Download fallito definitivamente: {url}")

                if checkpoint_manager:
                    checkpoint_manager.mark_failed(task_id=task_id, error=str(e))
                raise

        except Exception as e:
            logger.error(f"❌ Errore inatteso durante download: {e}")

            if checkpoint_manager:
                checkpoint_manager.mark_failed(task_id=task_id, error=str(e))
            raise

    # Non dovrebbe mai arrivare qui
    if last_error:
        raise last_error


class ResilientBrowser:
    """
    Browser wrapper con gestione resiliente degli errori.
    """

    def __init__(self, driver, max_retries: int = 3):
        """
        Inizializza browser resiliente.

        Args:
            driver: WebDriver Selenium
            max_retries: Numero massimo di retry
        """
        self.driver = driver
        self.max_retries = max_retries
        self.page_load_timeout = 30
        self.implicit_wait = 10

        # Configura timeout
        driver.set_page_load_timeout(self.page_load_timeout)
        driver.implicitly_wait(self.implicit_wait)

    @retry_on_exception(
        exceptions=(TimeoutException, WebDriverException),
        config=RetryConfig(max_attempts=3, initial_delay=2.0),
    )
    def get(self, url: str) -> None:
        """
        Naviga a URL con retry automatico.

        Args:
            url: URL da visitare
        """
        try:
            self.driver.get(url)
        except TimeoutException:
            logger.warning(f"Timeout caricamento pagina, stop forzato: {url}")
            self.driver.execute_script("window.stop();")

    def safe_find_element(self, by: str, value: str, timeout: float = 10):
        """
        Trova elemento con gestione errori.

        Args:
            by: Metodo di ricerca
            value: Valore da cercare
            timeout: Timeout in secondi

        Returns:
            WebElement o None se non trovato
        """
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by, value))
            )
            return element
        except TimeoutException:
            logger.debug(f"Elemento non trovato: {by}={value}")
            return None

    def wait_for_download(self, download_dir: str, timeout: int = 60) -> Optional[str]:
        """
        Attende il completamento di un download.

        Args:
            download_dir: Directory dei download
            timeout: Timeout massimo in secondi

        Returns:
            Path del file scaricato o None
        """
        import os
        from pathlib import Path

        start_time = time.time()

        while time.time() - start_time < timeout:
            # Cerca file non temporanei
            for file in Path(download_dir).iterdir():
                if not file.name.endswith((".crdownload", ".tmp", ".part")):
                    # Verifica che il file sia stabile (dimensione non cambia)
                    size1 = file.stat().st_size
                    time.sleep(0.5)
                    size2 = file.stat().st_size

                    if size1 == size2 and size1 > 0:
                        return str(file)

            time.sleep(1)

        return None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit con cleanup."""
        try:
            self.driver.quit()
        except Exception as e:
            logger.warning(f"Errore chiusura browser: {e}")


def circuit_breaker(
    failure_threshold: int = 5,
    recovery_timeout: int = 60,
    expected_exception: Type[Exception] = Exception,
):
    """
    Implementa pattern Circuit Breaker per prevenire cascate di errori.

    Args:
        failure_threshold: Numero di fallimenti prima di aprire il circuito
        recovery_timeout: Secondi di attesa prima di riprovare
        expected_exception: Tipo di eccezione da monitorare

    Returns:
        Decoratore circuit breaker
    """

    def decorator(func):
        func._failures = 0
        func._last_failure_time = None
        func._circuit_open = False

        @wraps(func)
        def wrapper(*args, **kwargs):
            # Controlla se circuito è aperto
            if func._circuit_open:
                if func._last_failure_time:
                    time_since_failure = time.time() - func._last_failure_time
                    if time_since_failure < recovery_timeout:
                        raise Exception(
                            f"Circuit breaker aperto per {func.__name__}. "
                            f"Riprova tra {recovery_timeout - time_since_failure:.0f}s"
                        )
                    else:
                        # Prova a riaprire il circuito
                        func._circuit_open = False
                        func._failures = 0
                        logger.info(f"🔌 Circuit breaker reset per {func.__name__}")

            try:
                result = func(*args, **kwargs)
                # Reset su successo
                func._failures = 0
                return result

            except expected_exception as e:
                func._failures += 1
                func._last_failure_time = time.time()

                if func._failures >= failure_threshold:
                    func._circuit_open = True
                    logger.error(
                        f"⚡ Circuit breaker aperto per {func.__name__} "
                        f"dopo {func._failures} fallimenti"
                    )

                raise

        return wrapper

    return decorator
