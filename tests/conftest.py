"""
Configurazione pytest e fixtures condivise.

Questo modulo contiene le fixtures e le configurazioni
comuni per tutti i test del sistema di analisi bandi di gara.
"""

import os
import sys
import tempfile
import json
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pytest
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Generator
import requests
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Aggiungi src al path per import
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Importa moduli del progetto per testing
try:
    from models.lotto import Lotto
    from models.enums import TipoAppalto, Categoria
    from utils.checkpoint import CheckpointManager
    from utils.logging_config import setup_logging
except ImportError:
    pass  # Gestione import opzionali per testing


@pytest.fixture
def temp_dir():
    """
    Crea una directory temporanea per i test.
    
    Yields:
        Path: Percorso alla directory temporanea
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_lotto_data():
    """
    Dati di esempio per un lotto.
    
    Returns:
        dict: Dizionario con dati di un lotto
    """
    return {
        "Oggetto": "Servizio di illuminazione pubblica",
        "Categoria": "Illuminazione",
        "TipoIlluminazione": "Pubblica",
        "TipoEfficientamento": "Energetico",
        "TipoAppalto": "Servizio",
        "TipoIntervento": "Efficientamento Energetico",
        "TipoImpianto": "Pubblica Illuminazione",
        "TipoEnergia": "Energia Elettrica",
        "TipoOperazione": "Gestione",
        "Procedura": "Procedura aperta",
        "AmministrazioneAggiudicatrice": "Comune di Test",
        "OfferteRicevute": "5",
        "DurataAppalto": "365",
        "Scadenza": "31/12/2024",
        "ImportoAggiudicazione": "100000.00",
        "DataAggiudicazione": "01/01/2024",
        "Sconto": "10%",
        "Comune": "Test City",
        "Aggiudicatario": "Test Company SRL",
        "CIG": "1234567890",
        "CUP": "B12F11000370004"
    }


@pytest.fixture
def sample_dataframe():
    """
    DataFrame di esempio per i test.
    
    Returns:
        pd.DataFrame: DataFrame con dati di test
    """
    data = {
        "testo": [
            "Avviso di gara per illuminazione pubblica",
            "Servizio di videosorveglianza comunale",
            "Manutenzione impianti termici"
        ],
        "CIG": ["1234567890", "0987654321", "5555555555"],
        "ImportoAggiudicazione": [100000, 50000, 75000],
        "DataAggiudicazione": ["01/01/2024", "15/02/2024", "30/03/2024"]
    }
    return pd.DataFrame(data)


@pytest.fixture
def mock_openai_client():
    """
    Mock del client OpenAI per i test.
    
    Returns:
        Mock: Client OpenAI mockato
    """
    with patch("openai.OpenAI") as mock:
        client = Mock()
        mock.return_value = client
        
        # Configura risposte mock
        response = Mock()
        response.choices = [Mock(text="Mock response")]
        client.completions.create.return_value = response
        
        yield client


@pytest.fixture
def mock_config():
    """
    Configurazione mock per i test.
    
    Returns:
        Mock: Oggetto configurazione mockato
    """
    config = Mock()
    config.OPENAI_API_KEY = "test-key"
    config.TEMP_DIR = Path("/tmp/test")
    config.DATA_DIR = Path("/tmp/test/data")
    config.MAX_WORKERS = 2
    config.CHUNK_SIZE = 100
    
    return config


@pytest.fixture
def sample_json_data():
    """
    Dati JSON di esempio per i test OCDS.
    
    Returns:
        dict: Struttura JSON OCDS di esempio
    """
    return {
        "releases": [
            {
                "ocid": "ocds-test-001",
                "tender": {
                    "title": "Test Tender",
                    "description": "Illuminazione pubblica LED",
                    "tenderPeriod": {
                        "endDate": "2024-12-31T23:59:59Z"
                    },
                    "procurementMethodDetails": "Procedura aperta"
                },
                "buyer": {
                    "name": "Comune di Test"
                },
                "awards": [
                    {
                        "date": "2024-01-01T00:00:00Z",
                        "value": {
                            "amount": 100000,
                            "currency": "EUR"
                        },
                        "suppliers": [
                            {"name": "Test Supplier SRL"}
                        ],
                        "items": [
                            {
                                "description": "Fornitura e installazione LED",
                                "relatedLot": "1234567890"
                            }
                        ]
                    }
                ],
                "parties": [
                    {
                        "name": "Comune di Test",
                        "address": {
                            "locality": "Test City"
                        }
                    }
                ],
                "contracts": [
                    {
                        "period": {
                            "startDate": "2024-01-01T00:00:00Z",
                            "endDate": "2024-12-31T23:59:59Z"
                        }
                    }
                ]
            }
        ]
    }


@pytest.fixture(autouse=True)
def reset_environment():
    """
    Resetta le variabili d'ambiente prima di ogni test.
    """
    original_env = os.environ.copy()
    yield
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def capture_logs():
    """
    Cattura i log per verifica nei test.
    
    Returns:
        list: Lista dei log catturati
    """
    import logging
    
    logs = []
    
    class TestHandler(logging.Handler):
        def emit(self, record):
            logs.append(record)
    
    handler = TestHandler()
    logger = logging.getLogger()
    logger.addHandler(handler)
    
    yield logs
    
    logger.removeHandler(handler)


@pytest.fixture
def mock_selenium_driver():
    """
    Mock del driver Selenium per i test.
    
    Returns:
        Mock: Driver Selenium mockato
    """
    driver = Mock()
    driver.get = Mock()
    driver.find_element = Mock()
    driver.find_elements = Mock(return_value=[])
    driver.page_source = "<html><body>Test HTML</body></html>"
    driver.quit = Mock()
    
    return driver


@pytest.fixture
def mock_requests_session():
    """
    Mock della sessione requests per i test.
    
    Returns:
        Mock: Sessione requests mockata
    """
    session = Mock()
    
    # Mock response
    response = Mock()
    response.status_code = 200
    response.json.return_value = {"status": "success", "data": []}
    response.text = "<html><body>Mock HTML</body></html>"
    response.content = b"Mock content"
    
    session.get.return_value = response
    session.post.return_value = response
    
    return session


@pytest.fixture
def performance_monitor():
    """
    Monitor delle performance per i test.
    
    Yields:
        dict: Metriche di performance
    """
    start_time = time.time()
    start_memory = 0
    
    try:
        import psutil
        process = psutil.Process()
        start_memory = process.memory_info().rss / 1024 / 1024  # MB
    except ImportError:
        pass
    
    metrics = {
        "start_time": start_time,
        "start_memory": start_memory,
        "operations": 0
    }
    
    yield metrics
    
    end_time = time.time()
    end_memory = start_memory
    
    try:
        import psutil
        process = psutil.Process()
        end_memory = process.memory_info().rss / 1024 / 1024  # MB
    except ImportError:
        pass
    
    metrics.update({
        "end_time": end_time,
        "duration": end_time - start_time,
        "end_memory": end_memory,
        "memory_delta": end_memory - start_memory
    })


@pytest.fixture
def checkpoint_manager(temp_dir):
    """
    Manager dei checkpoint per i test.
    
    Args:
        temp_dir: Directory temporanea
        
    Returns:
        CheckpointManager: Manager dei checkpoint configurato
    """
    try:
        from utils.checkpoint import CheckpointManager
        return CheckpointManager(base_dir=temp_dir)
    except ImportError:
        # Mock fallback
        manager = Mock()
        manager.save_checkpoint = Mock()
        manager.load_checkpoint = Mock(return_value={})
        manager.checkpoint_exists = Mock(return_value=False)
        return manager


@pytest.fixture
def sample_ocds_large():
    """
    Dati OCDS di grandi dimensioni per test di performance.
    
    Returns:
        dict: Struttura OCDS con molti record
    """
    releases = []
    
    for i in range(1000):  # 1000 record per test performance
        release = {
            "ocid": f"ocds-test-{i:06d}",
            "tender": {
                "title": f"Test Tender {i}",
                "description": "Illuminazione pubblica LED efficiente",
                "tenderPeriod": {
                    "endDate": "2024-12-31T23:59:59Z"
                },
                "procurementMethodDetails": "Procedura aperta",
                "value": {
                    "amount": 50000 + (i * 1000),
                    "currency": "EUR"
                }
            },
            "buyer": {
                "name": f"Comune di Test {i % 50}"
            },
            "awards": [{
                "date": "2024-01-01T00:00:00Z",
                "value": {
                    "amount": 50000 + (i * 1000),
                    "currency": "EUR"
                },
                "suppliers": [{
                    "name": f"Test Supplier {i % 100} SRL"
                }],
                "items": [{
                    "description": "Fornitura e installazione LED",
                    "relatedLot": f"{1000000000 + i}"
                }]
            }]
        }
        releases.append(release)
    
    return {"releases": releases}


@pytest.fixture
def sample_csv_data():
    """
    Dati CSV di esempio per test.
    
    Returns:
        str: Contenuto CSV di esempio
    """
    return """
CIG,Oggetto,AmministrazioneAggiudicatrice,ImportoAggiudicazione,DataAggiudicazione
1234567890,"Illuminazione pubblica LED","Comune di Test",100000.00,01/01/2024
0987654321,"Videosorveglianza urbana","Comune di Prova",50000.00,15/02/2024
5555555555,"Manutenzione verde pubblico","Comune di Esempio",75000.00,30/03/2024
"""


@pytest.fixture
def error_scenarios():
    """
    Scenari di errore per test di resilienza.
    
    Returns:
        dict: Dizionario con scenari di errore
    """
    return {
        "network_timeout": requests.exceptions.Timeout("Connection timed out"),
        "connection_error": requests.exceptions.ConnectionError("Connection failed"),
        "http_500": requests.exceptions.HTTPError("500 Server Error"),
        "json_decode_error": json.JSONDecodeError("Invalid JSON", "doc", 0),
        "file_not_found": FileNotFoundError("File not found"),
        "permission_denied": PermissionError("Permission denied"),
        "memory_error": MemoryError("Out of memory"),
        "value_error": ValueError("Invalid value")
    }


@pytest.fixture(scope="session")
def test_database():
    """
    Database di test per l'intera sessione.
    
    Yields:
        dict: Database mock per i test
    """
    db = {
        "lotti": [],
        "gazzetta_records": [],
        "ocds_records": [],
        "metadata": {
            "created": datetime.now().isoformat(),
            "version": "2.0.0"
        }
    }
    
    yield db
    
    # Cleanup se necessario
    db.clear()


@pytest.fixture
def load_test_config():
    """
    Configurazione per test di carico.
    
    Returns:
        dict: Parametri per test di performance
    """
    return {
        "concurrent_users": 10,
        "requests_per_user": 100,
        "ramp_up_time": 5,  # secondi
        "test_duration": 30,  # secondi
        "acceptable_response_time": 2.0,  # secondi
        "acceptable_error_rate": 0.01,  # 1%
        "memory_limit_mb": 512
    }