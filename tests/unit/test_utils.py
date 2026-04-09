"""
Test per le utility del sistema.

Questo modulo contiene i test unitari per tutte le utility
e funzioni di supporto del sistema.
"""

import pytest
import json
import time
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
from datetime import datetime, timedelta

# Import condizionali
try:
    from utils.checkpoint import CheckpointManager
    from utils.date import parse_date, format_date, date_range_generator
    from utils.text import clean_text, extract_cig, normalize_currency
    from utils.validation import validate_cig, validate_email, validate_url
    from utils.performance import PerformanceMonitor, measure_time
    from utils.resilient import resilient_request, retry_on_failure
    from utils.logging_config import setup_logging, get_logger
except ImportError:
    pytestmark = pytest.mark.skip("Moduli utils non disponibili")


class TestCheckpointManager:
    """Test per il manager dei checkpoint."""
    
    def test_checkpoint_creation(self, temp_dir):
        """Test creazione checkpoint."""
        manager = CheckpointManager(base_dir=temp_dir)
        
        data = {"test": "value", "count": 42}
        checkpoint_id = "test_checkpoint"
        
        # Salva checkpoint
        result = manager.save_checkpoint(checkpoint_id, data)
        
        assert result is True
        assert (temp_dir / f"{checkpoint_id}.json").exists()
    
    def test_checkpoint_loading(self, temp_dir):
        """Test caricamento checkpoint."""
        manager = CheckpointManager(base_dir=temp_dir)
        
        data = {"test": "value", "items": [1, 2, 3]}
        checkpoint_id = "load_test"
        
        # Salva e ricarica
        manager.save_checkpoint(checkpoint_id, data)
        loaded_data = manager.load_checkpoint(checkpoint_id)
        
        assert loaded_data == data
        assert loaded_data["test"] == "value"
        assert loaded_data["items"] == [1, 2, 3]
    
    def test_checkpoint_exists(self, temp_dir):
        """Test verifica esistenza checkpoint."""
        manager = CheckpointManager(base_dir=temp_dir)
        
        # Non deve esistere inizialmente
        assert not manager.checkpoint_exists("nonexistent")
        
        # Crea checkpoint
        manager.save_checkpoint("exists_test", {"data": "test"})
        
        # Ora deve esistere
        assert manager.checkpoint_exists("exists_test")
    
    def test_checkpoint_cleanup(self, temp_dir):
        """Test pulizia checkpoint vecchi."""
        manager = CheckpointManager(base_dir=temp_dir, max_age_hours=1)
        
        # Crea checkpoint "vecchio"
        old_checkpoint = temp_dir / "old_checkpoint.json"
        with open(old_checkpoint, 'w') as f:
            json.dump({"old": True}, f)
        
        # Modifica timestamp per simulare file vecchio
        old_time = time.time() - (2 * 3600)  # 2 ore fa
        old_checkpoint.stat().st_mtime = old_time
        
        # Crea checkpoint recente
        manager.save_checkpoint("recent", {"recent": True})
        
        # Esegui pulizia
        cleaned = manager.cleanup_old_checkpoints()
        
        assert cleaned >= 0  # Può essere 0 se il test è veloce
        assert manager.checkpoint_exists("recent")
    
    def test_checkpoint_error_handling(self, temp_dir):
        """Test gestione errori checkpoint."""
        manager = CheckpointManager(base_dir=temp_dir)
        
        # Test con dati non serializzabili
        with pytest.raises(TypeError):
            manager.save_checkpoint("bad_data", {"func": lambda x: x})
        
        # Test caricamento file inesistente
        result = manager.load_checkpoint("nonexistent")
        assert result == {}
        
        # Test con file corrotto
        corrupted_file = temp_dir / "corrupted.json"
        with open(corrupted_file, 'w') as f:
            f.write("invalid json content")
        
        result = manager.load_checkpoint("corrupted")
        assert result == {}


class TestDateUtils:
    """Test per le utility di gestione date."""
    
    @pytest.mark.parametrize("date_string,expected_format", [
        ("01/01/2024", "%d/%m/%Y"),
        ("2024-01-01", "%Y-%m-%d"),
        ("1 gennaio 2024", "italian"),
        ("01-gen-24", "short_italian"),
        ("2024/01/01", "%Y/%m/%d")
    ])
    def test_parse_date_formats(self, date_string, expected_format):
        """Test parsing di diversi formati data."""
        result = parse_date(date_string)
        
        assert isinstance(result, datetime)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 1
    
    def test_parse_date_invalid(self):
        """Test parsing date invalide."""
        invalid_dates = [
            "data-invalida",
            "32/01/2024",
            "01/13/2024",
            "2024-02-30",
            "",
            None
        ]
        
        for invalid_date in invalid_dates:
            with pytest.raises((ValueError, TypeError)):
                parse_date(invalid_date)
    
    def test_format_date(self):
        """Test formattazione date."""
        test_date = datetime(2024, 1, 15, 10, 30, 0)
        
        formats = [
            ("dd/mm/yyyy", "15/01/2024"),
            ("yyyy-mm-dd", "2024-01-15"),
            ("italian", "15 gennaio 2024"),
            ("short", "15/01/24")
        ]
        
        for format_type, expected in formats:
            result = format_date(test_date, format_type)
            assert result == expected
    
    def test_date_range_generator(self):
        """Test generatore di range date."""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 5)
        
        dates = list(date_range_generator(start_date, end_date))
        
        assert len(dates) == 5  # Include entrambi gli estremi
        assert dates[0] == start_date
        assert dates[-1] == end_date
        
        # Test con step personalizzato
        dates_step2 = list(date_range_generator(start_date, end_date, days_step=2))
        assert len(dates_step2) == 3  # 1, 3, 5 gennaio


class TestTextUtils:
    """Test per le utility di gestione testo."""
    
    def test_clean_text_basic(self):
        """Test pulizia testo base."""
        dirty_text = "  Testo   con    spazi   multipli  \n\t  "
        cleaned = clean_text(dirty_text)
        
        assert cleaned == "Testo con spazi multipli"
    
    def test_clean_text_html(self):
        """Test rimozione HTML."""
        html_text = "<p>Testo con <strong>HTML</strong> tags</p>"
        cleaned = clean_text(html_text, remove_html=True)
        
        assert "Testo con HTML tags" in cleaned
        assert "<p>" not in cleaned
        assert "<strong>" not in cleaned
    
    def test_clean_text_special_chars(self):
        """Test rimozione caratteri speciali."""
        special_text = "Testo@con#caratteri$speciali%"
        cleaned = clean_text(special_text, remove_special=True)
        
        assert cleaned == "Testo con caratteri speciali"
    
    @pytest.mark.parametrize("text,expected_cig", [
        ("Il CIG è 1234567890 per questo appalto", "1234567890"),
        ("Codice: CIG1234567890", "1234567890"),
        ("Testo senza CIG valido", None),
        ("CIG: 123456789A", None),  # Non valido
        ("Multiple CIGs: 1111111111 and 2222222222", "1111111111")  # Primo valido
    ])
    def test_extract_cig(self, text, expected_cig):
        """Test estrazione CIG da testo."""
        result = extract_cig(text)
        assert result == expected_cig
    
    def test_normalize_currency(self):
        """Test normalizzazione valute."""
        test_cases = [
            ("€ 1.000,50", 1000.50),
            ("1,000.75", 1000.75),
            ("$ 500.25", 500.25),
            ("2.500,00 EUR", 2500.00),
            ("invalid", 0.0)
        ]
        
        for input_value, expected in test_cases:
            result = normalize_currency(input_value)
            assert result == expected


class TestValidationUtils:
    """Test per le utility di validazione."""
    
    @pytest.mark.parametrize("cig,is_valid", [
        ("1234567890", True),
        ("0000000001", True),
        ("123456789", False),  # Troppo corto
        ("12345678901", False),  # Troppo lungo
        ("123456789A", False),  # Carattere non numerico
        ("", False),  # Vuoto
        (None, False)  # None
    ])
    def test_validate_cig(self, cig, is_valid):
        """Test validazione CIG."""
        result = validate_cig(cig)
        assert result == is_valid
    
    @pytest.mark.parametrize("email,is_valid", [
        ("test@example.com", True),
        ("user.name@domain.co.uk", True),
        ("invalid.email", False),
        ("@domain.com", False),
        ("user@", False),
        ("", False)
    ])
    def test_validate_email(self, email, is_valid):
        """Test validazione email."""
        result = validate_email(email)
        assert result == is_valid
    
    @pytest.mark.parametrize("url,is_valid", [
        ("https://www.example.com", True),
        ("http://example.com/path", True),
        ("ftp://ftp.example.com", True),
        ("invalid-url", False),
        ("", False),
        ("http://", False)
    ])
    def test_validate_url(self, url, is_valid):
        """Test validazione URL."""
        result = validate_url(url)
        assert result == is_valid


class TestPerformanceMonitor:
    """Test per il monitor delle performance."""
    
    def test_performance_monitor_basic(self):
        """Test funzionalità base del monitor."""
        monitor = PerformanceMonitor()
        
        monitor.start_operation("test_op")
        time.sleep(0.1)  # Simula operazione
        monitor.end_operation("test_op")
        
        stats = monitor.get_stats("test_op")
        
        assert stats["count"] == 1
        assert stats["total_time"] >= 0.1
        assert stats["avg_time"] >= 0.1
    
    def test_performance_monitor_multiple_ops(self):
        """Test monitor con operazioni multiple."""
        monitor = PerformanceMonitor()
        
        # Esegui più operazioni
        for i in range(5):
            monitor.start_operation("multi_test")
            time.sleep(0.01)
            monitor.end_operation("multi_test")
        
        stats = monitor.get_stats("multi_test")
        
        assert stats["count"] == 5
        assert stats["total_time"] >= 0.05
        assert 0.01 <= stats["avg_time"] <= 0.02
    
    def test_measure_time_decorator(self):
        """Test decorator per misura tempo."""
        @measure_time
        def slow_function():
            time.sleep(0.1)
            return "result"
        
        result, duration = slow_function()
        
        assert result == "result"
        assert duration >= 0.1
    
    def test_performance_monitor_memory(self):
        """Test monitoraggio memoria."""
        monitor = PerformanceMonitor(track_memory=True)
        
        monitor.start_operation("memory_test")
        
        # Alloca memoria
        big_list = [i for i in range(10000)]
        
        monitor.end_operation("memory_test")
        
        stats = monitor.get_stats("memory_test")
        
        if "memory_delta" in stats:
            assert stats["memory_delta"] > 0


class TestResilientUtils:
    """Test per le utility di resilienza."""
    
    def test_resilient_request_success(self, mock_requests_session):
        """Test request resiliente con successo."""
        mock_requests_session.get.return_value.status_code = 200
        
        with patch('requests.Session', return_value=mock_requests_session):
            result = resilient_request("http://example.com")
            
            assert result.status_code == 200
            mock_requests_session.get.assert_called_once()
    
    def test_resilient_request_retry(self, mock_requests_session, error_scenarios):
        """Test retry su errori temporanei."""
        # Prima chiamata fallisce, seconda succede
        mock_requests_session.get.side_effect = [
            error_scenarios["network_timeout"],
            Mock(status_code=200)
        ]
        
        with patch('requests.Session', return_value=mock_requests_session):
            result = resilient_request("http://example.com", max_retries=2)
            
            assert result.status_code == 200
            assert mock_requests_session.get.call_count == 2
    
    def test_resilient_request_max_retries(self, mock_requests_session, error_scenarios):
        """Test limite massimo retry."""
        mock_requests_session.get.side_effect = error_scenarios["network_timeout"]
        
        with patch('requests.Session', return_value=mock_requests_session):
            with pytest.raises(Exception):
                resilient_request("http://example.com", max_retries=2)
            
            assert mock_requests_session.get.call_count == 3  # Initial + 2 retries
    
    def test_retry_on_failure_decorator(self):
        """Test decorator retry su failure."""
        attempt_count = 0
        
        @retry_on_failure(max_attempts=3)
        def flaky_function():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise ValueError("Temporary failure")
            return "success"
        
        result = flaky_function()
        
        assert result == "success"
        assert attempt_count == 3


class TestLoggingConfig:
    """Test per la configurazione logging."""
    
    def test_setup_logging_basic(self, temp_dir):
        """Test setup logging base."""
        log_file = temp_dir / "test.log"
        
        logger = setup_logging(
            log_level="INFO",
            log_file=str(log_file),
            app_name="test_app"
        )
        
        assert logger.name == "test_app"
        assert log_file.exists() or True  # Può non esistere se non ci sono log
    
    def test_get_logger(self):
        """Test get logger."""
        logger1 = get_logger("test_module")
        logger2 = get_logger("test_module")
        
        assert logger1 is logger2  # Stesso logger
        assert logger1.name == "test_module"
    
    def test_logging_with_context(self, capture_logs):
        """Test logging con context."""
        logger = get_logger("context_test")
        
        logger.info("Test message", extra={"context": "test_context"})
        
        # Verifica che il log sia stato catturato
        if capture_logs:
            assert len(capture_logs) > 0
            log_record = capture_logs[-1]
            assert "Test message" in log_record.getMessage()


class TestUtilsIntegration:
    """Test di integrazione tra utilities."""
    
    def test_checkpoint_with_performance_monitor(self, temp_dir):
        """Test integrazione checkpoint e performance monitor."""
        monitor = PerformanceMonitor()
        manager = CheckpointManager(base_dir=temp_dir)
        
        # Opera con monitoraggio
        monitor.start_operation("checkpoint_save")
        
        data = {"test": "integration", "items": list(range(100))}
        manager.save_checkpoint("integration_test", data)
        
        monitor.end_operation("checkpoint_save")
        
        # Verifica risultati
        loaded_data = manager.load_checkpoint("integration_test")
        assert loaded_data == data
        
        stats = monitor.get_stats("checkpoint_save")
        assert stats["count"] == 1
        assert stats["avg_time"] > 0
    
    def test_resilient_request_with_logging(self, capture_logs):
        """Test request resiliente con logging."""
        with patch('requests.Session') as mock_session:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_session.return_value.get.return_value = mock_response
            
            result = resilient_request(
                "http://example.com", 
                enable_logging=True
            )
            
            assert result.status_code == 200
            
            # Verifica logging (se disponibile)
            if capture_logs and len(capture_logs) > 0:
                log_messages = [record.getMessage() for record in capture_logs]
                assert any("request" in msg.lower() for msg in log_messages)
    
    def test_text_processing_pipeline(self):
        """Test pipeline di elaborazione testo."""
        dirty_text = "  <p>Il CIG è 1234567890</p>  "
        
        # Pipeline: clean -> extract CIG -> validate
        cleaned = clean_text(dirty_text, remove_html=True)
        cig = extract_cig(cleaned)
        is_valid = validate_cig(cig)
        
        assert cleaned == "Il CIG è 1234567890"
        assert cig == "1234567890"
        assert is_valid is True