"""
Test per il modulo utils.

Questo file contiene i test per le funzioni di utilità.
"""

import pytest
from datetime import datetime
from src.utils.text import clean_text, hash_text, extract_numbers, truncate_text
from src.utils.date import parse_date, format_date, calculate_duration, parse_duration_text
from src.utils.validation import (
    validate_cig, validate_cup, validate_partita_iva,
    validate_codice_fiscale, validate_importo
)


class TestTextUtils:
    """Test per le utilità di manipolazione testo."""
    
    def test_clean_text_basic(self):
        """Test pulizia testo base."""
        assert clean_text("  test  ") == "test"
        assert clean_text("test\\ntest") == "test test"
        assert clean_text("test\ntest") == "test test"
        assert clean_text("test\xa0test") == "test test"
        assert clean_text("test    test") == "test test"
    
    def test_clean_text_empty(self):
        """Test con testo vuoto."""
        assert clean_text("") == ""
        assert clean_text(None) == ""
    
    def test_hash_text(self):
        """Test generazione hash."""
        hash1 = hash_text("test")
        hash2 = hash_text("test")
        hash3 = hash_text("different")
        
        assert len(hash1) == 64  # SHA-256 produce 64 caratteri hex
        assert hash1 == hash2  # Stesso input produce stesso hash
        assert hash1 != hash3  # Input diverso produce hash diverso
    
    def test_extract_numbers(self):
        """Test estrazione numeri."""
        assert extract_numbers("Il prezzo è 123.45 euro") == 123.45
        assert extract_numbers("1.234,56") == 1234.56
        assert extract_numbers("Nessun numero") is None
    
    def test_truncate_text(self):
        """Test troncamento testo."""
        assert truncate_text("Testo breve", 20) == "Testo breve"
        assert truncate_text("Testo molto lungo", 10) == "Testo m..."
        assert truncate_text("Test", 10, "***") == "Test"


class TestDateUtils:
    """Test per le utilità di gestione date."""
    
    def test_parse_date_formats(self):
        """Test parsing di vari formati data."""
        assert parse_date("15/03/2024") == datetime(2024, 3, 15)
        assert parse_date("15-03-2024") == datetime(2024, 3, 15)
        assert parse_date("15.03.2024") == datetime(2024, 3, 15)
        assert parse_date("2024-03-15") == datetime(2024, 3, 15)
    
    def test_parse_date_italian(self):
        """Test parsing con mesi italiani."""
        assert parse_date("15 marzo 2024") == datetime(2024, 3, 15)
        assert parse_date("1 gennaio 2024") == datetime(2024, 1, 1)
    
    def test_parse_date_invalid(self):
        """Test con date non valide."""
        assert parse_date("non specificata") is None
        assert parse_date("") is None
        assert parse_date("invalid") is None
    
    def test_format_date(self):
        """Test formattazione date."""
        date = datetime(2024, 3, 15)
        assert format_date(date) == "15/03/2024"
        assert format_date(date, "%Y-%m-%d") == "2024-03-15"
        assert format_date(None) == ""
    
    def test_calculate_duration(self):
        """Test calcolo durata."""
        assert calculate_duration("01/01/2024", "31/01/2024") == 30
        assert calculate_duration("01/01/2024", "01/01/2025") == 365
        assert calculate_duration("invalid", "31/01/2024") is None
    
    def test_parse_duration_text(self):
        """Test parsing durata da testo."""
        assert parse_duration_text("3 anni") == 1095
        assert parse_duration_text("6 mesi") == 180
        assert parse_duration_text("30 giorni") == 30
        assert parse_duration_text("ventennale") == 7300
        assert parse_duration_text("non specificata") is None


class TestValidationUtils:
    """Test per le utilità di validazione."""
    
    def test_validate_cig(self):
        """Test validazione CIG."""
        assert validate_cig("1234567890") == "1234567890"
        assert validate_cig("ZB12345678") == "ZB12345678"
        assert validate_cig("X123456789") == "X123456789"
        assert validate_cig("invalid") is None
        assert validate_cig("") is None
        assert validate_cig("NON SPECIFICATO") is None
    
    def test_validate_cup(self):
        """Test validazione CUP."""
        assert validate_cup("B12F11000370004") == "B12F11000370004"
        assert validate_cup("invalid") is None
        assert validate_cup("") is None
        assert validate_cup("N/A") is None
    
    def test_validate_partita_iva(self):
        """Test validazione partita IVA."""
        # Test con partita IVA valida di esempio
        assert validate_partita_iva("12345678903") == "12345678903"
        assert validate_partita_iva("123") is None
        assert validate_partita_iva("") is None
    
    def test_validate_codice_fiscale(self):
        """Test validazione codice fiscale."""
        # Formato persona fisica
        assert validate_codice_fiscale("RSSMRA80A01H501U") == "RSSMRA80A01H501U"
        # Formato persona giuridica (11 cifre)
        assert validate_codice_fiscale("12345678903") == "12345678903"
        assert validate_codice_fiscale("invalid") is None
    
    def test_validate_importo(self):
        """Test validazione importi."""
        assert validate_importo("1.234,56") == 1234.56
        assert validate_importo("€ 1.000,00") == 1000.0
        assert validate_importo("1,234.56") == 1234.56
        assert validate_importo("$1000") == 1000.0
        assert validate_importo("-100") is None
        assert validate_importo("invalid") is None