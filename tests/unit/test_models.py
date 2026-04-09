"""
Test per i modelli di dominio.

Questo modulo contiene i test unitari per tutti i modelli
definiti nel sistema.
"""

import pytest
from datetime import datetime, date
from decimal import Decimal
from unittest.mock import Mock, patch

# Import condizionali per gestire test in ambienti diversi
try:
    from models.lotto import Lotto
    from models.enums import TipoAppalto, Categoria, TipoIlluminazione
    from models.categorization import LottoCategorizationModel
except ImportError:
    pytestmark = pytest.mark.skip("Modelli non disponibili")


class TestLotto:
    """Test per il modello Lotto."""
    
    def test_lotto_creation_valid_data(self, sample_lotto_data):
        """
        Test creazione lotto con dati validi.
        
        Args:
            sample_lotto_data: Fixture con dati di esempio
        """
        lotto = Lotto(**sample_lotto_data)
        
        assert lotto.Oggetto == "Servizio di illuminazione pubblica"
        assert lotto.Categoria == "Illuminazione"
        assert lotto.CIG == "1234567890"
        assert lotto.ImportoAggiudicazione == "100000.00"
    
    def test_lotto_validation_required_fields(self):
        """Test validazione campi obbligatori."""
        # Test con dati mancanti
        with pytest.raises(TypeError):
            Lotto()
        
        # Test con CIG mancante
        with pytest.raises(ValueError, match="CIG.*required"):
            Lotto(Oggetto="Test", CIG="")
    
    def test_lotto_import_amount_parsing(self):
        """Test parsing importo aggiudicazione."""
        test_cases = [
            ("100000.00", 100000.00),
            ("1.000.000,50", 1000000.50),
            ("50,000.75", 50000.75),
            ("€ 25.500,00", 25500.00)
        ]
        
        for input_value, expected in test_cases:
            lotto_data = {
                "Oggetto": "Test",
                "CIG": "1234567890",
                "ImportoAggiudicazione": input_value
            }
            
            lotto = Lotto(**lotto_data)
            assert lotto.get_numeric_amount() == expected
    
    def test_lotto_date_parsing(self):
        """Test parsing date."""
        date_formats = [
            "01/01/2024",
            "2024-01-01", 
            "1 gennaio 2024",
            "01-gen-24"
        ]
        
        for date_str in date_formats:
            lotto_data = {
                "Oggetto": "Test",
                "CIG": "1234567890",
                "DataAggiudicazione": date_str
            }
            
            lotto = Lotto(**lotto_data)
            parsed_date = lotto.get_parsed_date()
            assert isinstance(parsed_date, (date, datetime))
    
    def test_lotto_categorization(self):
        """Test categorizzazione automatica lotto."""
        test_cases = [
            ("Illuminazione pubblica LED", "Illuminazione"),
            ("Videosorveglianza urbana", "Sicurezza"), 
            ("Manutenzione verde pubblico", "Manutenzione"),
            ("Servizi informatici", "IT")
        ]
        
        for oggetto, expected_category in test_cases:
            lotto_data = {
                "Oggetto": oggetto,
                "CIG": "1234567890"
            }
            
            lotto = Lotto(**lotto_data)
            category = lotto.auto_categorize()
            assert category == expected_category
    
    def test_lotto_serialization(self, sample_lotto_data):
        """Test serializzazione/deserializzazione lotto."""
        lotto = Lotto(**sample_lotto_data)
        
        # Test to_dict
        lotto_dict = lotto.to_dict()
        assert isinstance(lotto_dict, dict)
        assert lotto_dict["CIG"] == "1234567890"
        
        # Test from_dict
        new_lotto = Lotto.from_dict(lotto_dict)
        assert new_lotto.CIG == lotto.CIG
        assert new_lotto.Oggetto == lotto.Oggetto
    
    def test_lotto_comparison(self):
        """Test confronto tra lotti."""
        lotto1 = Lotto(Oggetto="Test 1", CIG="1111111111")
        lotto2 = Lotto(Oggetto="Test 2", CIG="2222222222") 
        lotto3 = Lotto(Oggetto="Test 1", CIG="1111111111")
        
        assert lotto1 != lotto2
        assert lotto1 == lotto3
        assert hash(lotto1) == hash(lotto3)
    
    @pytest.mark.parametrize("field,value,expected_error", [
        ("CIG", "123", "CIG deve essere di 10 cifre"),
        ("CIG", "abc1234567", "CIG deve contenere solo cifre"),
        ("ImportoAggiudicazione", "abc", "Importo non valido"),
        ("DataAggiudicazione", "data-invalida", "Formato data non riconosciuto")
    ])
    def test_lotto_validation_errors(self, field, value, expected_error):
        """Test validazione con errori specifici."""
        lotto_data = {
            "Oggetto": "Test",
            "CIG": "1234567890"
        }
        lotto_data[field] = value
        
        with pytest.raises(ValueError, match=expected_error):
            Lotto(**lotto_data)


class TestEnums:
    """Test per le enumerazioni."""
    
    def test_tipo_appalto_values(self):
        """Test valori TipoAppalto."""
        assert TipoAppalto.SERVIZIO.value == "Servizio"
        assert TipoAppalto.FORNITURA.value == "Fornitura"
        assert TipoAppalto.LAVORI.value == "Lavori"
        
        # Test from string
        assert TipoAppalto.from_string("servizio") == TipoAppalto.SERVIZIO
        assert TipoAppalto.from_string("FORNITURA") == TipoAppalto.FORNITURA
    
    def test_categoria_hierarchy(self):
        """Test gerarchia categorie."""
        illuminazione = Categoria.ILLUMINAZIONE
        assert illuminazione.parent_category == Categoria.ENERGIA
        
        sicurezza = Categoria.SICUREZZA
        assert sicurezza.subcategories == ["Videosorveglianza", "Antifurto", "Controllo accessi"]
    
    def test_tipo_illuminazione_efficiency_rating(self):
        """Test rating efficienza per tipi illuminazione."""
        led = TipoIlluminazione.LED
        assert led.efficiency_rating >= 4.0
        
        tradizionale = TipoIlluminazione.TRADIZIONALE
        assert tradizionale.efficiency_rating <= 2.0


class TestCategorizationModel:
    """Test per il modello di categorizzazione."""
    
    def test_categorization_keywords(self):
        """Test categorizzazione basata su parole chiave."""
        model = LottoCategorizationModel()
        
        test_cases = [
            ("Fornitura LED per illuminazione pubblica", "Illuminazione"),
            ("Sistema videosorveglianza CCTV", "Sicurezza"),
            ("Manutenzione impianti termici", "Manutenzione"),
            ("Sviluppo software gestionale", "IT")
        ]
        
        for text, expected in test_cases:
            result = model.categorize_by_keywords(text)
            assert result == expected
    
    def test_categorization_ml_model(self):
        """Test categorizzazione con modello ML."""
        model = LottoCategorizationModel()
        
        # Mock del modello ML
        with patch.object(model, 'ml_model') as mock_ml:
            mock_ml.predict.return_value = ["Illuminazione"]
            
            result = model.categorize_with_ml("Test illuminazione LED")
            assert result == "Illuminazione"
            mock_ml.predict.assert_called_once()
    
    def test_categorization_confidence_score(self):
        """Test punteggio di confidenza categorizzazione."""
        model = LottoCategorizationModel()
        
        result = model.categorize_with_confidence("Illuminazione pubblica LED")
        
        assert isinstance(result, dict)
        assert "category" in result
        assert "confidence" in result
        assert 0 <= result["confidence"] <= 1
    
    def test_bulk_categorization(self):
        """Test categorizzazione in batch."""
        model = LottoCategorizationModel()
        
        texts = [
            "Illuminazione LED",
            "Videosorveglianza", 
            "Manutenzione verde",
            "Software gestionale"
        ]
        
        results = model.categorize_bulk(texts)
        
        assert len(results) == len(texts)
        assert all(isinstance(result, str) for result in results)
    
    @pytest.mark.parametrize("text,expected_confidence_min", [
        ("Illuminazione pubblica LED efficiente", 0.8),
        ("Sistema videosorveglianza CCTV HD", 0.7),
        ("Manutenzione ordinaria impianti", 0.6),
        ("Testo generico senza specifiche", 0.3)
    ])
    def test_categorization_confidence_levels(self, text, expected_confidence_min):
        """Test livelli di confidenza per diversi testi."""
        model = LottoCategorizationModel()
        
        result = model.categorize_with_confidence(text)
        assert result["confidence"] >= expected_confidence_min


class TestModelIntegration:
    """Test di integrazione tra modelli."""
    
    def test_lotto_with_categorization_model(self, sample_lotto_data):
        """Test integrazione Lotto con modello categorizzazione."""
        # Crea lotto
        lotto = Lotto(**sample_lotto_data)
        
        # Applica categorizzazione
        model = LottoCategorizationModel()
        category = model.categorize_by_keywords(lotto.Oggetto)
        lotto.Categoria = category
        
        assert lotto.Categoria in ["Illuminazione", "Sicurezza", "Manutenzione", "IT"]
    
    def test_batch_processing_integration(self, sample_dataframe):
        """Test elaborazione batch di lotti."""
        lotti = []
        model = LottoCategorizationModel()
        
        for _, row in sample_dataframe.iterrows():
            lotto_data = row.to_dict()
            lotto = Lotto(**lotto_data)
            
            # Applica categorizzazione
            category = model.categorize_by_keywords(lotto.Oggetto or lotto.testo)
            lotto.Categoria = category
            
            lotti.append(lotto)
        
        assert len(lotti) == len(sample_dataframe)
        assert all(hasattr(lotto, 'Categoria') for lotto in lotti)
    
    def test_model_persistence(self, temp_dir):
        """Test persistenza modelli."""
        model = LottoCategorizationModel()
        
        # Salva modello
        model_path = temp_dir / "test_model.pkl"
        model.save(model_path)
        
        # Carica modello
        loaded_model = LottoCategorizationModel.load(model_path)
        
        # Verifica funzionalità
        result1 = model.categorize_by_keywords("Test illuminazione")
        result2 = loaded_model.categorize_by_keywords("Test illuminazione")
        
        assert result1 == result2


class TestModelPerformance:
    """Test di performance per i modelli."""
    
    def test_lotto_creation_performance(self, performance_monitor):
        """Test performance creazione lotti."""
        lotti_count = 1000
        
        start_time = time.time()
        
        for i in range(lotti_count):
            lotto_data = {
                "Oggetto": f"Test {i}",
                "CIG": f"{1000000000 + i}",
                "ImportoAggiudicazione": str(50000 + i)
            }
            Lotto(**lotto_data)
            performance_monitor["operations"] += 1
        
        duration = time.time() - start_time
        
        # Performance requirements
        assert duration < 5.0  # Max 5 secondi per 1000 lotti
        assert performance_monitor["operations"] == lotti_count
    
    def test_categorization_performance(self, performance_monitor):
        """Test performance categorizzazione."""
        model = LottoCategorizationModel()
        texts = [f"Illuminazione pubblica test {i}" for i in range(500)]
        
        start_time = time.time()
        
        results = model.categorize_bulk(texts)
        
        duration = time.time() - start_time
        
        assert len(results) == len(texts)
        assert duration < 10.0  # Max 10 secondi per 500 categorizzazioni
    
    def test_memory_usage_models(self, performance_monitor):
        """Test utilizzo memoria modelli."""
        initial_memory = performance_monitor["start_memory"]
        
        # Crea molti lotti
        lotti = []
        for i in range(1000):
            lotto_data = {
                "Oggetto": f"Test molto lungo con descrizione dettagliata {i}",
                "CIG": f"{1000000000 + i}",
                "ImportoAggiudicazione": str(50000 + i),
                "Descrizione": f"Descrizione molto dettagliata del lotto numero {i}" * 10
            }
            lotti.append(Lotto(**lotto_data))
        
        # Verifica che la memoria non sia cresciuta eccessivamente
        if performance_monitor.get("memory_delta", 0) > 0:
            assert performance_monitor["memory_delta"] < 100  # Max 100MB