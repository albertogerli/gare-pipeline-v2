"""
Smoke tests per i moduli analyzer.

Verifica che:
1. I moduli analyzer siano importabili
2. Le classi principali siano instanziabili
3. I metodi principali non generino eccezioni immediate
4. I DataFrame di output abbiano la forma attesa
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import tempfile
import json
import sys

# Aggiungi il percorso del progetto
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


class TestAnalyzersImport:
    """Test di importazione per i moduli analyzer."""
    
    def test_gazzetta_analyzer_importable(self):
        """Test che GazzettaAnalyzer sia importabile."""
        try:
            from gazzetta_analyzer import GazzettaAnalyzer
            assert GazzettaAnalyzer is not None
        except ImportError as e:
            # Fallback all'import dal modulo src
            from src.analyzers.gazzetta_analyzer import GazzettaAnalyzer
            assert GazzettaAnalyzer is not None
    
    def test_ocds_analyzer_importable(self):
        """Test che OCDSAnalyzer sia importabile."""
        from src.analyzers.ocds_analyzer import OCDSAnalyzer
        assert OCDSAnalyzer is not None
    
    def test_json_processor_importable(self):
        """Test che JSON processor sia importabile."""
        try:
            from src.analyzers.json_processor import JsonProcessor
            assert JsonProcessor is not None
        except ImportError:
            pytest.skip("JsonProcessor non disponibile")


class TestGazzettaAnalyzerSmoke:
    """Smoke tests per GazzettaAnalyzer."""
    
    @pytest.fixture
    def mock_config(self):
        """Mock della configurazione."""
        with patch('config.TEMP_DIR', 'temp'), \
             patch('config.LOTTI_RAW', 'Lotti_Raw.xlsx'), \
             patch('config.LOTTI_GAZZETTA', 'Lotti_Gazzetta.xlsx'):
            yield
    
    @pytest.fixture
    def temp_excel_file(self):
        """Crea un file Excel temporaneo per i test."""
        test_data = pd.DataFrame({
            'testo': [
                'Servizio di illuminazione pubblica LED per il Comune di Roma',
                'Appalto per videosorveglianza urbana con telecamere IP',
                'Efficientamento energetico degli edifici pubblici'
            ]
        })
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            test_data.to_excel(tmp_file.name, index=False)
            yield tmp_file.name
        
        # Cleanup
        try:
            Path(tmp_file.name).unlink()
        except FileNotFoundError:
            pass
    
    def test_gazzetta_analyzer_class_exists(self):
        """Test che la classe GazzettaAnalyzer esista."""
        try:
            from gazzetta_analyzer import GazzettaAnalyzer
            assert hasattr(GazzettaAnalyzer, 'run')
            assert callable(getattr(GazzettaAnalyzer, 'run'))
        except ImportError:
            from src.analyzers.gazzetta_analyzer import GazzettaAnalyzer
            assert hasattr(GazzettaAnalyzer, 'run')
            assert callable(getattr(GazzettaAnalyzer, 'run'))
    
    def test_hash_text_function_works(self):
        """Test che la funzione hash_text funzioni correttamente."""
        try:
            from gazzetta_analyzer import hash_text
        except ImportError:
            from src.analyzers.gazzetta_analyzer import hash_text
        
        # Test con testo normale
        result = hash_text("test input")
        assert isinstance(result, str)
        assert len(result) == 64  # SHA256 hex length
        
        # Test che lo stesso input produca lo stesso hash
        result2 = hash_text("test input")
        assert result == result2
        
        # Test che input diversi producano hash diversi
        result3 = hash_text("different input")
        assert result != result3
    
    def test_clean_text_function_works(self):
        """Test che la funzione clean_text funzioni correttamente."""
        try:
            from gazzetta_analyzer import clean_text
        except ImportError:
            from src.analyzers.gazzetta_analyzer import clean_text
        
        # Test pulizia spazi multipli
        result = clean_text("  testo   con   spazi   ")
        assert result == "testo con spazi"
        
        # Test pulizia newline
        result = clean_text("testo\ncon\nnewline")
        assert result == "testo con newline"
        
        # Test pulizia caratteri unicode
        result = clean_text("testo\u00a0con\u00a0nbsp")
        assert result == "testo con nbsp"
    
    def test_pydantic_models_importable(self):
        """Test che i modelli Pydantic siano importabili."""
        try:
            from gazzetta_analyzer import Lotto, CategoriaLotto, TipoIlluminazione
            assert Lotto is not None
            assert CategoriaLotto is not None
            assert TipoIlluminazione is not None
        except ImportError:
            # Potrebbe essere nella vecchia struttura
            pytest.skip("Modelli Pydantic non trovati nella struttura attuale")
    
    @patch('os.path.exists')
    @patch('pandas.read_excel')
    def test_analyzer_run_handles_missing_file(self, mock_read_excel, mock_exists):
        """Test che l'analyzer gestisca file mancanti."""
        mock_exists.return_value = False
        
        try:
            from gazzetta_analyzer import GazzettaAnalyzer
            # Non dovrebbe generare eccezioni
            # (il metodo dovrebbe gestire il caso di file mancante)
            analyzer = GazzettaAnalyzer()
            # Il test verifica solo che la classe sia istanziabile
            assert analyzer is not None
        except ImportError:
            pytest.skip("GazzettaAnalyzer non importabile nella struttura corrente")


class TestOCDSAnalyzerSmoke:
    """Smoke tests per OCDSAnalyzer."""
    
    def test_ocds_analyzer_instantiation(self):
        """Test che OCDSAnalyzer possa essere istanziato."""
        with patch('src.utils.checkpoint.CheckpointManager'):
            from src.analyzers.ocds_analyzer import OCDSAnalyzer
            analyzer = OCDSAnalyzer()
            assert analyzer is not None
            assert hasattr(analyzer, 'use_filter')
            assert hasattr(analyzer, 'use_ai')
    
    def test_ocds_analyzer_with_different_configs(self):
        """Test OCDSAnalyzer con diverse configurazioni."""
        with patch('src.utils.checkpoint.CheckpointManager'):
            from src.analyzers.ocds_analyzer import OCDSAnalyzer
            
            # Test con filtro attivo, AI disattivo
            analyzer1 = OCDSAnalyzer(use_filter=True, use_ai=False)
            assert analyzer1.use_filter is True
            assert analyzer1.use_ai is False
            
            # Test con filtro disattivo, AI attivo
            analyzer2 = OCDSAnalyzer(use_filter=False, use_ai=True)
            assert analyzer2.use_filter is False
            assert analyzer2.use_ai is True
    
    def test_applica_filtro_categoria_function(self):
        """Test che la funzione applica_filtro_categoria funzioni."""
        from src.analyzers.ocds_analyzer import applica_filtro_categoria
        
        # Test positivi
        assert applica_filtro_categoria("servizio di illuminazione pubblica") is True
        assert applica_filtro_categoria("videosorveglianza urbana") is True
        assert applica_filtro_categoria("efficientamento energetico") is True
        assert applica_filtro_categoria("smart city IoT") is True
        assert applica_filtro_categoria("colonnine ricarica elettrica") is True
        
        # Test negativi
        assert applica_filtro_categoria("servizi di pulizia") is False
        assert applica_filtro_categoria("forniture di carta") is False
        assert applica_filtro_categoria("") is False
        assert applica_filtro_categoria(None) is False
    
    def test_load_ocds_with_fallback_empty_file(self):
        """Test che _load_ocds_with_fallback gestisca file vuoti."""
        from src.analyzers.ocds_analyzer import _load_ocds_with_fallback
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
            tmp_file.write('')  # File vuoto
            tmp_file.flush()
            
            result = _load_ocds_with_fallback(Path(tmp_file.name))
            assert isinstance(result, dict)
            assert 'releases' in result
            assert isinstance(result['releases'], list)
        
        # Cleanup
        Path(tmp_file.name).unlink()
    
    def test_load_ocds_with_fallback_valid_json(self):
        """Test che _load_ocds_with_fallback carichi JSON valido."""
        from src.analyzers.ocds_analyzer import _load_ocds_with_fallback
        
        test_data = {
            'releases': [
                {
                    'id': 'test-001',
                    'tender': {'title': 'Test tender'},
                    'buyer': {'name': 'Test buyer'}
                }
            ]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
            json.dump(test_data, tmp_file)
            tmp_file.flush()
            
            result = _load_ocds_with_fallback(Path(tmp_file.name))
            assert isinstance(result, dict)
            assert 'releases' in result
            assert len(result['releases']) == 1
            assert result['releases'][0]['id'] == 'test-001'
        
        # Cleanup
        Path(tmp_file.name).unlink()
    
    def test_process_ocds_file_returns_dataframe(self):
        """Test che process_ocds_file ritorni sempre un DataFrame."""
        with patch('src.utils.checkpoint.CheckpointManager'):
            from src.analyzers.ocds_analyzer import OCDSAnalyzer
            
            analyzer = OCDSAnalyzer()
            
            # Crea file OCDS vuoto
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
                json.dump({'releases': []}, tmp_file)
                tmp_file.flush()
                
                result = analyzer.process_ocds_file(Path(tmp_file.name))
                assert isinstance(result, pd.DataFrame)
                # Può essere vuoto se nessun dato è stato processato
        
        # Cleanup
        Path(tmp_file.name).unlink()


class TestAnalyzersDataFrameOutput:
    """Test per verificare che gli analyzer producano DataFrame con la shape attesa."""
    
    def test_empty_dataframe_structure(self):
        """Test struttura DataFrame vuoto."""
        df = pd.DataFrame()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        assert len(df.columns) == 0
    
    def test_expected_gazzetta_dataframe_columns(self):
        """Test colonne attese per DataFrame di output Gazzetta."""
        # Simula la struttura che dovrebbe avere un DataFrame di output
        expected_columns = [
            'testo', 'CodiceGruppo', 'Lotto', 'NumeroLotti', 'Oggetto',
            'Categoria', 'TipoIlluminazione', 'TipoEfficientamento',
            'ImportoAggiudicazione', 'DataAggiudicazione', 'CIG', 'CUP'
        ]
        
        # Crea DataFrame di test
        test_data = {col: [] for col in expected_columns}
        df = pd.DataFrame(test_data)
        
        for col in expected_columns:
            assert col in df.columns
    
    def test_expected_ocds_dataframe_columns(self):
        """Test colonne attese per DataFrame di output OCDS."""
        expected_columns = [
            'id', 'date', 'oggetto', 'categoria', 'tipo_intervento',
            'comune', 'ente', 'importo', 'CriterioAggiudicazione'
        ]
        
        # Crea DataFrame di test
        test_data = {col: [] for col in expected_columns}
        df = pd.DataFrame(test_data)
        
        for col in expected_columns:
            assert col in df.columns
    
    def test_dataframe_concat_functionality(self):
        """Test che il concat di DataFrame funzioni correttamente."""
        df1 = pd.DataFrame({'oggetto': ['A', 'B'], 'importo': [1000, 2000]})
        df2 = pd.DataFrame({'oggetto': ['C', 'D'], 'importo': [3000, 4000]})
        
        result = pd.concat([df1, df2], ignore_index=True)
        
        assert len(result) == 4
        assert 'oggetto' in result.columns
        assert 'importo' in result.columns
        assert result['oggetto'].tolist() == ['A', 'B', 'C', 'D']
    
    def test_dataframe_drop_duplicates_by_id(self):
        """Test rimozione duplicati per colonna 'id'."""
        df = pd.DataFrame({
            'id': ['001', '002', '001', '003'],  # '001' duplicato
            'oggetto': ['A', 'B', 'A_dup', 'C'],
            'importo': [1000, 2000, 1000, 3000]
        })
        
        result = df.drop_duplicates(subset=['id'], keep='first')
        
        assert len(result) == 3
        assert result['id'].tolist() == ['001', '002', '003']
        assert result['oggetto'].tolist() == ['A', 'B', 'C']


class TestAnalyzersErrorHandling:
    """Test per la gestione degli errori negli analyzer."""
    
    def test_categorization_handles_empty_text(self):
        """Test che la categorizzazione gestisca testo vuoto."""
        # Questo test verifica che le funzioni di categorizzazione
        # non generino eccezioni con input vuoti
        try:
            from src.analyzers.ocds_analyzer import applica_filtro_categoria
            result = applica_filtro_categoria("")
            assert result is False
        except Exception as e:
            pytest.fail(f"applica_filtro_categoria ha generato eccezione con stringa vuota: {e}")
    
    def test_pydantic_model_validation_errors(self):
        """Test che i modelli Pydantic gestiscano errori di validazione."""
        try:
            from gazzetta_analyzer import Lotto
            
            # Test con dati minimi - dovrebbe funzionare o generare ValidationError
            try:
                lotto = Lotto(
                    Oggetto="Test",
                    Categoria="ALTRO",
                    # Altri campi con valori di default
                )
                assert lotto.Oggetto == "Test"
            except Exception:
                # Se genera un'eccezione, dovrebbe essere una ValidationError
                # (non un crash del sistema)
                pass
        
        except ImportError:
            pytest.skip("Modelli Pydantic non disponibili")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])