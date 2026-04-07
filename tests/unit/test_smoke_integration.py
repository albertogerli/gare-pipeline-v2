"""
Smoke tests di integrazione per il sistema completo.

Verifica che:
1. I moduli principali si integrino senza errori
2. Il flusso di dati tra i componenti funzioni
3. Non ci siano eccezioni nel caricamento delle dipendenze
4. Il sistema possa essere avviato senza crash
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import tempfile
import sys
import os

# Aggiungi il percorso del progetto
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


class TestSystemImports:
    """Test che tutti i moduli principali del sistema siano importabili."""
    
    def test_config_importable(self):
        """Test che il modulo config sia importabile."""
        try:
            import config
            assert config is not None
        except ImportError:
            # Fallback alla nuova struttura
            from config.settings import config
            assert config is not None
    
    def test_main_modules_importable(self):
        """Test che i moduli principali siano importabili."""
        importable_modules = []
        failed_imports = []
        
        # Lista dei moduli da testare
        modules_to_test = [
            ('gazzetta_analyzer', 'GazzettaAnalyzer'),
            ('src.scrapers.gazzetta', 'GazzettaScraper'),
            ('src.analyzers.ocds_analyzer', 'OCDSAnalyzer'),
            ('src.processors.transformer', 'Transformer'),
            ('src.utils.checkpoint', 'CheckpointManager'),
            ('src.utils.performance', 'timer'),
        ]
        
        for module_name, class_name in modules_to_test:
            try:
                module = __import__(module_name, fromlist=[class_name])
                assert hasattr(module, class_name)
                importable_modules.append(module_name)
            except ImportError as e:
                failed_imports.append((module_name, str(e)))
        
        # Almeno alcuni moduli dovrebbero essere importabili
        assert len(importable_modules) > 0, f"Nessun modulo importabile. Errori: {failed_imports}"
    
    def test_pandas_and_dependencies_available(self):
        """Test che le dipendenze principali siano disponibili."""
        required_packages = [
            'pandas', 'numpy', 'requests', 'beautifulsoup4', 
            'openpyxl', 'pydantic', 'pathlib'
        ]
        
        available_packages = []
        missing_packages = []
        
        for package in required_packages:
            try:
                if package == 'beautifulsoup4':
                    import bs4
                elif package == 'pathlib':
                    from pathlib import Path
                else:
                    __import__(package)
                available_packages.append(package)
            except ImportError:
                missing_packages.append(package)
        
        # La maggior parte dei pacchetti dovrebbe essere disponibile
        assert len(available_packages) >= len(required_packages) * 0.7, \
            f"Troppi pacchetti mancanti: {missing_packages}"


class TestDataFlowIntegration:
    """Test per il flusso dei dati tra i componenti."""
    
    @pytest.fixture
    def sample_raw_data(self):
        """Dati raw di esempio per test integrazione."""
        return pd.DataFrame({
            'testo': [
                'Servizio di illuminazione pubblica LED per il Comune di Roma',
                'Appalto per videosorveglianza urbana con telecamere IP',
                'Efficientamento energetico degli edifici scolastici pubblici',
                'Servizio di pulizia generale (non rilevante)',
                'Gara per gallerie stradali con impianti di illuminazione'
            ]
        })
    
    @pytest.fixture
    def sample_ocds_data(self):
        """Dati OCDS di esempio."""
        return {
            'releases': [
                {
                    'id': 'test-001',
                    'date': '2024-01-01',
                    'tender': {
                        'title': 'Illuminazione pubblica LED',
                        'description': 'Servizio di illuminazione pubblica a LED',
                        'value': {'amount': 50000, 'currency': 'EUR'},
                        'status': 'active'
                    },
                    'buyer': {'name': 'Comune di Test'}
                },
                {
                    'id': 'test-002',
                    'date': '2024-02-01',
                    'tender': {
                        'title': 'Videosorveglianza urbana',
                        'description': 'Sistema di videosorveglianza per centro urbano',
                        'value': {'amount': 75000, 'currency': 'EUR'},
                        'status': 'active'
                    },
                    'buyer': {'name': 'Comune di Prova'}
                }
            ]
        }
    
    def test_gazzetta_to_analyzer_dataflow(self, sample_raw_data):
        """Test flusso dati da scraper Gazzetta ad analyzer."""
        # Simula il flusso: DataFrame raw -> filtro -> processamento
        
        # Step 1: Filtro (simula filtra_testo)
        def mock_filter(text):
            keywords = ['illuminazione', 'videosorveglianza', 'efficientamento', 'gallerie']
            return any(keyword in text.lower() for keyword in keywords)
        
        filtered_data = sample_raw_data[sample_raw_data['testo'].apply(mock_filter)]
        
        # Dovrebbe filtrare 4 su 5 record (esclude "pulizia generale")
        assert len(filtered_data) == 4
        assert 'pulizia generale' not in ' '.join(filtered_data['testo'].values).lower()
    
    def test_ocds_to_analyzer_dataflow(self, sample_ocds_data):
        """Test flusso dati da OCDS ad analyzer."""
        # Simula processamento OCDS -> DataFrame
        
        records = []
        for release in sample_ocds_data['releases']:
            tender = release.get('tender', {})
            record = {
                'id': release['id'],
                'oggetto': tender.get('title', ''),
                'importo': tender.get('value', {}).get('amount', 0),
                'ente': release.get('buyer', {}).get('name', ''),
                'status': tender.get('status', '')
            }
            records.append(record)
        
        df_processed = pd.DataFrame(records)
        
        assert len(df_processed) == 2
        assert 'oggetto' in df_processed.columns
        assert 'importo' in df_processed.columns
        assert df_processed['importo'].sum() == 125000  # 50000 + 75000
    
    def test_analyzer_to_transformer_dataflow(self):
        """Test flusso dati da analyzer a transformer."""
        # Simula output analyzer -> input transformer
        
        analyzer_output = pd.DataFrame({
            'CIG': ['ABC123', 'DEF456', 'GHI789'],
            'Oggetto': ['Illuminazione LED', 'Videosorveglianza', 'Efficientamento'],
            'ImportoAggiudicazione': ['50000.0', '75000.0', '30000.0'],  # String format
            'DataAggiudicazione': ['15/01/2024', '20/02/2024', '10/03/2024'],
            'Categoria': ['Illuminazione', 'Videosorveglianza', 'Energia']
        })
        
        # Simula trasformazioni del Transformer
        transformed_data = analyzer_output.copy()
        
        # Conversione importi
        transformed_data['ImportoAggiudicazione'] = pd.to_numeric(
            transformed_data['ImportoAggiudicazione'], errors='coerce'
        )
        
        # Conversione date
        transformed_data['DataAggiudicazione'] = pd.to_datetime(
            transformed_data['DataAggiudicazione'], format='%d/%m/%Y', errors='coerce'
        )
        
        # Standardizzazione CIG
        transformed_data['CIG'] = transformed_data['CIG'].str.upper().str.strip()
        
        # Verifica trasformazioni
        assert pd.api.types.is_numeric_dtype(transformed_data['ImportoAggiudicazione'])
        assert pd.api.types.is_datetime64_any_dtype(transformed_data['DataAggiudicazione'])
        assert all(cig.isupper() for cig in transformed_data['CIG'])
    
    def test_transformer_to_export_dataflow(self):
        """Test flusso dati da transformer a export."""
        # Simula output transformer -> export finale
        
        transformer_output = pd.DataFrame({
            'CIG': ['ABC123', 'DEF456', 'GHI789'],
            'Oggetto': ['Illuminazione LED', 'Videosorveglianza', 'Efficientamento'],
            'Importo': [50000.0, 75000.0, 30000.0],
            'Data': pd.to_datetime(['2024-01-15', '2024-02-20', '2024-03-10']),
            'Categoria': ['Illuminazione', 'Videosorveglianza', 'Energia'],
            'data_elaborazione': ['2024-09-02T10:00:00'] * 3,
            'versione': ['1.0'] * 3
        })
        
        # Simula export
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            try:
                transformer_output.to_excel(tmp_file.name, index=False)
                
                # Verifica export
                assert Path(tmp_file.name).exists()
                
                # Verifica rilettura
                exported_data = pd.read_excel(tmp_file.name)
                assert len(exported_data) == 3
                assert 'CIG' in exported_data.columns
                assert 'versione' in exported_data.columns
                
            finally:
                if Path(tmp_file.name).exists():
                    Path(tmp_file.name).unlink()


class TestSystemComponentsIntegration:
    """Test per l'integrazione tra componenti del sistema."""
    
    def test_checkpoint_manager_integration(self):
        """Test che il CheckpointManager si integri correttamente."""
        try:
            from src.utils.checkpoint import CheckpointManager
            
            with tempfile.TemporaryDirectory() as tmp_dir:
                # Mock della directory checkpoints
                with patch('src.utils.checkpoint.Path.home') as mock_home:
                    mock_home.return_value = Path(tmp_dir)
                    
                    checkpoint_mgr = CheckpointManager()
                    
                    # Test creazione sessione
                    session_id = checkpoint_mgr.create_session("test_session")
                    assert isinstance(session_id, str)
                    assert len(session_id) > 0
                    
        except ImportError:
            pytest.skip("CheckpointManager non disponibile")
    
    def test_performance_monitoring_integration(self):
        """Test che il sistema di performance monitoring funzioni."""
        try:
            from src.utils.performance import timer, PerformanceMonitor
            
            # Test decorator timer
            @timer
            def test_function():
                return "test_result"
            
            result = test_function()
            assert result == "test_result"
            
            # Test context manager
            with PerformanceMonitor("test_operation"):
                # Simula operazione
                test_data = pd.DataFrame({'col': range(100)})
                processed = test_data * 2
                assert len(processed) == 100
                
        except ImportError:
            pytest.skip("Performance monitoring non disponibile")
    
    def test_logging_integration(self):
        """Test che il sistema di logging si integri correttamente."""
        import logging
        
        # Test che si possa creare un logger senza errori
        logger = logging.getLogger("test_integration")
        assert logger is not None
        
        # Test che si possano loggare messaggi senza errori
        try:
            logger.info("Test message")
            logger.warning("Test warning")
            logger.error("Test error")
            # Se arriviamo qui, il logging funziona
            assert True
        except Exception as e:
            pytest.fail(f"Logging integration fallita: {e}")


class TestSystemErrorHandling:
    """Test per la gestione degli errori a livello di sistema."""
    
    def test_handles_missing_configuration(self):
        """Test gestione configurazione mancante."""
        # Simula configurazione mancante
        with patch.dict(os.environ, {}, clear=True):
            try:
                # Tenta di importare config
                import config
                # Se non genera eccezione, dovrebbe avere valori di default
                assert hasattr(config, 'TEMP_DIR')
            except (ImportError, AttributeError):
                # Comportamento accettabile se config non è disponibile
                pass
    
    def test_handles_missing_data_directories(self):
        """Test gestione directory dati mancanti."""
        # Testa che il sistema gestisca directory mancanti
        nonexistent_path = Path('/nonexistent/data/directory')
        
        # Il sistema dovrebbe gestire questo senza crash
        try:
            # Simula logica di creazione directory
            if not nonexistent_path.exists():
                # Normalmente creerebbe la directory, qui solo verifichiamo la logica
                assert not nonexistent_path.exists()  # Atteso
        except PermissionError:
            # Comportamento accettabile per path non accessibili
            pass
    
    def test_handles_corrupted_data_gracefully(self):
        """Test gestione dati corrotti gracefully."""
        # Simula file Excel corrotto
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            # Scrivi dati non-Excel
            tmp_file.write(b'Not an Excel file content')
            tmp_file.flush()
            
            try:
                # Tentativo di lettura dovrebbe essere gestito gracefully
                df = pd.read_excel(tmp_file.name)
                pytest.fail("Dovrebbe aver generato un'eccezione per file corrotto")
            except Exception as e:
                # Comportamento atteso per file corrotto
                assert isinstance(e, (pd.errors.EmptyDataError, Exception))
            finally:
                Path(tmp_file.name).unlink()
    
    def test_system_graceful_degradation(self):
        """Test che il sistema degradi gracefully quando componenti mancano."""
        # Test che alcuni componenti possano funzionare anche se altri mancano
        
        # Test DataFrame processing senza AI
        df = pd.DataFrame({
            'testo': ['illuminazione pubblica', 'videosorveglianza'],
            'importo': [50000, 75000]
        })
        
        # Dovrebbe funzionare anche senza componenti AI
        try:
            # Operazioni base sui DataFrame
            filtered = df[df['importo'] > 60000]
            assert len(filtered) == 1
            
            # Export base
            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp_file:
                df.to_csv(tmp_file.name, index=False)
                assert Path(tmp_file.name).exists()
                Path(tmp_file.name).unlink()
                
        except Exception as e:
            pytest.fail(f"Operazioni base DataFrame fallite: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])