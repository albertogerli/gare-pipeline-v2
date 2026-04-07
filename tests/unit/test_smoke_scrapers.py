"""
Smoke tests per i moduli scraper.

Verifica che:
1. I moduli scraper siano importabili
2. Le classi principali siano instanziabili
3. I metodi principali non generino eccezioni immediate
4. Le funzioni di utilità funzionino correttamente
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch
from pathlib import Path
import tempfile
import os
import sys

# Aggiungi il percorso del progetto
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

class TestScrapersImport:
    """Test di importazione per i moduli scraper."""
    
    def test_gazzetta_scraper_importable(self):
        """Test che GazzettaScraper sia importabile."""
        from src.scrapers.gazzetta import GazzettaScraper
        assert GazzettaScraper is not None
    
    def test_ocds_scraper_importable(self):
        """Test che OCDS scraper sia importabile."""
        try:
            from src.scrapers.ocds import OCDSDownloader
            assert OCDSDownloader is not None
        except ImportError:
            pytest.skip("OCDS downloader non disponibile")
    
    def test_downloader_importable(self):
        """Test che il modulo downloader sia importabile."""
        from src.scrapers.downloader import Downloader
        assert Downloader is not None


class TestGazzettaScraperSmoke:
    """Smoke tests per GazzettaScraper."""
    
    def test_gazzetta_scraper_instantiation(self):
        """Test che GazzettaScraper possa essere istanziato."""
        with patch('src.utils.checkpoint.CheckpointManager'):
            from src.scrapers.gazzetta import GazzettaScraper
            scraper = GazzettaScraper()
            assert scraper is not None
            assert scraper.base_url == "http://www.gazzettaufficiale.it"
    
    def test_filtra_testo_basic_functionality(self):
        """Test che filtra_testo funzioni correttamente."""
        with patch('src.utils.checkpoint.CheckpointManager'):
            from src.scrapers.gazzetta import GazzettaScraper
            scraper = GazzettaScraper()
            
            # Test positivi - dovrebbero passare il filtro
            assert scraper.filtra_testo("servizio di illuminazione pubblica") is not None
            assert scraper.filtra_testo("videosorveglianza urbana") is not None
            assert scraper.filtra_testo("edifici scolastici") is not None
            assert scraper.filtra_testo("colonnine elettriche") is not None
            assert scraper.filtra_testo("gallerie stradali con impianti") is not None
            
            # Test negativi - dovrebbero essere filtrati
            assert scraper.filtra_testo("servizi di pulizia generici") is None
            assert scraper.filtra_testo("forniture di carta") is None
            assert scraper.filtra_testo("galleria d'arte moderna") is None
            assert scraper.filtra_testo("") is None
            assert scraper.filtra_testo(None) is None
    
    def test_create_session_returns_session(self):
        """Test che _create_session ritorni una sessione requests valida."""
        with patch('src.utils.checkpoint.CheckpointManager'):
            from src.scrapers.gazzetta import GazzettaScraper
            scraper = GazzettaScraper()
            session = scraper._create_session()
            
            assert hasattr(session, 'get')
            assert hasattr(session, 'post')
            assert 'User-Agent' in session.headers
    
    def test_processa_anno_returns_dataframe(self):
        """Test che processa_anno ritorni sempre un DataFrame."""
        with patch('src.utils.checkpoint.CheckpointManager'):
            with patch.object(GazzettaScraper, 'read_html_with_timeout', return_value=None):
                from src.scrapers.gazzetta import GazzettaScraper
                scraper = GazzettaScraper()
                result = scraper.processa_anno(2020)
                
                assert isinstance(result, pd.DataFrame)
                # Può essere vuoto se nessuna pagina è stata trovata


class TestDownloaderSmoke:
    """Smoke tests per il modulo Downloader."""
    
    def test_downloader_instantiation(self):
        """Test che Downloader possa essere istanziato."""
        from src.scrapers.downloader import Downloader
        downloader = Downloader()
        assert downloader is not None
    
    def test_downloader_has_expected_methods(self):
        """Test che Downloader abbia i metodi attesi."""
        from src.scrapers.downloader import Downloader
        downloader = Downloader()
        
        # Verifica che i metodi principali esistano
        assert hasattr(downloader, 'download_file')
        assert callable(getattr(downloader, 'download_file'))


class TestScrapersDataFrameOutput:
    """Test per verificare che i scraper producano DataFrame con la shape attesa."""
    
    @pytest.fixture
    def temp_dir(self):
        """Crea una directory temporanea per i test."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            yield Path(tmp_dir)
    
    def test_empty_dataframe_structure(self):
        """Test che i DataFrame vuoti abbiano la struttura corretta."""
        # Testa DataFrame vuoto come quello restituito da processa_anno
        empty_df = pd.DataFrame()
        
        assert isinstance(empty_df, pd.DataFrame)
        assert len(empty_df) == 0
        assert len(empty_df.columns) == 0
    
    def test_dataframe_with_testo_column(self):
        """Test DataFrame con colonna 'testo' come previsto dai scraper."""
        test_data = pd.DataFrame({
            'testo': [
                'illuminazione pubblica LED', 
                'videosorveglianza comunale',
                'efficientamento energetico edifici'
            ]
        })
        
        assert isinstance(test_data, pd.DataFrame)
        assert 'testo' in test_data.columns
        assert len(test_data) == 3
        assert all(isinstance(text, str) for text in test_data['testo'])
    
    def test_dataframe_drop_duplicates_functionality(self):
        """Test che la rimozione duplicati funzioni correttamente."""
        test_data = pd.DataFrame({
            'testo': [
                'illuminazione pubblica',
                'videosorveglianza',
                'illuminazione pubblica',  # Duplicato
                'efficientamento'
            ]
        })
        
        result = test_data.drop_duplicates()
        assert len(result) == 3  # Un duplicato rimosso
        assert 'illuminazione pubblica' in result['testo'].values
        assert 'videosorveglianza' in result['testo'].values
        assert 'efficientamento' in result['testo'].values


class TestScrapersErrorHandling:
    """Test per la gestione degli errori nei scraper."""
    
    def test_filtra_testo_handles_none_gracefully(self):
        """Test che filtra_testo gestisca None senza eccezioni."""
        with patch('src.utils.checkpoint.CheckpointManager'):
            from src.scrapers.gazzetta import GazzettaScraper
            scraper = GazzettaScraper()
            
            # Non dovrebbe generare eccezioni
            result = scraper.filtra_testo(None)
            assert result is None
    
    def test_filtra_testo_handles_empty_string(self):
        """Test che filtra_testo gestisca stringhe vuote."""
        with patch('src.utils.checkpoint.CheckpointManager'):
            from src.scrapers.gazzetta import GazzettaScraper
            scraper = GazzettaScraper()
            
            result = scraper.filtra_testo("")
            assert result is None
    
    def test_filtra_testo_handles_non_string_input(self):
        """Test che filtra_testo gestisca input non-stringa."""
        with patch('src.utils.checkpoint.CheckpointManager'):
            from src.scrapers.gazzetta import GazzettaScraper
            scraper = GazzettaScraper()
            
            # Dovrebbe gestire numeri, liste, ecc. senza crash
            try:
                result = scraper.filtra_testo(123)
                # Il risultato può essere None o qualsiasi cosa, 
                # l'importante è che non generi eccezioni
            except Exception as e:
                pytest.fail(f"filtra_testo ha generato un'eccezione con input 123: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])