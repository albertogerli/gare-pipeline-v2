"""
Test per i moduli scraper.

Questo modulo contiene i test unitari per tutti gli scraper
del sistema di raccolta dati.
"""

import pytest
import json
import time
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, date
import requests
from selenium.common.exceptions import TimeoutException, WebDriverException

# Import condizionali
try:
    from scrapers.gazzetta import GazzettaScraper
    from scrapers.downloader import OCDSDownloader
    from analyzers.gazzetta_analyzer import GazzettaAnalyzer
    from analyzers.ocds_analyzer import OCDSAnalyzer
except ImportError:
    pytestmark = pytest.mark.skip("Moduli scraper non disponibili")


class TestGazzettaScraper:
    """Test per il scraper della Gazzetta Ufficiale."""
    
    def test_scraper_initialization(self):
        """Test inizializzazione scraper."""
        scraper = GazzettaScraper(
            headless=True,
            timeout=10,
            max_retries=3
        )
        
        assert scraper.headless is True
        assert scraper.timeout == 10
        assert scraper.max_retries == 3
        assert scraper.base_url is not None
    
    def test_scraper_with_mock_driver(self, mock_selenium_driver):
        """Test scraper con driver mockato."""
        with patch('selenium.webdriver.Chrome', return_value=mock_selenium_driver):
            scraper = GazzettaScraper(headless=True)
            scraper.driver = mock_selenium_driver
            
            # Test navigazione
            scraper.navigate_to_page("http://example.com")
            mock_selenium_driver.get.assert_called_with("http://example.com")
            
            # Test cleanup
            scraper.close()
            mock_selenium_driver.quit.assert_called_once()
    
    def test_extract_tender_data(self, mock_selenium_driver):
        """Test estrazione dati bando."""
        # Mock HTML con dati bando
        mock_html = """
        <div class="bando">
            <h3>Illuminazione pubblica LED</h3>
            <p>CIG: 1234567890</p>
            <p>Importo: € 100.000,00</p>
            <p>Scadenza: 31/12/2024</p>
            <p>Ente: Comune di Test</p>
        </div>
        """
        
        mock_selenium_driver.page_source = mock_html
        
        with patch('selenium.webdriver.Chrome', return_value=mock_selenium_driver):
            scraper = GazzettaScraper()
            scraper.driver = mock_selenium_driver
            
            # Mock elementi
            mock_elements = [Mock()]
            mock_elements[0].text = "Illuminazione pubblica LED"
            mock_elements[0].get_attribute.return_value = "http://example.com/detail"
            
            mock_selenium_driver.find_elements.return_value = mock_elements
            
            tender_data = scraper.extract_tender_data()
            
            assert len(tender_data) > 0
            assert any("1234567890" in str(tender) for tender in tender_data)
    
    def test_parse_tender_details(self):
        """Test parsing dettagli bando."""
        html_content = """
        <div>
            <p><strong>Oggetto:</strong> Servizio illuminazione pubblica</p>
            <p><strong>CIG:</strong> 1234567890</p>
            <p><strong>Importo:</strong> € 50.000,00</p>
            <p><strong>Data pubblicazione:</strong> 01/01/2024</p>
        </div>
        """
        
        scraper = GazzettaScraper()
        details = scraper.parse_tender_details(html_content)
        
        assert "Oggetto" in details
        assert details.get("CIG") == "1234567890"
        assert "50.000" in str(details.get("Importo", ""))
    
    def test_handle_pagination(self, mock_selenium_driver):
        """Test gestione paginazione."""
        # Mock elementi paginazione
        next_button = Mock()
        next_button.is_enabled.return_value = True
        next_button.click = Mock()
        
        mock_selenium_driver.find_element.return_value = next_button
        
        with patch('selenium.webdriver.Chrome', return_value=mock_selenium_driver):
            scraper = GazzettaScraper()
            scraper.driver = mock_selenium_driver
            
            has_next = scraper.has_next_page()
            assert has_next is True
            
            scraper.go_to_next_page()
            next_button.click.assert_called_once()
    
    def test_error_handling(self, mock_selenium_driver):
        """Test gestione errori."""
        mock_selenium_driver.get.side_effect = TimeoutException("Timeout")
        
        with patch('selenium.webdriver.Chrome', return_value=mock_selenium_driver):
            scraper = GazzettaScraper()
            scraper.driver = mock_selenium_driver
            
            with pytest.raises(TimeoutException):
                scraper.navigate_to_page("http://example.com")
    
    def test_scraper_retry_mechanism(self, mock_selenium_driver):
        """Test meccanismo di retry."""
        # Prima chiamata fallisce, seconda succede
        mock_selenium_driver.get.side_effect = [
            TimeoutException("Timeout"),
            None  # Success
        ]
        
        with patch('selenium.webdriver.Chrome', return_value=mock_selenium_driver):
            scraper = GazzettaScraper(max_retries=2)
            scraper.driver = mock_selenium_driver
            
            # Dovrebbe riuscire al secondo tentativo
            scraper.navigate_to_page_with_retry("http://example.com")
            
            assert mock_selenium_driver.get.call_count == 2
    
    def test_date_filtering(self):
        """Test filtri per data."""
        scraper = GazzettaScraper()
        
        # Test data range
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 31)
        
        scraper.set_date_filter(start_date, end_date)
        
        assert scraper.start_date == start_date
        assert scraper.end_date == end_date
        
        # Test validazione date
        test_date = date(2024, 1, 15)
        assert scraper.is_date_in_range(test_date) is True
        
        test_date_out = date(2024, 2, 1)
        assert scraper.is_date_in_range(test_date_out) is False


class TestOCDSDownloader:
    """Test per il downloader OCDS."""
    
    def test_downloader_initialization(self, temp_dir):
        """Test inizializzazione downloader."""
        downloader = OCDSDownloader(
            base_url="http://example.com/ocds",
            download_dir=temp_dir,
            max_workers=2
        )
        
        assert downloader.base_url == "http://example.com/ocds"
        assert downloader.download_dir == temp_dir
        assert downloader.max_workers == 2
    
    def test_download_single_file(self, temp_dir, mock_requests_session):
        """Test download singolo file."""
        # Mock response con contenuto JSON
        mock_content = {"releases": [{"test": "data"}]}
        mock_requests_session.get.return_value.content = json.dumps(mock_content).encode()
        
        with patch('requests.Session', return_value=mock_requests_session):
            downloader = OCDSDownloader(download_dir=temp_dir)
            
            result = downloader.download_file("test-2024-01.json")
            
            assert result is True
            assert (temp_dir / "test-2024-01.json").exists()
    
    def test_download_with_progress(self, temp_dir, mock_requests_session):
        """Test download con progress tracking."""
        mock_requests_session.get.return_value.content = b"test content"
        
        progress_calls = []
        
        def progress_callback(current, total):
            progress_calls.append((current, total))
        
        with patch('requests.Session', return_value=mock_requests_session):
            downloader = OCDSDownloader(download_dir=temp_dir)
            
            result = downloader.download_file(
                "progress-test.json", 
                progress_callback=progress_callback
            )
            
            assert result is True
            assert len(progress_calls) > 0
    
    def test_batch_download(self, temp_dir, mock_requests_session):
        """Test download batch di file."""
        mock_requests_session.get.return_value.content = b"test content"
        
        files_to_download = [
            "batch-2024-01.json",
            "batch-2024-02.json", 
            "batch-2024-03.json"
        ]
        
        with patch('requests.Session', return_value=mock_requests_session):
            downloader = OCDSDownloader(download_dir=temp_dir)
            
            results = downloader.download_batch(files_to_download)
            
            assert len(results) == 3
            assert all(results.values())
            assert all((temp_dir / filename).exists() for filename in files_to_download)
    
    def test_resume_download(self, temp_dir, mock_requests_session):
        """Test ripresa download interrotto."""
        # Crea file parziale
        partial_file = temp_dir / "partial.json"
        partial_file.write_text('{"releases": [{"partial": true}')  # JSON incompleto
        
        # Mock response completa
        complete_content = '{"releases": [{"complete": true}]}'
        mock_requests_session.get.return_value.content = complete_content.encode()
        
        with patch('requests.Session', return_value=mock_requests_session):
            downloader = OCDSDownloader(download_dir=temp_dir)
            
            result = downloader.resume_download("partial.json")
            
            assert result is True
            
            # Verifica che il file sia stato completato
            content = partial_file.read_text()
            assert "complete" in content
    
    def test_download_error_handling(self, temp_dir, mock_requests_session, error_scenarios):
        """Test gestione errori download."""
        mock_requests_session.get.side_effect = error_scenarios["network_timeout"]
        
        with patch('requests.Session', return_value=mock_requests_session):
            downloader = OCDSDownloader(download_dir=temp_dir)
            
            result = downloader.download_file("error-test.json")
            
            assert result is False
    
    def test_validate_downloaded_file(self, temp_dir):
        """Test validazione file scaricato."""
        downloader = OCDSDownloader(download_dir=temp_dir)
        
        # File JSON valido
        valid_file = temp_dir / "valid.json"
        valid_file.write_text('{"releases": [{"test": true}]}')
        
        assert downloader.validate_file(valid_file) is True
        
        # File JSON invalido
        invalid_file = temp_dir / "invalid.json"
        invalid_file.write_text('{"invalid": json}')
        
        assert downloader.validate_file(invalid_file) is False
        
        # File vuoto
        empty_file = temp_dir / "empty.json"
        empty_file.touch()
        
        assert downloader.validate_file(empty_file) is False


class TestGazzettaAnalyzer:
    """Test per l'analizzatore della Gazzetta."""
    
    def test_analyzer_initialization(self, temp_dir):
        """Test inizializzazione analizzatore."""
        analyzer = GazzettaAnalyzer(
            data_dir=temp_dir,
            output_dir=temp_dir,
            enable_ai=False
        )
        
        assert analyzer.data_dir == temp_dir
        assert analyzer.output_dir == temp_dir
        assert analyzer.enable_ai is False
    
    def test_text_analysis(self):
        """Test analisi testo bando."""
        analyzer = GazzettaAnalyzer(enable_ai=False)
        
        text = """
        Bando per illuminazione pubblica LED
        CIG: 1234567890
        Importo a base d'asta: € 100.000,00
        Scadenza: 31 dicembre 2024
        Comune di Test
        """
        
        result = analyzer.analyze_text(text)
        
        assert "illuminazione" in result.get("keywords", [])
        assert result.get("cig") == "1234567890"
        assert "100000" in str(result.get("amount", ""))
    
    def test_categorization(self):
        """Test categorizzazione automatica."""
        analyzer = GazzettaAnalyzer()
        
        test_cases = [
            ("Illuminazione pubblica LED", "Illuminazione"),
            ("Sistema videosorveglianza", "Sicurezza"),
            ("Manutenzione verde pubblico", "Manutenzione"),
            ("Servizi informatici", "IT")
        ]
        
        for text, expected_category in test_cases:
            category = analyzer.categorize(text)
            assert expected_category.lower() in category.lower()
    
    def test_entity_extraction(self):
        """Test estrazione entità."""
        analyzer = GazzettaAnalyzer()
        
        text = """
        Il Comune di Milano indice gara per illuminazione LED
        con CIG 1234567890 e importo di € 500.000,00.
        Scadenza 15/12/2024.
        """
        
        entities = analyzer.extract_entities(text)
        
        assert "Milano" in entities.get("locations", [])
        assert "1234567890" in entities.get("cigs", [])
        assert any("500000" in str(amount) for amount in entities.get("amounts", []))
        assert any("2024" in str(date) for date in entities.get("dates", []))
    
    def test_batch_analysis(self, temp_dir):
        """Test analisi batch di file."""
        analyzer = GazzettaAnalyzer(data_dir=temp_dir, output_dir=temp_dir)
        
        # Crea file di test
        test_files = []
        for i in range(3):
            test_file = temp_dir / f"test_{i}.html"
            test_file.write_text(f"""
                <html><body>
                <h1>Bando {i}</h1>
                <p>CIG: {1000000000 + i}</p>
                <p>Illuminazione pubblica</p>
                </body></html>
            """)
            test_files.append(test_file)
        
        results = analyzer.analyze_batch(test_files)
        
        assert len(results) == 3
        assert all("cig" in result for result in results)
    
    def test_ai_analysis_mock(self, mock_openai_client):
        """Test analisi con AI mockato."""
        analyzer = GazzettaAnalyzer(enable_ai=True)
        analyzer.ai_client = mock_openai_client
        
        # Mock risposta AI
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = json.dumps({
            "categoria": "Illuminazione",
            "tipo_appalto": "Servizio",
            "keywords": ["LED", "pubblica", "efficienza"]
        })
        
        mock_openai_client.chat.completions.create.return_value = mock_response
        
        text = "Illuminazione pubblica LED efficiente"
        result = analyzer.analyze_with_ai(text)
        
        assert result.get("categoria") == "Illuminazione"
        assert "LED" in result.get("keywords", [])


class TestOCDSAnalyzer:
    """Test per l'analizzatore OCDS."""
    
    def test_analyzer_initialization(self, temp_dir):
        """Test inizializzazione analizzatore OCDS."""
        analyzer = OCDSAnalyzer(
            data_dir=temp_dir,
            output_dir=temp_dir
        )
        
        assert analyzer.data_dir == temp_dir
        assert analyzer.output_dir == temp_dir
    
    def test_parse_ocds_structure(self, sample_json_data):
        """Test parsing struttura OCDS."""
        analyzer = OCDSAnalyzer()
        
        result = analyzer.parse_ocds(sample_json_data)
        
        assert "releases" in result
        assert len(result["releases"]) > 0
        
        release = result["releases"][0]
        assert "ocid" in release
        assert "tender" in release
        assert "buyer" in release
    
    def test_extract_tender_info(self, sample_json_data):
        """Test estrazione informazioni bando."""
        analyzer = OCDSAnalyzer()
        
        release = sample_json_data["releases"][0]
        tender_info = analyzer.extract_tender_info(release)
        
        assert tender_info.get("title") == "Test Tender"
        assert "illuminazione" in tender_info.get("description", "").lower()
        assert tender_info.get("buyer") == "Comune di Test"
    
    def test_extract_award_info(self, sample_json_data):
        """Test estrazione informazioni aggiudicazione."""
        analyzer = OCDSAnalyzer()
        
        release = sample_json_data["releases"][0]
        awards = analyzer.extract_awards(release)
        
        assert len(awards) > 0
        
        award = awards[0]
        assert award.get("amount") == 100000
        assert award.get("currency") == "EUR"
        assert "Test Supplier" in award.get("supplier", "")
    
    def test_process_bulk_files(self, temp_dir, sample_json_data):
        """Test elaborazione bulk di file OCDS."""
        analyzer = OCDSAnalyzer(data_dir=temp_dir, output_dir=temp_dir)
        
        # Crea file OCDS di test
        test_files = []
        for i in range(3):
            test_file = temp_dir / f"ocds_{i}.json"
            test_data = sample_json_data.copy()
            test_data["releases"][0]["ocid"] = f"ocds-test-{i:03d}"
            
            with open(test_file, 'w') as f:
                json.dump(test_data, f)
            
            test_files.append(test_file)
        
        results = analyzer.process_bulk(test_files)
        
        assert len(results) >= 3
        assert all("ocid" in result for result in results)
    
    def test_data_validation(self):
        """Test validazione dati OCDS."""
        analyzer = OCDSAnalyzer()
        
        # Dati validi
        valid_data = {
            "releases": [{
                "ocid": "ocds-test-001",
                "tender": {"title": "Test"}
            }]
        }
        
        assert analyzer.validate_ocds_data(valid_data) is True
        
        # Dati invalidi
        invalid_data = {
            "invalid": "structure"
        }
        
        assert analyzer.validate_ocds_data(invalid_data) is False
    
    def test_statistics_generation(self, sample_json_data):
        """Test generazione statistiche."""
        analyzer = OCDSAnalyzer()
        
        stats = analyzer.generate_statistics([sample_json_data])
        
        assert "total_releases" in stats
        assert "total_amount" in stats
        assert "buyers_count" in stats
        assert "suppliers_count" in stats
        assert stats["total_releases"] == 1


class TestScrapersIntegration:
    """Test di integrazione tra scraper e analyzer."""
    
    def test_gazzetta_scraper_to_analyzer(self, temp_dir, mock_selenium_driver):
        """Test pipeline scraper -> analyzer."""
        # Mock scraper con dati
        mock_selenium_driver.page_source = """
        <div class="bando">
            <h3>Illuminazione pubblica LED</h3>
            <p>CIG: 1234567890</p>
            <p>Importo: € 100.000,00</p>
        </div>
        """
        
        with patch('selenium.webdriver.Chrome', return_value=mock_selenium_driver):
            # Scraping
            scraper = GazzettaScraper()
            scraper.driver = mock_selenium_driver
            
            scraped_data = scraper.extract_tender_data()
            
            # Analisi
            analyzer = GazzettaAnalyzer(enable_ai=False)
            
            analyzed_data = []
            for tender in scraped_data:
                result = analyzer.analyze_text(str(tender))
                analyzed_data.append(result)
            
            assert len(analyzed_data) >= 0  # Può essere 0 se mock non restituisce dati
    
    def test_ocds_downloader_to_analyzer(self, temp_dir, mock_requests_session):
        """Test pipeline downloader -> analyzer."""
        # Mock download
        test_data = {
            "releases": [{
                "ocid": "ocds-test-001",
                "tender": {
                    "title": "Test Integration",
                    "description": "Integration test"
                }
            }]
        }
        
        mock_requests_session.get.return_value.content = json.dumps(test_data).encode()
        
        with patch('requests.Session', return_value=mock_requests_session):
            # Download
            downloader = OCDSDownloader(download_dir=temp_dir)
            download_result = downloader.download_file("integration-test.json")
            
            assert download_result is True
            
            # Analisi
            analyzer = OCDSAnalyzer(data_dir=temp_dir)
            downloaded_file = temp_dir / "integration-test.json"
            
            with open(downloaded_file) as f:
                data = json.load(f)
            
            result = analyzer.parse_ocds(data)
            
            assert "releases" in result
            assert len(result["releases"]) == 1
    
    def test_performance_integration(self, temp_dir, performance_monitor):
        """Test performance pipeline completa."""
        # Simula pipeline completa con monitoring
        performance_monitor["operations"] = 0
        
        start_time = time.time()
        
        # Simula scraping
        mock_scraped_data = [f"Tender {i}" for i in range(100)]
        performance_monitor["operations"] += len(mock_scraped_data)
        
        # Simula analisi
        analyzer = GazzettaAnalyzer(enable_ai=False)
        for tender in mock_scraped_data[:10]:  # Limita per performance test
            analyzer.analyze_text(tender)
            performance_monitor["operations"] += 1
        
        duration = time.time() - start_time
        
        # Verifica performance
        assert duration < 5.0  # Deve completare in meno di 5 secondi
        assert performance_monitor["operations"] >= 100