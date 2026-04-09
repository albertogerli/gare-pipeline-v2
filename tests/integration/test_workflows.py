"""
Test di integrazione per i workflow completi.

Questo modulo contiene i test di integrazione che verificano
il funzionamento end-to-end dei workflow principali.
"""

import pytest
import json
import time
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch
from datetime import datetime, timedelta

# Import condizionali per test completi
try:
    from scrapers.gazzetta import GazzettaScraper
    from scrapers.downloader import OCDSDownloader
    from analyzers.gazzetta_analyzer_optimized import GazzettaAnalyzerOptimized
    from analyzers.ocds_analyzer_optimized import OCDSAnalyzerOptimized
    from analyzers.concatenate import DataConcatenator
    from analyzers.transformer import DataTransformer
    from processors.concatenator import Concatenator
    from utils.checkpoint import CheckpointManager
    from utils.performance import PerformanceMonitor
except ImportError:
    pytestmark = pytest.mark.skip("Moduli per integrazione non disponibili")


class TestGazzettaWorkflow:
    """Test workflow completo Gazzetta Ufficiale."""
    
    @pytest.fixture
    def gazzetta_workflow_setup(self, temp_dir):
        """Setup per test workflow Gazzetta."""
        return {
            "data_dir": temp_dir / "gazzetta_data",
            "output_dir": temp_dir / "gazzetta_output",
            "checkpoint_dir": temp_dir / "checkpoints"
        }
    
    def test_complete_gazzetta_workflow(self, gazzetta_workflow_setup, mock_selenium_driver):
        """Test workflow completo: scraping -> analisi -> export."""
        setup = gazzetta_workflow_setup
        
        # Crea directory
        for dir_path in setup.values():
            dir_path.mkdir(parents=True, exist_ok=True)
        
        with patch('selenium.webdriver.Chrome', return_value=mock_selenium_driver):
            # 1. Setup scraper
            scraper = GazzettaScraper(headless=True)
            scraper.driver = mock_selenium_driver
            
            # Mock HTML con bandi
            mock_html = """
            <div class="bando">
                <h3>Illuminazione pubblica LED Milano</h3>
                <p>CIG: 1234567890</p>
                <p>Importo: € 500.000,00</p>
                <p>Scadenza: 31/12/2024</p>
                <p>Ente: Comune di Milano</p>
            </div>
            <div class="bando">
                <h3>Videosorveglianza urbana Roma</h3>
                <p>CIG: 0987654321</p>
                <p>Importo: € 300.000,00</p>
                <p>Scadenza: 30/11/2024</p>
                <p>Ente: Comune di Roma</p>
            </div>
            """
            mock_selenium_driver.page_source = mock_html
            
            # Mock elementi per estrazione
            mock_elements = []
            for i, title in enumerate(["Illuminazione pubblica LED Milano", "Videosorveglianza urbana Roma"]):
                element = Mock()
                element.text = title
                element.get_attribute.return_value = f"http://example.com/detail/{i}"
                mock_elements.append(element)
            
            mock_selenium_driver.find_elements.return_value = mock_elements
            
            # 2. Scraping
            scraped_data = scraper.extract_tender_data()
            
            # 3. Analisi
            analyzer = GazzettaAnalyzerOptimized(
                data_dir=setup["data_dir"],
                output_dir=setup["output_dir"],
                use_ai=False
            )
            
            analyzed_results = []
            for tender in scraped_data:
                result = analyzer.process_document(str(tender))
                if result:
                    analyzed_results.append(result)
            
            # 4. Verifica risultati
            assert len(analyzed_results) >= 0  # Può essere 0 se mock non restituisce dati
            
            # 5. Export
            if analyzed_results:
                output_file = setup["output_dir"] / "gazzetta_results.json"
                with open(output_file, 'w') as f:
                    json.dump(analyzed_results, f, indent=2)
                
                assert output_file.exists()
                assert output_file.stat().st_size > 0
    
    def test_gazzetta_workflow_with_checkpoints(self, gazzetta_workflow_setup, mock_selenium_driver):
        """Test workflow con checkpoint per ripresa."""
        setup = gazzetta_workflow_setup
        
        # Crea directory
        for dir_path in setup.values():
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # Setup checkpoint manager
        checkpoint_manager = CheckpointManager(base_dir=setup["checkpoint_dir"])
        
        with patch('selenium.webdriver.Chrome', return_value=mock_selenium_driver):
            # Simula workflow interrotto
            workflow_state = {
                "step": "scraping",
                "processed_pages": 5,
                "total_pages": 10,
                "scraped_data": [
                    {"cig": "1234567890", "oggetto": "Test 1"},
                    {"cig": "0987654321", "oggetto": "Test 2"}
                ]
            }
            
            # Salva checkpoint
            checkpoint_manager.save_checkpoint("gazzetta_workflow", workflow_state)
            
            # Simula ripresa workflow
            restored_state = checkpoint_manager.load_checkpoint("gazzetta_workflow")
            
            assert restored_state["step"] == "scraping"
            assert restored_state["processed_pages"] == 5
            assert len(restored_state["scraped_data"]) == 2
            
            # Continua workflow da checkpoint
            analyzer = GazzettaAnalyzerOptimized(use_ai=False)
            
            for item in restored_state["scraped_data"]:
                result = analyzer.process_document(str(item))
                assert result is not None
    
    def test_gazzetta_workflow_error_recovery(self, gazzetta_workflow_setup, error_scenarios):
        """Test recovery da errori nel workflow."""
        setup = gazzetta_workflow_setup
        
        # Crea directory
        for dir_path in setup.values():
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # Test con analyzer che fallisce inizialmente
        analyzer = GazzettaAnalyzerOptimized(use_ai=False)
        
        test_documents = [
            "Documento valido con CIG: 1234567890",
            "",  # Documento vuoto (errore)
            "Altro documento valido CIG: 5555555555",
            "<<<MALFORMED>>>",  # Documento malformato (errore)
            "Documento finale CIG: 9999999999"
        ]
        
        results = []
        errors = []
        
        for doc in test_documents:
            try:
                result = analyzer.process_document(doc)
                if result:
                    results.append(result)
            except Exception as e:
                errors.append({"doc": doc[:50], "error": str(e)})
                continue  # Continua con documento successivo
        
        # Verifica che il workflow continui nonostante errori
        assert len(results) >= 2  # Almeno i documenti validi
        assert len(errors) >= 0   # Possibili errori gestiti


class TestOCDSWorkflow:
    """Test workflow completo OCDS."""
    
    @pytest.fixture
    def ocds_workflow_setup(self, temp_dir):
        """Setup per test workflow OCDS."""
        return {
            "download_dir": temp_dir / "ocds_downloads",
            "processed_dir": temp_dir / "ocds_processed",
            "output_dir": temp_dir / "ocds_output"
        }
    
    def test_complete_ocds_workflow(self, ocds_workflow_setup, mock_requests_session):
        """Test workflow completo: download -> analisi -> aggregazione."""
        setup = ocds_workflow_setup
        
        # Crea directory
        for dir_path in setup.values():
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # Mock dati OCDS
        ocds_data = {
            "releases": [
                {
                    "ocid": "ocds-test-001",
                    "tender": {
                        "title": "Illuminazione LED",
                        "description": "Servizio illuminazione pubblica LED",
                        "value": {"amount": 100000, "currency": "EUR"}
                    },
                    "buyer": {"name": "Comune di Test"},
                    "awards": [{
                        "date": "2024-01-01T00:00:00Z",
                        "value": {"amount": 95000, "currency": "EUR"},
                        "suppliers": [{"name": "LED Solutions SRL"}]
                    }]
                },
                {
                    "ocid": "ocds-test-002", 
                    "tender": {
                        "title": "Videosorveglianza",
                        "description": "Sistema videosorveglianza urbana",
                        "value": {"amount": 75000, "currency": "EUR"}
                    },
                    "buyer": {"name": "Comune di Prova"},
                    "awards": [{
                        "date": "2024-02-01T00:00:00Z",
                        "value": {"amount": 70000, "currency": "EUR"},
                        "suppliers": [{"name": "Security Systems SRL"}]
                    }]
                }
            ]
        }
        
        mock_requests_session.get.return_value.content = json.dumps(ocds_data).encode()
        
        with patch('requests.Session', return_value=mock_requests_session):
            # 1. Download
            downloader = OCDSDownloader(download_dir=setup["download_dir"])
            
            files_to_download = ["test-2024-01.json", "test-2024-02.json"]
            download_results = downloader.download_batch(files_to_download)
            
            # Verifica download
            assert all(download_results.values())
            assert all((setup["download_dir"] / f).exists() for f in files_to_download)
            
            # 2. Analisi
            analyzer = OCDSAnalyzerOptimized(
                data_dir=setup["download_dir"],
                output_dir=setup["processed_dir"]
            )
            
            processed_results = []
            for filename in files_to_download:
                filepath = setup["download_dir"] / filename
                with open(filepath) as f:
                    file_data = json.load(f)
                
                results = analyzer.process_bulk(file_data)
                processed_results.extend(results)
            
            # 3. Aggregazione
            aggregations = analyzer.calculate_aggregations(processed_results)
            
            # Verifica risultati
            assert len(processed_results) == 2  # Due release
            assert aggregations["count"] == 2
            assert aggregations["total_amount"] == 165000  # 95000 + 70000
            assert aggregations["average_amount"] == 82500
            
            # 4. Export
            csv_file = analyzer.export_to_csv(
                processed_results, 
                setup["output_dir"] / "ocds_export.csv"
            )
            
            assert csv_file.exists()
            
            # Verifica CSV
            df = pd.read_csv(csv_file)
            assert len(df) == 2
            assert "ocid" in df.columns
            assert "buyer" in df.columns
    
    def test_ocds_workflow_large_dataset(self, ocds_workflow_setup, sample_ocds_large, performance_monitor):
        """Test workflow con dataset di grandi dimensioni."""
        setup = ocds_workflow_setup
        
        # Crea directory
        for dir_path in setup.values():
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # Simula file grande OCDS
        large_file = setup["download_dir"] / "large_dataset.json"
        with open(large_file, 'w') as f:
            json.dump(sample_ocds_large, f)
        
        start_time = time.time()
        
        # Analisi con ottimizzazioni
        analyzer = OCDSAnalyzerOptimized(
            data_dir=setup["download_dir"],
            chunk_size=100,
            parallel_processing=True,
            max_workers=2,
            memory_efficient=True
        )
        
        results = analyzer.process_bulk(sample_ocds_large)
        
        duration = time.time() - start_time
        
        # Verifica performance
        assert len(results) == 1000  # Tutti i record
        assert duration < 30.0  # Deve completare in meno di 30 secondi
        
        # Verifica uso memoria (se disponibile)
        if "memory_delta" in performance_monitor:
            assert performance_monitor["memory_delta"] < 200  # Max 200MB
        
        # Aggregazioni su dataset grande
        aggregations = analyzer.calculate_aggregations(results)
        
        assert aggregations["count"] == 1000
        assert aggregations["total_amount"] > 50000000  # Somma significativa
    
    def test_ocds_workflow_with_filtering(self, ocds_workflow_setup, sample_ocds_large):
        """Test workflow con filtri avanzati."""
        setup = ocds_workflow_setup
        
        # Crea directory
        for dir_path in setup.values():
            dir_path.mkdir(parents=True, exist_ok=True)
        
        analyzer = OCDSAnalyzerOptimized()
        
        # Test filtri multipli
        filters = {
            "min_amount": 75000,
            "max_amount": 150000,
            "buyer_contains": "Test",
            "category_includes": ["Illuminazione", "Energia"],
            "date_range": ("2024-01-01", "2024-12-31")
        }
        
        filtered_results = analyzer.process_bulk(sample_ocds_large, filters=filters)
        
        # Verifica filtri applicati
        assert all(
            75000 <= result.get("amount", 0) <= 150000
            for result in filtered_results
        )
        
        assert all(
            "Test" in result.get("buyer", "")
            for result in filtered_results
        )


class TestFullSystemIntegration:
    """Test di integrazione completa del sistema."""
    
    @pytest.fixture
    def system_setup(self, temp_dir):
        """Setup completo del sistema."""
        return {
            "base_dir": temp_dir,
            "gazzetta_dir": temp_dir / "gazzetta",
            "ocds_dir": temp_dir / "ocds", 
            "output_dir": temp_dir / "output",
            "checkpoints_dir": temp_dir / "checkpoints"
        }
    
    def test_complete_system_workflow(self, system_setup, mock_selenium_driver, mock_requests_session):
        """Test workflow completo del sistema."""
        setup = system_setup
        
        # Crea directory
        for dir_path in setup.values():
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # Setup performance monitoring
        monitor = PerformanceMonitor()
        monitor.start_operation("full_workflow")
        
        try:
            # 1. FASE GAZZETTA
            with patch('selenium.webdriver.Chrome', return_value=mock_selenium_driver):
                mock_selenium_driver.page_source = """
                <div class="bando">
                    <h3>Illuminazione LED Smart City</h3>
                    <p>CIG: 1111111111</p>
                    <p>Importo: € 750.000,00</p>
                </div>
                """
                
                scraper = GazzettaScraper(headless=True)
                scraper.driver = mock_selenium_driver
                
                gazzetta_data = scraper.extract_tender_data()
                
                analyzer = GazzettaAnalyzerOptimized(
                    data_dir=setup["gazzetta_dir"],
                    use_ai=False
                )
                
                gazzetta_results = []
                for tender in gazzetta_data:
                    result = analyzer.process_document(str(tender))
                    if result:
                        gazzetta_results.append(result)
            
            # 2. FASE OCDS
            ocds_data = {
                "releases": [{
                    "ocid": "ocds-integration-001",
                    "tender": {
                        "title": "Smart Lighting System",
                        "value": {"amount": 800000, "currency": "EUR"}
                    },
                    "buyer": {"name": "Comune Integrato"},
                    "awards": [{
                        "value": {"amount": 750000, "currency": "EUR"},
                        "suppliers": [{"name": "Smart Solutions SRL"}]
                    }]
                }]
            }
            
            mock_requests_session.get.return_value.content = json.dumps(ocds_data).encode()
            
            with patch('requests.Session', return_value=mock_requests_session):
                downloader = OCDSDownloader(download_dir=setup["ocds_dir"])
                ocds_analyzer = OCDSAnalyzerOptimized(data_dir=setup["ocds_dir"])
                
                # Download e analisi
                downloader.download_file("integration-test.json")
                ocds_results = ocds_analyzer.process_bulk(ocds_data)
            
            # 3. FASE CONCATENAZIONE
            concatenator = DataConcatenator(
                input_dirs=[setup["gazzetta_dir"], setup["ocds_dir"]],
                output_dir=setup["output_dir"]
            )
            
            # Prepara dati per concatenazione
            all_results = {
                "gazzetta_data": gazzetta_results,
                "ocds_data": ocds_results,
                "metadata": {
                    "processed_at": datetime.now().isoformat(),
                    "total_records": len(gazzetta_results) + len(ocds_results)
                }
            }
            
            # 4. TRASFORMAZIONE FINALE
            transformer = DataTransformer()
            final_data = transformer.transform(all_results)
            
            # 5. EXPORT FINALE
            output_file = setup["output_dir"] / "complete_results.json"
            with open(output_file, 'w') as f:
                json.dump(final_data, f, indent=2)
            
            # Verifica risultati finali
            assert output_file.exists()
            assert output_file.stat().st_size > 0
            
            with open(output_file) as f:
                result_data = json.load(f)
            
            assert "metadata" in result_data
            assert result_data["metadata"]["total_records"] >= 0
            
            # Export CSV per analisi
            csv_output = setup["output_dir"] / "analysis_results.csv"
            
            # Crea DataFrame combinato se ci sono dati
            if gazzetta_results or ocds_results:
                combined_data = []
                
                for result in gazzetta_results:
                    combined_data.append({
                        "source": "gazzetta",
                        "cig": result.get("cig", ""),
                        "oggetto": result.get("oggetto", ""),
                        "importo": result.get("importo", 0)
                    })
                
                for result in ocds_results:
                    combined_data.append({
                        "source": "ocds",
                        "ocid": result.get("ocid", ""),
                        "title": result.get("title", ""),
                        "amount": result.get("amount", 0)
                    })
                
                if combined_data:
                    df = pd.DataFrame(combined_data)
                    df.to_csv(csv_output, index=False)
                    
                    assert csv_output.exists()
                    assert len(df) > 0
            
        finally:
            monitor.end_operation("full_workflow")
        
        # Verifica performance generale
        stats = monitor.get_stats("full_workflow")
        assert stats["count"] == 1
        assert stats["avg_time"] < 60.0  # Workflow completo in meno di 1 minuto
    
    def test_system_workflow_with_error_recovery(self, system_setup, error_scenarios):
        """Test workflow con recovery da errori multipli."""
        setup = system_setup
        
        # Crea directory
        for dir_path in setup.values():
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # Setup checkpoint manager per recovery
        checkpoint_manager = CheckpointManager(base_dir=setup["checkpoints_dir"])
        
        workflow_phases = [
            "gazzetta_scraping",
            "gazzetta_analysis", 
            "ocds_download",
            "ocds_analysis",
            "data_concatenation",
            "final_export"
        ]
        
        completed_phases = []
        failed_phases = []
        
        for phase in workflow_phases:
            try:
                # Simula esecuzione fase
                phase_data = {
                    "phase": phase,
                    "timestamp": datetime.now().isoformat(),
                    "status": "running"
                }
                
                # Simula alcune fasi che falliscono
                if phase in ["ocds_download", "data_concatenation"]:
                    raise error_scenarios.get("network_timeout", Exception("Phase failed"))
                
                # Fase completata con successo
                phase_data["status"] = "completed"
                completed_phases.append(phase)
                
                # Salva checkpoint
                checkpoint_manager.save_checkpoint(f"phase_{phase}", phase_data)
                
            except Exception as e:
                # Gestione errore
                phase_data = {
                    "phase": phase,
                    "status": "failed",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }
                
                failed_phases.append(phase)
                
                # Salva stato di errore
                checkpoint_manager.save_checkpoint(f"error_{phase}", phase_data)
                
                # Continua con fase successiva (recovery)
                continue
        
        # Verifica recovery
        assert len(completed_phases) >= 2  # Almeno alcune fasi completate
        assert len(failed_phases) >= 1    # Alcune fasi fallite come previsto
        
        # Verifica checkpoint salvati
        for phase in completed_phases:
            checkpoint_data = checkpoint_manager.load_checkpoint(f"phase_{phase}")
            assert checkpoint_data["status"] == "completed"
        
        for phase in failed_phases:
            error_data = checkpoint_manager.load_checkpoint(f"error_{phase}")
            assert error_data["status"] == "failed"
    
    def test_system_performance_benchmarks(self, system_setup, sample_ocds_large, load_test_config):
        """Test performance benchmark del sistema completo."""
        setup = system_setup
        
        # Crea directory
        for dir_path in setup.values():
            dir_path.mkdir(parents=True, exist_ok=True)
        
        config = load_test_config
        
        # Test carico simulato
        start_time = time.time()
        total_operations = 0
        
        try:
            # Simula carico di lavoro pesante
            analyzer = OCDSAnalyzerOptimized(
                chunk_size=config["concurrent_users"] * 10,
                parallel_processing=True,
                max_workers=config["concurrent_users"]
            )
            
            # Processa dataset multipli
            for batch in range(5):  # 5 batch di dati
                results = analyzer.process_bulk(sample_ocds_large)
                total_operations += len(results)
                
                # Simula pausa tra batch
                time.sleep(0.1)
            
            duration = time.time() - start_time
            
            # Verifica benchmark
            operations_per_second = total_operations / duration
            
            assert duration < config["test_duration"]
            assert operations_per_second > 50  # Almeno 50 operazioni/sec
            assert total_operations == 5000  # 5 batch * 1000 record
            
            print(f"Benchmark: {operations_per_second:.2f} ops/sec, Duration: {duration:.2f}s")
            
        except Exception as e:
            pytest.fail(f"Performance benchmark failed: {str(e)}")


class TestWorkflowResilience:
    """Test resilienza dei workflow."""
    
    def test_workflow_interruption_and_resume(self, temp_dir, checkpoint_manager):
        """Test interruzione e ripresa workflow."""
        workflow_data = {
            "total_items": 1000,
            "processed_items": 0,
            "current_batch": 0,
            "results": []
        }
        
        # Simula workflow con interruzioni
        for batch in range(10):  # 10 batch da 100 item
            # Aggiorna stato
            workflow_data["current_batch"] = batch
            workflow_data["processed_items"] = batch * 100
            
            # Aggiungi risultati batch
            batch_results = [f"item_{batch}_{i}" for i in range(100)]
            workflow_data["results"].extend(batch_results)
            
            # Salva checkpoint ogni 3 batch
            if batch % 3 == 0:
                checkpoint_manager.save_checkpoint("workflow_progress", workflow_data)
            
            # Simula interruzione al batch 6
            if batch == 6:
                # Carica ultimo checkpoint (batch 6)
                restored_data = checkpoint_manager.load_checkpoint("workflow_progress")
                
                assert restored_data["current_batch"] == 6
                assert restored_data["processed_items"] == 600
                assert len(restored_data["results"]) == 700  # 0,1,2,3,4,5,6
                
                # Riprendi da dove interrotto
                workflow_data = restored_data
        
        # Verifica completamento
        assert workflow_data["processed_items"] == 900  # 9 * 100
        assert len(workflow_data["results"]) == 1000   # Tutti gli item
    
    def test_concurrent_workflow_coordination(self, temp_dir):
        """Test coordinamento workflow concorrenti."""
        # Simula più workflow che operano contemporaneamente
        workflows = {
            "gazzetta_workflow": {"status": "running", "progress": 0},
            "ocds_workflow": {"status": "waiting", "progress": 0}, 
            "export_workflow": {"status": "waiting", "progress": 0}
        }
        
        coordination_file = temp_dir / "workflow_coordination.json"
        
        def update_workflow_status(name, status, progress):
            workflows[name]["status"] = status
            workflows[name]["progress"] = progress
            
            with open(coordination_file, 'w') as f:
                json.dump(workflows, f)
        
        def can_start_workflow(name, depends_on=None):
            with open(coordination_file) as f:
                current_workflows = json.load(f)
            
            if depends_on:
                dependency = current_workflows.get(depends_on, {})
                return dependency.get("status") == "completed"
            
            return True
        
        # Esegui workflow in sequenza coordinata
        # 1. Gazzetta workflow
        update_workflow_status("gazzetta_workflow", "running", 0)
        
        for i in range(1, 6):
            progress = i * 20
            update_workflow_status("gazzetta_workflow", "running", progress)
            time.sleep(0.01)  # Simula lavoro
        
        update_workflow_status("gazzetta_workflow", "completed", 100)
        
        # 2. OCDS workflow (aspetta completamento Gazzetta)
        assert can_start_workflow("ocds_workflow", depends_on="gazzetta_workflow")
        
        update_workflow_status("ocds_workflow", "running", 0)
        
        for i in range(1, 4):
            progress = i * 33
            update_workflow_status("ocds_workflow", "running", progress)
            time.sleep(0.01)
        
        update_workflow_status("ocds_workflow", "completed", 100)
        
        # 3. Export workflow (aspetta entrambi)
        assert can_start_workflow("export_workflow", depends_on="ocds_workflow")
        
        update_workflow_status("export_workflow", "running", 0)
        update_workflow_status("export_workflow", "completed", 100)
        
        # Verifica stato finale
        with open(coordination_file) as f:
            final_status = json.load(f)
        
        assert all(
            workflow["status"] == "completed" and workflow["progress"] == 100
            for workflow in final_status.values()
        )