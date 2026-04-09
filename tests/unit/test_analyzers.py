"""
Test per i moduli analyzer.

Questo modulo contiene i test unitari per tutti gli analyzer
del sistema di elaborazione dati.
"""

import pytest
import pandas as pd
import json
import time
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
from datetime import datetime

# Import condizionali
try:
    from analyzers.gazzetta_analyzer_optimized import GazzettaAnalyzerOptimized
    from analyzers.ocds_analyzer_optimized import OCDSAnalyzerOptimized
    from analyzers.json_processor import JsonProcessor
    from analyzers.concatenate import DataConcatenator
    from analyzers.transformer import DataTransformer
    from analyzers.servizio_luce import ServizioLuceAnalyzer
    from analyzers.verbali import VerbaliProcessor
except ImportError:
    pytestmark = pytest.mark.skip("Moduli analyzer non disponibili")


class TestGazzettaAnalyzerOptimized:
    """Test per l'analyzer ottimizzato della Gazzetta."""
    
    def test_analyzer_initialization(self, temp_dir, mock_config):
        """Test inizializzazione analyzer."""
        analyzer = GazzettaAnalyzerOptimized(
            config=mock_config,
            data_dir=temp_dir,
            use_ai=False
        )
        
        assert analyzer.data_dir == temp_dir
        assert analyzer.use_ai is False
        assert analyzer.batch_size > 0
    
    def test_process_single_document(self):
        """Test elaborazione singolo documento."""
        analyzer = GazzettaAnalyzerOptimized(use_ai=False)
        
        test_text = """
        GAZZETTA UFFICIALE
        Bando per illuminazione pubblica LED
        CIG: 1234567890
        Importo: € 100.000,00
        Ente: Comune di Test
        Scadenza: 31/12/2024
        """
        
        result = analyzer.process_document(test_text)
        
        assert result is not None
        assert "cig" in result
        assert result["cig"] == "1234567890"
        assert "illuminazione" in result.get("categoria", "").lower()
    
    def test_batch_processing(self, temp_dir):
        """Test elaborazione batch documenti."""
        analyzer = GazzettaAnalyzerOptimized(
            data_dir=temp_dir,
            batch_size=10,
            use_ai=False
        )
        
        # Crea documenti di test
        documents = []
        for i in range(25):
            doc = f"""
            Bando {i} per servizi comunali
            CIG: {1000000000 + i}
            Importo: € {50000 + i * 1000},00
            Categoria: {'Illuminazione' if i % 2 == 0 else 'Sicurezza'}
            """
            documents.append(doc)
        
        results = analyzer.process_batch(documents)
        
        assert len(results) == 25
        assert all("cig" in result for result in results)
        assert len(set(result["cig"] for result in results)) == 25  # CIG univoci
    
    def test_optimization_features(self):
        """Test funzionalità di ottimizzazione."""
        analyzer = GazzettaAnalyzerOptimized(
            use_caching=True,
            parallel_processing=True,
            max_workers=2
        )
        
        # Test stesso documento più volte (cache)
        test_text = "Illuminazione pubblica LED CIG: 1234567890"
        
        start_time = time.time()
        result1 = analyzer.process_document(test_text)
        first_duration = time.time() - start_time
        
        start_time = time.time()
        result2 = analyzer.process_document(test_text)
        second_duration = time.time() - start_time
        
        assert result1 == result2
        # La seconda chiamata dovrebbe essere più veloce (cache hit)
        assert second_duration <= first_duration or second_duration < 0.01
    
    def test_category_detection(self):
        """Test rilevamento categoria."""
        analyzer = GazzettaAnalyzerOptimized(use_ai=False)
        
        test_cases = [
            ("Illuminazione pubblica LED", "Illuminazione"),
            ("Sistema videosorveglianza", "Sicurezza"),
            ("Manutenzione verde pubblico", "Manutenzione"),
            ("Servizi informatici", "IT"),
            ("Fornitura energia elettrica", "Energia")
        ]
        
        for text, expected_category in test_cases:
            result = analyzer.process_document(text)
            assert expected_category.lower() in result.get("categoria", "").lower()
    
    def test_ai_integration_mock(self, mock_openai_client):
        """Test integrazione AI con mock."""
        analyzer = GazzettaAnalyzerOptimized(use_ai=True)
        analyzer.ai_client = mock_openai_client
        
        # Mock risposta AI
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = json.dumps({
            "categoria": "Illuminazione",
            "tipo_appalto": "Servizio",
            "tipo_illuminazione": "LED",
            "efficientamento": True,
            "keywords": ["LED", "efficienza", "pubblica"]
        })
        
        mock_openai_client.chat.completions.create.return_value = mock_response
        
        result = analyzer.process_document_with_ai("Illuminazione LED efficiente")
        
        assert result.get("categoria") == "Illuminazione"
        assert result.get("efficientamento") is True
        assert "LED" in result.get("keywords", [])
    
    def test_error_handling(self):
        """Test gestione errori."""
        analyzer = GazzettaAnalyzerOptimized()
        
        # Test con testo vuoto
        result = analyzer.process_document("")
        assert result is not None
        assert result.get("error") is not None or len(result) == 0
        
        # Test con testo malformato
        malformed_text = "<<<INVALID>>>DATA>>>"
        result = analyzer.process_document(malformed_text)
        assert result is not None
    
    def test_performance_metrics(self, performance_monitor):
        """Test metriche di performance."""
        analyzer = GazzettaAnalyzerOptimized(
            enable_performance_tracking=True
        )
        
        # Processa documenti con tracking
        documents = [f"Test document {i}" for i in range(100)]
        
        start_time = time.time()
        results = analyzer.process_batch(documents)
        duration = time.time() - start_time
        
        # Verifica performance
        assert len(results) == 100
        assert duration < 10.0  # Deve completare in meno di 10 secondi
        
        # Verifica metriche interne
        metrics = analyzer.get_performance_metrics()
        if metrics:
            assert "processed_documents" in metrics
            assert metrics["processed_documents"] == 100


class TestOCDSAnalyzerOptimized:
    """Test per l'analyzer ottimizzato OCDS."""
    
    def test_analyzer_initialization(self, temp_dir):
        """Test inizializzazione analyzer OCDS."""
        analyzer = OCDSAnalyzerOptimized(
            data_dir=temp_dir,
            chunk_size=1000,
            parallel_processing=True
        )
        
        assert analyzer.data_dir == temp_dir
        assert analyzer.chunk_size == 1000
        assert analyzer.parallel_processing is True
    
    def test_process_single_release(self, sample_json_data):
        """Test elaborazione singola release."""
        analyzer = OCDSAnalyzerOptimized()
        
        release = sample_json_data["releases"][0]
        result = analyzer.process_release(release)
        
        assert result is not None
        assert "ocid" in result
        assert result["ocid"] == "ocds-test-001"
        assert "buyer" in result
    
    def test_bulk_processing(self, sample_ocds_large):
        """Test elaborazione bulk con grandi dataset."""
        analyzer = OCDSAnalyzerOptimized(
            chunk_size=100,
            parallel_processing=True,
            max_workers=2
        )
        
        start_time = time.time()
        results = analyzer.process_bulk(sample_ocds_large)
        duration = time.time() - start_time
        
        assert len(results) == 1000  # Tutti i record del sample_ocds_large
        assert duration < 30.0  # Deve completare in meno di 30 secondi
        assert all("ocid" in result for result in results)
    
    def test_data_filtering(self, sample_ocds_large):
        """Test filtri sui dati."""
        analyzer = OCDSAnalyzerOptimized()
        
        # Filtro per importo minimo
        filters = {
            "min_amount": 75000,
            "buyer_contains": "Test",
            "date_range": ("2024-01-01", "2024-12-31")
        }
        
        results = analyzer.process_bulk(sample_ocds_large, filters=filters)
        
        # Verifica che i filtri siano applicati
        assert all(
            result.get("amount", 0) >= 75000 
            for result in results
        )
    
    def test_aggregation_functions(self, sample_json_data):
        """Test funzioni di aggregazione."""
        analyzer = OCDSAnalyzerOptimized()
        
        # Crea più release per test aggregazione
        test_data = {"releases": []}
        for i in range(50):
            release = sample_json_data["releases"][0].copy()
            release["ocid"] = f"ocds-test-{i:03d}"
            release["awards"][0]["value"]["amount"] = 50000 + (i * 1000)
            test_data["releases"].append(release)
        
        results = analyzer.process_bulk(test_data)
        aggregations = analyzer.calculate_aggregations(results)
        
        assert "total_amount" in aggregations
        assert "count" in aggregations
        assert "average_amount" in aggregations
        assert aggregations["count"] == 50
        assert aggregations["total_amount"] > 2500000  # 50 * 50000 base
    
    def test_memory_optimization(self, sample_ocds_large, performance_monitor):
        """Test ottimizzazioni memoria."""
        analyzer = OCDSAnalyzerOptimized(
            chunk_size=100,
            memory_efficient=True
        )
        
        initial_memory = performance_monitor.get("start_memory", 0)
        
        # Processa dataset grande
        results = analyzer.process_bulk(sample_ocds_large)
        
        final_memory = performance_monitor.get("end_memory", initial_memory)
        memory_delta = final_memory - initial_memory
        
        assert len(results) == 1000
        # Verifica che l'uso di memoria sia ragionevole
        if memory_delta > 0:
            assert memory_delta < 200  # Max 200MB per 1000 record
    
    def test_export_functionality(self, temp_dir, sample_json_data):
        """Test funzionalità di export."""
        analyzer = OCDSAnalyzerOptimized(output_dir=temp_dir)
        
        results = analyzer.process_bulk(sample_json_data)
        
        # Export CSV
        csv_file = analyzer.export_to_csv(results, "test_export.csv")
        assert csv_file.exists()
        
        # Verifica contenuto CSV
        df = pd.read_csv(csv_file)
        assert len(df) == len(results)
        assert "ocid" in df.columns
        
        # Export Excel
        excel_file = analyzer.export_to_excel(results, "test_export.xlsx")
        assert excel_file.exists()
        
        # Verifica contenuto Excel
        df_excel = pd.read_excel(excel_file)
        assert len(df_excel) == len(results)


class TestJsonProcessor:
    """Test per il processore JSON."""
    
    def test_processor_initialization(self, temp_dir):
        """Test inizializzazione processore."""
        processor = JsonProcessor(
            input_dir=temp_dir,
            output_dir=temp_dir,
            chunk_size=500
        )
        
        assert processor.input_dir == temp_dir
        assert processor.output_dir == temp_dir
        assert processor.chunk_size == 500
    
    def test_json_validation(self):
        """Test validazione JSON."""
        processor = JsonProcessor()
        
        # JSON valido
        valid_json = '{"test": true, "array": [1, 2, 3]}'
        assert processor.validate_json(valid_json) is True
        
        # JSON invalido
        invalid_json = '{"test": true, "invalid": }'
        assert processor.validate_json(invalid_json) is False
    
    def test_json_filtering(self):
        """Test filtri JSON."""
        processor = JsonProcessor()
        
        test_data = {
            "releases": [
                {"ocid": "001", "value": 50000, "category": "A"},
                {"ocid": "002", "value": 75000, "category": "B"},
                {"ocid": "003", "value": 100000, "category": "A"}
            ]
        }
        
        # Filtro per valore
        filtered = processor.filter_data(
            test_data, 
            lambda x: x.get("value", 0) > 60000
        )
        
        assert len(filtered["releases"]) == 2
        assert all(r["value"] > 60000 for r in filtered["releases"])
    
    def test_json_transformation(self):
        """Test trasformazioni JSON."""
        processor = JsonProcessor()
        
        test_data = {
            "releases": [
                {"amount": "€ 50.000,00", "date": "01/01/2024"},
                {"amount": "€ 75.000,50", "date": "15/02/2024"}
            ]
        }
        
        # Trasformazione con normalizzazione
        transformed = processor.transform_data(test_data, {
            "amount": lambda x: float(x.replace("€", "").replace(".", "").replace(",", ".")),
            "date": lambda x: datetime.strptime(x, "%d/%m/%Y").isoformat()
        })
        
        assert transformed["releases"][0]["amount"] == 50000.0
        assert "2024-01-01" in transformed["releases"][0]["date"]


class TestDataConcatenator:
    """Test per il concatenatore di dati."""
    
    def test_concatenator_initialization(self, temp_dir):
        """Test inizializzazione concatenatore."""
        concatenator = DataConcatenator(
            input_dirs=[temp_dir / "input1", temp_dir / "input2"],
            output_dir=temp_dir / "output"
        )
        
        assert len(concatenator.input_dirs) == 2
        assert concatenator.output_dir == temp_dir / "output"
    
    def test_file_discovery(self, temp_dir):
        """Test discovery file."""
        concatenator = DataConcatenator(input_dirs=[temp_dir])
        
        # Crea file di test
        test_files = []
        for i in range(5):
            test_file = temp_dir / f"data_{i}.json"
            test_file.write_text('{"test": true}')
            test_files.append(test_file)
        
        discovered = concatenator.discover_files(pattern="*.json")
        
        assert len(discovered) == 5
        assert all(f.suffix == ".json" for f in discovered)
    
    def test_concatenation_process(self, temp_dir):
        """Test processo di concatenazione."""
        concatenator = DataConcatenator(
            input_dirs=[temp_dir],
            output_dir=temp_dir
        )
        
        # Crea file JSON da concatenare
        files_data = []
        for i in range(3):
            data = {
                "releases": [
                    {"id": f"{i}_1", "value": i * 100},
                    {"id": f"{i}_2", "value": i * 100 + 50}
                ]
            }
            
            test_file = temp_dir / f"input_{i}.json"
            with open(test_file, 'w') as f:
                json.dump(data, f)
            files_data.append(test_file)
        
        # Esegui concatenazione
        output_file = concatenator.concatenate_files(
            files_data, 
            "concatenated.json"
        )
        
        assert output_file.exists()
        
        # Verifica risultato
        with open(output_file) as f:
            result = json.load(f)
        
        assert "releases" in result
        assert len(result["releases"]) == 6  # 2 release per 3 file
    
    def test_deduplication(self, temp_dir):
        """Test deduplicazione."""
        concatenator = DataConcatenator(
            input_dirs=[temp_dir],
            deduplicate=True,
            dedup_key="id"
        )
        
        # Crea file con duplicati
        duplicate_data = [
            {"releases": [{"id": "001", "value": 100}, {"id": "002", "value": 200}]},
            {"releases": [{"id": "001", "value": 100}, {"id": "003", "value": 300}]}  # 001 duplicato
        ]
        
        files_data = []
        for i, data in enumerate(duplicate_data):
            test_file = temp_dir / f"dup_{i}.json"
            with open(test_file, 'w') as f:
                json.dump(data, f)
            files_data.append(test_file)
        
        output_file = concatenator.concatenate_files(files_data, "dedup.json")
        
        with open(output_file) as f:
            result = json.load(f)
        
        # Verifica deduplicazione
        ids = [r["id"] for r in result["releases"]]
        assert len(ids) == len(set(ids))  # Nessun duplicato
        assert len(result["releases"]) == 3  # 001, 002, 003


class TestDataTransformer:
    """Test per il trasformatore di dati."""
    
    def test_transformer_initialization(self):
        """Test inizializzazione trasformatore."""
        transformer = DataTransformer(
            transformations={
                "amount": "normalize_currency",
                "date": "parse_date",
                "text": "clean_text"
            }
        )
        
        assert len(transformer.transformations) == 3
        assert "amount" in transformer.transformations
    
    def test_data_transformation(self):
        """Test trasformazione dati."""
        transformer = DataTransformer()
        
        test_data = {
            "records": [
                {
                    "amount": "€ 1.000,50",
                    "date": "01/01/2024",
                    "text": "  Testo   con   spazi  ",
                    "category": "illuminazione"
                }
            ]
        }
        
        transformed = transformer.transform(test_data)
        
        record = transformed["records"][0]
        assert isinstance(record["amount"], float)
        assert record["amount"] == 1000.50
        assert "2024" in str(record["date"])
        assert record["text"] == "Testo con spazi"
    
    def test_custom_transformations(self):
        """Test trasformazioni personalizzate."""
        def custom_uppercase(value):
            return str(value).upper()
        
        transformer = DataTransformer()
        transformer.add_transformation("uppercase", custom_uppercase)
        
        test_data = {"items": [{"name": "test"}]}
        
        rules = {"name": "uppercase"}
        result = transformer.apply_transformations(test_data, rules)
        
        assert result["items"][0]["name"] == "TEST"
    
    def test_bulk_transformation(self, temp_dir):
        """Test trasformazione bulk."""
        transformer = DataTransformer()
        
        # Crea file da trasformare
        input_files = []
        for i in range(3):
            data = {
                "items": [
                    {"amount": f"€ {1000 + i * 100},00", "id": i * 10 + j}
                    for j in range(5)
                ]
            }
            
            input_file = temp_dir / f"transform_input_{i}.json"
            with open(input_file, 'w') as f:
                json.dump(data, f)
            input_files.append(input_file)
        
        # Trasforma bulk
        output_dir = temp_dir / "transformed"
        results = transformer.transform_bulk(
            input_files, 
            output_dir,
            transformations={"amount": "normalize_currency"}
        )
        
        assert len(results) == 3
        assert all(r.exists() for r in results)
        
        # Verifica trasformazione
        with open(results[0]) as f:
            result = json.load(f)
        
        assert isinstance(result["items"][0]["amount"], float)


class TestServizioLuceAnalyzer:
    """Test per l'analyzer dei servizi luce."""
    
    def test_analyzer_initialization(self):
        """Test inizializzazione analyzer luce."""
        analyzer = ServizioLuceAnalyzer(
            efficiency_threshold=0.8,
            led_preference=True
        )
        
        assert analyzer.efficiency_threshold == 0.8
        assert analyzer.led_preference is True
    
    def test_lighting_categorization(self):
        """Test categorizzazione illuminazione."""
        analyzer = ServizioLuceAnalyzer()
        
        test_cases = [
            ("Illuminazione pubblica LED", "LED"),
            ("Lampioni tradizionali sodio", "Tradizionale"),
            ("Sistema smart lighting", "Smart"),
            ("Illuminazione stradale efficiente", "Efficiente")
        ]
        
        for text, expected_type in test_cases:
            result = analyzer.analyze_lighting(text)
            assert expected_type.lower() in result.get("tipo", "").lower()
    
    def test_efficiency_calculation(self):
        """Test calcolo efficienza."""
        analyzer = ServizioLuceAnalyzer()
        
        # Test dati LED (alta efficienza)
        led_data = {
            "tipo": "LED",
            "potenza": "100W",
            "flusso_luminoso": "10000lm"
        }
        
        efficiency = analyzer.calculate_efficiency(led_data)
        assert efficiency > 0.8  # LED dovrebbe essere molto efficiente
        
        # Test dati tradizionali (bassa efficienza)
        trad_data = {
            "tipo": "Tradizionale",
            "potenza": "250W", 
            "flusso_luminoso": "8000lm"
        }
        
        efficiency = analyzer.calculate_efficiency(trad_data)
        assert efficiency < 0.5  # Tradizionale dovrebbe essere meno efficiente
    
    def test_cost_benefit_analysis(self):
        """Test analisi costi-benefici."""
        analyzer = ServizioLuceAnalyzer()
        
        scenario_data = {
            "costo_iniziale": 100000,
            "risparmio_annuo": 15000,
            "durata_anni": 10,
            "manutenzione_annua": 2000
        }
        
        analysis = analyzer.cost_benefit_analysis(scenario_data)
        
        assert "payback_years" in analysis
        assert "total_savings" in analysis
        assert "roi_percentage" in analysis
        
        # Verifica calcoli
        expected_payback = scenario_data["costo_iniziale"] / scenario_data["risparmio_annuo"]
        assert abs(analysis["payback_years"] - expected_payback) < 0.1


class TestVerbaliProcessor:
    """Test per il processore verbali."""
    
    def test_processor_initialization(self, temp_dir):
        """Test inizializzazione processore verbali."""
        processor = VerbaliProcessor(
            input_dir=temp_dir,
            output_dir=temp_dir,
            extract_signatures=True
        )
        
        assert processor.input_dir == temp_dir
        assert processor.extract_signatures is True
    
    def test_verbale_parsing(self):
        """Test parsing verbali."""
        processor = VerbaliProcessor()
        
        test_verbale = """
        VERBALE DI GARA
        
        Data: 15/03/2024
        Oggetto: Illuminazione pubblica LED
        CIG: 1234567890
        
        Commissione:
        - Presidente: Mario Rossi
        - Membro: Luigi Verdi
        - Segretario: Anna Bianchi
        
        Offerte ricevute: 5
        Aggiudicatario: LED Solutions SRL
        Importo aggiudicazione: € 150.000,00
        
        Firma del Presidente
        Mario Rossi
        """
        
        result = processor.parse_verbale(test_verbale)
        
        assert result.get("cig") == "1234567890"
        assert "Mario Rossi" in result.get("commissione", {}).get("presidente", "")
        assert result.get("offerte_ricevute") == 5
        assert "LED Solutions" in result.get("aggiudicatario", "")
    
    def test_signature_extraction(self):
        """Test estrazione firme."""
        processor = VerbaliProcessor(extract_signatures=True)
        
        text_with_signatures = """
        Documento ufficiale
        
        Il Presidente
        Mario Rossi
        
        Il Segretario  
        Anna Bianchi
        
        Per copia conforme
        Luigi Verdi
        Responsabile del Procedimento
        """
        
        signatures = processor.extract_signatures(text_with_signatures)
        
        assert len(signatures) >= 2
        assert any("Mario Rossi" in sig.get("nome", "") for sig in signatures)
        assert any("Presidente" in sig.get("ruolo", "") for sig in signatures)
    
    def test_batch_verbali_processing(self, temp_dir):
        """Test elaborazione batch verbali."""
        processor = VerbaliProcessor(
            input_dir=temp_dir,
            output_dir=temp_dir
        )
        
        # Crea verbali di test
        verbali_data = []
        for i in range(3):
            verbale = f"""
            VERBALE {i}
            Data: {15 + i}/03/2024
            CIG: {1000000000 + i}
            Aggiudicatario: Company {i} SRL
            Importo: € {50000 + i * 10000},00
            """
            
            verbale_file = temp_dir / f"verbale_{i}.txt"
            verbale_file.write_text(verbale)
            verbali_data.append(verbale_file)
        
        results = processor.process_batch(verbali_data)
        
        assert len(results) == 3
        assert all("cig" in result for result in results)
        
        # Verifica output
        output_file = temp_dir / "verbali_processed.json"
        if output_file.exists():
            with open(output_file) as f:
                output_data = json.load(f)
            assert len(output_data) == 3


class TestAnalyzersIntegration:
    """Test di integrazione tra analyzer."""
    
    def test_gazzetta_to_ocds_pipeline(self, temp_dir, sample_json_data):
        """Test pipeline Gazzetta -> OCDS."""
        # Analyzer Gazzetta
        gazzetta_analyzer = GazzettaAnalyzerOptimized(use_ai=False)
        
        gazzetta_text = """
        Bando illuminazione LED
        CIG: 1234567890
        Importo: € 100.000,00
        Comune di Test
        """
        
        gazzetta_result = gazzetta_analyzer.process_document(gazzetta_text)
        
        # Simula conversione a formato OCDS-like
        ocds_like_data = {
            "releases": [{
                "ocid": f"ocds-gazzetta-{gazzetta_result.get('cig', 'unknown')}",
                "tender": {
                    "title": gazzetta_result.get("oggetto", ""),
                    "value": {"amount": gazzetta_result.get("importo", 0)}
                },
                "buyer": {"name": gazzetta_result.get("ente", "")}
            }]
        }
        
        # Analyzer OCDS
        ocds_analyzer = OCDSAnalyzerOptimized()
        ocds_result = ocds_analyzer.process_bulk(ocds_like_data)
        
        assert len(ocds_result) == 1
        assert "1234567890" in ocds_result[0].get("ocid", "")
    
    def test_full_processing_pipeline(self, temp_dir, performance_monitor):
        """Test pipeline completa di elaborazione."""
        # Simula pipeline completa
        start_time = time.time()
        
        # 1. Analisi Gazzetta
        gazzetta_analyzer = GazzettaAnalyzerOptimized(use_ai=False)
        gazzetta_docs = [f"Bando {i} LED" for i in range(50)]
        gazzetta_results = gazzetta_analyzer.process_batch(gazzetta_docs)
        
        # 2. Trasformazione dati
        transformer = DataTransformer()
        transformed_data = {"items": gazzetta_results}
        transformed_results = transformer.transform(transformed_data)
        
        # 3. Concatenazione
        concatenator = DataConcatenator(input_dirs=[temp_dir])
        
        # Salva dati intermedi per concatenazione
        temp_file = temp_dir / "intermediate.json"
        with open(temp_file, 'w') as f:
            json.dump(transformed_results, f)
        
        # 4. Export finale
        final_results = transformed_results["items"]
        
        duration = time.time() - start_time
        
        # Verifica risultati
        assert len(final_results) == 50
        assert duration < 15.0  # Pipeline deve completare in meno di 15 secondi
        
        performance_monitor["operations"] = len(final_results)
        performance_monitor["duration"] = duration