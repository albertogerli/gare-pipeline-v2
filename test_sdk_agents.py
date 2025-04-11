"""
Script di test per il sistema di agenti implementato con l'SDK ufficiale di OpenAI Agents.

Questo script testa l'integrazione e il funzionamento di tutti gli agenti specializzati
implementati con l'SDK ufficiale di OpenAI Agents.
"""

import os
import sys
import logging
import asyncio
import json
from datetime import datetime

# Configurazione del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("test_sdk_agents.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("TestSDKAgents")

# Importa gli agenti
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from coordinator_agent_sdk import coordinator_agent, run_coordinator
    from gazzetta_agent_sdk import gazzetta_agent, run_gazzetta_extraction
    from json_agent_sdk import json_agent, run_json_analysis
    from cig_agent_sdk import cig_agent, run_cig_download
    from consip_agent_sdk import consip_agent, run_consip_analysis
    from categorization_agent_sdk import categorization_agent, run_categorization
    from link_agent_sdk import link_agent, run_json_scraping_link
    from persistence_agent_sdk import persistence_agent, run_data_persistence
    
    logger.info("Tutti gli agenti importati con successo")
except ImportError as e:
    logger.error(f"Errore durante l'importazione degli agenti: {str(e)}")
    sys.exit(1)

# Funzione per testare l'agente di estrazione dalla Gazzetta Ufficiale
async def test_gazzetta_extraction():
    logger.info("Test dell'agente di estrazione dalla Gazzetta Ufficiale")
    
    try:
        # Parametri di test
        max_pages = 2
        output_file = "temp/test_gazzetta_output.json"
        
        # Esecuzione dell'agente
        result = await run_gazzetta_extraction(max_pages=max_pages, output_file=output_file)
        
        # Verifica del risultato
        if os.path.exists(output_file):
            with open(output_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"Test dell'agente di estrazione dalla Gazzetta Ufficiale completato con successo: {len(data.get('tenders', []))} bandi estratti")
            return True
        else:
            logger.error("Test dell'agente di estrazione dalla Gazzetta Ufficiale fallito: file di output non trovato")
            return False
    except Exception as e:
        logger.error(f"Test dell'agente di estrazione dalla Gazzetta Ufficiale fallito: {str(e)}")
        return False

# Funzione per testare l'agente di analisi JSON
async def test_json_analysis():
    logger.info("Test dell'agente di analisi JSON")
    
    try:
        # Crea un file JSON di esempio
        example_file = "temp/test_ocds_example.json"
        os.makedirs(os.path.dirname(example_file), exist_ok=True)
        
        example_data = {
            "uri": "http://example.com/ocds",
            "publisher": {
                "name": "Test Publisher",
                "scheme": "TEST"
            },
            "publishedDate": datetime.now().isoformat(),
            "version": "1.1",
            "releases": [
                {
                    "ocid": "ocds-test-1",
                    "id": "1",
                    "date": datetime.now().isoformat(),
                    "tag": ["tender"],
                    "initiationType": "tender",
                    "buyer": {
                        "id": "buyer-1",
                        "name": "Test Buyer"
                    },
                    "tender": {
                        "id": "CIG1234567",
                        "title": "Test Tender",
                        "description": "Test Description",
                        "status": "active",
                        "value": {
                            "amount": 100000,
                            "currency": "EUR"
                        }
                    }
                }
            ]
        }
        
        with open(example_file, 'w', encoding='utf-8') as f:
            json.dump(example_data, f, ensure_ascii=False, indent=2)
        
        # Parametri di test
        output_file = "temp/test_json_output.json"
        
        # Esecuzione dell'agente
        result = await run_json_analysis(example_file, output_file)
        
        # Verifica del risultato
        if os.path.exists(output_file):
            with open(output_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"Test dell'agente di analisi JSON completato con successo")
            return True
        else:
            logger.error("Test dell'agente di analisi JSON fallito: file di output non trovato")
            return False
    except Exception as e:
        logger.error(f"Test dell'agente di analisi JSON fallito: {str(e)}")
        return False

# Funzione per testare l'agente di download CIG
async def test_cig_download():
    logger.info("Test dell'agente di download CIG")
    
    try:
        # Parametri di test
        cig_dir = "temp/cig"
        ocds_dir = "temp/ocds"
        max_files = 1
        report_file = "temp/test_download_report.json"
        
        # Esecuzione dell'agente
        result = await run_cig_download(cig_dir, ocds_dir, max_files, report_file)
        
        # Verifica del risultato
        if os.path.exists(report_file):
            with open(report_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"Test dell'agente di download CIG completato con successo")
            return True
        else:
            logger.error("Test dell'agente di download CIG fallito: file di report non trovato")
            return False
    except Exception as e:
        logger.error(f"Test dell'agente di download CIG fallito: {str(e)}")
        return False

# Funzione per testare l'agente Consip
async def test_consip_analysis():
    logger.info("Test dell'agente Consip")
    
    try:
        # Crea un file CIG di esempio
        example_file = "temp/test_cig_example.csv"
        os.makedirs(os.path.dirname(example_file), exist_ok=True)
        
        with open(example_file, 'w', encoding='utf-8') as f:
            f.write("cig;oggetto_gara;importo;data_pubblicazione\n")
            f.write("CIG1234567;Consip Servizio Luce 4 - Esempio di gara;100000;2023-01-01\n")
            f.write("CIG7654321;Consip Servizio Luce 3 - Altro esempio;200000;2022-01-01\n")
            f.write("CIG9876543;Gara non Consip;300000;2023-02-01\n")
        
        # Parametri di test
        output_json = "temp/test_consip_analysis.json"
        output_excel = "temp/test_servizio_luce_consip_cig.xlsx"
        
        # Esecuzione dell'agente
        result = await run_consip_analysis(example_file, None, output_json, output_excel)
        
        # Verifica del risultato
        if os.path.exists(output_json) and os.path.exists(output_excel):
            logger.info(f"Test dell'agente Consip completato con successo")
            return True
        else:
            logger.error("Test dell'agente Consip fallito: file di output non trovati")
            return False
    except Exception as e:
        logger.error(f"Test dell'agente Consip fallito: {str(e)}")
        return False

# Funzione per testare l'agente di categorizzazione
async def test_categorization():
    logger.info("Test dell'agente di categorizzazione")
    
    try:
        # Crea un file di input di esempio
        example_file = "temp/test_tenders_example.json"
        os.makedirs(os.path.dirname(example_file), exist_ok=True)
        
        example_data = {
            "tenders": [
                {
                    "tender_id": "1",
                    "title": "Installazione di illuminazione pubblica a LED",
                    "description": "Fornitura e installazione di lampioni a LED per l'illuminazione pubblica"
                },
                {
                    "tender_id": "2",
                    "title": "Sistema di videosorveglianza per il centro città",
                    "description": "Installazione di telecamere di sicurezza e sistema di monitoraggio"
                },
                {
                    "tender_id": "3",
                    "title": "Manutenzione galleria stradale",
                    "description": "Interventi di manutenzione e illuminazione della galleria"
                }
            ]
        }
        
        with open(example_file, 'w', encoding='utf-8') as f:
            json.dump(example_data, f, ensure_ascii=False, indent=2)
        
        # Parametri di test
        output_file = "temp/test_categorized_tenders.json"
        
        # Esecuzione dell'agente
        result = await run_categorization(example_file, output_file)
        
        # Verifica del risultato
        if os.path.exists(output_file):
            with open(output_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"Test dell'agente di categorizzazione completato con successo")
            return True
        else:
            logger.error("Test dell'agente di categorizzazione fallito: file di output non trovato")
            return False
    except Exception as e:
        logger.error(f"Test dell'agente di categorizzazione fallito: {str(e)}")
        return False

# Funzione per testare l'agente di collegamento JSON-Scraping
async def test_json_scraping_link():
    logger.info("Test dell'agente di collegamento JSON-Scraping")
    
    try:
        # Crea file di esempio
        gazzetta_file = "temp/test_gazzetta_example.json"
        ocds_file = "temp/test_ocds_example.json"
        os.makedirs(os.path.dirname(gazzetta_file), exist_ok=True)
        
        gazzetta_data = {
            "tenders": [
                {
                    "tender_id": "1",
                    "title": "Gara con CIG1234567",
                    "description": "Descrizione della gara con CIG: CIG1234567",
                    "cig": "CIG1234567"
                },
                {
                    "tender_id": "2",
                    "title": "Gara con CIG7654321",
                    "description": "Descrizione della gara con CIG: CIG7654321",
                    "cig": "CIG7654321"
                }
            ]
        }
        
        ocds_data = {
            "releases": [
                {
                    "ocid": "ocds-test-1",
                    "id": "1",
                    "date": datetime.now().isoformat(),
                    "tag": ["tender"],
                    "tender": {
                        "id": "CIG1234567",
                        "title": "Test Tender",
                        "description": "Test Description"
                    },
                    "cig": "CIG1234567"
                }
            ]
        }
        
        with open(gazzetta_file, 'w', encoding='utf-8') as f:
            json.dump(gazzetta_data, f, ensure_ascii=False, indent=2)
        
        with open(ocds_file, 'w', encoding='utf-8') as f:
            json.dump(ocds_data, f, ensure_ascii=False, indent=2)
        
        # Parametri di test
        output_json = "temp/test_linked_records.json"
        output_excel = "temp/test_gare_collegate.xlsx"
        
        # Esecuzione dell'agente
        result = await run_json_scraping_link(gazzetta_file, ocds_file, output_json, output_excel)
        
        # Verifica del risultato
        if os.path.exists(output_json) and os.path.exists(output_excel):
            logger.info(f"Test dell'agente di collegamento JSON-Scraping completato con successo")
            return True
        else:
            logger.error("Test dell'agente di collegamento JSON-Scraping fallito: file di output non trovati")
            return False
    except Exception as e:
        logger.error(f"Test dell'agente di collegamento JSON-Scraping fallito: {str(e)}")
        return False

# Funzione per testare l'agente di persistenza dati
async def test_data_persistence():
    logger.info("Test dell'agente di persistenza dati")
    
    try:
        # Crea dati di esempio
        data = {
            "tenders": [
                {
                    "cig": "CIG1234567",
                    "title": "Test Tender",
                    "description": "Test Description",
                    "contracting_authority": "Test Authority",
                    "publication_date": "2023-01-01",
                    "deadline": "2023-02-01",
                    "amount": 100000,
                    "status": "active",
                    "procurement_method": "open",
                    "source": "gazzetta"
                }
            ],
            "categories": [
                {
                    "name": "illumination",
                    "description": "Illumination category"
                }
            ],
            "subcategories": [
                {
                    "category_id": 1,
                    "name": "led_technology",
                    "description": "LED Technology subcategory"
                }
            ]
        }
        
        # Parametri di test
        database_path = "temp/test_gare_appalto.db"
        
        # Esecuzione dell'agente
        result = await run_data_persistence(database_path, data, ["excel", "json"])
        
        # Verifica del risultato
        if os.path.exists(database_path):
            logger.info(f"Test dell'agente di persistenza dati completato con successo")
            return True
        else:
            logger.error("Test dell'agente di persistenza dati fallito: database non trovato")
            return False
    except Exception as e:
        logger.error(f"Test dell'agente di persistenza dati fallito: {str(e)}")
        return False

# Funzione per testare l'agente coordinatore
async def test_coordinator():
    logger.info("Test dell'agente coordinatore")
    
    try:
        # Parametri di test
        operation = "test"
        config = {
            "max_pages": 1,
            "max_files": 1,
            "output_dir": "temp",
            "database_path": "temp/test_coordinator.db"
        }
        
        # Esecuzione dell'agente
        result = await run_coordinator(operation, config)
        
        logger.info(f"Test dell'agente coordinatore completato con successo")
        return True
    except Exception as e:
        logger.error(f"Test dell'agente coordinatore fallito: {str(e)}")
        return False

# Funzione principale per eseguire tutti i test
async def run_all_tests():
    logger.info("Avvio dei test di tutti gli agenti")
    
    # Crea la directory temp se non esiste
    os.makedirs("temp", exist_ok=True)
    
    # Esegui i test
    test_results = {
        "gazzetta_extraction": await test_gazzetta_extraction(),
        "json_analysis": await test_json_analysis(),
        "cig_download": await test_cig_download(),
        "consip_analysis": await test_consip_analysis(),
        "categorization": await test_categorization(),
        "json_scraping_link": await test_json_scraping_link(),
        "data_persistence": await test_data_persistence(),
        "coordinator": await test_coordinator()
    }
    
    # Verifica i risultati
    all_passed = all(test_results.values())
    
    if all_passed:
        logger.info("Tutti i test sono stati completati con successo")
    else:
        failed_tests = [name for name, result in test_results.items() if not result]
        logger.error(f"I seguenti test sono falliti: {', '.join(failed_tests)}")
    
    # Salva i risultati in un file
    results_file = "test_results.json"
    with open(results_file, 'w', encoding='utf-8') as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "all_passed": all_passed,
            "results": test_results
        }, f, ensure_ascii=False, indent=2)
    
    logger.info(f"Risultati dei test salvati in {results_file}")
    
    return all_passed

# Punto di ingresso per l'esecuzione da linea di comando
if __name__ == "__main__":
    # Verifica che la chiave API di OpenAI sia impostata
    if not os.environ.get("OPENAI_API_KEY"):
        print("ATTENZIONE: La variabile d'ambiente OPENAI_API_KEY non è impostata.")
        print("Per utilizzare il sistema, esegui:")
        print("export OPENAI_API_KEY='la_tua_chiave_api'")
        sys.exit(1)
    
    # Esegui tutti i test
    success = asyncio.run(run_all_tests())
    
    # Esci con il codice appropriato
    sys.exit(0 if success else 1)
