"""
Implementazione dell'agente di download CIG utilizzando l'SDK ufficiale di OpenAI Agents.

Questo modulo implementa l'agente specializzato per il download dei dati CIG e OCDS
dall'Autorità Nazionale Anticorruzione.
"""

import os
import logging
import requests
import csv
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
import asyncio
import re
from pydantic import BaseModel

from agents import Agent, Runner, Context, tool

# Configurazione del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("cig_agent.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("CIGAgent")

# Definizione dell'agente di download CIG
cig_agent = Agent(
    name="CIG Download",
    handoff_description="Specialist agent for downloading CIG and OCDS data",
    instructions="""
    Download CIG (Codice Identificativo Gara) and OCDS (Open Contracting Data Standard) data 
    from the National Anti-Corruption Authority. Handle the download process with proper error 
    handling, verify the integrity of downloaded files, and organize them in the appropriate 
    directory structure.
    
    Follow these steps:
    1. Download CIG data from the specified URL
    2. Download OCDS data from the specified URL
    3. Verify the integrity of the downloaded files
    4. Organize the files in the appropriate directory structure
    5. Generate a report of the downloaded data
    
    Focus on these aspects:
    - Proper error handling during the download process
    - Verification of file integrity
    - Organization of files in a clear directory structure
    - Generation of a comprehensive report
    """
)

# Modelli di dati per l'output strutturato
class DownloadResult(BaseModel):
    """Modello per il risultato del download."""
    url: str
    file_path: str
    success: bool
    error: Optional[str] = None
    file_size: Optional[int] = None
    download_time: Optional[float] = None
    timestamp: str

class VerificationResult(BaseModel):
    """Modello per il risultato della verifica."""
    file_path: str
    success: bool
    error: Optional[str] = None
    file_type: str
    file_size: int
    record_count: Optional[int] = None
    timestamp: str

class DownloadReport(BaseModel):
    """Modello per il report del download."""
    cig_downloads: List[DownloadResult]
    ocds_downloads: List[DownloadResult]
    verifications: List[VerificationResult]
    total_cig_files: int
    total_ocds_files: int
    total_cig_records: Optional[int] = None
    total_ocds_records: Optional[int] = None
    total_download_time: float
    execution_time: float
    timestamp: str

# Configurazione delle URL per il download
CIG_DOWNLOAD_URL = "https://dati.anticorruzione.it/opendata/download/dataset/cig"
OCDS_DOWNLOAD_URL = "https://dati.anticorruzione.it/opendata/download/dataset/ocds"

# Strumenti (tools) per l'agente
@cig_agent.tool
def download_file(url: str, output_path: str) -> Dict[str, Any]:
    """
    Scarica un file da un URL.
    
    Args:
        url: URL del file da scaricare
        output_path: Percorso dove salvare il file
        
    Returns:
        Risultato del download
    """
    logger.info(f"Download del file da {url} a {output_path}")
    
    start_time = datetime.now()
    
    try:
        # Crea la directory di output se non esiste
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Scarica il file
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(url, headers=headers, stream=True, timeout=60)
        response.raise_for_status()
        
        # Salva il file
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        # Calcola il tempo di download
        download_time = (datetime.now() - start_time).total_seconds()
        
        # Ottieni la dimensione del file
        file_size = os.path.getsize(output_path)
        
        logger.info(f"File scaricato con successo: {output_path} ({file_size} bytes in {download_time:.2f} secondi)")
        
        # Crea il risultato del download
        result = DownloadResult(
            url=url,
            file_path=output_path,
            success=True,
            file_size=file_size,
            download_time=download_time,
            timestamp=datetime.now().isoformat()
        )
        
        return result.dict()
    except requests.RequestException as e:
        logger.error(f"Errore durante il download del file {url}: {str(e)}")
        
        # Crea il risultato del download con errore
        result = DownloadResult(
            url=url,
            file_path=output_path,
            success=False,
            error=str(e),
            timestamp=datetime.now().isoformat()
        )
        
        return result.dict()
    except Exception as e:
        logger.error(f"Errore durante il salvataggio del file {output_path}: {str(e)}")
        
        # Crea il risultato del download con errore
        result = DownloadResult(
            url=url,
            file_path=output_path,
            success=False,
            error=str(e),
            timestamp=datetime.now().isoformat()
        )
        
        return result.dict()

@cig_agent.tool
def verify_file(file_path: str) -> Dict[str, Any]:
    """
    Verifica l'integrità di un file.
    
    Args:
        file_path: Percorso del file da verificare
        
    Returns:
        Risultato della verifica
    """
    logger.info(f"Verifica dell'integrità del file: {file_path}")
    
    try:
        # Verifica che il file esista
        if not os.path.exists(file_path):
            logger.error(f"File non trovato: {file_path}")
            
            # Crea il risultato della verifica con errore
            result = VerificationResult(
                file_path=file_path,
                success=False,
                error="File non trovato",
                file_type="unknown",
                file_size=0,
                timestamp=datetime.now().isoformat()
            )
            
            return result.dict()
        
        # Ottieni la dimensione del file
        file_size = os.path.getsize(file_path)
        
        # Determina il tipo di file
        file_type = "unknown"
        record_count = None
        
        if file_path.endswith('.csv'):
            file_type = "csv"
            
            # Conta il numero di record nel file CSV
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                record_count = sum(1 for _ in reader) - 1  # Escludi l'intestazione
        
        elif file_path.endswith('.json'):
            file_type = "json"
            
            # Verifica che il file JSON sia valido
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Conta il numero di record nel file JSON
            if 'releases' in data and isinstance(data['releases'], list):
                record_count = len(data['releases'])
        
        logger.info(f"File verificato con successo: {file_path} ({file_type}, {file_size} bytes, {record_count} record)")
        
        # Crea il risultato della verifica
        result = VerificationResult(
            file_path=file_path,
            success=True,
            file_type=file_type,
            file_size=file_size,
            record_count=record_count,
            timestamp=datetime.now().isoformat()
        )
        
        return result.dict()
    except json.JSONDecodeError as e:
        logger.error(f"Errore di decodifica JSON nel file {file_path}: {str(e)}")
        
        # Crea il risultato della verifica con errore
        result = VerificationResult(
            file_path=file_path,
            success=False,
            error=f"Errore di decodifica JSON: {str(e)}",
            file_type="json",
            file_size=file_size if 'file_size' in locals() else 0,
            timestamp=datetime.now().isoformat()
        )
        
        return result.dict()
    except Exception as e:
        logger.error(f"Errore durante la verifica del file {file_path}: {str(e)}")
        
        # Crea il risultato della verifica con errore
        result = VerificationResult(
            file_path=file_path,
            success=False,
            error=str(e),
            file_type=file_type if 'file_type' in locals() else "unknown",
            file_size=file_size if 'file_size' in locals() else 0,
            timestamp=datetime.now().isoformat()
        )
        
        return result.dict()

@cig_agent.tool
def download_cig_data(output_dir: str, max_files: int = 3) -> List[Dict[str, Any]]:
    """
    Scarica i dati CIG.
    
    Args:
        output_dir: Directory dove salvare i file
        max_files: Numero massimo di file da scaricare
        
    Returns:
        Lista dei risultati del download
    """
    logger.info(f"Download dei dati CIG in {output_dir}, max_files={max_files}")
    
    # In un'implementazione reale, questa funzione scaricherebbe i dati CIG
    # dall'Autorità Nazionale Anticorruzione. Qui simuliamo il download.
    
    results = []
    
    try:
        # Crea la directory di output se non esiste
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Simula il download di più file
        for i in range(1, max_files + 1):
            file_name = f"cig_data_{i}.csv"
            file_path = os.path.join(output_dir, file_name)
            
            # Crea un file CSV di esempio
            with open(file_path, 'w', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['cig', 'oggetto_gara', 'importo', 'data_pubblicazione'])
                
                # Aggiungi alcune righe di esempio
                for j in range(1, 101):
                    writer.writerow([
                        f"CIG{i}{j:03d}",
                        f"Esempio di gara {i}-{j}",
                        f"{i * j * 1000}",
                        f"2023-{i:02d}-{j%28+1:02d}"
                    ])
            
            # Verifica il file
            verification_result = verify_file(file_path)
            
            # Crea il risultato del download
            result = DownloadResult(
                url=f"{CIG_DOWNLOAD_URL}/{file_name}",
                file_path=file_path,
                success=True,
                file_size=os.path.getsize(file_path),
                download_time=0.5,  # Tempo simulato
                timestamp=datetime.now().isoformat()
            )
            
            results.append(result.dict())
        
        logger.info(f"Download dei dati CIG completato: {len(results)} file scaricati")
        return results
    except Exception as e:
        logger.error(f"Errore durante il download dei dati CIG: {str(e)}")
        
        # Crea un risultato del download con errore
        result = DownloadResult(
            url=CIG_DOWNLOAD_URL,
            file_path=os.path.join(output_dir, "cig_data.csv"),
            success=False,
            error=str(e),
            timestamp=datetime.now().isoformat()
        )
        
        return [result.dict()]

@cig_agent.tool
def download_ocds_data(output_dir: str, max_files: int = 3) -> List[Dict[str, Any]]:
    """
    Scarica i dati OCDS.
    
    Args:
        output_dir: Directory dove salvare i file
        max_files: Numero massimo di file da scaricare
        
    Returns:
        Lista dei risultati del download
    """
    logger.info(f"Download dei dati OCDS in {output_dir}, max_files={max_files}")
    
    # In un'implementazione reale, questa funzione scaricherebbe i dati OCDS
    # dall'Autorità Nazionale Anticorruzione. Qui simuliamo il download.
    
    results = []
    
    try:
        # Crea la directory di output se non esiste
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Simula il download di più file
        for i in range(1, max_files + 1):
            file_name = f"ocds_data_{i}.json"
            file_path = os.path.join(output_dir, file_name)
            
            # Crea un file JSON di esempio
            data = {
                "uri": f"{OCDS_DOWNLOAD_URL}/{file_name}",
                "publisher": {
                    "name": "Autorità Nazionale Anticorruzione",
                    "scheme": "IT-ANAC"
                },
                "publishedDate": datetime.now().isoformat(),
                "version": "1.1",
                "releases": []
            }
            
            # Aggiungi alcune release di esempio
            for j in range(1, 101):
                release = {
                    "ocid": f"ocds-b5fd17-{i}{j:03d}",
                    "id": f"{i}{j:03d}",
                    "date": datetime.now().isoformat(),
                    "tag": ["tender"],
                    "initiationType": "tender",
                    "buyer": {
                        "id": f"buyer-{i}{j:03d}",
                        "name": f"Ente Appaltante {i}-{j}"
                    },
                    "tender": {
                        "id": f"CIG{i}{j:03d}",
                        "title": f"Esempio di gara {i}-{j}",
                        "description": f"Descrizione della gara {i}-{j}",
                        "status": "active",
                        "value": {
                            "amount": i * j * 1000,
                            "currency": "EUR"
                        }
                    }
                }
                
                data["releases"].append(release)
            
            # Salva il file JSON
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            # Verifica il file
            verification_result = verify_file(file_path)
            
            # Crea il risultato del download
            result = DownloadResult(
                url=f"{OCDS_DOWNLOAD_URL}/{file_name}",
                file_path=file_path,
                success=True,
                file_size=os.path.getsize(file_path),
                download_time=0.8,  # Tempo simulato
                timestamp=datetime.now().isoformat()
            )
            
            results.append(result.dict())
        
        logger.info(f"Download dei dati OCDS completato: {len(results)} file scaricati")
        return results
    except Exception as e:
        logger.error(f"Errore durante il download dei dati OCDS: {str(e)}")
        
        # Crea un risultato del download con errore
        result = DownloadResult(
            url=OCDS_DOWNLOAD_URL,
            file_path=os.path.join(output_dir, "ocds_data.json"),
            success=False,
            error=str(e),
            timestamp=datetime.now().isoformat()
        )
        
        return [result.dict()]

@cig_agent.tool
def generate_download_report(cig_results: List[Dict[str, Any]], ocds_results: List[Dict[str, Any]], verifications: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Genera un report del download.
    
    Args:
        cig_results: Risultati del download dei dati CIG
        ocds_results: Risultati del download dei dati OCDS
        verifications: Risultati della verifica dei file
        
    Returns:
        Report del download
    """
    logger.info("Generazione del report del download")
    
    start_time = datetime.now()
    
    # Calcola le statistiche
    total_cig_files = len(cig_results)
    total_ocds_files = len(ocds_results)
    
    total_cig_records = 0
    total_ocds_records = 0
    
    for verification in verifications:
        if verification['file_type'] == 'csv' and verification['success'] and verification['record_count'] is not None:
            total_cig_records += verification['record_count']
        elif verification['file_type'] == 'json' and verification['success'] and verification['record_count'] is not None:
            total_ocds_records += verification['record_count']
    
    total_download_time = 0
    for result in cig_results + ocds_results:
        if result['success'] and result['download_time'] is not None:
            total_download_time += result['download_time']
    
    # Crea il report
    report = DownloadReport(
        cig_downloads=cig_results,
        ocds_downloads=ocds_results,
        verifications=verifications,
        total_cig_files=total_cig_files,
        total_ocds_files=total_ocds_files,
        total_cig_records=total_cig_records,
        total_ocds_records=total_ocds_records,
        total_download_time=total_download_time,
        execution_time=(datetime.now() - start_time).total_seconds(),
        timestamp=datetime.now().isoformat()
    )
    
    logger.info(f"Report generato: {total_cig_files} file CIG, {total_ocds_files} file OCDS, {total_cig_records} record CIG, {total_ocds_records} record OCDS")
    
    return report.dict()

@cig_agent.tool
def save_report(report: Dict[str, Any], output_file: str) -> str:
    """
    Salva il report in un file.
    
    Args:
        report: Report da salvare
        output_file: Percorso del file di output
        
    Returns:
        Messaggio di conferma
    """
    logger.info(f"Salvataggio del report nel file: {output_file}")
    
    try:
        # Crea la directory di output se non esiste
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Salva il report in formato JSON
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Report salvato con successo nel file: {output_file}")
        return f"Report salvato con successo nel file: {output_file}"
    except Exception as e:
        logger.error(f"Errore durante il salvataggio del report: {str(e)}")
        return f"Errore durante il salvataggio del report: {str(e)}"

# Funzione principale per l'esecuzione dell'agente
async def run_cig_download(cig_dir: str = "cig", ocds_dir: str = "ocds", max_files: int = 3, report_file: str = "output/download_report.json") -> Any:
    """
    Esegue il download dei dati CIG e OCDS.
    
    Args:
        cig_dir: Directory dove salvare i dati CIG
        ocds_dir: Directory dove salvare i dati OCDS
        max_files: Numero massimo di file da scaricare per tipo
        report_file: Percorso del file di report
        
    Returns:
        Risultato dell'esecuzione dell'agente
    """
    logger.info(f"Avvio del download dei dati CIG e OCDS: cig_dir={cig_dir}, ocds_dir={ocds_dir}, max_files={max_files}")
    
    # Configurazione del contesto
    context = Context()
    
    # Prepara l'input per l'agente
    input_data = f"""
    Scarica i dati CIG e OCDS dall'Autorità Nazionale Anticorruzione.
    
    Directory CIG: {cig_dir}
    Directory OCDS: {ocds_dir}
    Numero massimo di file: {max_files}
    File di report: {report_file}
    
    Segui questi passaggi:
    1. Scarica i dati CIG nella directory specificata
    2. Scarica i dati OCDS nella directory specificata
    3. Verifica l'integrità dei file scaricati
    4. Genera un report del download
    5. Salva il report nel file specificato
    """
    
    # Esecuzione dell'agente
    result = await Runner.run(
        cig_agent,
        input_data,
        context=context
    )
    
    logger.info("Download dei dati CIG e OCDS completato")
    return result

# Punto di ingresso per l'esecuzione da linea di comando
if __name__ == "__main__":
    import argparse
    
    # Parsing degli argomenti da linea di comando
    parser = argparse.ArgumentParser(description="Download dei dati CIG e OCDS")
    parser.add_argument("--cig-dir", default="cig",
                        help="Directory dove salvare i dati CIG")
    parser.add_argument("--ocds-dir", default="ocds",
                        help="Directory dove salvare i dati OCDS")
    parser.add_argument("--max-files", type=int, default=3,
                        help="Numero massimo di file da scaricare per tipo")
    parser.add_argument("--report-file", default="output/download_report.json",
                        help="Percorso del file di report")
    args = parser.parse_args()
    
    # Verifica che la chiave API di OpenAI sia impostata
    if not os.environ.get("OPENAI_API_KEY"):
        print("ATTENZIONE: La variabile d'ambiente OPENAI_API_KEY non è impostata.")
        print("Per utilizzare il sistema, esegui:")
        print("export OPENAI_API_KEY='la_tua_chiave_api'")
        exit(1)
    
    # Esecuzione dell'agente
    result = asyncio.run(run_cig_download(args.cig_dir, args.ocds_dir, args.max_files, args.report_file))
    
    # Stampa dei risultati
    print("\nRisultato dell'esecuzione:")
    print(result.final_output)
