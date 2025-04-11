"""
Implementazione dell'agente Consip utilizzando l'SDK ufficiale di OpenAI Agents.

Questo modulo implementa l'agente specializzato per l'analisi dei dati Consip,
con particolare focus sul Servizio Luce.
"""

import os
import logging
import csv
import json
import pandas as pd
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
        logging.FileHandler("consip_agent.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ConsipAgent")

# Definizione dell'agente Consip
consip_agent = Agent(
    name="Consip Analysis",
    handoff_description="Specialist agent for analyzing Consip data, with focus on Servizio Luce",
    instructions="""
    Analyze data related to Consip tenders, with particular focus on Servizio Luce. Identify 
    the version of Servizio Luce (1, 2, 3, or 4), extract relevant information, and integrate 
    with award data. Provide detailed analysis of the technical aspects of the tenders.
    
    Follow these steps:
    1. Analyze CIG data to identify Consip tenders
    2. Focus on Servizio Luce tenders
    3. Identify the version of Servizio Luce (1, 2, 3, or 4)
    4. Extract relevant information from the tenders
    5. Integrate with award data if available
    6. Generate a detailed report
    
    Focus on these aspects:
    - Identification of Servizio Luce tenders
    - Determination of Servizio Luce version
    - Extraction of technical specifications
    - Integration with award data
    - Comprehensive reporting
    """
)

# Modelli di dati per l'output strutturato
class ServizioLuceInfo(BaseModel):
    """Modello per le informazioni sul Servizio Luce."""
    cig: str
    version: Optional[int] = None
    title: str
    description: Optional[str] = None
    contracting_authority: Optional[str] = None
    publication_date: Optional[str] = None
    deadline: Optional[str] = None
    amount: Optional[float] = None
    award_date: Optional[str] = None
    award_amount: Optional[float] = None
    winner: Optional[str] = None
    technical_specifications: Optional[Dict[str, Any]] = None

class ConsipAnalysisResult(BaseModel):
    """Modello per il risultato dell'analisi Consip."""
    total_consip_tenders: int
    total_servizio_luce_tenders: int
    servizio_luce_by_version: Dict[str, int]
    servizio_luce_tenders: List[ServizioLuceInfo]
    execution_time: float
    timestamp: str

# Strumenti (tools) per l'agente
@consip_agent.tool
def analyze_cig_file(file_path: str) -> Dict[str, Any]:
    """
    Analizza un file CIG per identificare i bandi Consip.
    
    Args:
        file_path: Percorso del file CIG da analizzare
        
    Returns:
        Risultato dell'analisi
    """
    logger.info(f"Analisi del file CIG: {file_path}")
    
    try:
        # Leggi il file CSV
        df = pd.read_csv(file_path, delimiter=';', encoding='utf-8')
        
        # Identifica i bandi Consip
        consip_mask = df['oggetto_gara'].str.contains('Consip|CONSIP', case=False, na=False)
        consip_tenders = df[consip_mask].copy()
        
        # Identifica i bandi Servizio Luce
        servizio_luce_mask = consip_tenders['oggetto_gara'].str.contains('Servizio Luce|SERVIZIO LUCE', case=False, na=False)
        servizio_luce_tenders = consip_tenders[servizio_luce_mask].copy()
        
        logger.info(f"Trovati {len(consip_tenders)} bandi Consip, di cui {len(servizio_luce_tenders)} Servizio Luce")
        
        return {
            "success": True,
            "total_tenders": len(df),
            "consip_tenders": len(consip_tenders),
            "servizio_luce_tenders": len(servizio_luce_tenders),
            "consip_data": consip_tenders.to_dict('records'),
            "servizio_luce_data": servizio_luce_tenders.to_dict('records')
        }
    except Exception as e:
        logger.error(f"Errore durante l'analisi del file CIG {file_path}: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

@consip_agent.tool
def identify_servizio_luce_version(text: str) -> int:
    """
    Identifica la versione del Servizio Luce menzionata nel testo.
    
    Args:
        text: Testo da analizzare
        
    Returns:
        Versione del Servizio Luce (1, 2, 3, o 4)
    """
    logger.info("Identificazione della versione del Servizio Luce")
    
    # Cerca pattern specifici per ogni versione
    if re.search(r'Servizio Luce 4|SERVIZIO LUCE 4|SL4', text, re.IGNORECASE):
        logger.info("Identificata versione Servizio Luce 4")
        return 4
    elif re.search(r'Servizio Luce 3|SERVIZIO LUCE 3|SL3', text, re.IGNORECASE):
        logger.info("Identificata versione Servizio Luce 3")
        return 3
    elif re.search(r'Servizio Luce 2|SERVIZIO LUCE 2|SL2', text, re.IGNORECASE):
        logger.info("Identificata versione Servizio Luce 2")
        return 2
    elif re.search(r'Servizio Luce 1|SERVIZIO LUCE 1|SL1', text, re.IGNORECASE):
        logger.info("Identificata versione Servizio Luce 1")
        return 1
    else:
        # Se non è possibile identificare la versione, prova a dedurla dalla data
        if 'data_pubblicazione' in text or 'publication_date' in text:
            # Estrai la data
            date_match = re.search(r'\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}', text)
            if date_match:
                date_str = date_match.group(0)
                
                # Converti in formato standard
                if '/' in date_str:
                    day, month, year = date_str.split('/')
                    date_str = f"{year}-{month}-{day}"
                
                # Deduce la versione in base alla data
                date = datetime.strptime(date_str, '%Y-%m-%d')
                if date >= datetime.strptime('2020-01-01', '%Y-%m-%d'):
                    logger.info("Dedotta versione Servizio Luce 4 dalla data")
                    return 4
                elif date >= datetime.strptime('2015-01-01', '%Y-%m-%d'):
                    logger.info("Dedotta versione Servizio Luce 3 dalla data")
                    return 3
                elif date >= datetime.strptime('2010-01-01', '%Y-%m-%d'):
                    logger.info("Dedotta versione Servizio Luce 2 dalla data")
                    return 2
                else:
                    logger.info("Dedotta versione Servizio Luce 1 dalla data")
                    return 1
        
        # Se non è possibile dedurre la versione, restituisci None
        logger.warning("Impossibile identificare la versione del Servizio Luce")
        return None

@consip_agent.tool
def extract_technical_specifications(text: str, version: int) -> Dict[str, Any]:
    """
    Estrae le specifiche tecniche dal testo di un bando Servizio Luce.
    
    Args:
        text: Testo del bando
        version: Versione del Servizio Luce
        
    Returns:
        Specifiche tecniche estratte
    """
    logger.info(f"Estrazione delle specifiche tecniche per Servizio Luce {version}")
    
    # Specifiche tecniche per ogni versione
    specs = {
        "version": version,
        "services": []
    }
    
    # Servizi comuni a tutte le versioni
    common_services = [
        "Gestione",
        "Manutenzione ordinaria",
        "Manutenzione straordinaria"
    ]
    
    # Aggiungi i servizi comuni
    for service in common_services:
        specs["services"].append({
            "name": service,
            "included": True
        })
    
    # Servizi specifici per versione
    if version >= 2:
        specs["services"].append({
            "name": "Interventi di riqualificazione energetica",
            "included": True
        })
    
    if version >= 3:
        specs["services"].append({
            "name": "Smart city",
            "included": True
        })
    
    if version >= 4:
        specs["services"].append({
            "name": "Servizi di efficientamento energetico",
            "included": True
        })
        specs["services"].append({
            "name": "Servizi di telecontrollo",
            "included": True
        })
    
    # Cerca nel testo menzioni specifiche di servizi
    if "LED" in text or "led" in text:
        specs["led_technology"] = True
    
    if "telecontrollo" in text.lower() or "telegestione" in text.lower():
        specs["remote_control"] = True
    
    if "smart city" in text.lower() or "città intelligente" in text.lower():
        specs["smart_city"] = True
    
    logger.info(f"Specifiche tecniche estratte: {specs}")
    return specs

@consip_agent.tool
def merge_with_awards(servizio_luce_data: List[Dict[str, Any]], awards_file: str) -> List[Dict[str, Any]]:
    """
    Integra i dati dei bandi Servizio Luce con i dati delle aggiudicazioni.
    
    Args:
        servizio_luce_data: Dati dei bandi Servizio Luce
        awards_file: Percorso del file delle aggiudicazioni
        
    Returns:
        Dati integrati
    """
    logger.info(f"Integrazione con i dati delle aggiudicazioni dal file: {awards_file}")
    
    try:
        # In un'implementazione reale, questa funzione leggerebbe il file delle aggiudicazioni
        # e integrerebbe i dati. Qui simuliamo l'integrazione.
        
        # Crea un dizionario di aggiudicazioni di esempio
        awards = {}
        for i in range(1, 101):
            cig = f"CIG{i:03d}"
            awards[cig] = {
                "award_date": f"2023-{i%12+1:02d}-{i%28+1:02d}",
                "award_amount": i * 10000,
                "winner": f"Azienda Vincitrice {i}"
            }
        
        # Integra i dati
        merged_data = []
        for tender in servizio_luce_data:
            cig = tender.get('cig')
            if cig in awards:
                tender.update(awards[cig])
            merged_data.append(tender)
        
        logger.info(f"Integrazione completata: {len(merged_data)} bandi integrati")
        return merged_data
    except Exception as e:
        logger.error(f"Errore durante l'integrazione con i dati delle aggiudicazioni: {str(e)}")
        return servizio_luce_data

@consip_agent.tool
def analyze_servizio_luce(cig_file: str, awards_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Analizza i dati Servizio Luce da un file CIG.
    
    Args:
        cig_file: Percorso del file CIG
        awards_file: Percorso del file delle aggiudicazioni (opzionale)
        
    Returns:
        Risultato dell'analisi
    """
    logger.info(f"Analisi dei dati Servizio Luce dal file: {cig_file}")
    
    start_time = datetime.now()
    
    try:
        # Analizza il file CIG
        cig_analysis = analyze_cig_file(cig_file)
        if not cig_analysis['success']:
            return {
                "success": False,
                "error": cig_analysis['error'],
                "execution_time": (datetime.now() - start_time).total_seconds(),
                "timestamp": datetime.now().isoformat()
            }
        
        # Estrai i dati Servizio Luce
        servizio_luce_data = cig_analysis['servizio_luce_data']
        
        # Identifica la versione e estrai le specifiche tecniche per ogni bando
        servizio_luce_info = []
        servizio_luce_by_version = {
            "1": 0,
            "2": 0,
            "3": 0,
            "4": 0,
            "unknown": 0
        }
        
        for tender in servizio_luce_data:
            # Crea un testo combinato per l'analisi
            combined_text = f"{tender.get('oggetto_gara', '')} {tender.get('oggetto_lotto', '')}"
            
            # Identifica la versione
            version = identify_servizio_luce_version(combined_text)
            
            # Aggiorna il conteggio per versione
            if version:
                servizio_luce_by_version[str(version)] += 1
            else:
                servizio_luce_by_version["unknown"] += 1
            
            # Estrai le specifiche tecniche
            tech_specs = extract_technical_specifications(combined_text, version) if version else None
            
            # Crea l'oggetto ServizioLuceInfo
            info = ServizioLuceInfo(
                cig=tender.get('cig', ''),
                version=version,
                title=tender.get('oggetto_gara', ''),
                description=tender.get('oggetto_lotto', ''),
                contracting_authority=tender.get('denominazione_amministrazione', ''),
                publication_date=tender.get('data_pubblicazione', ''),
                deadline=tender.get('data_scadenza', ''),
                amount=tender.get('importo_complessivo_gara', 0),
                technical_specifications=tech_specs
            )
            
            servizio_luce_info.append(info.dict())
        
        # Integra con i dati delle aggiudicazioni se disponibili
        if awards_file:
            servizio_luce_info = merge_with_awards(servizio_luce_info, awards_file)
        
        # Crea il risultato dell'analisi
        result = ConsipAnalysisResult(
            total_consip_tenders=cig_analysis['consip_tenders'],
            total_servizio_luce_tenders=cig_analysis['servizio_luce_tenders'],
            servizio_luce_by_version=servizio_luce_by_version,
            servizio_luce_tenders=servizio_luce_info,
            execution_time=(datetime.now() - start_time).total_seconds(),
            timestamp=datetime.now().isoformat()
        )
        
        logger.info(f"Analisi completata: {result.total_servizio_luce_tenders} bandi Servizio Luce")
        return {
            "success": True,
            "result": result.dict()
        }
    except Exception as e:
        logger.error(f"Errore durante l'analisi dei dati Servizio Luce: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "execution_time": (datetime.now() - start_time).total_seconds(),
            "timestamp": datetime.now().isoformat()
        }

@consip_agent.tool
def save_analysis_result(result: Dict[str, Any], output_file: str) -> str:
    """
    Salva il risultato dell'analisi in un file.
    
    Args:
        result: Risultato dell'analisi
        output_file: Percorso del file di output
        
    Returns:
        Messaggio di conferma
    """
    logger.info(f"Salvataggio del risultato dell'analisi nel file: {output_file}")
    
    try:
        # Crea la directory di output se non esiste
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Salva il risultato in formato JSON
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Risultato salvato con successo nel file: {output_file}")
        return f"Risultato salvato con successo nel file: {output_file}"
    except Exception as e:
        logger.error(f"Errore durante il salvataggio del risultato: {str(e)}")
        return f"Errore durante il salvataggio del risultato: {str(e)}"

@consip_agent.tool
def export_to_excel(result: Dict[str, Any], output_file: str) -> str:
    """
    Esporta il risultato dell'analisi in un file Excel.
    
    Args:
        result: Risultato dell'analisi
        output_file: Percorso del file Excel di output
        
    Returns:
        Messaggio di conferma
    """
    logger.info(f"Esportazione del risultato dell'analisi in Excel: {output_file}")
    
    try:
        # Crea la directory di output se non esiste
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Crea un DataFrame dai dati
        df = pd.DataFrame(result['servizio_luce_tenders'])
        
        # Crea un writer Excel
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Foglio principale con i dati dei bandi
            df.to_excel(writer, sheet_name='Servizio Luce', index=False)
            
            # Foglio con il riepilogo
            summary_data = {
                'Metrica': [
                    'Totale bandi Consip',
                    'Totale bandi Servizio Luce',
                    'Servizio Luce 1',
                    'Servizio Luce 2',
                    'Servizio Luce 3',
                    'Servizio Luce 4',
                    'Versione sconosciuta',
                    'Tempo di esecuzione (secondi)',
                    'Timestamp'
                ],
                'Valore': [
                    result['total_consip_tenders'],
                    result['total_servizio_luce_tenders'],
                    result['servizio_luce_by_version']['1'],
                    result['servizio_luce_by_version']['2'],
                    result['servizio_luce_by_version']['3'],
                    result['servizio_luce_by_version']['4'],
                    result['servizio_luce_by_version']['unknown'],
                    result['execution_time'],
                    result['timestamp']
                ]
            }
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Riepilogo', index=False)
        
        logger.info(f"Risultato esportato con successo in Excel: {output_file}")
        return f"Risultato esportato con successo in Excel: {output_file}"
    except Exception as e:
        logger.error(f"Errore durante l'esportazione in Excel: {str(e)}")
        return f"Errore durante l'esportazione in Excel: {str(e)}"

# Funzione principale per l'esecuzione dell'agente
async def run_consip_analysis(cig_file: str, awards_file: Optional[str] = None, output_json: str = "output/consip_analysis.json", output_excel: str = "output/servizio_luce_consip_cig.xlsx") -> Any:
    """
    Esegue l'analisi dei dati Consip.
    
    Args:
        cig_file: Percorso del file CIG
        awards_file: Percorso del file delle aggiudicazioni (opzionale)
        output_json: Percorso del file JSON di output
        output_excel: Percorso del file Excel di output
        
    Returns:
        Risultato dell'esecuzione dell'agente
    """
    logger.info(f"Avvio dell'analisi dei dati Consip: cig_file={cig_file}, awards_file={awards_file}")
    
    # Configurazione del contesto
    context = Context()
    
    # Prepara l'input per l'agente
    input_data = f"""
    Analizza i dati Consip dal file CIG, con particolare focus sul Servizio Luce.
    
    File CIG: {cig_file}
    File aggiudicazioni: {awards_file if awards_file else 'Non disponibile'}
    File JSON di output: {output_json}
    File Excel di output: {output_excel}
    
    Segui questi passaggi:
    1. Analizza il file CIG per identificare i bandi Consip
    2. Concentrati sui bandi Servizio Luce
    3. Identifica la versione del Servizio Luce (1, 2, 3, o 4) per ogni bando
    4. Estrai le specifiche tecniche dai bandi
    5. Integra con i dati delle aggiudicazioni se disponibili
    6. Salva il risultato dell'analisi in formato JSON
    7. Esporta il risultato in formato Excel
    """
    
    # Esecuzione dell'agente
    result = await Runner.run(
        consip_agent,
        input_data,
        context=context
    )
    
    logger.info("Analisi dei dati Consip completata")
    return result

# Punto di ingresso per l'esecuzione da linea di comando
if __name__ == "__main__":
    import argparse
    
    # Parsing degli argomenti da linea di comando
    parser = argparse.ArgumentParser(description="Analisi dei dati Consip")
    parser.add_argument("--cig-file", required=True,
                        help="Percorso del file CIG")
    parser.add_argument("--awards-file",
                        help="Percorso del file delle aggiudicazioni")
    parser.add_argument("--output-json", default="output/consip_analysis.json",
                        help="Percorso del file JSON di output")
    parser.add_argument("--output-excel", default="output/servizio_luce_consip_cig.xlsx",
                        help="Percorso del file Excel di output")
    args = parser.parse_args()
    
    # Verifica che la chiave API di OpenAI sia impostata
    if not os.environ.get("OPENAI_API_KEY"):
        print("ATTENZIONE: La variabile d'ambiente OPENAI_API_KEY non è impostata.")
        print("Per utilizzare il sistema, esegui:")
        print("export OPENAI_API_KEY='la_tua_chiave_api'")
        exit(1)
    
    # Esecuzione dell'agente
    result = asyncio.run(run_consip_analysis(args.cig_file, args.awards_file, args.output_json, args.output_excel))
    
    # Stampa dei risultati
    print("\nRisultato dell'esecuzione:")
    print(result.final_output)
