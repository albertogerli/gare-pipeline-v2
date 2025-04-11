"""
Integrazione dell'agente Gazzetta con il sistema di coordinamento degli agenti.

Questo modulo integra gli agenti specializzati, come l'agente Gazzetta, con il sistema
di coordinamento degli agenti esistente in coordinator_agent_sdk.py.
"""

import os
import logging
import asyncio
from typing import Dict, List, Any, Optional
import json

# Importa i moduli degli agenti
from agents import Agent, Runner, RunContextWrapper, Tool
from gazzetta_agent_impl import run_gazzetta_agent

# Configurazione del logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("coordinator_integration.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("CoordinatorIntegration")

# Tool per l'agente Gazzetta
async def gazzetta_scraping_tool(input_data: str) -> Dict[str, Any]:
    """
    Tool per eseguire lo scraping della Gazzetta Ufficiale.
    
    Args:
        input_data: Input dell'utente per lo scraping
        
    Returns:
        Risultati dello scraping
    """
    logger.info(f"Esecuzione dello strumento di scraping Gazzetta con input: {input_data}")
    
    # Esegui l'agente Gazzetta in modo sincrono (per semplicità)
    result = run_gazzetta_agent(input_data)
    
    # Log dei risultati
    if result.get("success"):
        logger.info(f"Scraping Gazzetta completato con successo: {result.get('message')}")
    else:
        logger.warning(f"Errore nello scraping Gazzetta: {result.get('message')}")
        
    return result

# Tool per l'estrazione dei CIG
async def extract_cig_tool(input_data: str) -> Dict[str, Any]:
    """
    Tool per estrarre i codici CIG dai dati scaricati.
    
    Args:
        input_data: Input dell'utente per l'estrazione
        
    Returns:
        Risultati dell'estrazione dei CIG
    """
    logger.info(f"Esecuzione dello strumento di estrazione CIG con input: {input_data}")
    
    # Trasforma l'input per richiedere l'estrazione
    modified_input = f"estrai {input_data}"
    
    # Esegui l'agente Gazzetta in modalità estrazione
    result = run_gazzetta_agent(modified_input)
    
    # Log dei risultati
    if result.get("success"):
        logger.info(f"Estrazione CIG completata con successo: {result.get('message')}")
    else:
        logger.warning(f"Errore nell'estrazione CIG: {result.get('message')}")
        
    return result

# Inizializza l'agente Gazzetta con gli strumenti
gazzetta_agent_with_tools = Agent(
    name="Gazzetta Extraction Enhanced",
    handoff_description="Enhanced agent for extracting data from Gazzetta Ufficiale with real scraping capabilities",
    instructions="""
    You are a specialized agent for extracting procurement data from Gazzetta Ufficiale. You can:
    1. Scrape data from Gazzetta Ufficiale related to public tenders
    2. Extract CIG codes and other metadata from the scraped texts
    3. Filter relevant information for sectors like public lighting, video surveillance, 
       tunnels, buildings, electric charging stations, and parking
    
    When a user requests data extraction from Gazzetta Ufficiale, use the gazzetta_scraping_tool
    to download the data. If they specifically need CIG codes, use the extract_cig_tool.
    
    Log each step of your process in detail and provide clear information about the results.
    """,
    tools=[
        Tool(
            name="gazzetta_scraping_tool",
            description="Scrapes data from Gazzetta Ufficiale based on the given input",
            function=gazzetta_scraping_tool
        ),
        Tool(
            name="extract_cig_tool",
            description="Extracts CIG codes from previously scraped Gazzetta Ufficiale data",
            function=extract_cig_tool
        )
    ]
)

# Funzione per eseguire il flusso di lavoro Gazzetta avanzato
async def enhanced_gazzetta_workflow(input_data: str) -> Any:
    """
    Esegue il flusso di lavoro avanzato dell'agente Gazzetta con strumenti di scraping reali.
    
    Args:
        input_data: Input dell'utente
        
    Returns:
        Risultato dell'esecuzione dell'agente Gazzetta
    """
    logger.info(f"Avvio del flusso di lavoro Gazzetta avanzato con input: {input_data}")
    
    # Configurazione del contesto
    context = {}
    context_wrapper = RunContextWrapper(context)
    
    # Esecuzione dell'agente Gazzetta
    try:
        result = await Runner.run(
            gazzetta_agent_with_tools,
            input_data,
            context=context_wrapper
        )
        logger.info("Flusso di lavoro Gazzetta avanzato completato")
        return result
    except Exception as e:
        logger.error(f"Errore durante l'esecuzione del flusso di lavoro Gazzetta avanzato: {str(e)}")
        logger.exception("Dettaglio dell'errore:")
        raise

# Funzione di utilità per l'esecuzione sincrona
def enhanced_gazzetta_workflow_sync(input_data: str) -> Any:
    """
    Versione sincrona della funzione enhanced_gazzetta_workflow.
    
    Args:
        input_data: Input dell'utente
        
    Returns:
        Risultato dell'esecuzione dell'agente Gazzetta
    """
    return asyncio.run(enhanced_gazzetta_workflow(input_data))

# Punto di ingresso per l'esecuzione da linea di comando
if __name__ == "__main__":
    import argparse
    
    # Parsing degli argomenti da linea di comando
    parser = argparse.ArgumentParser(description="Sistema integrato per l'analisi di dati dalla Gazzetta Ufficiale")
    parser.add_argument("--input", "-i", default="Scarica dati dalla Gazzetta Ufficiale relativi a illuminazione pubblica",
                        help="Input per l'agente Gazzetta")
    args = parser.parse_args()
    
    # Verifica che la chiave API di OpenAI sia impostata
    if not os.environ.get("OPENAI_API_KEY"):
        logger.error("La variabile d'ambiente OPENAI_API_KEY non è impostata")
        print("ATTENZIONE: La variabile d'ambiente OPENAI_API_KEY non è impostata.")
        print("Per utilizzare il sistema, esegui:")
        print("export OPENAI_API_KEY='la_tua_chiave_api'")
        exit(1)
    else:
        logger.debug("Chiave API OpenAI trovata nelle variabili d'ambiente")
    
    # Esecuzione dell'agente Gazzetta con gli strumenti
    result = enhanced_gazzetta_workflow_sync(args.input)
    
    # Stampa dei risultati
    print("\nRisultato dell'esecuzione:")
    print(result.final_output) 