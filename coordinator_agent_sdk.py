"""
Implementazione dell'agente coordinatore utilizzando l'SDK ufficiale di OpenAI Agents.

Questo modulo implementa l'agente coordinatore che gestisce il flusso di lavoro complessivo
e coordina la comunicazione tra gli altri agenti del sistema.
"""

import os
import logging
from typing import Dict, List, Any, Optional
import asyncio

from agents import Agent, Runner, RunContextWrapper, GuardrailFunctionOutput, Tool
from pydantic import BaseModel

# Importa l'agente Gazzetta implementato
from gazzetta_agent_impl import run_gazzetta_agent

# Definiamo la classe per l'output del guardrail mancante
class GuardrailFunctionOutput(BaseModel):
    output_info: Any
    tripwire_triggered: bool

# Configurazione del logging
logging.basicConfig(
    level=logging.DEBUG,  # Cambio da INFO a DEBUG per avere log più dettagliati
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("coordinator_agent.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("CoordinatorAgent")

# Log aggiuntivo per monitorare tutte le richieste HTTP
logging.getLogger("httpx").setLevel(logging.DEBUG)
logging.getLogger("openai").setLevel(logging.DEBUG)

# Modelli di output per i guardrails
class InputValidationOutput(BaseModel):
    is_valid: bool
    reasoning: str

# Agente di validazione degli input
validation_agent = Agent(
    name="Input Validation",
    instructions="""
    Validate if the input is appropriate for processing in the procurement data analysis system.
    Check if the input contains a clear request related to procurement data analysis, extraction, 
    or categorization. The input should be related to public tenders, especially in sectors like 
    public lighting, video surveillance, tunnels, buildings, electric charging stations, parking, etc.
    """,
    output_type=InputValidationOutput,
)

# Funzione di guardrail per la validazione degli input
async def input_validation_guardrail(ctx, agent, input_data):
    """
    Guardrail per validare gli input prima di processarli.
    """
    result = await Runner.run(validation_agent, input_data, context=ctx.context)
    final_output = result.final_output_as(InputValidationOutput)
    return GuardrailFunctionOutput(
        output_info=final_output,
        tripwire_triggered=not final_output.is_valid,
    )

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
    
    # Esegui l'agente Gazzetta in modo sincrono
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

# Definizione degli agenti specializzati
# Nota: questi agenti verranno importati dai rispettivi moduli nell'implementazione completa
# Qui li definiamo come placeholder per illustrare la struttura

gazzetta_agent = Agent(
    name="Gazzetta Extraction",
    handoff_description="Specialist agent for extracting data from Gazzetta Ufficiale with real scraping capabilities",
    instructions="""
    Extract relevant data from Gazzetta Ufficiale related to public lighting, video surveillance, 
    tunnels, buildings, electric charging stations, parking, etc. Analyze the text of the tenders 
    and identify relevant information such as tender object, publication date, deadline, contracting 
    authority, and other metadata.
    
    IMPORTANT: Log every step of your process in detail, including:
    1. When you start accessing the Gazzetta Ufficiale
    2. When you perform searches
    3. When you download documents
    4. When you extract information from documents
    5. Any errors or issues encountered during the process
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

json_agent = Agent(
    name="JSON Analysis",
    handoff_description="Specialist agent for analyzing JSON OCDS data",
    instructions="""
    Analyze JSON files in OCDS (Open Contracting Data Standard) format, extract structured data, 
    and normalize formats. Parse complex JSON structures, handle malformed or incomplete files, 
    and extract key information such as tender details, awards, contracts, and related metadata.
    
    IMPORTANT: Log every step of your process in detail, including:
    1. When you open JSON files
    2. When you parse JSON structures
    3. When you extract specific data fields
    4. Any validation or normalization performed
    5. Any errors or malformed data encountered
    """
)

cig_agent = Agent(
    name="CIG Download",
    handoff_description="Specialist agent for downloading CIG and OCDS data",
    instructions="""
    Download CIG (Codice Identificativo Gara) and OCDS (Open Contracting Data Standard) data 
    from the National Anti-Corruption Authority. Handle the download process with proper error 
    handling, verify the integrity of downloaded files, and organize them in the appropriate 
    directory structure.
    
    IMPORTANT: Log every step of your process in detail, including:
    1. When you start accessing the National Anti-Corruption Authority
    2. Each API call or web request made
    3. When downloads begin and complete
    4. File verification steps
    5. Organization of downloaded files
    6. Any errors or issues encountered during the process
    """
)

consip_agent = Agent(
    name="Consip Analysis",
    handoff_description="Specialist agent for analyzing Consip data, with focus on Servizio Luce",
    instructions="""
    Analyze data related to Consip tenders, with particular focus on Servizio Luce. Identify 
    the version of Servizio Luce (1, 2, 3, or 4), extract relevant information, and integrate 
    with award data. Provide detailed analysis of the technical aspects of the tenders.
    """
)

categorization_agent = Agent(
    name="Categorization",
    handoff_description="Specialist agent for categorizing tenders into a hierarchical system",
    instructions="""
    Categorize tenders into a hierarchical system of categories and subcategories. The main 
    categories include: Illumination, Video Surveillance, Tunnels, Buildings, Electric Charging 
    Stations, Parking, Smart City, and Energy. Each category has several subcategories. Analyze 
    the text of the tenders and assign the appropriate categories and subcategories.
    """
)

link_agent = Agent(
    name="JSON-Scraping Link",
    handoff_description="Specialist agent for linking data from Gazzetta Ufficiale with OCDS data via CIG",
    instructions="""
    Link data from Gazzetta Ufficiale with OCDS data using CIG codes as keys. Extract CIG codes 
    from the text of the tenders, find matching OCDS records, and merge the information from 
    both sources. Handle cases where the same CIG appears in multiple sources and resolve conflicts.
    """
)

persistence_agent = Agent(
    name="Data Persistence",
    handoff_description="Specialist agent for storing data in a structured database",
    instructions="""
    Store processed data in a SQLite database with a relational structure. Create the appropriate 
    schema, insert data into the tables, and ensure data integrity. Provide functionality to 
    export data to Excel or other formats for analysis.
    """
)

# Agente coordinatore
coordinator_agent = Agent(
    name="Coordinator",
    instructions="""
    You coordinate the workflow for analyzing procurement data. Determine which specialist agent 
    to use based on the task. You have access to the following specialist agents:
    
    1. Gazzetta Extraction: Extract data from Gazzetta Ufficiale
    2. JSON Analysis: Analyze JSON OCDS data
    3. CIG Download: Download CIG and OCDS data
    4. Consip Analysis: Analyze Consip data, with focus on Servizio Luce
    5. Categorization: Categorize tenders into a hierarchical system
    6. JSON-Scraping Link: Link data from Gazzetta Ufficiale with OCDS data via CIG
    7. Data Persistence: Store data in a structured database
    
    Based on the user's request, determine which agent or sequence of agents should handle the task.
    """,
    handoffs=[
        gazzetta_agent, 
        json_agent, 
        cig_agent, 
        consip_agent, 
        categorization_agent, 
        link_agent, 
        persistence_agent
    ]
)

# Funzione per eseguire il flusso di lavoro completo
async def complete_workflow(input_data: str) -> Any:
    """
    Esegue il flusso di lavoro completo utilizzando l'agente coordinatore.
    
    Args:
        input_data: Dati di input forniti dall'utente
        
    Returns:
        Risultato dell'esecuzione dell'agente coordinatore
    """
    logger.info(f"Avvio del flusso di lavoro completo con input: {input_data[:100]}...")
    logger.debug(f"Input completo: {input_data}")
    
    # Configurazione del contesto
    context = {}  # In questa versione dell'SDK, il contesto è un semplice dizionario
    context_wrapper = RunContextWrapper(context)
    logger.debug("Contesto inizializzato")
    
    # Esecuzione dell'agente coordinatore
    try:
        logger.info("Inizializzazione dell'agente coordinatore...")
        result = await Runner.run(
            coordinator_agent,
            input_data,
            context=context_wrapper
        )
        logger.info("Flusso di lavoro completo completato")
        logger.debug(f"Risultato del flusso di lavoro: {result}")
        return result
    except Exception as e:
        logger.error(f"Errore durante l'esecuzione del flusso di lavoro completo: {str(e)}")
        logger.exception("Dettaglio dell'errore:")
        raise

# Funzione per eseguire flussi di lavoro specifici
async def specific_workflow(agent: Agent, input_data: str) -> Any:
    """
    Esegue un flusso di lavoro specifico utilizzando un agente specializzato.
    
    Args:
        agent: Agente specializzato da utilizzare
        input_data: Dati di input forniti dall'utente
        
    Returns:
        Risultato dell'esecuzione dell'agente specializzato
    """
    logger.info(f"Avvio del flusso di lavoro specifico con agente {agent.name} e input: {input_data[:100]}...")
    logger.debug(f"Input completo per {agent.name}: {input_data}")
    
    # Configurazione del contesto
    context = {}  # In questa versione dell'SDK, il contesto è un semplice dizionario
    context_wrapper = RunContextWrapper(context)
    logger.debug(f"Contesto inizializzato per {agent.name}")
    
    # Esecuzione dell'agente specializzato
    try:
        logger.info(f"Inizializzazione dell'agente {agent.name}...")
        logger.debug(f"Istruzioni dell'agente {agent.name}: {agent.instructions}")
        result = await Runner.run(
            agent,
            input_data,
            context=context_wrapper
        )
        logger.info(f"Flusso di lavoro specifico con agente {agent.name} completato")
        logger.debug(f"Risultato del flusso di lavoro {agent.name}: {result}")
        return result
    except Exception as e:
        logger.error(f"Errore durante l'esecuzione del flusso di lavoro specifico {agent.name}: {str(e)}")
        logger.exception(f"Dettaglio dell'errore per {agent.name}:")
        raise

# Funzioni di utilità per i diversi flussi di lavoro
async def gazzetta_workflow(input_data: str) -> Any:
    """Esegue il flusso di lavoro di estrazione dalla Gazzetta Ufficiale."""
    logger.info("Utilizzo dell'agente Gazzetta migliorato con capacità di scraping reali")
    return await specific_workflow(gazzetta_agent, input_data)

async def json_workflow(input_data: str) -> Any:
    """Esegue il flusso di lavoro di analisi JSON."""
    return await specific_workflow(json_agent, input_data)

async def cig_workflow(input_data: str) -> Any:
    """Esegue il flusso di lavoro di download CIG."""
    return await specific_workflow(cig_agent, input_data)

async def consip_workflow(input_data: str) -> Any:
    """Esegue il flusso di lavoro di analisi Consip."""
    return await specific_workflow(consip_agent, input_data)

async def categorization_workflow(input_data: str) -> Any:
    """Esegue il flusso di lavoro di categorizzazione."""
    return await specific_workflow(categorization_agent, input_data)

async def link_workflow(input_data: str) -> Any:
    """Esegue il flusso di lavoro di collegamento JSON-Scraping."""
    return await specific_workflow(link_agent, input_data)

async def persistence_workflow(input_data: str) -> Any:
    """Esegue il flusso di lavoro di persistenza dati."""
    return await specific_workflow(persistence_agent, input_data)

# Funzione principale per l'esecuzione del sistema
async def run_system(workflow_type: str, input_data: str) -> Any:
    """
    Esegue il sistema con il flusso di lavoro specificato.
    
    Args:
        workflow_type: Tipo di flusso di lavoro da eseguire
        input_data: Dati di input forniti dall'utente
        
    Returns:
        Risultato dell'esecuzione del flusso di lavoro
    """
    logger.info(f"Avvio del sistema con flusso di lavoro {workflow_type}")
    logger.debug(f"Parametri - workflow_type: {workflow_type}, input_data: {input_data}")
    
    # Seleziona il flusso di lavoro appropriato
    logger.info(f"Selezione del flusso di lavoro: {workflow_type}")
    if workflow_type == "complete":
        logger.debug("Avvio del flusso di lavoro completo")
        return await complete_workflow(input_data)
    elif workflow_type == "gazzetta":
        logger.debug("Avvio del flusso di lavoro Gazzetta")
        return await gazzetta_workflow(input_data)
    elif workflow_type == "json":
        logger.debug("Avvio del flusso di lavoro JSON")
        return await json_workflow(input_data)
    elif workflow_type == "cig":
        logger.debug("Avvio del flusso di lavoro CIG")
        return await cig_workflow(input_data)
    elif workflow_type == "consip":
        logger.debug("Avvio del flusso di lavoro Consip")
        return await consip_workflow(input_data)
    elif workflow_type == "categorization":
        logger.debug("Avvio del flusso di lavoro di categorizzazione")
        return await categorization_workflow(input_data)
    elif workflow_type == "link":
        logger.debug("Avvio del flusso di lavoro di collegamento")
        return await link_workflow(input_data)
    elif workflow_type == "persistence":
        logger.debug("Avvio del flusso di lavoro di persistenza")
        return await persistence_workflow(input_data)
    else:
        logger.error(f"Tipo di flusso di lavoro non valido: {workflow_type}")
        raise ValueError(f"Tipo di flusso di lavoro non valido: {workflow_type}")

# Funzione di utilità per l'esecuzione sincrona
def run_system_sync(workflow_type: str, input_data: str) -> Any:
    """
    Versione sincrona della funzione run_system.
    
    Args:
        workflow_type: Tipo di flusso di lavoro da eseguire
        input_data: Dati di input forniti dall'utente
        
    Returns:
        Risultato dell'esecuzione del flusso di lavoro
    """
    return asyncio.run(run_system(workflow_type, input_data))

# Punto di ingresso per l'esecuzione da linea di comando
if __name__ == "__main__":
    import argparse
    
    # Parsing degli argomenti da linea di comando
    parser = argparse.ArgumentParser(description="Sistema di agenti per l'analisi di dati di gare d'appalto")
    parser.add_argument("workflow", nargs="?", default="complete", 
                        choices=["complete", "gazzetta", "json", "cig", "consip", "categorization", "link", "persistence"],
                        help="Tipo di flusso di lavoro da eseguire")
    parser.add_argument("--input", "-i", default="Analizza i dati delle gare d'appalto",
                        help="Dati di input per il flusso di lavoro")
    parser.add_argument("--debug", "-d", action="store_true",
                        help="Attiva la modalità debug con log più dettagliati")
    args = parser.parse_args()
    
    # Configura il livello di log in base all'argomento debug
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logging.getLogger("httpx").setLevel(logging.DEBUG)
        logging.getLogger("openai").setLevel(logging.DEBUG)
        logger.debug("Modalità debug attivata")
    
    # Verifica che la chiave API di OpenAI sia impostata
    if not os.environ.get("OPENAI_API_KEY"):
        logger.error("La variabile d'ambiente OPENAI_API_KEY non è impostata")
        print("ATTENZIONE: La variabile d'ambiente OPENAI_API_KEY non è impostata.")
        print("Per utilizzare il sistema, esegui:")
        print("export OPENAI_API_KEY='la_tua_chiave_api'")
        exit(1)
    else:
        logger.debug("Chiave API OpenAI trovata nelle variabili d'ambiente")
    
    # Esecuzione del sistema
    logger.info(f"Avvio dell'esecuzione con workflow={args.workflow}, input={args.input[:50]}...")
    result = run_system_sync(args.workflow, args.input)
    
    # Stampa dei risultati
    logger.info("Esecuzione completata, stampa dei risultati")
    print("\nRisultato dell'esecuzione:")
    print(result.final_output)
