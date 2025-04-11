"""
Implementazione dell'agente di estrazione dalla Gazzetta Ufficiale utilizzando l'SDK ufficiale di OpenAI Agents.

Questo modulo implementa l'agente specializzato per l'estrazione di dati dalla Gazzetta Ufficiale
relativi a gare d'appalto nei settori di interesse.
"""

import os
import logging
import re
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
import asyncio
import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel

from agents import Agent, Runner, RunContextWrapper, function_tool

# Configurazione del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("gazzetta_agent.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("GazzettaAgent")

# Definizione dell'agente di estrazione dalla Gazzetta Ufficiale
gazzetta_agent = Agent(
    name="Gazzetta Extraction",
    handoff_description="Specialist agent for extracting data from Gazzetta Ufficiale",
    instructions="""
    Extract relevant data from Gazzetta Ufficiale related to public lighting, video surveillance, 
    tunnels, buildings, electric charging stations, parking, etc. Analyze the text of the tenders 
    and identify relevant information such as tender object, publication date, deadline, contracting 
    authority, and other metadata.
    
    Follow these steps:
    1. Navigate to the Gazzetta Ufficiale website and search for relevant tenders
    2. Extract the text of each tender
    3. Analyze the text to identify relevant information
    4. Filter tenders based on relevance to the sectors of interest
    5. Extract metadata such as tender object, publication date, deadline, contracting authority
    6. Save the extracted data in a structured format
    
    Focus on these sectors:
    - Public lighting (illuminazione pubblica, impianti di illuminazione, etc.)
    - Video surveillance (videosorveglianza, telecamere, etc.)
    - Tunnels and galleries (gallerie, tunnel, etc.)
    - Buildings (edifici, scuole, ospedali, etc.)
    - Electric charging stations (colonnine di ricarica, etc.)
    - Parking (parcheggi, aree di sosta, etc.)
    - Smart city (smart city, città intelligente, etc.)
    - Energy (energia, efficienza energetica, etc.)
    """
)

# Modelli di dati per l'output strutturato
class TenderMetadata(BaseModel):
    """Modello per i metadati di un bando di gara."""
    title: str
    publication_date: Optional[str] = None
    deadline: Optional[str] = None
    contracting_authority: Optional[str] = None
    cig: Optional[str] = None
    tender_type: Optional[str] = None
    amount: Optional[float] = None
    description: Optional[str] = None
    url: str
    relevant_sectors: List[str] = []
    relevance_score: float = 0.0
    relevance_explanation: Optional[str] = None

class ExtractionResult(BaseModel):
    """Modello per il risultato dell'estrazione."""
    tenders: List[TenderMetadata]
    total_processed: int
    total_relevant: int
    execution_time: float
    timestamp: str

# Configurazione dei settori di interesse e parole chiave
SECTORS_KEYWORDS = {
    "illuminazione": ["illuminazione", "pubblica illuminazione", "impianti di illuminazione", "lampade", "led", "lampioni"],
    "videosorveglianza": ["videosorveglianza", "telecamere", "tvcc", "controllo accessi", "sicurezza"],
    "gallerie": ["gallerie", "tunnel", "sottopassi", "galleria stradale"],
    "edifici": ["edifici", "scuole", "ospedali", "uffici pubblici", "edifici pubblici"],
    "colonnine_elettriche": ["colonnine", "ricarica", "veicoli elettrici", "stazioni di ricarica"],
    "parcheggi": ["parcheggi", "aree di sosta", "parcheggio", "sosta"],
    "smart_city": ["smart city", "città intelligente", "iot", "internet of things"],
    "energia": ["energia", "efficienza energetica", "risparmio energetico"]
}

# Strumenti (tools) per l'agente
@function_tool
def scrape_gazzetta_page(url: str) -> str:
    """
    Scarica e analizza una pagina della Gazzetta Ufficiale.
    
    Args:
        url: URL della pagina da scaricare
        
    Returns:
        Contenuto HTML della pagina
    """
    logger.info(f"Scaricamento della pagina: {url}")
    
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        logger.info(f"Pagina scaricata con successo: {url}")
        return response.text
    except requests.RequestException as e:
        logger.error(f"Errore durante il download della pagina {url}: {str(e)}")
        return f"Errore durante il download della pagina: {str(e)}"

@function_tool
def extract_text_from_html(html: str) -> str:
    """
    Estrae il testo da un contenuto HTML.
    
    Args:
        html: Contenuto HTML da cui estrarre il testo
        
    Returns:
        Testo estratto
    """
    logger.info("Estrazione del testo dal contenuto HTML")
    
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        # Rimuovi script e stili
        for script in soup(["script", "style"]):
            script.extract()
        
        # Estrai il testo
        text = soup.get_text()
        
        # Pulisci il testo
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = '\n'.join(chunk for chunk in chunks if chunk)
        
        logger.info(f"Testo estratto con successo ({len(text)} caratteri)")
        return text
    except Exception as e:
        logger.error(f"Errore durante l'estrazione del testo: {str(e)}")
        return f"Errore durante l'estrazione del testo: {str(e)}"

@function_tool
def analyze_tender_relevance(text: str, sectors_keywords: Dict[str, List[str]]) -> Dict[str, Any]:
    """
    Analizza la rilevanza di un bando di gara rispetto ai settori di interesse.
    
    Args:
        text: Testo del bando di gara
        sectors_keywords: Dizionario di settori e parole chiave
        
    Returns:
        Dizionario con i risultati dell'analisi
    """
    logger.info("Analisi della rilevanza del bando")
    
    text_lower = text.lower()
    relevant_sectors = []
    matches = {}
    
    # Cerca le parole chiave nel testo
    for sector, keywords in sectors_keywords.items():
        sector_matches = []
        for keyword in keywords:
            if keyword.lower() in text_lower:
                sector_matches.append(keyword)
        
        if sector_matches:
            relevant_sectors.append(sector)
            matches[sector] = sector_matches
    
    # Calcola un punteggio di rilevanza
    relevance_score = len(relevant_sectors) / len(sectors_keywords)
    
    # Prepara la spiegazione della rilevanza
    if relevant_sectors:
        relevance_explanation = f"Il bando è rilevante per i seguenti settori: {', '.join(relevant_sectors)}. "
        relevance_explanation += "Parole chiave trovate: "
        for sector, sector_matches in matches.items():
            relevance_explanation += f"{sector}: {', '.join(sector_matches)}; "
    else:
        relevance_explanation = "Il bando non è rilevante per nessuno dei settori di interesse."
    
    logger.info(f"Analisi completata. Rilevanza: {relevance_score:.2f}, Settori: {relevant_sectors}")
    
    return {
        "relevant_sectors": relevant_sectors,
        "relevance_score": relevance_score,
        "relevance_explanation": relevance_explanation,
        "matches": matches
    }

@function_tool
def extract_tender_metadata(text: str, url: str) -> Dict[str, Any]:
    """
    Estrae i metadati di un bando di gara dal testo.
    
    Args:
        text: Testo del bando di gara
        url: URL del bando di gara
        
    Returns:
        Dizionario con i metadati estratti
    """
    logger.info("Estrazione dei metadati dal bando")
    
    # Estrai il titolo (prima riga non vuota)
    lines = [line for line in text.splitlines() if line.strip()]
    title = lines[0] if lines else "Titolo non disponibile"
    
    # Estrai la data di pubblicazione
    publication_date = None
    pub_date_patterns = [
        r'Data di pubblicazione:?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
        r'Pubblicato il:?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
        r'Pubblicazione:?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})'
    ]
    for pattern in pub_date_patterns:
        match = re.search(pattern, text)
        if match:
            publication_date = match.group(1)
            break
    
    # Estrai la scadenza
    deadline = None
    deadline_patterns = [
        r'Scadenza:?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
        r'Termine:?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
        r'Data di scadenza:?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})'
    ]
    for pattern in deadline_patterns:
        match = re.search(pattern, text)
        if match:
            deadline = match.group(1)
            break
    
    # Estrai l'ente appaltante
    contracting_authority = None
    authority_patterns = [
        r'Ente appaltante:?\s*([^\n]+)',
        r'Stazione appaltante:?\s*([^\n]+)',
        r'Amministrazione aggiudicatrice:?\s*([^\n]+)'
    ]
    for pattern in authority_patterns:
        match = re.search(pattern, text)
        if match:
            contracting_authority = match.group(1).strip()
            break
    
    # Estrai il CIG
    cig = None
    cig_patterns = [
        r'CIG:?\s*([A-Z0-9]+)',
        r'Codice CIG:?\s*([A-Z0-9]+)',
        r'Codice Identificativo Gara:?\s*([A-Z0-9]+)'
    ]
    for pattern in cig_patterns:
        match = re.search(pattern, text)
        if match:
            cig = match.group(1)
            break
    
    # Estrai il tipo di bando
    tender_type = None
    type_patterns = [
        r'Tipo di appalto:?\s*([^\n]+)',
        r'Procedura:?\s*([^\n]+)',
        r'Tipo di procedura:?\s*([^\n]+)'
    ]
    for pattern in type_patterns:
        match = re.search(pattern, text)
        if match:
            tender_type = match.group(1).strip()
            break
    
    # Estrai l'importo
    amount = None
    amount_patterns = [
        r'Importo:?\s*(?:€|EUR|Euro)?\s*([\d\.]+(?:,\d+)?)',
        r'Valore:?\s*(?:€|EUR|Euro)?\s*([\d\.]+(?:,\d+)?)',
        r'Importo a base di gara:?\s*(?:€|EUR|Euro)?\s*([\d\.]+(?:,\d+)?)'
    ]
    for pattern in amount_patterns:
        match = re.search(pattern, text)
        if match:
            amount_str = match.group(1).replace('.', '').replace(',', '.')
            try:
                amount = float(amount_str)
            except ValueError:
                pass
            break
    
    # Estrai la descrizione (primi 500 caratteri dopo il titolo)
    description = None
    if len(lines) > 1:
        description_text = ' '.join(lines[1:])
        description = description_text[:500] + '...' if len(description_text) > 500 else description_text
    
    logger.info(f"Metadati estratti: Titolo='{title[:30]}...', Data='{publication_date}', Scadenza='{deadline}', Ente='{contracting_authority}'")
    
    return {
        "title": title,
        "publication_date": publication_date,
        "deadline": deadline,
        "contracting_authority": contracting_authority,
        "cig": cig,
        "tender_type": tender_type,
        "amount": amount,
        "description": description,
        "url": url
    }

@function_tool
def save_extracted_data(data: Dict[str, Any], output_file: str) -> str:
    """
    Salva i dati estratti in un file JSON.
    
    Args:
        data: Dati da salvare
        output_file: Percorso del file di output
        
    Returns:
        Messaggio di conferma
    """
    logger.info(f"Salvataggio dei dati estratti nel file: {output_file}")
    
    try:
        # Crea la directory di output se non esiste
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Salva i dati in formato JSON
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Dati salvati con successo nel file: {output_file}")
        return f"Dati salvati con successo nel file: {output_file}"
    except Exception as e:
        logger.error(f"Errore durante il salvataggio dei dati: {str(e)}")
        return f"Errore durante il salvataggio dei dati: {str(e)}"

@function_tool
def search_gazzetta(query: str, max_pages: int = 5) -> List[str]:
    """
    Cerca bandi di gara nella Gazzetta Ufficiale.
    
    Args:
        query: Query di ricerca
        max_pages: Numero massimo di pagine da scaricare
        
    Returns:
        Lista di URL dei bandi di gara trovati
    """
    logger.info(f"Esecuzione della ricerca: '{query}', max_pages={max_pages}")
    
    base_url = "https://www.gazzettaufficiale.it/ricerca/bandi"
    results = []
    
    try:
        # Simula una ricerca (in un'implementazione reale, questa sarebbe una vera richiesta HTTP)
        # Qui forniamo alcuni URL di esempio per illustrare la struttura
        for i in range(1, max_pages + 1):
            results.append(f"{base_url}/risultati?query={query}&page={i}")
        
        logger.info(f"Ricerca completata. Trovati {len(results)} risultati")
        return results
    except Exception as e:
        logger.error(f"Errore durante la ricerca: {str(e)}")
        return [f"Errore durante la ricerca: {str(e)}"]

@function_tool
def process_gazzetta_results(urls: List[str], sectors_keywords: Dict[str, List[str]]) -> Dict[str, Any]:
    """
    Processa i risultati della ricerca nella Gazzetta Ufficiale.
    
    Args:
        urls: Lista di URL dei bandi di gara
        sectors_keywords: Dizionario di settori e parole chiave
        
    Returns:
        Dizionario con i risultati dell'elaborazione
    """
    logger.info(f"Elaborazione di {len(urls)} risultati")
    
    start_time = datetime.now()
    tenders = []
    total_processed = 0
    total_relevant = 0
    
    # In un'implementazione reale, questo ciclo processerebbe effettivamente gli URL
    # Qui forniamo alcuni dati di esempio per illustrare la struttura
    for i, url in enumerate(urls):
        logger.info(f"Elaborazione del risultato {i+1}/{len(urls)}: {url}")
        
        # Simula il download e l'estrazione del testo
        html = f"<html><body><h1>Bando di gara {i+1}</h1><p>Questo è un esempio di bando di gara per l'illuminazione pubblica.</p></body></html>"
        text = f"Bando di gara {i+1}\n\nQuesto è un esempio di bando di gara per l'illuminazione pubblica.\n\nEnte appaltante: Comune di Esempio\nData di pubblicazione: 01/01/2023\nScadenza: 31/12/2023\nCIG: ABC123XYZ\nImporto: € 100.000,00"
        
        # Analizza la rilevanza
        relevance_result = analyze_tender_relevance(text, sectors_keywords)
        
        # Se il bando è rilevante, estrai i metadati
        if relevance_result["relevant_sectors"]:
            metadata = extract_tender_metadata(text, url)
            
            # Crea un oggetto TenderMetadata
            tender = TenderMetadata(
                title=metadata["title"],
                publication_date=metadata["publication_date"],
                deadline=metadata["deadline"],
                contracting_authority=metadata["contracting_authority"],
                cig=metadata["cig"],
                tender_type=metadata["tender_type"],
                amount=metadata["amount"],
                description=metadata["description"],
                url=url,
                relevant_sectors=relevance_result["relevant_sectors"],
                relevance_score=relevance_result["relevance_score"],
                relevance_explanation=relevance_result["relevance_explanation"]
            )
            
            tenders.append(tender.dict())
            total_relevant += 1
        
        total_processed += 1
    
    # Calcola il tempo di esecuzione
    execution_time = (datetime.now() - start_time).total_seconds()
    
    # Crea il risultato dell'estrazione
    result = ExtractionResult(
        tenders=tenders,
        total_processed=total_processed,
        total_relevant=total_relevant,
        execution_time=execution_time,
        timestamp=datetime.now().isoformat()
    )
    
    logger.info(f"Elaborazione completata. Processati {total_processed} bandi, di cui {total_relevant} rilevanti. Tempo di esecuzione: {execution_time:.2f} secondi")
    
    return result.dict()

# Funzione principale per l'esecuzione dell'agente
async def run_gazzetta_extraction(query: str, max_pages: int = 5, output_file: str = "output/gazzetta_output.json") -> Any:
    """
    Esegue l'estrazione dei dati dalla Gazzetta Ufficiale.
    
    Args:
        query: Query di ricerca
        max_pages: Numero massimo di pagine da processare
        output_file: Percorso del file di output
        
    Returns:
        Risultato dell'esecuzione dell'agente
    """
    logger.info(f"Avvio dell'estrazione dalla Gazzetta Ufficiale con query: {query}, max_pages={max_pages}")
    
    # Configurazione del contesto
    context = {}  # In questa versione dell'SDK, il contesto è un semplice dizionario
    context_wrapper = RunContextWrapper(context)
    
    # Prepara l'input per l'agente
    input_data = json.dumps({
        "query": query,
        "max_pages": max_pages,
        "output_file": output_file,
        "sectors_keywords": SECTORS_KEYWORDS
    })
    
    # Esecuzione dell'agente
    try:
        result = await Runner.run(
            gazzetta_agent,
            input_data,
            context=context_wrapper
        )
        logger.info("Estrazione dalla Gazzetta Ufficiale completata")
        return result
    except Exception as e:
        logger.error(f"Errore durante l'estrazione dalla Gazzetta Ufficiale: {str(e)}")
        raise

# Punto di ingresso per l'esecuzione da linea di comando
if __name__ == "__main__":
    import argparse
    
    # Parsing degli argomenti da linea di comando
    parser = argparse.ArgumentParser(description="Estrazione di dati dalla Gazzetta Ufficiale")
    parser.add_argument("--query", "-q", default="illuminazione OR videosorveglianza OR gallerie OR edifici OR colonnine OR parcheggi",
                        help="Query di ricerca")
    parser.add_argument("--max-pages", "-m", type=int, default=5,
                        help="Numero massimo di pagine da processare")
    parser.add_argument("--output", "-o", default="output/gazzetta_output.json",
                        help="Percorso del file di output")
    args = parser.parse_args()
    
    # Verifica che la chiave API di OpenAI sia impostata
    if not os.environ.get("OPENAI_API_KEY"):
        print("ATTENZIONE: La variabile d'ambiente OPENAI_API_KEY non è impostata.")
        print("Per utilizzare il sistema, esegui:")
        print("export OPENAI_API_KEY='la_tua_chiave_api'")
        exit(1)
    
    # Esecuzione dell'agente
    result = asyncio.run(run_gazzetta_extraction(args.query, args.max_pages, args.output))
    
    # Stampa dei risultati
    print("\nRisultato dell'esecuzione:")
    print(result.final_output)
