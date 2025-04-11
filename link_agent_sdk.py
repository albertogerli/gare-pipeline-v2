"""
Implementazione dell'agente di collegamento JSON-Scraping utilizzando l'SDK ufficiale di OpenAI Agents.

Questo modulo implementa l'agente specializzato per il collegamento dei dati provenienti 
dallo scraping della Gazzetta Ufficiale con i dati JSON OCDS tramite CIG.
"""

import os
import logging
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
        logging.FileHandler("link_agent.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("LinkAgent")

# Definizione dell'agente di collegamento JSON-Scraping
link_agent = Agent(
    name="JSON-Scraping Link",
    handoff_description="Specialist agent for linking data from Gazzetta Ufficiale with OCDS data via CIG",
    instructions="""
    Link data from Gazzetta Ufficiale with OCDS data using CIG codes as keys. Extract CIG codes 
    from the text of the tenders, find matching OCDS records, and merge the information from 
    both sources. Handle cases where the same CIG appears in multiple sources and resolve conflicts.
    
    Follow these steps:
    1. Extract CIG codes from Gazzetta Ufficiale data
    2. Find matching OCDS records using CIG codes
    3. Merge the information from both sources
    4. Handle conflicts and duplicates
    5. Generate a comprehensive report
    
    Focus on these aspects:
    - Accurate extraction of CIG codes
    - Proper matching of records
    - Comprehensive merging of information
    - Conflict resolution
    - Detailed reporting
    """
)

# Modelli di dati per l'output strutturato
class LinkedRecord(BaseModel):
    """Modello per un record collegato."""
    cig: str
    gazzetta_data: Optional[Dict[str, Any]] = None
    ocds_data: Optional[Dict[str, Any]] = None
    match_confidence: float
    match_explanation: str
    merged_data: Dict[str, Any]

class LinkingResult(BaseModel):
    """Modello per il risultato del collegamento."""
    total_gazzetta_records: int
    total_ocds_records: int
    total_linked_records: int
    linked_records: List[LinkedRecord]
    unmatched_gazzetta_records: int
    unmatched_ocds_records: int
    execution_time: float
    timestamp: str

# Strumenti (tools) per l'agente
@link_agent.tool
def load_gazzetta_data(file_path: str) -> Dict[str, Any]:
    """
    Carica i dati della Gazzetta Ufficiale da un file.
    
    Args:
        file_path: Percorso del file contenente i dati della Gazzetta Ufficiale
        
    Returns:
        Dati caricati
    """
    logger.info(f"Caricamento dei dati della Gazzetta Ufficiale dal file: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        logger.info(f"Dati della Gazzetta Ufficiale caricati con successo: {len(data.get('tenders', []))} record")
        return {
            "success": True,
            "data": data
        }
    except Exception as e:
        logger.error(f"Errore durante il caricamento dei dati della Gazzetta Ufficiale: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

@link_agent.tool
def load_ocds_data(file_path: str) -> Dict[str, Any]:
    """
    Carica i dati OCDS da un file.
    
    Args:
        file_path: Percorso del file contenente i dati OCDS
        
    Returns:
        Dati caricati
    """
    logger.info(f"Caricamento dei dati OCDS dal file: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        logger.info(f"Dati OCDS caricati con successo: {len(data.get('releases', []))} record")
        return {
            "success": True,
            "data": data
        }
    except Exception as e:
        logger.error(f"Errore durante il caricamento dei dati OCDS: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

@link_agent.tool
def extract_cig_from_text(text: str) -> List[str]:
    """
    Estrae i codici CIG da un testo.
    
    Args:
        text: Testo da cui estrarre i codici CIG
        
    Returns:
        Lista di codici CIG estratti
    """
    logger.info("Estrazione dei codici CIG dal testo")
    
    # Pattern per i codici CIG (7-10 caratteri alfanumerici)
    cig_patterns = [
        r'CIG:?\s*([A-Z0-9]{7,10})',
        r'Codice CIG:?\s*([A-Z0-9]{7,10})',
        r'Codice Identificativo Gara:?\s*([A-Z0-9]{7,10})',
        r'C\.I\.G\.?:?\s*([A-Z0-9]{7,10})',
        r'[^A-Z0-9]([A-Z0-9]{7,10})[^A-Z0-9]'  # Pattern generico per codici isolati
    ]
    
    cig_codes = []
    
    for pattern in cig_patterns:
        matches = re.findall(pattern, text)
        cig_codes.extend(matches)
    
    # Rimuovi duplicati e filtra per lunghezza
    unique_cigs = []
    for cig in cig_codes:
        if cig not in unique_cigs and 7 <= len(cig) <= 10:
            unique_cigs.append(cig)
    
    logger.info(f"Estratti {len(unique_cigs)} codici CIG unici")
    return unique_cigs

@link_agent.tool
def extract_cigs_from_gazzetta(gazzetta_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Estrae i codici CIG dai dati della Gazzetta Ufficiale.
    
    Args:
        gazzetta_data: Dati della Gazzetta Ufficiale
        
    Returns:
        Dati con codici CIG estratti
    """
    logger.info("Estrazione dei codici CIG dai dati della Gazzetta Ufficiale")
    
    tenders = gazzetta_data.get('tenders', [])
    tenders_with_cig = []
    
    for tender in tenders:
        # Verifica se il CIG è già presente
        if 'cig' in tender and tender['cig']:
            tenders_with_cig.append(tender)
            continue
        
        # Crea un testo combinato per l'estrazione
        combined_text = f"{tender.get('title', '')} {tender.get('description', '')}"
        
        # Estrai i codici CIG
        cig_codes = extract_cig_from_text(combined_text)
        
        if cig_codes:
            # Usa il primo CIG trovato
            tender['cig'] = cig_codes[0]
            
            # Se ci sono più CIG, salvali come CIG aggiuntivi
            if len(cig_codes) > 1:
                tender['additional_cigs'] = cig_codes[1:]
            
            tenders_with_cig.append(tender)
    
    logger.info(f"Trovati {len(tenders_with_cig)} bandi con CIG su {len(tenders)} totali")
    
    return {
        "tenders_with_cig": tenders_with_cig,
        "tenders_without_cig": len(tenders) - len(tenders_with_cig)
    }

@link_agent.tool
def extract_cigs_from_ocds(ocds_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Estrae i codici CIG dai dati OCDS.
    
    Args:
        ocds_data: Dati OCDS
        
    Returns:
        Dati con codici CIG estratti
    """
    logger.info("Estrazione dei codici CIG dai dati OCDS")
    
    releases = ocds_data.get('releases', [])
    releases_with_cig = []
    
    for release in releases:
        # Verifica se il CIG è già presente
        if 'cig' in release and release['cig']:
            releases_with_cig.append(release)
            continue
        
        # Cerca il CIG nell'ID del tender
        if 'tender' in release and 'id' in release['tender']:
            tender_id = release['tender']['id']
            if re.match(r'^[A-Z0-9]{7,10}$', tender_id):
                release['cig'] = tender_id
                releases_with_cig.append(release)
                continue
        
        # Cerca il CIG nei lotti
        cig_found = False
        if 'tender' in release and 'lots' in release['tender'] and isinstance(release['tender']['lots'], list):
            for lot in release['tender']['lots']:
                if 'id' in lot and re.match(r'^[A-Z0-9]{7,10}$', lot['id']):
                    release['cig'] = lot['id']
                    releases_with_cig.append(release)
                    cig_found = True
                    break
            
            if cig_found:
                continue
        
        # Cerca il CIG nel titolo o nella descrizione
        if 'tender' in release:
            combined_text = f"{release['tender'].get('title', '')} {release['tender'].get('description', '')}"
            cig_codes = extract_cig_from_text(combined_text)
            
            if cig_codes:
                release['cig'] = cig_codes[0]
                
                if len(cig_codes) > 1:
                    release['additional_cigs'] = cig_codes[1:]
                
                releases_with_cig.append(release)
    
    logger.info(f"Trovati {len(releases_with_cig)} rilasci con CIG su {len(releases)} totali")
    
    return {
        "releases_with_cig": releases_with_cig,
        "releases_without_cig": len(releases) - len(releases_with_cig)
    }

@link_agent.tool
def find_matching_records(gazzetta_records: List[Dict[str, Any]], ocds_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Trova i record corrispondenti tra i dati della Gazzetta Ufficiale e i dati OCDS.
    
    Args:
        gazzetta_records: Record della Gazzetta Ufficiale
        ocds_records: Record OCDS
        
    Returns:
        Record corrispondenti
    """
    logger.info("Ricerca dei record corrispondenti")
    
    # Crea dizionari per accesso rapido
    gazzetta_by_cig = {}
    for record in gazzetta_records:
        cig = record.get('cig')
        if cig:
            if cig in gazzetta_by_cig:
                # Se il CIG è già presente, crea una lista
                if isinstance(gazzetta_by_cig[cig], list):
                    gazzetta_by_cig[cig].append(record)
                else:
                    gazzetta_by_cig[cig] = [gazzetta_by_cig[cig], record]
            else:
                gazzetta_by_cig[cig] = record
    
    ocds_by_cig = {}
    for record in ocds_records:
        cig = record.get('cig')
        if cig:
            if cig in ocds_by_cig:
                # Se il CIG è già presente, crea una lista
                if isinstance(ocds_by_cig[cig], list):
                    ocds_by_cig[cig].append(record)
                else:
                    ocds_by_cig[cig] = [ocds_by_cig[cig], record]
            else:
                ocds_by_cig[cig] = record
    
    # Trova le corrispondenze
    matches = []
    matched_gazzetta_cigs = set()
    matched_ocds_cigs = set()
    
    for cig in gazzetta_by_cig:
        if cig in ocds_by_cig:
            gazzetta_record = gazzetta_by_cig[cig]
            ocds_record = ocds_by_cig[cig]
            
            # Gestisci il caso in cui ci sono più record per lo stesso CIG
            if isinstance(gazzetta_record, list):
                for g_record in gazzetta_record:
                    if isinstance(ocds_record, list):
                        for o_record in ocds_record:
                            matches.append({
                                "cig": cig,
                                "gazzetta_data": g_record,
                                "ocds_data": o_record,
                                "match_confidence": 1.0,
                                "match_explanation": "Corrispondenza esatta del CIG"
                            })
                    else:
                        matches.append({
                            "cig": cig,
                            "gazzetta_data": g_record,
                            "ocds_data": ocds_record,
                            "match_confidence": 1.0,
                            "match_explanation": "Corrispondenza esatta del CIG"
                        })
            else:
                if isinstance(ocds_record, list):
                    for o_record in ocds_record:
                        matches.append({
                            "cig": cig,
                            "gazzetta_data": gazzetta_record,
                            "ocds_data": o_record,
                            "match_confidence": 1.0,
                            "match_explanation": "Corrispondenza esatta del CIG"
                        })
                else:
                    matches.append({
                        "cig": cig,
                        "gazzetta_data": gazzetta_record,
                        "ocds_data": ocds_record,
                        "match_confidence": 1.0,
                        "match_explanation": "Corrispondenza esatta del CIG"
                    })
            
            matched_gazzetta_cigs.add(cig)
            matched_ocds_cigs.add(cig)
    
    # Cerca corrispondenze fuzzy per i record non corrispondenti
    for cig_g in gazzetta_by_cig:
        if cig_g not in matched_gazzetta_cigs:
            gazzetta_record = gazzetta_by_cig[cig_g]
            
            # Cerca corrispondenze fuzzy
            for cig_o in ocds_by_cig:
                if cig_o not in matched_ocds_cigs:
                    # Verifica se i CIG sono simili (es. differiscono per un carattere)
                    if len(cig_g) == len(cig_o):
                        diff_count = sum(1 for a, b in zip(cig_g, cig_o) if a != b)
                        if diff_count <= 1:
                            ocds_record = ocds_by_cig[cig_o]
                            
                            confidence = 1.0 - (diff_count / len(cig_g))
                            explanation = f"Corrispondenza fuzzy del CIG ({diff_count} caratteri diversi)"
                            
                            if isinstance(gazzetta_record, list):
                                for g_record in gazzetta_record:
                                    if isinstance(ocds_record, list):
                                        for o_record in ocds_record:
                                            matches.append({
                                                "cig": cig_g,
                                                "gazzetta_data": g_record,
                                                "ocds_data": o_record,
                                                "match_confidence": confidence,
                                                "match_explanation": explanation
                                            })
                                    else:
                                        matches.append({
                                            "cig": cig_g,
                                            "gazzetta_data": g_record,
                                            "ocds_data": ocds_record,
                                            "match_confidence": confidence,
                                            "match_explanation": explanation
                                        })
                            else:
                                if isinstance(ocds_record, list):
                                    for o_record in ocds_record:
                                        matches.append({
                                            "cig": cig_g,
                                            "gazzetta_data": gazzetta_record,
                                            "ocds_data": o_record,
                                            "match_confidence": confidence,
                                            "match_explanation": explanation
                                        })
                                else:
                                    matches.append({
                                        "cig": cig_g,
                                        "gazzetta_data": gazzetta_record,
                                        "ocds_data": ocds_record,
                                        "match_confidence": confidence,
                                        "match_explanation": explanation
                                    })
                            
                            matched_gazzetta_cigs.add(cig_g)
                            matched_ocds_cigs.add(cig_o)
    
    logger.info(f"Trovate {len(matches)} corrispondenze")
    
    return {
        "matches": matches,
        "unmatched_gazzetta_records": len(gazzetta_by_cig) - len(matched_gazzetta_cigs),
        "unmatched_ocds_records": len(ocds_by_cig) - len(matched_ocds_cigs)
    }

@link_agent.tool
def merge_record_data(gazzetta_data: Dict[str, Any], ocds_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Unisce i dati di un record della Gazzetta Ufficiale con i dati di un record OCDS.
    
    Args:
        gazzetta_data: Dati del record della Gazzetta Ufficiale
        ocds_data: Dati del record OCDS
        
    Returns:
        Dati uniti
    """
    logger.info("Unione dei dati dei record")
    
    merged_data = {
        "cig": gazzetta_data.get('cig') or ocds_data.get('cig'),
        "sources": {
            "gazzetta": True,
            "ocds": True
        }
    }
    
    # Dati di base
    merged_data["title"] = gazzetta_data.get('title') or ocds_data.get('tender', {}).get('title')
    merged_data["description"] = gazzetta_data.get('description') or ocds_data.get('tender', {}).get('description')
    
    # Date
    merged_data["publication_date"] = gazzetta_data.get('publication_date') or ocds_data.get('date')
    merged_data["deadline"] = gazzetta_data.get('deadline')
    
    # Ente appaltante
    merged_data["contracting_authority"] = gazzetta_data.get('contracting_authority') or ocds_data.get('buyer', {}).get('name')
    
    # Importo
    if gazzetta_data.get('amount'):
        merged_data["amount"] = gazzetta_data.get('amount')
    elif ocds_data.get('tender', {}).get('value', {}).get('amount'):
        merged_data["amount"] = ocds_data.get('tender', {}).get('value', {}).get('amount')
    
    # Stato
    merged_data["status"] = ocds_data.get('tender', {}).get('status')
    
    # Tipo di procedura
    merged_data["procurement_method"] = ocds_data.get('tender', {}).get('procurementMethod')
    
    # Categorie (se disponibili)
    if gazzetta_data.get('relevant_sectors'):
        merged_data["categories"] = gazzetta_data.get('relevant_sectors')
    
    # Dati specifici della Gazzetta
    merged_data["gazzetta_data"] = {
        "url": gazzetta_data.get('url'),
        "relevance_score": gazzetta_data.get('relevance_score'),
        "relevance_explanation": gazzetta_data.get('relevance_explanation')
    }
    
    # Dati specifici OCDS
    merged_data["ocds_data"] = {
        "ocid": ocds_data.get('ocid'),
        "id": ocds_data.get('id'),
        "tag": ocds_data.get('tag'),
        "initiationType": ocds_data.get('initiationType')
    }
    
    # Dati del tender
    if 'tender' in ocds_data:
        merged_data["tender_details"] = {
            "id": ocds_data['tender'].get('id'),
            "status": ocds_data['tender'].get('status'),
            "procurementMethod": ocds_data['tender'].get('procurementMethod'),
            "procurementMethodDetails": ocds_data['tender'].get('procurementMethodDetails')
        }
    
    # Dati delle aggiudicazioni
    if 'awards' in ocds_data and ocds_data['awards']:
        merged_data["awards"] = []
        for award in ocds_data['awards']:
            merged_data["awards"].append({
                "id": award.get('id'),
                "title": award.get('title'),
                "description": award.get('description'),
                "status": award.get('status'),
                "date": award.get('date'),
                "value": award.get('value'),
                "suppliers": award.get('suppliers')
            })
    
    # Dati dei contratti
    if 'contracts' in ocds_data and ocds_data['contracts']:
        merged_data["contracts"] = []
        for contract in ocds_data['contracts']:
            merged_data["contracts"].append({
                "id": contract.get('id'),
                "awardID": contract.get('awardID'),
                "title": contract.get('title'),
                "description": contract.get('description'),
                "status": contract.get('status'),
                "period": contract.get('period'),
                "value": contract.get('value')
            })
    
    logger.info("Dati dei record uniti con successo")
    return merged_data

@link_agent.tool
def process_matches(matches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Processa le corrispondenze trovate.
    
    Args:
        matches: Lista di corrispondenze
        
    Returns:
        Lista di record collegati
    """
    logger.info(f"Elaborazione di {len(matches)} corrispondenze")
    
    linked_records = []
    
    for match in matches:
        # Unisci i dati
        merged_data = merge_record_data(match['gazzetta_data'], match['ocds_data'])
        
        # Crea il record collegato
        linked_record = LinkedRecord(
            cig=match['cig'],
            gazzetta_data=match['gazzetta_data'],
            ocds_data=match['ocds_data'],
            match_confidence=match['match_confidence'],
            match_explanation=match['match_explanation'],
            merged_data=merged_data
        )
        
        linked_records.append(linked_record.dict())
    
    logger.info(f"Elaborazione completata: {len(linked_records)} record collegati")
    return linked_records

@link_agent.tool
def generate_linking_result(linked_records: List[Dict[str, Any]], total_gazzetta_records: int, total_ocds_records: int, unmatched_gazzetta_records: int, unmatched_ocds_records: int) -> Dict[str, Any]:
    """
    Genera il risultato del collegamento.
    
    Args:
        linked_records: Lista di record collegati
        total_gazzetta_records: Numero totale di record della Gazzetta Ufficiale
        total_ocds_records: Numero totale di record OCDS
        unmatched_gazzetta_records: Numero di record della Gazzetta Ufficiale non corrispondenti
        unmatched_ocds_records: Numero di record OCDS non corrispondenti
        
    Returns:
        Risultato del collegamento
    """
    logger.info("Generazione del risultato del collegamento")
    
    start_time = datetime.now()
    
    # Crea il risultato del collegamento
    result = LinkingResult(
        total_gazzetta_records=total_gazzetta_records,
        total_ocds_records=total_ocds_records,
        total_linked_records=len(linked_records),
        linked_records=linked_records,
        unmatched_gazzetta_records=unmatched_gazzetta_records,
        unmatched_ocds_records=unmatched_ocds_records,
        execution_time=(datetime.now() - start_time).total_seconds(),
        timestamp=datetime.now().isoformat()
    )
    
    logger.info(f"Risultato generato: {result.total_linked_records} record collegati")
    return result.dict()

@link_agent.tool
def save_linking_result(result: Dict[str, Any], output_file: str) -> str:
    """
    Salva il risultato del collegamento in un file.
    
    Args:
        result: Risultato del collegamento
        output_file: Percorso del file di output
        
    Returns:
        Messaggio di conferma
    """
    logger.info(f"Salvataggio del risultato del collegamento nel file: {output_file}")
    
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

@link_agent.tool
def export_to_excel(result: Dict[str, Any], output_file: str) -> str:
    """
    Esporta il risultato del collegamento in un file Excel.
    
    Args:
        result: Risultato del collegamento
        output_file: Percorso del file Excel di output
        
    Returns:
        Messaggio di conferma
    """
    logger.info(f"Esportazione del risultato del collegamento in Excel: {output_file}")
    
    try:
        # Crea la directory di output se non esiste
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Crea un DataFrame dai dati
        records = []
        for record in result['linked_records']:
            merged_data = record['merged_data']
            
            # Estrai i dati principali
            row = {
                'CIG': record['cig'],
                'Titolo': merged_data.get('title', ''),
                'Descrizione': merged_data.get('description', ''),
                'Ente Appaltante': merged_data.get('contracting_authority', ''),
                'Data Pubblicazione': merged_data.get('publication_date', ''),
                'Scadenza': merged_data.get('deadline', ''),
                'Importo': merged_data.get('amount', ''),
                'Stato': merged_data.get('status', ''),
                'Procedura': merged_data.get('procurement_method', ''),
                'Confidenza Match': record['match_confidence'],
                'Spiegazione Match': record['match_explanation'],
                'Fonte Gazzetta': 'Sì' if record['gazzetta_data'] else 'No',
                'Fonte OCDS': 'Sì' if record['ocds_data'] else 'No'
            }
            
            # Aggiungi categorie se disponibili
            if 'categories' in merged_data:
                row['Categorie'] = ', '.join(merged_data['categories'])
            
            # Aggiungi dati delle aggiudicazioni se disponibili
            if 'awards' in merged_data and merged_data['awards']:
                award = merged_data['awards'][0]  # Prendi la prima aggiudicazione
                row['Aggiudicazione - Stato'] = award.get('status', '')
                row['Aggiudicazione - Data'] = award.get('date', '')
                
                if 'value' in award and 'amount' in award['value']:
                    row['Aggiudicazione - Importo'] = award['value']['amount']
                
                if 'suppliers' in award and award['suppliers']:
                    supplier = award['suppliers'][0]  # Prendi il primo fornitore
                    row['Aggiudicazione - Fornitore'] = supplier.get('name', '')
            
            records.append(row)
        
        df = pd.DataFrame(records)
        
        # Crea un writer Excel
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Foglio principale con i dati dei record collegati
            df.to_excel(writer, sheet_name='Record Collegati', index=False)
            
            # Foglio con il riepilogo
            summary_data = {
                'Metrica': [
                    'Totale record Gazzetta',
                    'Totale record OCDS',
                    'Totale record collegati',
                    'Record Gazzetta non corrispondenti',
                    'Record OCDS non corrispondenti',
                    'Tempo di esecuzione (secondi)',
                    'Timestamp'
                ],
                'Valore': [
                    result['total_gazzetta_records'],
                    result['total_ocds_records'],
                    result['total_linked_records'],
                    result['unmatched_gazzetta_records'],
                    result['unmatched_ocds_records'],
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
async def run_json_scraping_link(gazzetta_file: str, ocds_file: str, output_json: str = "output/linked_records.json", output_excel: str = "output/gare_collegate.xlsx") -> Any:
    """
    Esegue il collegamento dei dati della Gazzetta Ufficiale con i dati OCDS.
    
    Args:
        gazzetta_file: Percorso del file contenente i dati della Gazzetta Ufficiale
        ocds_file: Percorso del file contenente i dati OCDS
        output_json: Percorso del file JSON di output
        output_excel: Percorso del file Excel di output
        
    Returns:
        Risultato dell'esecuzione dell'agente
    """
    logger.info(f"Avvio del collegamento dei dati: gazzetta_file={gazzetta_file}, ocds_file={ocds_file}")
    
    # Configurazione del contesto
    context = Context()
    
    # Prepara l'input per l'agente
    input_data = f"""
    Collega i dati della Gazzetta Ufficiale con i dati OCDS tramite i codici CIG.
    
    File Gazzetta: {gazzetta_file}
    File OCDS: {ocds_file}
    File JSON di output: {output_json}
    File Excel di output: {output_excel}
    
    Segui questi passaggi:
    1. Carica i dati della Gazzetta Ufficiale e i dati OCDS
    2. Estrai i codici CIG da entrambi i set di dati
    3. Trova le corrispondenze tra i record
    4. Unisci i dati dei record corrispondenti
    5. Genera un risultato completo
    6. Salva il risultato in formato JSON
    7. Esporta il risultato in formato Excel
    """
    
    # Esecuzione dell'agente
    result = await Runner.run(
        link_agent,
        input_data,
        context=context
    )
    
    logger.info("Collegamento dei dati completato")
    return result

# Punto di ingresso per l'esecuzione da linea di comando
if __name__ == "__main__":
    import argparse
    
    # Parsing degli argomenti da linea di comando
    parser = argparse.ArgumentParser(description="Collegamento dei dati della Gazzetta Ufficiale con i dati OCDS")
    parser.add_argument("--gazzetta-file", required=True,
                        help="Percorso del file contenente i dati della Gazzetta Ufficiale")
    parser.add_argument("--ocds-file", required=True,
                        help="Percorso del file contenente i dati OCDS")
    parser.add_argument("--output-json", default="output/linked_records.json",
                        help="Percorso del file JSON di output")
    parser.add_argument("--output-excel", default="output/gare_collegate.xlsx",
                        help="Percorso del file Excel di output")
    args = parser.parse_args()
    
    # Verifica che la chiave API di OpenAI sia impostata
    if not os.environ.get("OPENAI_API_KEY"):
        print("ATTENZIONE: La variabile d'ambiente OPENAI_API_KEY non è impostata.")
        print("Per utilizzare il sistema, esegui:")
        print("export OPENAI_API_KEY='la_tua_chiave_api'")
        exit(1)
    
    # Esecuzione dell'agente
    result = asyncio.run(run_json_scraping_link(args.gazzetta_file, args.ocds_file, args.output_json, args.output_excel))
    
    # Stampa dei risultati
    print("\nRisultato dell'esecuzione:")
    print(result.final_output)
