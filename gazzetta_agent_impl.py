"""
Implementazione dell'agente Gazzetta Ufficiale utilizzando lo scraper esistente.

Questo modulo implementa un agente specializzato per l'estrazione di dati dalla Gazzetta Ufficiale,
integrando il gazzetta_scraper_improved.py nel sistema di agenti.
"""

import os
import logging
import pandas as pd
import time
import re
from typing import List, Dict, Any, Optional
import json

# Importa lo scraper della Gazzetta
from gazzetta_scraper_improved import GazzettaScraper
import config

# Configurazione del logging
logger = logging.getLogger("GazzettaAgent")

class GazzettaAgent:
    """
    Agente specializzato per l'estrazione di dati dalla Gazzetta Ufficiale.
    Utilizza lo scraper esistente e aggiunge funzionalità specifiche per l'estrazione
    dei codici CIG e altri metadati.
    """
    
    @staticmethod
    def estrai_cig(testo: str) -> List[str]:
        """
        Estrae tutti i codici CIG presenti nel testo.
        
        Args:
            testo: Testo da cui estrarre i codici CIG
            
        Returns:
            Lista di codici CIG trovati
        """
        # Pattern per il codice CIG (7 caratteri alfanumerici)
        pattern_cig = re.compile(r'\bCIG:?\s*([A-Z0-9]{7,10})\b', re.IGNORECASE)
        matches = pattern_cig.findall(testo)
        
        # Rimuovi duplicati e normalizza
        cig_list = list(set([cig.upper() for cig in matches]))
        logger.debug(f"Estratti {len(cig_list)} codici CIG: {', '.join(cig_list)}")
        return cig_list

    @staticmethod
    def estrai_metadata(testo: str) -> Dict[str, Any]:
        """
        Estrae metadati rilevanti dal testo della gara.
        
        Args:
            testo: Testo della gara
            
        Returns:
            Dizionario con i metadati estratti
        """
        metadata = {
            "oggetto": None,
            "data_pubblicazione": None,
            "ente_appaltante": None,
            "importo": None,
            "cig": GazzettaAgent.estrai_cig(testo)
        }
        
        # Pattern per l'oggetto della gara
        pattern_oggetto = re.compile(r'OGGETTO:?\s*(.+?)(?:\.|ENTE APPALTANTE|\n\n)', re.IGNORECASE | re.DOTALL)
        match_oggetto = pattern_oggetto.search(testo)
        if match_oggetto:
            metadata["oggetto"] = match_oggetto.group(1).strip()
            
        # Pattern per l'ente appaltante
        pattern_ente = re.compile(r'ENTE APPALTANTE:?\s*(.+?)(?:\.|OGGETTO|\n\n)', re.IGNORECASE | re.DOTALL)
        match_ente = pattern_ente.search(testo)
        if match_ente:
            metadata["ente_appaltante"] = match_ente.group(1).strip()
            
        # Pattern per la data di pubblicazione
        pattern_data = re.compile(r'DATA:?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', re.IGNORECASE)
        match_data = pattern_data.search(testo)
        if match_data:
            metadata["data_pubblicazione"] = match_data.group(1)
            
        # Pattern per l'importo
        pattern_importo = re.compile(r'(?:IMPORTO|VALORE):?\s*(?:EURO|€)?\s*([0-9.,]+)', re.IGNORECASE)
        match_importo = pattern_importo.search(testo)
        if match_importo:
            # Rimuovi punti e converti virgola in punto per avere un formato numerico
            importo_str = match_importo.group(1).replace('.', '').replace(',', '.')
            try:
                metadata["importo"] = float(importo_str)
            except ValueError:
                logger.warning(f"Impossibile convertire l'importo: {match_importo.group(1)}")

        return metadata

    @staticmethod
    def process_gazzetta_data(input_data: str) -> Dict[str, Any]:
        """
        Processa i dati della Gazzetta Ufficiale in base all'input dell'utente.
        
        Args:
            input_data: Input dell'utente
            
        Returns:
            Dizionario con i risultati dell'elaborazione
        """
        logger.info(f"Avvio elaborazione dati Gazzetta con input: {input_data}")
        start_time = time.time()
        
        # Analizza l'input per determinare l'azione da eseguire
        if "scarica" in input_data.lower() or "download" in input_data.lower():
            # Esegui lo scraping completo
            logger.info("Avvio scaricamento dati dalla Gazzetta Ufficiale")
            GazzettaScraper.run()
            
            # Verifica se il file è stato creato
            final_file_path = os.path.join(os.getcwd(), config.TEMP_DIR, config.LOTTI_RAW)
            if os.path.exists(final_file_path):
                df = pd.read_excel(final_file_path)
                logger.info(f"Scaricamento completato. {len(df)} record trovati.")
                
                # Estrai metadati
                risultati = []
                for idx, row in df.iterrows():
                    testo = row['testo']
                    metadata = GazzettaAgent.estrai_metadata(testo)
                    risultati.append(metadata)
                    
                # Salva i risultati in un file JSON
                output_file = os.path.join(os.getcwd(), config.TEMP_DIR, "gazzetta_data.json")
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(risultati, f, indent=2, ensure_ascii=False)
                    
                elapsed_time = time.time() - start_time
                logger.info(f"Elaborazione completata in {elapsed_time:.2f} secondi. Risultati salvati in {output_file}")
                
                return {
                    "success": True,
                    "message": f"Elaborazione completata. {len(risultati)} record elaborati.",
                    "file_path": output_file,
                    "elapsed_time": elapsed_time,
                    "sample_data": risultati[:5] if risultati else []
                }
            else:
                logger.error(f"File non trovato: {final_file_path}")
                return {
                    "success": False,
                    "message": "Errore durante lo scaricamento dei dati. File finale non trovato.",
                    "elapsed_time": time.time() - start_time
                }
        
        elif "estrai" in input_data.lower() or "extract" in input_data.lower():
            # Verifica se esistono già dati scaricati
            final_file_path = os.path.join(os.getcwd(), config.TEMP_DIR, config.LOTTI_RAW)
            if not os.path.exists(final_file_path):
                logger.error(f"File non trovato: {final_file_path}. Esegui prima lo scaricamento dei dati.")
                return {
                    "success": False,
                    "message": "Dati non trovati. Esegui prima lo scaricamento con 'scarica dati'.",
                    "elapsed_time": time.time() - start_time
                }
                
            # Estrai metadati dai dati esistenti
            df = pd.read_excel(final_file_path)
            logger.info(f"Avvio estrazione metadati da {len(df)} record.")
            
            risultati = []
            for idx, row in df.iterrows():
                testo = row['testo']
                metadata = GazzettaAgent.estrai_metadata(testo)
                risultati.append(metadata)
                
            # Filtra i risultati per tenere solo quelli con CIG
            risultati_con_cig = [r for r in risultati if r.get("cig")]
            logger.info(f"Trovati {len(risultati_con_cig)} record con codici CIG su {len(risultati)} totali.")
            
            # Salva i risultati in un file JSON
            output_file = os.path.join(os.getcwd(), config.TEMP_DIR, "gazzetta_data.json")
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(risultati_con_cig, f, indent=2, ensure_ascii=False)
                
            elapsed_time = time.time() - start_time
            logger.info(f"Estrazione completata in {elapsed_time:.2f} secondi. Risultati salvati in {output_file}")
            
            return {
                "success": True,
                "message": f"Estrazione completata. {len(risultati_con_cig)} record con CIG estratti.",
                "file_path": output_file,
                "elapsed_time": elapsed_time,
                "sample_data": risultati_con_cig[:5] if risultati_con_cig else []
            }
        
        else:
            logger.warning(f"Comando non riconosciuto: {input_data}")
            return {
                "success": False,
                "message": f"Comando non riconosciuto. Utilizza 'scarica' per scaricare nuovi dati o 'estrai' per estrarre metadati da dati esistenti.",
                "elapsed_time": time.time() - start_time
            }

# Funzione principale per l'esecuzione dell'agente
def run_gazzetta_agent(input_data: str) -> Dict[str, Any]:
    """
    Esegue l'agente Gazzetta con l'input specificato.
    
    Args:
        input_data: Input dell'utente
        
    Returns:
        Dizionario con i risultati dell'elaborazione
    """
    try:
        return GazzettaAgent.process_gazzetta_data(input_data)
    except Exception as e:
        logger.error(f"Errore durante l'esecuzione dell'agente Gazzetta: {str(e)}", exc_info=True)
        return {
            "success": False,
            "message": f"Errore durante l'esecuzione dell'agente: {str(e)}",
            "error": str(e)
        }

# Punto di ingresso per l'esecuzione da linea di comando
if __name__ == "__main__":
    import sys
    
    # Configurazione del logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("gazzetta_agent.log"),
            logging.StreamHandler()
        ]
    )
    
    # Input da linea di comando o predefinito
    input_data = sys.argv[1] if len(sys.argv) > 1 else "estrai dati dalla Gazzetta Ufficiale"
    
    # Esecuzione dell'agente
    result = run_gazzetta_agent(input_data)
    
    # Stampa dei risultati
    print("\nRisultato dell'esecuzione:")
    print(json.dumps(result, indent=2, ensure_ascii=False)) 