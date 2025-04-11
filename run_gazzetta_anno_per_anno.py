#!/usr/bin/env python3
"""
Script per eseguire lo scraping della Gazzetta Ufficiale anno per anno.
"""

import os
import sys
import logging
import argparse
import pandas as pd
from datetime import datetime
import time
import shutil

# Importa lo scraper e la configurazione
from gazzetta_scraper_improved import GazzettaScraper
import config

# Configura logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("gazzetta_scraping.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("GazzettaRunner")

def configura_anno(anno, indice_ripresa=None):
    """
    Configura le variabili del modulo config per l'anno specificato.
    
    Args:
        anno (int): Anno da elaborare
        indice_ripresa (int, optional): Indice da cui riprendere lo scraping. Se None, parte dall'inizio.
    """
    config.ANNO_INIZIO = anno
    config.ANNO_FINE = anno
    
    if indice_ripresa is not None:
        config.RIPRENDI_DA_INDICE = indice_ripresa
    elif hasattr(config, 'RIPRENDI_DA_INDICE'):
        delattr(config, 'RIPRENDI_DA_INDICE')
        
    logger.info(f"Configurato l'anno {anno}" + 
               (f" con ripresa dall'indice {indice_ripresa}" if indice_ripresa else ""))

def combina_file_annuali():
    """
    Combina tutti i file annuali in un unico file Excel.
    
    Returns:
        bool: True se la combinazione è avvenuta con successo, False altrimenti
    """
    try:
        all_years_files = [os.path.join(config.TEMP_DIR, f) for f in os.listdir(config.TEMP_DIR) if f.startswith('parziale_')]
        
        if all_years_files:
            logger.info(f"Combinazione di {len(all_years_files)} file annuali")
            
            # Leggi tutti i file Excel e combinali
            dfs = []
            for f in all_years_files:
                try:
                    df = pd.read_excel(os.path.join(os.getcwd(), f))
                    dfs.append(df)
                except Exception as e:
                    logger.error(f"Errore durante la lettura di {f}: {str(e)}")
                    continue
            
            if dfs:
                df_finale = pd.concat(dfs, ignore_index=True).drop_duplicates()
                final_file_name = os.path.join(os.getcwd(), config.TEMP_DIR, config.LOTTI_RAW)
                
                # Backup del file esistente se presente
                if os.path.exists(final_file_name):
                    backup_name = f"{final_file_name}.bak.{datetime.now().strftime('%Y%m%d%H%M%S')}"
                    shutil.copy2(final_file_name, backup_name)
                    logger.info(f"Creato backup del file esistente: {backup_name}")
                
                df_finale.to_excel(final_file_name, index=False)
                logger.info(f"Salvataggio finale effettuato: {final_file_name} con {len(df_finale)} record")
                return True
            else:
                logger.warning("Nessun dato da combinare")
        else:
            logger.warning("Nessun file annuale trovato")
            
    except Exception as e:
        logger.error(f"Errore durante la combinazione dei file annuali: {str(e)}")
    
    return False

def run_scraping(anno_inizio, anno_fine, indice_ripresa=None):
    """
    Esegue lo scraping anno per anno.
    
    Args:
        anno_inizio (int): Anno di inizio
        anno_fine (int): Anno di fine
        indice_ripresa (int, optional): Indice da cui riprendere lo scraping per il primo anno.
    """
    logger.info(f"Avvio scraping anno per anno da {anno_inizio} a {anno_fine}")
    
    # Esegui lo scraping anno per anno
    for anno in range(anno_inizio, anno_fine + 1):
        logger.info(f"Inizia scraping per l'anno {anno}")
        
        # Configura l'anno corrente
        ripresa = indice_ripresa if anno == anno_inizio and indice_ripresa else None
        configura_anno(anno, ripresa)
        
        # Esegui lo scraping
        start_time = time.time()
        try:
            GazzettaScraper.run()
        except Exception as e:
            logger.error(f"Errore durante lo scraping dell'anno {anno}: {str(e)}")
            logger.error("Passa all'anno successivo...")
            continue
            
        elapsed_time = time.time() - start_time
        logger.info(f"Completato scraping dell'anno {anno} in {elapsed_time:.2f} secondi")
    
    # Combina i file annuali in un unico file
    logger.info("Combinazione dei file annuali")
    if combina_file_annuali():
        logger.info("Combinazione completata con successo")
    else:
        logger.warning("Errore nella combinazione dei file annuali")

def main():
    """
    Punto di ingresso principale.
    """
    parser = argparse.ArgumentParser(description='Scraper anno per anno della Gazzetta Ufficiale')
    parser.add_argument('--anno-inizio', type=int, default=2022, help='Anno di inizio (default: 2022)')
    parser.add_argument('--anno-fine', type=int, default=2022, help='Anno di fine (default: 2022)')
    parser.add_argument('--indice-ripresa', type=int, help='Indice da cui riprendere lo scraping per il primo anno')
    parser.add_argument('--solo-combinazione', action='store_true', help='Esegui solo la combinazione dei file annuali')
    
    args = parser.parse_args()
    
    if args.solo_combinazione:
        logger.info("Esecuzione della sola combinazione dei file annuali")
        combina_file_annuali()
    else:
        run_scraping(args.anno_inizio, args.anno_fine, args.indice_ripresa)

if __name__ == "__main__":
    logger.info("Avvio del programma")
    try:
        main()
        logger.info("Programma completato con successo")
    except Exception as e:
        logger.error(f"Errore non gestito: {str(e)}")
        logger.exception("Dettaglio dell'errore:")
        sys.exit(1) 