#!/usr/bin/env python
"""
Test dello scraping Gazzetta per verificare che funzioni.
"""

import sys
import logging
from pathlib import Path

# Aggiungi il path del progetto
sys.path.insert(0, str(Path(__file__).parent))

from src.scrapers.gazzetta import GazzettaScraper

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_single_page():
    """Test su una singola pagina della Gazzetta."""
    scraper = GazzettaScraper()
    
    # Test su una gazzetta specifica del 2024
    test_url = "http://www.gazzettaufficiale.it/gazzetta/contratti/caricaDettaglio?dataPubblicazioneGazzetta=2024-01-05&numeroGazzetta=1"
    
    print(f"\n🧪 Test scraping su: {test_url}")
    
    try:
        # Estrai dati
        testi = scraper.estrai_dati_da_url(test_url, 2024)
        
        print(f"✅ Testi estratti: {len(testi)}")
        
        if testi:
            print("\n📝 Primi 3 testi (max 200 caratteri ciascuno):")
            for i, testo in enumerate(testi[:3], 1):
                preview = testo[:200] + "..." if len(testo) > 200 else testo
                print(f"\n{i}. {preview}")
        else:
            print("⚠️ Nessun testo estratto - verificare la struttura HTML")
            
    except Exception as e:
        print(f"❌ Errore: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_single_page()