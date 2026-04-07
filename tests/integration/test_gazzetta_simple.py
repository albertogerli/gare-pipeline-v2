#!/usr/bin/env python
"""
Test semplice dello scraping Gazzetta.
"""

import requests
from bs4 import BeautifulSoup

def test_basic():
    """Test basico per verificare la struttura HTML."""
    url = "http://www.gazzettaufficiale.it/gazzetta/contratti/caricaDettaglio?dataPubblicazioneGazzetta=2024-01-05&numeroGazzetta=1"
    
    print(f"📥 Downloading: {url}")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        print(f"✅ Status: {response.status_code}")
        print(f"📏 Content length: {len(response.text)} bytes")
        
        # Cerca "AVVISI ESITI DI GARA"
        if "AVVISI ESITI DI GARA" in response.text:
            print("✅ Found 'AVVISI ESITI DI GARA'")
            
            # Conta i link nella sezione
            html_lines = response.text.split("\n")
            
            # Trova dove inizia AVVISI ESITI DI GARA
            for i, line in enumerate(html_lines):
                if "AVVISI ESITI DI GARA" in line:
                    print(f"📍 Found at line {i}")
                    # Mostra le prossime 10 righe
                    print("\n📄 Next 10 lines after 'AVVISI ESITI DI GARA':")
                    for j in range(i, min(i+10, len(html_lines))):
                        print(f"  {j}: {html_lines[j][:100]}...")
                    break
                    
            # Conta i link
            soup = BeautifulSoup(response.text, 'html.parser')
            links = soup.find_all('a')
            print(f"\n🔗 Total links found: {len(links)}")
            
        else:
            print("⚠️ 'AVVISI ESITI DI GARA' NOT found")
            
            # Mostra cosa c'è nella pagina
            soup = BeautifulSoup(response.text, 'html.parser')
            rubriche = soup.find_all('span', class_='rubrica')
            print(f"\n📚 Rubriche trovate: {len(rubriche)}")
            for r in rubriche[:5]:
                print(f"  - {r.get_text(strip=True)}")
                
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_basic()