#!/usr/bin/env python
"""
Debug della struttura HTML della Gazzetta.
"""

import requests
from bs4 import BeautifulSoup

def debug_html():
    """Debug della struttura HTML."""
    url = "http://www.gazzettaufficiale.it/gazzetta/contratti/caricaDettaglio?dataPubblicazioneGazzetta=2024-01-05&numeroGazzetta=1"
    
    print(f"📥 Downloading: {url}")
    
    response = requests.get(url, timeout=30)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Salva il contenuto per analisi
    with open("debug_gazzetta.html", "w", encoding="utf-8") as f:
        f.write(response.text)
    print("💾 Saved to debug_gazzetta.html")
    
    # Mostra la struttura
    print("\n📋 Page title:", soup.title.string if soup.title else "No title")
    
    # Cerca tutti i div principali
    divs = soup.find_all('div', class_=True)
    print(f"\n📦 Main divs with classes: {len(divs)}")
    for div in divs[:10]:
        classes = ' '.join(div.get('class', []))
        text_preview = div.get_text(strip=True)[:50]
        print(f"  - class='{classes}': {text_preview}...")
    
    # Cerca tutti i link
    links = soup.find_all('a', href=True)
    print(f"\n🔗 Links found: {len(links)}")
    for link in links[:5]:
        href = link.get('href', '')
        text = link.get_text(strip=True)[:30]
        print(f"  - {text}: {href[:50]}...")
    
    # Cerca testo con pattern specifici
    text_content = soup.get_text()
    keywords = ["AVVISI", "ESITI", "GARA", "BANDI", "CONTRATTI", "avvisi", "esiti", "gara"]
    print("\n🔍 Keywords found:")
    for kw in keywords:
        if kw in text_content:
            print(f"  ✅ '{kw}' found")
        else:
            print(f"  ❌ '{kw}' NOT found")

if __name__ == "__main__":
    debug_html()