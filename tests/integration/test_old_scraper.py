#!/usr/bin/env python
"""
Test del vecchio scraper Gazzetta per verificare se funziona.
"""

import requests
from bs4 import BeautifulSoup
import re

def filtra_testo(testo):
    """Funzione di filtraggio dal vecchio scraper."""
    if re.search(r"illuminazion(?:e|i)?", testo, re.IGNORECASE):
        return testo
    elif re.search(r"videosorveglianz(?:a|e)?", testo, re.IGNORECASE):
        return testo
    elif (re.search(r"galleri(?:a|e|i)|tunnel(?:i)?", testo, re.IGNORECASE) and
          re.search(r"impiant(?:o|i|istica|iche)", testo, re.IGNORECASE) and
          not re.search(r"muse(?:o|ale|i|ali)?", testo, re.IGNORECASE) and
          not re.search(r"via Galler(?:ia|ie)?", testo, re.IGNORECASE) and
          not re.search(r"MINISTERO DELLA CULTURA", testo, re.IGNORECASE) and
          not re.search(r"ferrovia(?:r|ria|rio|rie)", testo, re.IGNORECASE)):
        return testo
    elif re.search(r"edific(?:io|i|ia|azione|azioni)|termic(?:o|i|a|he)", testo, re.IGNORECASE):
        return testo
    elif ((re.search(r"colonnin(?:a|e|i)", testo, re.IGNORECASE) and 
           re.search(r"elettr(?:ico|ici|ica|iche)", testo, re.IGNORECASE)) or
          (re.search(r"ricaric(?:a|he|he)", testo, re.IGNORECASE) and 
           re.search(r"elettr(?:ico|ici|ica|iche)", testo, re.IGNORECASE))):
        return testo
    elif (re.search(r"parchegg(?:io|i|e)", testo, re.IGNORECASE) and
          re.search(r"gestion(?:e|i)|parcom(?:etro|etri)|parchim(?:etro|etri)", testo, re.IGNORECASE)):
        return testo
    return None

def test_old_scraper():
    """Test della logica del vecchio scraper."""
    
    # Test URL del 2024
    url = "http://www.gazzettaufficiale.it/gazzetta/contratti/caricaDettaglio?dataPubblicazioneGazzetta=2024-01-05&numeroGazzetta=1"
    
    print(f"🧪 Testing old scraper logic on: {url}\n")
    
    try:
        # Download pagina
        print("📥 Downloading page...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        print(f"✅ Status: {response.status_code}")
        print(f"📏 Content length: {len(response.text)} bytes\n")
        
        # Metodo del vecchio scraper
        html_strs = response.text.split("\n")
        print(f"📄 HTML lines: {len(html_strs)}")
        
        # Trova le righe dove iniziano i nodi 'span'
        span_start_indices = [i for i, line in enumerate(html_strs) if '<span class="rubrica">' in line]
        print(f"🔍 Found {len(span_start_indices)} span.rubrica elements")
        
        if span_start_indices:
            print("\n📚 Span.rubrica content:")
            for idx in span_start_indices[:5]:
                content = html_strs[idx][:100]
                print(f"  Line {idx}: {content}...")
        
        # Trova l'indice dove inizia "AVVISI ESITI DI GARA"
        start_index = next((i for i in span_start_indices if "AVVISI ESITI DI GARA" in html_strs[i]), None)
        
        if start_index:
            print(f"\n✅ Found 'AVVISI ESITI DI GARA' at line {start_index}")
            
            # Trova l'indice dove inizia il prossimo nodo 'span'
            end_index = next((i for i in span_start_indices if i > start_index), None)
            
            if end_index:
                print(f"📍 Next span at line {end_index}")
                print(f"📊 Section size: {end_index - start_index} lines")
            else:
                print("📍 No next span found, taking to end of document")
                end_index = len(html_strs)
            
            # Estrai le righe di interesse
            relevant_html_strs = html_strs[start_index:end_index]
            relevant_html = "\n".join(relevant_html_strs)
            
            # Parse con BeautifulSoup
            relevant_soup = BeautifulSoup(relevant_html, 'html.parser')
            
            # Estrai i link
            link_nodes = relevant_soup.select("a")
            links = [link['href'] for link in link_nodes if link.get('href')]
            
            print(f"\n🔗 Links found in section: {len(links)}")
            if links:
                print("\n📎 First 5 links:")
                for link in links[:5]:
                    print(f"  - {link[:80]}...")
                    
                # Test download di un link
                if links:
                    test_link = links[0]
                    test_url = f"http://www.gazzettaufficiale.it{test_link}"
                    print(f"\n🧪 Testing detail page: {test_url[:80]}...")
                    
                    try:
                        detail_response = requests.get(test_url, timeout=10)
                        detail_soup = BeautifulSoup(detail_response.text, 'html.parser')
                        testo = detail_soup.get_text()
                        
                        print(f"✅ Detail page downloaded: {len(testo)} chars")
                        
                        # Test filtro
                        testo_filtrato = filtra_testo(testo)
                        if testo_filtrato:
                            print(f"✅ Text passed filter")
                            print(f"\n📝 Preview (first 300 chars):")
                            print(testo_filtrato[:300])
                        else:
                            print("❌ Text did not pass filter")
                            print("\n📝 Raw preview (first 300 chars):")
                            print(testo[:300])
                            
                    except Exception as e:
                        print(f"❌ Error downloading detail: {e}")
            else:
                print("⚠️ No links found in AVVISI ESITI DI GARA section")
        else:
            print("\n❌ 'AVVISI ESITI DI GARA' NOT found")
            
            # Cerca pattern alternativi
            print("\n🔍 Searching for alternative patterns...")
            patterns = ["AVVISI", "ESITI", "GARA", "BANDI", "CONTRATTI"]
            for pattern in patterns:
                found_lines = [i for i, line in enumerate(html_strs) if pattern in line.upper()]
                if found_lines:
                    print(f"  ✅ '{pattern}' found in {len(found_lines)} lines")
                    print(f"     First occurrence at line {found_lines[0]}: {html_strs[found_lines[0]][:80]}...")
                else:
                    print(f"  ❌ '{pattern}' not found")
                    
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_old_scraper()