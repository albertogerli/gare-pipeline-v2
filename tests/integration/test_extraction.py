#!/usr/bin/env python
"""
Test estrazione completa da una gazzetta con AVVISI ESITI DI GARA.
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

def test_extraction():
    """Test estrazione dalla prima gazzetta del 2024."""
    
    # URL corretto della prima gazzetta 2024
    url = "http://www.gazzettaufficiale.it/gazzetta/contratti/caricaDettaglio?dataPubblicazioneGazzetta=2024-01-03&numeroGazzetta=1"
    
    print(f"🧪 Testing extraction from: {url}\n")
    
    try:
        print("📥 Downloading gazette page...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        print(f"✅ Downloaded: {len(response.text)} bytes\n")
        
        # Metodo del vecchio scraper
        html_strs = response.text.split("\n")
        
        # Trova le righe dove iniziano i nodi 'span'
        span_start_indices = [i for i, line in enumerate(html_strs) if '<span class="rubrica">' in line]
        
        # Trova l'indice dove inizia "AVVISI ESITI DI GARA"
        start_index = next((i for i in span_start_indices if "AVVISI ESITI DI GARA" in html_strs[i]), None)
        
        if start_index:
            print(f"✅ Found 'AVVISI ESITI DI GARA' at line {start_index}")
            
            # Trova l'indice dove inizia il prossimo nodo 'span'
            end_index = next((i for i in span_start_indices if i > start_index), None)
            
            if end_index is None:
                end_index = len(html_strs)
                print(f"📍 Section ends at: end of document (line {end_index})")
            else:
                print(f"📍 Section ends at: line {end_index}")
            
            print(f"📊 Section size: {end_index - start_index} lines\n")
            
            # Estrai le righe di interesse
            relevant_html_strs = html_strs[start_index:end_index]
            relevant_html = "\n".join(relevant_html_strs)
            
            # Parse con BeautifulSoup
            relevant_soup = BeautifulSoup(relevant_html, 'html.parser')
            
            # Estrai i link
            link_nodes = relevant_soup.select("a")
            links = [link['href'] for link in link_nodes if link.get('href')]
            
            print(f"🔗 Found {len(links)} links in AVVISI ESITI DI GARA section\n")
            
            if links:
                print("📎 Testing first 3 links:")
                
                filtered_count = 0
                unfiltered_count = 0
                
                for idx, link in enumerate(links[:3]):
                    test_url = f"http://www.gazzettaufficiale.it{link}"
                    print(f"\n  {idx+1}. Testing: {test_url[:80]}...")
                    
                    try:
                        detail_response = requests.get(test_url, timeout=10)
                        detail_soup = BeautifulSoup(detail_response.text, 'html.parser')
                        testo = detail_soup.get_text()
                        
                        print(f"     ✅ Downloaded: {len(testo)} chars")
                        
                        # Test filtro
                        testo_filtrato = filtra_testo(testo)
                        if testo_filtrato:
                            filtered_count += 1
                            print(f"     ✅ PASSED filter")
                            print(f"     📝 Preview: {testo_filtrato[:150]}...")
                        else:
                            unfiltered_count += 1
                            print(f"     ❌ FAILED filter")
                            # Mostra perché non passa il filtro
                            keywords = ["illuminazion", "videosorveglianz", "galleri", "tunnel", 
                                      "edific", "termic", "colonnin", "elettr", "parchegg"]
                            found_any = False
                            for kw in keywords:
                                if re.search(kw, testo, re.IGNORECASE):
                                    print(f"        Found '{kw}' but didn't match full criteria")
                                    found_any = True
                                    break
                            if not found_any:
                                print(f"        No relevant keywords found")
                                # Mostra un po' di testo per debug
                                clean_text = ' '.join(testo.split()[:30])
                                print(f"        Text start: {clean_text}...")
                            
                    except Exception as e:
                        print(f"     ❌ Error: {e}")
                
                print(f"\n📊 Summary of first 3 links:")
                print(f"   ✅ Passed filter: {filtered_count}")
                print(f"   ❌ Failed filter: {unfiltered_count}")
                
                # Test tutti i link per avere statistiche complete
                print(f"\n🔄 Processing all {len(links)} links...")
                total_filtered = 0
                total_errors = 0
                
                for link in links:
                    try:
                        test_url = f"http://www.gazzettaufficiale.it{link}"
                        detail_response = requests.get(test_url, timeout=5)
                        detail_soup = BeautifulSoup(detail_response.text, 'html.parser')
                        testo = detail_soup.get_text()
                        if filtra_testo(testo):
                            total_filtered += 1
                    except:
                        total_errors += 1
                
                print(f"\n📊 Final statistics for all links:")
                print(f"   Total links: {len(links)}")
                print(f"   Passed filter: {total_filtered}")
                print(f"   Failed filter: {len(links) - total_filtered - total_errors}")
                print(f"   Errors: {total_errors}")
                
        else:
            print("❌ 'AVVISI ESITI DI GARA' section not found")
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_extraction()