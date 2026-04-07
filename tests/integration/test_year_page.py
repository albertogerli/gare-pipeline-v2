#!/usr/bin/env python
"""
Test della pagina annuale della Gazzetta.
"""

import requests
from bs4 import BeautifulSoup

def test_year_page():
    """Test della pagina con lista delle gazzette dell'anno."""
    
    # URL della pagina annuale (come nel vecchio scraper)
    anno = 2024
    url = f"https://www.gazzettaufficiale.it/ricercaArchivioCompleto/contratti/{anno}?anno={anno}"
    
    print(f"🧪 Testing year page: {url}\n")
    
    try:
        print("📥 Downloading year page...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        print(f"✅ Status: {response.status_code}")
        print(f"📏 Content length: {len(response.text)} bytes\n")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Cerca i link alle gazzette (come nel vecchio scraper)
        link_nodes = soup.select(".elenco_pdf a")
        print(f"📚 Found {len(link_nodes)} gazette links\n")
        
        if link_nodes:
            # Mostra i primi 5 link
            print("📎 First 5 gazette links:")
            for idx, link in enumerate(link_nodes[:5]):
                href = link.get('href', '')
                text = link.get_text(strip=True).replace('\xa0', ' ')
                print(f"  {idx+1}. {text}")
                print(f"     URL: {href[:80]}...")
            
            # Test del primo link
            if link_nodes:
                first_link = link_nodes[0]
                first_href = first_link.get('href', '')
                first_url = f"http://www.gazzettaufficiale.it{first_href}"
                
                print(f"\n🧪 Testing first gazette: {first_url[:100]}...")
                
                detail_response = requests.get(first_url, timeout=30)
                print(f"✅ Detail status: {detail_response.status_code}")
                print(f"📏 Detail content: {len(detail_response.text)} bytes")
                
                # Cerca AVVISI ESITI DI GARA
                if "AVVISI ESITI DI GARA" in detail_response.text:
                    print("✅ Found 'AVVISI ESITI DI GARA' in detail page!")
                    
                    # Conta i link nella sezione
                    detail_soup = BeautifulSoup(detail_response.text, 'html.parser')
                    
                    # Metodo del vecchio scraper
                    html_strs = detail_response.text.split("\n")
                    span_start_indices = [i for i, line in enumerate(html_strs) if '<span class="rubrica">' in line]
                    
                    print(f"\n📊 Span.rubrica elements: {len(span_start_indices)}")
                    
                    # Mostra le rubriche trovate
                    if span_start_indices:
                        print("\n📚 Rubriche trovate:")
                        for idx in span_start_indices[:10]:
                            line = html_strs[idx]
                            # Estrai il testo dalla rubrica
                            rubrica_soup = BeautifulSoup(line, 'html.parser')
                            rubrica_text = rubrica_soup.get_text(strip=True)
                            if rubrica_text:
                                print(f"  - {rubrica_text}")
                                
                else:
                    print("❌ 'AVVISI ESITI DI GARA' NOT found in detail page")
                    
                    # Mostra cosa c'è nella pagina
                    detail_soup = BeautifulSoup(detail_response.text, 'html.parser')
                    
                    # Cerca tutte le sezioni
                    spans = detail_soup.find_all('span', class_='rubrica')
                    if spans:
                        print(f"\n📚 Found {len(spans)} rubriche:")
                        for span in spans[:10]:
                            print(f"  - {span.get_text(strip=True)}")
                    else:
                        # Cerca h2, h3, div con class specifiche
                        headers = detail_soup.find_all(['h2', 'h3'])
                        if headers:
                            print(f"\n📄 Found {len(headers)} headers:")
                            for h in headers[:10]:
                                print(f"  - {h.get_text(strip=True)}")
                                
        else:
            print("❌ No gazette links found")
            
            # Debug: mostra struttura della pagina
            print("\n🔍 Page structure debug:")
            
            # Cerca div con classi
            divs = soup.find_all('div', class_=True)
            print(f"Divs with classes: {len(divs)}")
            for div in divs[:5]:
                classes = ' '.join(div.get('class', []))
                print(f"  - class='{classes}'")
                
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_year_page()