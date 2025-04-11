import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import re
import sys
import logging
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import RequestException, Timeout, ConnectionError
import config

class GazzettaScraper:
    """
    Classe per lo scraping dei dati dalla Gazzetta Ufficiale relativi a gare d'appalto.
    Estrae e filtra i testi relativi a illuminazione, videosorveglianza, gallerie, edifici,
    colonnine elettriche e parcheggi.
    """
    
    @staticmethod
    def filtra_testo(testo):
        """
        Filtra il testo in base a criteri specifici relativi a illuminazione, videosorveglianza, ecc.
        
        Args:
            testo (str): Il testo da filtrare
            
        Returns:
            str or None: Il testo se corrisponde ai criteri, altrimenti None
        """
        if not testo:
            return None
            
        # Ottimizzazione: compila le espressioni regolari una sola volta
        patterns = {
            "illuminazione": re.compile(r"illuminazion(?:e|i)?", re.IGNORECASE),
            "videosorveglianza": re.compile(r"videosorveglianz(?:a|e)?", re.IGNORECASE),
            "galleria": re.compile(r"galleri(?:a|e|i)|tunnel(?:i)?", re.IGNORECASE),
            "impianti": re.compile(r"impiant(?:o|i|istica|iche)", re.IGNORECASE),
            "museo": re.compile(r"muse(?:o|ale|i|ali)?", re.IGNORECASE),
            "via_galleria": re.compile(r"via Galler(?:ia|ie)?", re.IGNORECASE),
            "ministero_cultura": re.compile(r"MINISTERO DELLA CULTURA", re.IGNORECASE),
            "ferrovia": re.compile(r"ferrovia(?:r|ria|rio|rie)", re.IGNORECASE),
            "edifici": re.compile(r"edific(?:io|i|ia|azione|azioni)|termic(?:o|i|a|he)", re.IGNORECASE),
            "colonnine": re.compile(r"colonnin(?:a|e|i)", re.IGNORECASE),
            "elettrico": re.compile(r"elettr(?:ico|ici|ica|iche)", re.IGNORECASE),
            "ricarica": re.compile(r"ricaric(?:a|he|he)", re.IGNORECASE),
            "parcheggio": re.compile(r"parchegg(?:io|i|e)", re.IGNORECASE),
            "gestione": re.compile(r"gestion(?:e|i)|parcom(?:etro|etri)|parchim(?:etro|etri)", re.IGNORECASE)
        }
        
        # Verifica i criteri di filtro
        if patterns["illuminazione"].search(testo):
            return testo
        elif patterns["videosorveglianza"].search(testo):
            return testo
        elif (patterns["galleria"].search(testo) and 
              patterns["impianti"].search(testo) and
              not patterns["museo"].search(testo) and
              not patterns["via_galleria"].search(testo) and
              not patterns["ministero_cultura"].search(testo) and
              not patterns["ferrovia"].search(testo)):
            return testo
        elif patterns["edifici"].search(testo):
            return testo
        elif ((patterns["colonnine"].search(testo) and patterns["elettrico"].search(testo)) or
              (patterns["ricarica"].search(testo) and patterns["elettrico"].search(testo))):
            return testo
        elif (patterns["parcheggio"].search(testo) and patterns["gestione"].search(testo)):
            return testo
        
        return None

    @staticmethod
    def crea_sessione():
        """
        Crea una sessione HTTP con gestione dei tentativi di riconnessione.
        
        Returns:
            requests.Session: Sessione HTTP configurata
        """
        session = requests.Session()
        retries = Retry(
            total=config.RETRY_ATTEMPTS,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"]  # Metodi consentiti per i tentativi
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=100, pool_maxsize=100)
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        return session

    @staticmethod
    def read_html_with_timeout(url, timeout_sec=None, max_retries=None):
        """
        Legge il contenuto HTML di una pagina web con timeout configurabile e gestione dei tentativi.
        
        Args:
            url (str): URL della pagina da leggere
            timeout_sec (int, optional): Timeout in secondi. Se None, usa il valore da config.
            max_retries (int, optional): Numero massimo di tentativi. Se None, usa il valore da config.
            
        Returns:
            str: Contenuto HTML della pagina
            
        Raises:
            RequestException: In caso di errori nella richiesta HTTP dopo tutti i tentativi
        """
        logger = logging.getLogger("Gare")
        if timeout_sec is None:
            timeout_sec = config.TIMEOUT_DOWNLOAD
            
        if max_retries is None:
            max_retries = config.RETRY_ATTEMPTS
            
        retry_delay = config.RETRY_DELAY
        
        for attempt in range(max_retries):
            try:
                logger.debug(f"Scaricamento pagina: {url} (tentativo {attempt+1}/{max_retries})")
                response = requests.get(url, timeout=timeout_sec)
                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)  # Backoff esponenziale
                    logger.warning(f"Errore nella richiesta: {e}. Nuovo tentativo tra {wait_time} secondi...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Errore nella richiesta dopo {max_retries} tentativi: {e}")
                    raise

    @staticmethod
    def scarica_testo_async(link, url_base, session):
        """
        Scarica e filtra il testo di una pagina di dettaglio in modo asincrono.
        
        Args:
            link (str): Link relativo alla pagina di dettaglio
            url_base (str): URL base da preporre al link
            session (requests.Session): Sessione HTTP da utilizzare
            
        Returns:
            str or None: Testo filtrato se corrisponde ai criteri, altrimenti None
            
        Raises:
            RequestException: In caso di errori nella richiesta HTTP
            Timeout: In caso di timeout nella richiesta
        """
        logger = logging.getLogger("Gare")
        url_completo_dettaglio = f"{url_base}{link}"
        
        for attempt in range(config.RETRY_ATTEMPTS):
            try:
                response = session.get(url_completo_dettaglio, timeout=10)
                response.raise_for_status()
                soup_dettagliata = BeautifulSoup(response.text, 'html.parser')
                testo = soup_dettagliata.get_text()
                return GazzettaScraper.filtra_testo(testo)
            except (RequestException, Timeout) as e:
                if attempt < config.RETRY_ATTEMPTS - 1:
                    wait_time = config.RETRY_DELAY * (2 ** attempt)  # Backoff esponenziale
                    logger.warning(f"Errore nella richiesta a {url_completo_dettaglio}: {e}. Nuovo tentativo tra {wait_time} secondi...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Errore nella richiesta a {url_completo_dettaglio} dopo {config.RETRY_ATTEMPTS} tentativi: {e}")
                    raise

    @staticmethod
    def processa_links(url_risultati):
        """
        Processa una lista di link per estrarre e filtrare i testi.
        
        Args:
            url_risultati (list): Lista di link relativi da processare
            
        Returns:
            list: Lista di testi filtrati
        """
        logger = logging.getLogger("Gare")
        url_base = "http://www.gazzettaufficiale.it"
        testo_dettagliato = []
        sessione = GazzettaScraper.crea_sessione()
        
        # Calcola il numero ottimale di worker in base al numero di link
        max_workers = min(100, max(1, len(url_risultati)))
        if len(url_risultati) == 0:
            logger.warning("Nessun link da processare")
            return []
            
        logger.info(f"Elaborazione di {len(url_risultati)} link con {max_workers} worker")
        
        # Implementazione con gestione degli errori migliorata
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Crea un dizionario di future per tenere traccia dei link
            futures = {}
            for link in url_risultati:
                future = executor.submit(GazzettaScraper.scarica_testo_async, link, url_base, sessione)
                futures[future] = link
            
            # Processa i risultati man mano che sono disponibili
            completed = 0
            errors = 0
            for future in as_completed(futures):
                link = futures[future]
                try:
                    testo_filtrato = future.result()
                    if testo_filtrato:
                        testo_dettagliato.append(testo_filtrato)
                    completed += 1
                    
                    # Log di avanzamento ogni 10 link completati o alla fine
                    if completed % 10 == 0 or completed == len(url_risultati):
                        logger.info(f"Progresso: {completed}/{len(url_risultati)} link elaborati ({errors} errori)")
                        
                except (RequestException, Timeout) as e:
                    errors += 1
                    logger.error(f"Errore nell'elaborazione del link {link}: {e}")
                    # Continuiamo con gli altri link invece di terminare completamente
                    continue

        logger.info(f"Elaborazione completata: trovati {len(testo_dettagliato)} testi rilevanti su {len(url_risultati)} link ({errors} errori)")
        return testo_dettagliato

    @staticmethod
    def estrai_dati_da_url(url):
        """
        Estrae i dati rilevanti da un URL della Gazzetta Ufficiale.
        
        Args:
            url (str): URL della pagina da cui estrarre i dati
            
        Returns:
            list: Lista di testi filtrati
        """
        logger = logging.getLogger("Gare")
        logger.info(f"Estrazione dati da: {url}")
        
        try:
            pagina = GazzettaScraper.read_html_with_timeout(url)
            if not pagina:
                logger.warning(f"Nessun contenuto trovato in: {url}")
                return []
                
            # Converti l'HTML in una lista di stringhe
            html_strs = pagina.split("\n")
            
            # Trova le righe dove iniziano i nodi 'span'
            span_start_indices = [i for i, line in enumerate(html_strs) if '<span class="rubrica">' in line]
            
            if not span_start_indices:
                logger.warning(f"Nessun nodo 'span' trovato in {url}")
                return []
                
            # Trova l'indice dove inizia "AVVISI ESITI DI GARA"
            start_index = next((i for i in span_start_indices if "AVVISI ESITI DI GARA" in html_strs[i]), None)
            
            if start_index is None:
                logger.warning(f"Nessun 'AVVISI ESITI DI GARA' trovato in {url}")
                return []
            
            # Trova l'indice dove inizia il prossimo nodo 'span'
            end_index = next((i for i in span_start_indices if i > start_index), None)
            
            if end_index is None:
                # Se non c'è un nodo span successivo, usa la fine del file
                logger.warning(f"Nessun nodo 'span' successivo trovato in {url}, usando la fine del file")
                end_index = len(html_strs)
            
            # Estrai le righe di interesse
            relevant_html_strs = html_strs[start_index:end_index]
            
            # Combina le stringhe in un unico pezzo di HTML
            relevant_html = "\n".join(relevant_html_strs)
            relevant_soup = BeautifulSoup(relevant_html, 'html.parser')
            
            # Estrai i link
            link_nodes = relevant_soup.select("a")
            url_risultati = [link['href'] for link in link_nodes if link.get('href')]
            
            logger.info(f"Trovati {len(url_risultati)} link da processare")
            
            # Estrai il testo dettagliato per ogni URL trovato
            return GazzettaScraper.processa_links(url_risultati)
            
        except Exception as e:
            logger.error(f"Errore durante l'estrazione dei dati da {url}: {str(e)}")
            return []

    @staticmethod
    def run():
        """
        Esegue lo scraping della Gazzetta Ufficiale per gli anni configurati.
        """
        logger = logging.getLogger("Gare")
        logger.info("Avvio scraping della Gazzetta Ufficiale")
        
        # Assicurati che le directory necessarie esistano
        if not os.path.exists(config.TEMP_DIR):
            os.makedirs(config.TEMP_DIR)
            
        # Contatori per le statistiche
        totale_testi = 0
        totale_link_processati = 0
        totale_errori = 0
        
        # Timestamp di inizio per calcolare il tempo totale
        start_time = time.time()
            
        for anno in range(config.ANNO_INIZIO, config.ANNO_FINE + 1):
            anno_start_time = time.time()
            logger.info(f"Elaborazione anno {anno}")

            folder_path = os.path.join(os.getcwd(), config.TEMP_DIR, str(anno))
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)

            # URL della pagina per l'anno corrente
            url0 = f"https://www.gazzettaufficiale.it/ricercaArchivioCompleto/contratti/{anno}?anno={anno}"
            
            try:
                # Leggi la pagina HTML principale per l'anno
                pagina0 = GazzettaScraper.read_html_with_timeout(url0)
                if not pagina0:
                    logger.warning(f"Nessun contenuto trovato per l'anno {anno}")
                    continue  # Salta l'anno se la pagina non viene caricata

                soup0 = BeautifulSoup(pagina0, 'html.parser')
                
                # Estrai i nodi che contengono i link all'interno del div
                link_nodes0 = soup0.select(".elenco_pdf a")

                links = [
                    {
                        'indice': idx + 1, 
                        'url_parziale': link['href'], 
                        'testo': ' '.join(link.get_text(strip=True).split()).replace('\xa0', '')
                    } 
                    for idx, link in enumerate(link_nodes0) if link.get('href')
                ]
                
                if not links:
                    logger.warning(f"Nessun link trovato per l'anno {anno}")
                    continue
                    
                logger.info(f"Trovati {len(links)} link per l'anno {anno}")
                
                # Inizializza una lista vuota per conservare tutti i testi dell'anno
                testo_completo_annuale = []
                link_processati_anno = 0
                errori_anno = 0
                
                # Filtra i link usando l'indice di ripresa configurato
                indice_ripresa = getattr(config, 'RIPRENDI_DA_INDICE', 1)
                if anno == config.ANNO_INIZIO and hasattr(config, 'RIPRENDI_DA_INDICE'):
                    links = [link for link in links if int(link['indice']) >= indice_ripresa]
                    logger.info(f"Ripresa dello scraping dell'anno {anno} dal link {indice_ripresa} - Rimangono {len(links)} link da processare")
                
                # Itera su ciascun URL nella lista links
                for curr_link in links:
                    url_indice = int(curr_link['indice'])
                    url_parziale = curr_link['url_parziale']
                    url_testo = curr_link['testo'].replace('\xa0', '')
                    url_completo = f"http://www.gazzettaufficiale.it{url_parziale}"

                    excel_file_path = os.path.join(os.getcwd(), config.TEMP_DIR, str(anno), f"{url_testo}.xlsx")
                    
                    # Verifica se il file Excel esiste già
                    if not os.path.exists(excel_file_path):
                        logger.info(f"Elaborazione link {url_testo} - {url_indice} di {len(links)} - anno {anno}")
                            
                        try:
                            testi_estratti = GazzettaScraper.estrai_dati_da_url(url_completo)
                            link_processati_anno += 1
                            totale_link_processati += 1
                            
                            # Crea un DataFrame e salva in Excel
                            if testi_estratti:
                                df = pd.DataFrame(testi_estratti, columns=["testo"])
                                df.to_excel(excel_file_path, index=False)
                                logger.info(f"Salvati {len(testi_estratti)} testi in {excel_file_path}")
                                testo_completo_annuale.extend(testi_estratti)
                            else:
                                logger.warning(f"Nessun testo rilevante trovato in {url_completo}")
                                df = pd.DataFrame(columns=["testo"])
                                df.to_excel(excel_file_path, index=False)
                                
                        except Exception as e:
                            logger.error(f"Errore durante l'elaborazione di {url_completo}: {str(e)}")
                            errori_anno += 1
                            totale_errori += 1
                            # Continuiamo con il prossimo link invece di terminare completamente
                            continue
                    else:
                        logger.info(f"File già esistente: {excel_file_path}")
                        
                        # Carica i dati dal file Excel esistente
                        try:
                            df_from_excel = pd.read_excel(excel_file_path)
                            testi_estratti = df_from_excel['testo'].tolist() if 'testo' in df_from_excel.columns else []
                            testo_completo_annuale.extend(testi_estratti)
                            logger.info(f"Caricati {len(testi_estratti)} testi da {excel_file_path}")
                        except Exception as e:
                            logger.error(f"Errore durante la lettura di {excel_file_path}: {str(e)}")
                            errori_anno += 1
                            totale_errori += 1
                            continue
            
                # Salvataggio del file per l'anno corrente
                if testo_completo_annuale:
                    df_anno = pd.DataFrame({'testo': testo_completo_annuale})
                    excel_file_name = os.path.join(os.getcwd(), config.TEMP_DIR, f"parziale_{anno}.xlsx")
                    df_anno.to_excel(excel_file_name, index=False)
                    logger.info(f"Salvataggio annuale {anno} effettuato in {excel_file_name} con {len(testo_completo_annuale)} testi")
                    totale_testi += len(testo_completo_annuale)
                else:
                    logger.warning(f"Nessun testo trovato per l'anno {anno}")
                
                # Calcola e registra il tempo impiegato per l'anno
                anno_elapsed_time = time.time() - anno_start_time
                logger.info(f"Anno {anno} completato in {anno_elapsed_time:.2f} secondi. "
                           f"Link processati: {link_processati_anno}, Errori: {errori_anno}, "
                           f"Testi trovati: {len(testo_completo_annuale)}")
                    
            except Exception as e:
                logger.error(f"Errore durante l'elaborazione dell'anno {anno}: {str(e)}")
                totale_errori += 1
                # Continuiamo con il prossimo anno invece di terminare completamente
                continue

        # Combina tutti i file degli anni in un unico Excel finale
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
                        totale_errori += 1
                        continue
                
                if dfs:
                    df_finale = pd.concat(dfs, ignore_index=True).drop_duplicates()
                    final_file_name = os.path.join(os.getcwd(), config.TEMP_DIR, config.LOTTI_RAW)
                    df_finale.to_excel(final_file_name, index=False)
                    logger.info(f"Salvataggio finale effettuato: {final_file_name} con {len(df_finale)} record")
                else:
                    logger.warning("Nessun dato da combinare")
            else:
                logger.warning("Nessun file annuale trovato")
                
        except Exception as e:
            logger.error(f"Errore durante la combinazione dei file annuali: {str(e)}")
            totale_errori += 1
            
        # Calcola e registra il tempo totale e le statistiche
        total_elapsed_time = time.time() - start_time
        logger.info(f"Scraping della Gazzetta Ufficiale completato in {total_elapsed_time:.2f} secondi")
        logger.info(f"Statistiche finali: Link processati: {totale_link_processati}, "
                   f"Testi trovati: {totale_testi}, Errori: {totale_errori}")
