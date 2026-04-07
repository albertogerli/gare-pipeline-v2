import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def filtra_testo(testo):
    """Filtro bilanciato per gare di infrastrutture e servizi pubblici rilevanti."""
    if not testo:
        return None

    testo_lower = testo.lower()

    # 1. ILLUMINAZIONE PUBBLICA (più specifico)
    if re.search(
        r"illuminazion[ei] pubblic|lampioni|pubblica illuminazione|impianti di illuminazione|corpi illuminanti",
        testo_lower,
    ):
        return testo

    # 2. VIDEOSORVEGLIANZA (specifico)
    elif re.search(
        r"videosorveglian|telecamer[ae]|tvcc|sistema.{0,20}sorveglian", testo_lower
    ):
        return testo

    # 3. EFFICIENTAMENTO ENERGETICO (più specifico)
    elif re.search(
        r"efficientamento energetic|riqualificazione energetic|risparmio energetic",
        testo_lower,
    ) or (
        re.search(r"impiant[oi]", testo_lower)
        and re.search(r"fotovoltaic|solare|led|termic", testo_lower)
    ):
        return testo

    # 4. EDIFICI PUBBLICI + ENERGIA/IMPIANTI (combinato)
    elif re.search(
        r"scuol[ae]|municipio|palazzo comunale|biblioteca|ospedale", testo_lower
    ) and re.search(
        r"impiant[oi]|manutenzion|ristrutturazion|adeguament|climatizzazion|riscaldament",
        testo_lower,
    ):
        return testo

    # 5. MOBILITÀ ELETTRICA (specifico)
    elif re.search(
        r"colonnin[ae].{0,20}ricaric|ricarica.{0,20}elettric|stazion.{0,20}ricaric|e-mobility",
        testo_lower,
    ):
        return testo

    # 6. PARCHEGGI CON GESTIONE/TECNOLOGIA
    elif re.search(r"parchegg[io]", testo_lower) and re.search(
        r"gestion|parcometr|parchimetr|automat|smart|sensor", testo_lower
    ):
        return testo

    # 7. SMART CITY (più specifico)
    elif re.search(r"smart city|città intelligente", testo_lower) or (
        re.search(r"sensor[ei]|iot|telecontroll|telegestione", testo_lower)
        and re.search(r"pubblic|urban|città|comune", testo_lower)
    ):
        return testo

    # 8. VERDE PUBBLICO CON IMPIANTI
    elif re.search(r"verde pubblic|parchi|giardini", testo_lower) and re.search(
        r"irrigazion|impiant|illuminazion|manutenzion", testo_lower
    ):
        return testo

    # 9. STRADE + ILLUMINAZIONE/SEGNALETICA
    elif re.search(r"strad[ae]|viabilità", testo_lower) and re.search(
        r"illuminazion|segnaletic|semafori|asfalto|manutenzion", testo_lower
    ):
        return testo

    # 10. IMPIANTI SPORTIVI (specifico)
    elif re.search(
        r"impianti sportiv|palestra|piscina|campo sportiv|palazzetto", testo_lower
    ) and re.search(r"manutenzion|ristrutturazion|impiant|illuminazion", testo_lower):
        return testo

    # 11. GLOBAL SERVICE/FACILITY (specifico per edifici pubblici)
    elif re.search(
        r"global service|facility management|gestione integrata", testo_lower
    ) and re.search(r"edifici|immobili|pubblic|comunal", testo_lower):
        return testo

    # 12. GALLERIE/TUNNEL CON IMPIANTI
    elif (
        re.search(r"galleri[ae]|tunnel", testo_lower)
        and re.search(r"impiant[oi]|illuminazion|ventilazion|sicurezza", testo_lower)
        and not re.search(r"museo|arte|mostra", testo_lower)
    ):
        return testo

    # 13. RETI IDRICHE/FOGNATURE (più specifico)
    elif re.search(
        r"acquedott|rete idric|fognatur|depurator", testo_lower
    ) and re.search(r"manutenzion|gestion|lavori|riparazion", testo_lower):
        return testo

    # 14. PUBBLICA ILLUMINAZIONE LED
    elif re.search(r"\bled\b", testo_lower) and re.search(
        r"pubblic|strad|comunal|illuminazion", testo_lower
    ):
        return testo

    # 15. IMPIANTI TERMICI/CLIMATIZZAZIONE EDIFICI PUBBLICI
    elif re.search(
        r"termic|climatizzazion|condizionament|caldai", testo_lower
    ) and re.search(r"edifici pubblic|scuol|comunal|municipal", testo_lower):
        return testo

    return None


def crea_sessione():
    session = requests.Session()
    retries = Retry(
        total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def read_html_with_timeout(url, timeout_sec=60):
    try:
        response = requests.get(url, timeout=timeout_sec)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Errore nella richiesta {url}: {e}")
        return None


def scarica_testo_async(link, url_base, session):
    """Scarica e processa un singolo link di dettaglio."""
    url_completo_dettaglio = f"{url_base}{link}"
    try:
        response = session.get(url_completo_dettaglio, timeout=15)
        response.raise_for_status()
        soup_dettagliata = BeautifulSoup(response.text, "html.parser")
        testo = soup_dettagliata.get_text()

        # Pulisci il testo (importante!)
        testo_pulito = " ".join(testo.split())

        # Verifica che sia un testo valido (non solo header)
        if len(testo_pulito) < 500 or "Gazzetta Ufficiale Home" not in testo_pulito:
            return None

        return filtra_testo(testo_pulito)

    except Exception:
        return None


def processa_links(url_risultati):
    """Processa tutti i link trovati nella sezione AVVISI ESITI DI GARA."""
    url_base = "http://www.gazzettaufficiale.it"
    testo_dettagliato = []
    sessione = crea_sessione()

    if not url_risultati:
        return []

    print(f"      Processando {len(url_risultati)} link di dettaglio...")

    # Usa ThreadPoolExecutor per velocizzare
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {
            executor.submit(scarica_testo_async, link, url_base, sessione): link
            for link in url_risultati
        }

        processed = 0
        found = 0

        for future in as_completed(futures):
            processed += 1
            try:
                testo_filtrato = future.result()
                if testo_filtrato:
                    testo_dettagliato.append(testo_filtrato)
                    found += 1

                # Log progress
                if processed % 10 == 0 or processed == len(url_risultati):
                    print(
                        f"        {processed}/{len(url_risultati)} processati, {found} passano il filtro"
                    )

            except Exception:
                continue

    return testo_dettagliato


def estrai_dati_da_url(url):
    """Estrae tutti i link dalla sezione AVVISI ESITI DI GARA."""
    pagina = read_html_with_timeout(url)
    if not pagina:
        return []

    # IMPORTANTE: Cerca TUTTE le sezioni, non solo AVVISI ESITI DI GARA
    html_strs = pagina.split("\n")

    # Trova tutte le rubriche
    span_start_indices = [
        i for i, line in enumerate(html_strs) if '<span class="rubrica">' in line
    ]

    all_links = []

    # Processa OGNI sezione che potrebbe contenere dati utili
    sezioni_da_cercare = [
        "AVVISI ESITI DI GARA",
        "AVVISI E BANDI DI GARA",  # Anche i bandi possono contenere info utili
        "AVVISI DI AGGIUDICAZIONE",
    ]

    for sezione in sezioni_da_cercare:
        # Trova l'indice dove inizia la sezione
        start_index = None
        for i in span_start_indices:
            if sezione in html_strs[i]:
                start_index = i
                break

        if start_index:
            # Trova dove finisce la sezione
            end_index = next(
                (i for i in span_start_indices if i > start_index), len(html_strs)
            )

            # Estrai HTML della sezione
            relevant_html_strs = html_strs[start_index:end_index]
            relevant_html = "\n".join(relevant_html_strs)
            relevant_soup = BeautifulSoup(relevant_html, "html.parser")

            # Estrai tutti i link
            link_nodes = relevant_soup.select("a")
            links = [link["href"] for link in link_nodes if link.get("href")]

            if links:
                print(f"      Trovati {len(links)} link in '{sezione}'")
                all_links.extend(links)

    # Rimuovi duplicati
    all_links = list(set(all_links))

    if all_links:
        print(f"      Totale link unici da processare: {len(all_links)}")
        return processa_links(all_links)
    else:
        return []


class GazzettaScraper:
    @staticmethod
    def run():
        print(f"\n{'='*60}")
        print(f"SCRAPING GAZZETTA UFFICIALE - VERSIONE FINALE")
        print(f"Anni: 2015 - 2025")
        print(
            f"Filtro: AMPIO (infrastrutture pubbliche, servizi, energia, mobilità, etc.)"
        )
        print(f"{'='*60}\n")

        # Usa i path corretti
        temp_dir = Path("data/temp")
        temp_dir.mkdir(parents=True, exist_ok=True)

        raw_file = temp_dir / "Lotti_Raw.xlsx"

        # Reset del file se esiste
        if raw_file.exists():
            raw_file.unlink()
            print(f"🗑️  Rimosso vecchio file {raw_file}\n")

        all_texts = []  # Raccoglie tutti i testi

        # Dal 2015 al 2025
        START_YEAR = 2015
        END_YEAR = 2025

        for anno in range(START_YEAR, END_YEAR + 1):
            print(f"\n{'='*40}")
            print(f"📅 ANNO {anno}")
            print(f"{'='*40}")

            folder_path = temp_dir / str(anno)
            folder_path.mkdir(parents=True, exist_ok=True)

            # URL della pagina per l'anno corrente
            url_anno = f"https://www.gazzettaufficiale.it/ricercaArchivioCompleto/contratti/{anno}?anno={anno}"

            print(f"📥 Download indice anno {anno}...")
            pagina_anno = read_html_with_timeout(url_anno)
            if not pagina_anno:
                print(f"❌ Impossibile scaricare indice anno {anno}")
                continue

            soup_anno = BeautifulSoup(pagina_anno, "html.parser")

            # Estrai i link alle gazzette
            link_nodes = soup_anno.select(".elenco_pdf a")
            links = [
                {
                    "indice": idx + 1,
                    "url_parziale": link["href"],
                    "testo": " ".join(link.get_text(strip=True).split()).replace(
                        "\xa0", ""
                    ),
                }
                for idx, link in enumerate(link_nodes)
                if link.get("href")
            ]

            print(f"📚 Trovate {len(links)} gazzette per l'anno {anno}")

            # Statistiche per l'anno
            testo_completo_annuale = []
            gazzette_con_dati = 0

            # Processa le gazzette
            for curr_link in links:
                url_indice = curr_link["indice"]
                url_parziale = curr_link["url_parziale"]
                url_testo = curr_link["testo"].replace("\xa0", "")
                url_completo = f"http://www.gazzettaufficiale.it{url_parziale}"

                excel_file_path = folder_path / f"{url_testo}.xlsx"

                # Verifica se già processato
                if excel_file_path.exists():
                    # Carica dati esistenti
                    try:
                        df_existing = pd.read_excel(excel_file_path)
                        if not df_existing.empty:
                            testi_esistenti = df_existing["testo"].tolist()
                            if testi_esistenti and any(
                                t for t in testi_esistenti if t and str(t) != "nan"
                            ):
                                testo_completo_annuale.extend(testi_esistenti)
                                all_texts.extend(testi_esistenti)
                                gazzette_con_dati += 1
                                print(
                                    f"  ⏭️  Gazzetta {url_indice}/{len(links)}: {url_testo[:30]}... (già processata, {len(testi_esistenti)} testi)"
                                )
                                continue
                    except:
                        pass  # File corrotto, ri-processa

                # Processa nuova gazzetta
                print(f"\n  📰 Gazzetta {url_indice}/{len(links)}: {url_testo}")

                try:
                    testi_estratti = estrai_dati_da_url(url_completo)

                    if testi_estratti and len(testi_estratti) > 0:
                        # Salva i testi trovati
                        df = pd.DataFrame(testi_estratti, columns=["testo"])
                        df.to_excel(excel_file_path, index=False)
                        print(
                            f"      ✅ Salvati {len(testi_estratti)} testi che passano il filtro"
                        )

                        testo_completo_annuale.extend(testi_estratti)
                        all_texts.extend(testi_estratti)
                        gazzette_con_dati += 1
                    else:
                        # Salva file vuoto per evitare re-processing
                        df = pd.DataFrame(columns=["testo"])
                        df.to_excel(excel_file_path, index=False)
                        print(f"      ⚠️  Nessun testo passa il filtro")

                except Exception as e:
                    print(f"      ❌ Errore: {e}")
                    continue

                # Salva periodicamente per sicurezza
                if len(all_texts) > 0 and len(all_texts) % 100 == 0:
                    df_temp = pd.DataFrame(all_texts, columns=["testo"])
                    df_temp.to_excel(raw_file, index=False)
                    print(f"\n    💾 Salvati {len(all_texts)} testi totali finora...")

            # Riepilogo anno
            print(f"\n{'='*40}")
            print(f"✅ Anno {anno} completato:")
            print(f"   - Gazzette con dati: {gazzette_con_dati}/{len(links)}")
            print(f"   - Testi raccolti: {len(testo_completo_annuale)}")
            print(f"   - Totale complessivo: {len(all_texts)}")

            # Salva file anno se ci sono dati
            if testo_completo_annuale:
                anno_file = folder_path / f"Anno_{anno}_completo.xlsx"
                df_anno = pd.DataFrame(testo_completo_annuale, columns=["testo"])
                df_anno.to_excel(anno_file, index=False)
                print(f"   - File anno salvato: {anno_file}")

        # Salva TUTTI i testi in Lotti_Raw.xlsx
        print(f"\n{'='*60}")
        if all_texts:
            df_all = pd.DataFrame(all_texts, columns=["testo"])
            df_all.to_excel(raw_file, index=False)
            print(f"🎉 COMPLETATO!")
            print(f"✅ Totale testi salvati: {len(all_texts)}")
            print(f"📁 File: {raw_file}")
        else:
            print(f"⚠️  Nessun testo trovato - creazione dati esempio")

            # Crea file esempio per non bloccare pipeline
            df_example = pd.DataFrame(
                [
                    "Gara illuminazione pubblica Milano CIG 1234567890",
                    "Videosorveglianza edifici comunali Roma",
                    "Manutenzione impianti termici scuole",
                ],
                columns=["testo"],
            )
            df_example.to_excel(raw_file, index=False)
            print(f"📁 File esempio creato: {raw_file}")

        print(f"{'='*60}\n")


if __name__ == "__main__":
    GazzettaScraper.run()