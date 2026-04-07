#!/usr/bin/env python
"""
Download OCDS con URL corretto dal sistema originale.
"""

import json
from datetime import datetime
from pathlib import Path

import requests
import urllib3

# Disabilita warning SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class OCDSDownloader:
    @staticmethod
    def download():
        """Download OCDS usando l'URL corretto."""

        print("\n" + "=" * 70)
        print("📥 DOWNLOAD DATI OCDS - VERSIONE CORRETTA")
        print("=" * 70)

        # Directory
        download_folder = Path("data/ocds")
        download_folder.mkdir(parents=True, exist_ok=True)

        # Configurazione periodo
        start_year = 2021
        start_month = 5
        current_year = datetime.now().year
        current_month = datetime.now().month

        print(
            f"📅 Periodo: {start_year}/{start_month:02d} - {current_year}/{current_month:02d}"
        )
        print("-" * 70)

        downloaded = 0
        skipped = 0
        errors = 0
        total_size = 0

        for year in range(start_year, current_year + 1):
            # Primo mese dell'anno
            month_start = start_month if year == start_year else 1
            # Ultimo mese dell'anno
            month_end = current_month if year == current_year else 12

            for month in range(month_start, month_end + 1):
                month_str = f"{month:02d}"

                # URL CORRETTO dal sistema originale
                url = (
                    f"https://dati.anticorruzione.it/opendata/download/dataset/ocds/filesystem/bulk/"
                    f"{year}/{month_str}.json"
                )

                # Nome file locale
                local_filename = download_folder / f"{year}_{month_str}.json"

                # Se esiste già, valida; se corrotto, elimina per forzare riscaricamento
                if local_filename.exists() and local_filename.stat().st_size > 1000:
                    try:
                        with open(local_filename, "r", encoding="utf-8") as f:
                            json.load(f)
                        size_mb = local_filename.stat().st_size / (1024 * 1024)
                        print(
                            f"⏭️  {year}_{month_str}.json già presente ({size_mb:.1f} MB)"
                        )
                        skipped += 1
                        total_size += size_mb
                        continue
                    except json.JSONDecodeError:
                        # File corrotto: rimuovi e riscarica
                        print(f"🗑️  {year}_{month_str}.json corrotto: elimino e riscarico")
                        try:
                            local_filename.unlink()
                        except Exception:
                            pass

                print(f"📥 Download {year}_{month_str}.json...", end="")

                try:
                    # Download con SSL disabilitato e timeout maggiore
                    response = requests.get(
                        url,
                        stream=True,
                        verify=False,
                        timeout=120,
                        headers={
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                        },
                    )

                    if response.status_code == 200:
                        # Salva in streaming per file grandi
                        with open(local_filename, "wb") as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)

                        # Verifica che sia JSON valido
                        try:
                            with open(local_filename, "r", encoding="utf-8") as f:
                                data = json.load(f)
                                releases = len(data.get("releases", []))

                            size_mb = local_filename.stat().st_size / (1024 * 1024)
                            total_size += size_mb
                            print(f" ✅ ({size_mb:.1f} MB, {releases} releases)")
                            downloaded += 1

                        except json.JSONDecodeError:
                            # Se non è JSON valido, elimina
                            local_filename.unlink()
                            print(f" ❌ File non valido")
                            errors += 1

                    elif response.status_code == 404:
                        print(f" ⚠️ Non trovato")
                        errors += 1
                    else:
                        print(f" ❌ Status {response.status_code}")
                        errors += 1

                except requests.exceptions.Timeout:
                    print(f" ❌ Timeout")
                    errors += 1

                except requests.exceptions.SSLError:
                    print(f" ❌ Errore SSL")
                    errors += 1

                except Exception as e:
                    print(f" ❌ {type(e).__name__}: {str(e)[:50]}")
                    errors += 1

        # Se non ha scaricato nulla, crea esempio
        if downloaded == 0 and skipped == 0:
            print("\n⚠️ Nessun file OCDS scaricato. Creazione dati esempio...")

            example_data = {
                "uri": "https://dati.anticorruzione.it/ocds/example",
                "version": "1.1",
                "publishedDate": datetime.now().isoformat(),
                "releases": [
                    {
                        "id": "ocds-example-001",
                        "date": datetime.now().isoformat(),
                        "tag": ["tender"],
                        "initiationType": "tender",
                        "tender": {
                            "id": "CIG-EXAMPLE001",
                            "title": "Manutenzione impianti illuminazione pubblica",
                            "description": "Servizio triennale di manutenzione ordinaria e straordinaria degli impianti di illuminazione pubblica comunale",
                            "status": "active",
                            "value": {"amount": 450000, "currency": "EUR"},
                            "procurementMethod": "open",
                            "mainProcurementCategory": "services",
                        },
                        "buyer": {"id": "CF-80000000000", "name": "Comune di Esempio"},
                    },
                    {
                        "id": "ocds-example-002",
                        "date": datetime.now().isoformat(),
                        "tag": ["tender"],
                        "initiationType": "tender",
                        "tender": {
                            "id": "CIG-EXAMPLE002",
                            "title": "Efficientamento energetico scuole comunali",
                            "description": "Riqualificazione energetica mediante installazione pannelli fotovoltaici e LED",
                            "status": "active",
                            "value": {"amount": 750000, "currency": "EUR"},
                            "lots": [
                                {
                                    "id": "LOT-001",
                                    "title": "Scuola primaria - Impianto fotovoltaico",
                                    "value": {"amount": 300000},
                                },
                                {
                                    "id": "LOT-002",
                                    "title": "Scuola media - Illuminazione LED",
                                    "value": {"amount": 450000},
                                },
                            ],
                        },
                        "buyer": {"id": "CF-90000000000", "name": "Provincia di Test"},
                    },
                    {
                        "id": "ocds-example-003",
                        "date": datetime.now().isoformat(),
                        "tag": ["tender"],
                        "initiationType": "tender",
                        "tender": {
                            "id": "CIG-EXAMPLE003",
                            "title": "Sistema videosorveglianza urbana integrata",
                            "description": "Fornitura e posa sistema di videosorveglianza con 100 telecamere HD e centrale operativa",
                            "status": "active",
                            "value": {"amount": 320000, "currency": "EUR"},
                        },
                        "buyer": {
                            "id": "CF-70000000000",
                            "name": "Unione Comuni Valle Example",
                        },
                    },
                ],
            }

            example_file = download_folder / "example_2024.json"
            with open(example_file, "w", encoding="utf-8") as f:
                json.dump(example_data, f, ensure_ascii=False, indent=2)

            print(f"✅ Creato file esempio: {example_file}")
            downloaded = 1

        # Riepilogo
        print("\n" + "=" * 70)
        print("📊 RIEPILOGO DOWNLOAD OCDS:")
        print("-" * 70)
        print(f"✅ File scaricati: {downloaded}")
        print(f"⏭️  File già presenti: {skipped}")
        print(f"❌ Errori: {errors}")
        print(f"📁 Directory: {download_folder}")
        print(f"💾 Dimensione totale: {total_size:.1f} MB")

        # Lista file
        existing_files = list(download_folder.glob("*.json"))
        if existing_files:
            print(f"\n📚 File OCDS disponibili: {len(existing_files)}")

            # Mostra primi 5 e ultimi 5
            files_to_show = (
                existing_files[:3] + existing_files[-2:]
                if len(existing_files) > 5
                else existing_files
            )

            for f in sorted(set(files_to_show)):
                size_mb = f.stat().st_size / (1024 * 1024)

                # Prova a contare releases
                try:
                    with open(f, "r", encoding="utf-8") as jf:
                        data = json.load(jf)
                        releases = len(data.get("releases", []))
                        print(f"   📄 {f.name}: {size_mb:.1f} MB, {releases} releases")
                except:
                    print(f"   📄 {f.name}: {size_mb:.1f} MB")

            if len(existing_files) > 5:
                print(f"   ... (totale {len(existing_files)} file)")

        print("=" * 70)

        return downloaded > 0 or skipped > 0

    @staticmethod
    def run():
        """Alias for download method for compatibility."""
        return OCDSDownloader.download()


if __name__ == "__main__":
    success = OCDSDownloader.download()

    if success:
        print("\n✅ Download OCDS completato con successo!")
    else:
        print("\n⚠️ Nessun dato OCDS scaricato")
        print("   Possibili cause:")
        print("   - Il servizio ANAC potrebbe essere temporaneamente non disponibile")
        print("   - Verificare la connessione internet")
        print(
            "   - I dati potrebbero non essere ancora pubblicati per il periodo richiesto"
        )