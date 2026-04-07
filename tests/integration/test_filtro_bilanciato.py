#!/usr/bin/env python
"""
Test del filtro bilanciato per verificare che sia più selettivo.
"""

import re

def filtra_testo(testo):
    """Filtro bilanciato per gare di infrastrutture e servizi pubblici rilevanti."""
    if not testo:
        return None
    
    testo_lower = testo.lower()
    
    # 1. ILLUMINAZIONE PUBBLICA (più specifico)
    if re.search(r"illuminazion[ei] pubblic|lampioni|pubblica illuminazione|impianti di illuminazione|corpi illuminanti", testo_lower):
        return testo
    
    # 2. VIDEOSORVEGLIANZA (specifico)
    elif re.search(r"videosorveglian|telecamer[ae]|tvcc|sistema.{0,20}sorveglian", testo_lower):
        return testo
    
    # 3. EFFICIENTAMENTO ENERGETICO (più specifico)
    elif (re.search(r"efficientamento energetic|riqualificazione energetic|risparmio energetic", testo_lower) or
          (re.search(r"impiant[oi]", testo_lower) and re.search(r"fotovoltaic|solare|led|termic", testo_lower))):
        return testo
    
    # 4. EDIFICI PUBBLICI + ENERGIA/IMPIANTI (combinato)
    elif (re.search(r"scuol[ae]|municipio|palazzo comunale|biblioteca|ospedale", testo_lower) and
          re.search(r"impiant[oi]|manutenzion|ristrutturazion|adeguament|climatizzazion|riscaldament", testo_lower)):
        return testo
    
    # 5. MOBILITÀ ELETTRICA (specifico)
    elif re.search(r"colonnin[ae].{0,20}ricaric|ricarica.{0,20}elettric|stazion.{0,20}ricaric|e-mobility", testo_lower):
        return testo
    
    # 6. PARCHEGGI CON GESTIONE/TECNOLOGIA
    elif (re.search(r"parchegg[io]", testo_lower) and 
          re.search(r"gestion|parcometr|parchimetr|automat|smart|sensor", testo_lower)):
        return testo
    
    # 7. SMART CITY (più specifico)
    elif (re.search(r"smart city|città intelligente", testo_lower) or
          (re.search(r"sensor[ei]|iot|telecontroll|telegestione", testo_lower) and 
           re.search(r"pubblic|urban|città|comune", testo_lower))):
        return testo
    
    # 8. VERDE PUBBLICO CON IMPIANTI
    elif (re.search(r"verde pubblic|parchi|giardini", testo_lower) and
          re.search(r"irrigazion|impiant|illuminazion|manutenzion", testo_lower)):
        return testo
    
    # 9. STRADE + ILLUMINAZIONE/SEGNALETICA
    elif (re.search(r"strad[ae]|viabilità", testo_lower) and
          re.search(r"illuminazion|segnaletic|semafori|asfalto|manutenzion", testo_lower)):
        return testo
    
    # 10. IMPIANTI SPORTIVI (specifico)
    elif (re.search(r"impianti sportiv|palestra|piscina|campo sportiv|palazzetto", testo_lower) and
          re.search(r"manutenzion|ristrutturazion|impiant|illuminazion", testo_lower)):
        return testo
    
    # 11. GLOBAL SERVICE/FACILITY (specifico per edifici pubblici)
    elif (re.search(r"global service|facility management|gestione integrata", testo_lower) and
          re.search(r"edifici|immobili|pubblic|comunal", testo_lower)):
        return testo
    
    # 12. GALLERIE/TUNNEL CON IMPIANTI
    elif (re.search(r"galleri[ae]|tunnel", testo_lower) and
          re.search(r"impiant[oi]|illuminazion|ventilazion|sicurezza", testo_lower) and
          not re.search(r"museo|arte|mostra", testo_lower)):
        return testo
    
    # 13. RETI IDRICHE/FOGNATURE (più specifico)
    elif (re.search(r"acquedott|rete idric|fognatur|depurator", testo_lower) and
          re.search(r"manutenzion|gestion|lavori|riparazion", testo_lower)):
        return testo
    
    # 14. PUBBLICA ILLUMINAZIONE LED
    elif re.search(r"\bled\b", testo_lower) and re.search(r"pubblic|strad|comunal|illuminazion", testo_lower):
        return testo
    
    # 15. IMPIANTI TERMICI/CLIMATIZZAZIONE EDIFICI PUBBLICI
    elif (re.search(r"termic|climatizzazion|condizionament|caldai", testo_lower) and
          re.search(r"edifici pubblic|scuol|comunal|municipal", testo_lower)):
        return testo
    
    return None


def test_filtro():
    """Test del filtro bilanciato."""
    
    # Test che DOVREBBERO PASSARE
    test_positivi = [
        "Gara per illuminazione pubblica comunale",
        "Manutenzione impianti termici scuole comunali",
        "Installazione telecamere videosorveglianza",
        "Efficientamento energetico palazzo comunale",
        "Colonnine di ricarica elettrica parcheggi pubblici",
        "Smart city - sensori IoT per monitoraggio urbano",
        "Manutenzione verde pubblico con impianti irrigazione",
        "Rifacimento segnaletica stradale e illuminazione",
        "Ristrutturazione impianti sportivi comunali",
        "Global service edifici pubblici",
        "Impianti illuminazione galleria stradale",
        "Manutenzione rete idrica comunale",
        "LED per illuminazione stradale",
        "Climatizzazione scuole comunali",
        "Riqualificazione energetica edifici pubblici",
        "Sistema videosorveglianza cittadina",
        "Gestione parcheggi con parcometri",
    ]
    
    # Test che NON dovrebbero passare
    test_negativi = [
        "Fornitura carta per uffici",
        "Servizio pulizia ordinaria",
        "Mensa scolastica",
        "Noleggio fotocopiatrici",
        "Servizi assicurativi",
        "Consulenza legale",
        "Formazione personale",
        "Software gestionale",
        "Trasporto scolastico",
        "Servizi postali",
        "Vigilanza privata",
        "Manutenzione ascensori",  # Senza specificare edifici pubblici
        "Fornitura carburante",
        "Servizio energia",  # Troppo generico
        "Manutenzione generica",  # Troppo generico
        "Strada provinciale",  # Senza specificare lavori/illuminazione
        "Edificio privato",
        "Parcheggio privato",
    ]
    
    print("=" * 70)
    print("TEST FILTRO BILANCIATO")
    print("=" * 70)
    
    passati = 0
    falliti = 0
    
    print("\n✅ DOVREBBERO PASSARE:")
    print("-" * 40)
    for testo in test_positivi:
        risultato = filtra_testo(testo)
        if risultato:
            print(f"  ✅ {testo}")
            passati += 1
        else:
            print(f"  ❌ {testo} (ERRORE: non passa)")
            falliti += 1
    
    print(f"\nPassati: {passati}/{len(test_positivi)}")
    
    print("\n❌ NON DOVREBBERO PASSARE:")
    print("-" * 40)
    corretti = 0
    for testo in test_negativi:
        risultato = filtra_testo(testo)
        if not risultato:
            print(f"  ✅ {testo} (correttamente filtrato)")
            corretti += 1
        else:
            print(f"  ❌ {testo} (ERRORE: passa il filtro)")
            falliti += 1
    
    print(f"\nFiltrati correttamente: {corretti}/{len(test_negativi)}")
    
    print("\n" + "=" * 70)
    print("RIEPILOGO:")
    print(f"  Test positivi passati: {passati}/{len(test_positivi)}")
    print(f"  Test negativi filtrati: {corretti}/{len(test_negativi)}")
    print(f"  Errori totali: {falliti}")
    
    if falliti == 0:
        print("\n✅ FILTRO FUNZIONA CORRETTAMENTE!")
    else:
        print(f"\n⚠️ CI SONO {falliti} ERRORI DA CORREGGERE")
    
    print("=" * 70)


if __name__ == "__main__":
    test_filtro()