#!/usr/bin/env python
"""
Test dei filtri ampliati per verificare che catturino più tipologie di gare.
"""

import re

def applica_filtro_categoria(testo: str) -> bool:
    """
    Applica filtri più ampi per catturare gare di infrastrutture e servizi pubblici.
    """
    if not testo:
        return False
        
    testo_lower = str(testo).lower()
    
    # 1. ILLUMINAZIONE (più ampio)
    if re.search(r"illumin|lampioni|lampade|pubblica illuminazione|led", testo_lower):
        return True
    
    # 2. VIDEOSORVEGLIANZA E SICUREZZA (più ampio)
    if re.search(r"videosorveglian|telecamer|tvcc|sicurezza urbana|controllo accessi|antintrus", testo_lower):
        return True
    
    # 3. ENERGIA E IMPIANTI (molto più ampio)
    if re.search(r"energ|elettric|termic|riscaldament|climatizzazion|condizionament|fotovoltaic|solare|rinnovabil|efficientamento|riqualificazion", testo_lower):
        return True
    
    # 4. EDIFICI E MANUTENZIONE (più ampio)
    if re.search(r"edific|manutenzion|ristrutturazion|riqualificazion|adeguament|scuol|palazz|municipio|comune|biblioteca|palestra", testo_lower):
        return True
    
    # 5. MOBILITÀ ELETTRICA E SOSTENIBILE
    if re.search(r"colonnin|ricaric|e-mobility|mobilità elettrica|veicoli elettrici|biciclette elettriche|monopattini", testo_lower):
        return True
    
    # 6. PARCHEGGI E MOBILITÀ URBANA (più ampio)
    if re.search(r"parchegg|sosta|mobilità|viabilità|traffico|ztl|zone traffic|parchimetri|strisce blu", testo_lower):
        return True
    
    # 7. SMART CITY E TECNOLOGIE
    if re.search(r"smart city|smart|iot|sensori|monitoraggio|telecontrollo|telegestione|wi-fi|connettività|banda larga|fibra ottica", testo_lower):
        return True
    
    # 8. VERDE PUBBLICO E AMBIENTE
    if re.search(r"verde pubblic|irrigazion|parchi|giardini|arredo urbano|decoro urbano|fontane", testo_lower):
        return True
    
    # 9. STRADE E INFRASTRUTTURE
    if re.search(r"strad|marciapiedi|asfalto|bitume|segnaletic|rotond|viabilità|ponti|viadotti|sottopas", testo_lower):
        return True
    
    # 10. IMPIANTI SPORTIVI
    if re.search(r"impianti sportiv|campo sportiv|palestra|piscina|stadio|palasp", testo_lower):
        return True
    
    # 11. SERVIZI PUBBLICI GENERALI
    if re.search(r"servizio pubblic|pubblica utilità|gestione integrat|global service|facility management", testo_lower):
        return True
    
    # 12. EMERGENZA E PROTEZIONE CIVILE
    if re.search(r"emergenza|protezione civile|antincendio|evacuazione|allarme|sirene", testo_lower):
        return True
    
    # 13. GALLERIE E TUNNEL (semplificato)
    if re.search(r"galleri|tunnel", testo_lower) and not re.search(r"museo|arte|cultura", testo_lower):
        return True
    
    # 14. ACQUA E FOGNATURE
    if re.search(r"acquedott|idric|fognatur|depurator|potabilizzazion", testo_lower):
        return True
    
    # 15. RIFIUTI E IGIENE URBANA
    if re.search(r"rifiuti|nettezza|igiene urbana|spazzamento|raccolta differenziata|isola ecologic", testo_lower):
        return True
    
    return False


def test_filtri():
    """Test dei filtri con vari esempi di gare."""
    
    test_cases = [
        # Dovrebbero passare
        ("Gara per illuminazione pubblica comunale", True),
        ("Servizio di manutenzione impianti elettrici scuole", True),
        ("Fornitura LED per strade comunali", True),
        ("Installazione telecamere di videosorveglianza", True),
        ("Riqualificazione energetica edifici pubblici", True),
        ("Manutenzione ordinaria palazzo comunale", True),
        ("Gestione parcheggi a pagamento centro storico", True),
        ("Realizzazione rete Wi-Fi pubblica", True),
        ("Manutenzione verde pubblico e parchi", True),
        ("Asfaltatura strade comunali", True),
        ("Servizio di facility management edifici comunali", True),
        ("Fornitura colonnine ricarica elettrica", True),
        ("Impianto fotovoltaico scuola media", True),
        ("Ristrutturazione palestra comunale", True),
        ("Servizio raccolta rifiuti differenziata", True),
        ("Manutenzione rete idrica comunale", True),
        ("Sistema antincendio edifici pubblici", True),
        ("Segnaletica stradale orizzontale e verticale", True),
        ("Gestione integrata servizi pubblici", True),
        ("Efficientamento energetico municipio", True),
        ("Installazione sensori IoT monitoraggio traffico", True),
        ("Arredo urbano piazze e vie", True),
        ("Manutenzione fontane pubbliche", True),
        ("Realizzazione pista ciclabile", True),
        ("Climatizzazione uffici comunali", True),
        
        # Non dovrebbero passare (ma ora con filtri più ampi potrebbero)
        ("Fornitura cancelleria uffici", False),
        ("Servizio pulizia ordinaria", False),
        ("Catering mensa scolastica", False),
        ("Noleggio fotocopiatrici", False),
        ("Servizi assicurativi", False),
    ]
    
    print("=" * 70)
    print("TEST FILTRI AMPLIATI")
    print("=" * 70)
    
    passed = 0
    failed = 0
    
    for testo, expected in test_cases:
        result = applica_filtro_categoria(testo)
        status = "✅" if result == expected else "❌"
        
        if result == expected:
            passed += 1
        else:
            failed += 1
            
        if result:
            print(f"{status} PASSA:     {testo}")
        else:
            print(f"{status} NON PASSA: {testo}")
    
    print("\n" + "=" * 70)
    print(f"RISULTATI: {passed}/{len(test_cases)} test passati")
    
    if failed > 0:
        print(f"⚠️  {failed} test falliti")
    else:
        print("✅ Tutti i test passati!")
    
    print("=" * 70)
    
    # Test su testi più lunghi e realistici
    print("\n" + "=" * 70)
    print("TEST SU TESTI REALISTICI")
    print("=" * 70)
    
    testi_realistici = [
        """PROCEDURA APERTA PER L'AFFIDAMENTO DEL SERVIZIO DI GESTIONE, 
        CONDUZIONE E MANUTENZIONE DEGLI IMPIANTI DI ILLUMINAZIONE PUBBLICA 
        DEL COMUNE DI MILANO COMPRENSIVO DI FORNITURA ENERGIA ELETTRICA""",
        
        """Gara europea per la riqualificazione energetica degli edifici scolastici 
        comunali mediante installazione di pannelli fotovoltaici e sostituzione 
        infissi per miglioramento efficienza termica""",
        
        """AFFIDAMENTO SERVIZIO GLOBAL SERVICE PER LA GESTIONE INTEGRATA 
        DEGLI IMMOBILI COMUNALI COMPRENSIVO DI MANUTENZIONE ORDINARIA E 
        STRAORDINARIA IMPIANTI TECNOLOGICI""",
        
        """Realizzazione sistema di videosorveglianza urbana integrato con 
        centrale operativa e installazione di n. 150 telecamere HD nelle 
        aree sensibili del territorio comunale""",
        
        """GARA PER LA FORNITURA E POSA IN OPERA DI COLONNINE DI RICARICA 
        PER VEICOLI ELETTRICI DA INSTALLARE NEI PARCHEGGI PUBBLICI E 
        NELLE AREE DI SOSTA COMUNALI""",
    ]
    
    for i, testo in enumerate(testi_realistici, 1):
        result = applica_filtro_categoria(testo)
        status = "✅ PASSA" if result else "❌ NON PASSA"
        print(f"\nTesto {i}: {status}")
        print(f"  {testo[:100]}...")
    
    print("\n" + "=" * 70)
    print("TEST COMPLETATO")
    print("I filtri ora catturano molte più tipologie di gare rilevanti")
    print("mantenendo il focus su infrastrutture e servizi pubblici")
    print("=" * 70)


if __name__ == "__main__":
    test_filtri()