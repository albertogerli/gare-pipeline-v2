"""
Enumerazioni per le categorie e i tipi di dati.

Questo modulo definisce tutte le enumerazioni utilizzate per categorizzare
e tipizzare i dati dei bandi di gara.
"""

from enum import Enum


class CategoriaLotto(str, Enum):
    """Categorie principali dei lotti di gara."""

    ILLUMINAZIONE = "Illuminazione"
    VIDEOSORVEGLIANZA = "Videosorveglianza"
    GALLERIE = "Gallerie"
    TUNNEL = "Tunnel"
    IMPIANTI = "Impianti"
    EDIFICI = "Edifici"
    TERMICI = "Termici"
    COLONNINE = "Colonnine"
    RICARICA = "Ricarica"
    PARCHEGGI = "Parcheggi"
    STRUTTURE_SPORTIVE = "Strutture Sportive"
    TRASPORTI_PUBBLICI = "Trasporti Pubblici"
    INFRASTRUTTURE_DIGITALI = "Infrastrutture Digitali"
    ACQUEDOTTI = "Acquedotti"
    SCUOLE = "Scuole"
    SANITARIO = "Sanitario"
    RIFIUTI = "Rifiuti"
    ALTRO = ""


class TipoIlluminazione(str, Enum):
    """Tipi specifici di illuminazione."""

    PUBBLICA = "Pubblica"
    SPAZI_ARCHITETTURALI = "Spazi Architetturali"
    STRADALE = "Stradale"
    SPORTIVA = "Sportiva"
    CIMITERIALE = "Cimiteriale"
    VOTIVA = "Votiva"
    INTERNA = "Interna"
    ESTERNA = "Esterna"
    DECORATIVA = "Decorativa"
    EMERGENZA = "Emergenza"
    INDUSTRIALE = "Industriale"
    COMMERCIALE = "Commerciale"
    SMART_STRADALE = "Stradale Intelligente"
    PARCHEGGI = "Per Parcheggi"
    GALLERIE = "Per Gallerie"
    PARCHI_GIARDINI = "Per Parchi e Giardini"
    MUSEI_GALLERIE_ARTE = "Per Musei e Gallerie d'Arte"
    TEATRI_PALCOSCENICI = "Per Teatri e Palcoscenici"
    FACCIATE_EDIFICI = "Per Facciate Edifici"
    SICUREZZA = "Di Sicurezza"
    SANITARIA = "Sanitaria"
    TRASPORTI = "Per Trasporti"
    RESIDENZIALE_AREE = "Per Aree Residenziali"
    SPORTIVA_INTERNA = "Per Impianti Sportivi al Coperto"
    URBANA = "Urbana"
    ALTRO = ""


class TipoEfficientamento(str, Enum):
    """Tipi di efficientamento energetico."""

    ENERGETICO = "Energetico"
    TECNOLOGICO = "Tecnologico"
    MANUTENZIONE_ORDINARIA = "Manutenzione Ordinaria"
    MANUTENZIONE_STRAORDINARIA = "Manutenzione Straordinaria"
    ADEGUAMENTO_NORMATIVO = "Adeguamento Normativo"
    RIQUALIFICAZIONE = "Riqualificazione"
    AUTOMAZIONE = "Automazione"
    MONITORAGGIO = "Monitoraggio"
    ILLUMINAZIONE_LED = "Illuminazione LED"
    RISPARMIO_CONSUMI = "Risparmio Consumi"
    RICERCA_SVILUPPO = "Ricerca e Sviluppo"
    SICUREZZA_OPERATIVA = "Sicurezza Operativa"
    INTEGRAZIONE_IOT = "Integrazione IoT"
    GESTIONE_ILLUMINAZIONE = "Gestione Illuminazione"
    CONTROLLO_AUTOMATICO = "Controllo Automatico"
    EFFICIENZA_IDRICA = "Efficienza Idrica"
    EFFICIENZA_SPAZIALE = "Efficienza Spaziale"
    ALTRO = ""


class TipoAppalto(str, Enum):
    """Tipologie di appalto."""

    AFFIDAMENTO = "Affidamento"
    APPALTO = "Appalto"
    CONCESSIONE = "Concessione"
    PROJECT_FINANCING = "Project Financing"
    SERVIZIO = "Servizio"
    FORNITURA = "Fornitura"
    ACCORDO_QUADRO = "Accordo Quadro"
    PROCEDURA_APERTA = "Procedura Aperta"
    PARTENARIATO_PUBBLICO_PRIVATO = "Partenariato Pubblico Privato"
    GESTIONE = "Gestione"
    NOLEGGIO = "Noleggio"
    ALTRO = ""


class TipoIntervento(str, Enum):
    """Tipologie di intervento."""

    EFFICIENTAMENTO_ENERGETICO = "Efficientamento Energetico"
    RIQUALIFICAZIONE = "Riqualificazione"
    MANUTENZIONE_ORDINARIA = "Manutenzione Ordinaria"
    MANUTENZIONE_STRAORDINARIA = "Manutenzione Straordinaria"
    ADEGUAMENTO_NORMATIVO = "Adeguamento Normativo"
    GESTIONE_IMPIANTI = "Gestione Impianti"
    RINNOVO_IMPIANTI = "Rinnovo Impianti"
    INSTALLAZIONE_NUOVI_IMPIANTI = "Installazione Nuovi Impianti"
    RIFACIMENTO = "Rifacimento"
    COSTRUZIONE = "Costruzione"
    SOSTITUZIONE_COMPONENTI = "Sostituzione Componenti"
    RESTAURO = "Restauro"
    POTENZIAMENTO = "Potenziamento"
    ALTRO = ""


class TipoImpianto(str, Enum):
    """Tipologie di impianto."""

    PUBBLICA_ILLUMINAZIONE = "Pubblica Illuminazione"
    ILLUMINAZIONE_STRADALE = "Illuminazione Stradale"
    ILLUMINAZIONE_SPORTIVA = "Illuminazione Sportiva"
    ILLUMINAZIONE_ARCHITETTURALE = "Illuminazione Architetturale"
    ILLUMINAZIONE_VOTIVA = "Illuminazione Votiva"
    IMPIANTI_ELETTRICI = "Impianti Elettrici"
    SEMAFORI = "Semafori"
    VIDEOSORVEGLIANZA = "Videosorveglianza"
    SMART_CITY = "Smart City"
    CLIMATIZZAZIONE = "Climatizzazione"
    IMPIANTI_TERMICI = "Impianti Termici"
    IRRIGAZIONE = "Irrigazione"
    SISTEMI_DI_CONTROLLO = "Sistemi di Controllo"
    PALI_ILLUMINAZIONE = "Pali di Illuminazione"
    TORRI_FARO = "Torri Faro"
    RIFIUTI = "Rifiuti"
    FIBRA = "Fibra Ottica"
    RETI_IDRICHE = "Reti Idriche"
    ALTRO = ""


class TipoEnergia(str, Enum):
    """Tipologie di energia."""

    FORNITURA_ENERGIA = "Fornitura di Energia"
    GESTIONE_ENERGIA = "Gestione dell'Energia"
    ENERGIA_ELETTRICA = "Energia Elettrica"
    ENERGIA_TERMICA = "Energia Termica"
    FONTI_RENEWABLE = "Fonti Rinnovabili"
    RISPARMIO_ENERGETICO = "Risparmio Energetico"
    SOLARE = "Solare"
    EOLICA = "Eolica"
    ALTRO = ""


class TipoOperazione(str, Enum):
    """Tipologie di operazione."""

    GESTIONE = "Gestione"
    MANUTENZIONE = "Manutenzione"
    LAVORI = "Lavori"
    EFFICIENTAMENTO = "Efficientamento"
    FORNITURE = "Forniture"
    ALTRO = ""
