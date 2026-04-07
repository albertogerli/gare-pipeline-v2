"""
Analizzatore OCDS completo con tutti i campi strutturati usando o3.
Replica esattamente tutti i campi del sistema originale.
"""

import hashlib
import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from openai import OpenAI
from pydantic import BaseModel, Field, field_validator

from config.settings import config
from src.utils.checkpoint import CheckpointManager
from src.utils.performance import timer

logger = logging.getLogger(__name__)

# Configurazione OpenAI o3
try:
    import config as old_config

    client = OpenAI(api_key=old_config.OPENAI_KEY)
    MODEL_NAME = "o3-mini"  # Usa o3-mini per efficienza
except:
    client = OpenAI(api_key=config.OPENAI_API_KEY)
    MODEL_NAME = "o3-mini"


# ============= ENUMS COMPLETI DAL SISTEMA ORIGINALE =============


class CategoriaLotto(str, Enum):
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
    GESTIONE = "Gestione"
    MANUTENZIONE = "Manutenzione"
    LAVORI = "Lavori"
    EFFICIENTAMENTO = "Efficientamento"
    FORNITURE = "Forniture"
    ALTRO = ""


# ============= MODELLO LOTTO COMPLETO =============


class LottoOCDS(BaseModel):
    """Modello completo per un lotto/appalto con tutti i campi strutturati."""

    # Campi principali
    Oggetto: str = Field(default="", description="Oggetto dell'appalto")
    Categoria: CategoriaLotto = Field(default=CategoriaLotto.ALTRO)
    TipoIlluminazione: TipoIlluminazione = Field(default=TipoIlluminazione.ALTRO)
    TipoEfficientamento: TipoEfficientamento = Field(default=TipoEfficientamento.ALTRO)
    TipoAppalto: TipoAppalto = Field(default=TipoAppalto.ALTRO)
    TipoIntervento: TipoIntervento = Field(default=TipoIntervento.ALTRO)
    TipoImpianto: TipoImpianto = Field(default=TipoImpianto.ALTRO)
    TipoEnergia: TipoEnergia = Field(default=TipoEnergia.ALTRO)
    TipoOperazione: TipoOperazione = Field(default=TipoOperazione.ALTRO)

    # Dati procedura
    Procedura: str = Field(default="", description="Tipo di procedura")
    AmministrazioneAggiudicatrice: str = Field(
        default="", description="Ente appaltante"
    )
    OfferteRicevute: str = Field(default="", description="Numero offerte ricevute")

    # Dati temporali
    DurataAppalto: str = Field(default="", description="Durata in giorni")
    DataAggiudicazione: str = Field(
        default="", description="Data aggiudicazione formato dd/mm/yyyy"
    )
    Scadenza: str = Field(default="", description="Data scadenza formato dd/mm/yyyy")

    # Dati economici
    ImportoAggiudicazione: str = Field(default="", description="Importo in euro")
    Sconto: str = Field(default="", description="Sconto applicato")

    # Dati geografici e soggetti
    Comune: str = Field(default="", description="Comune/Località")
    Aggiudicatario: str = Field(default="", description="Soggetto aggiudicatario")

    # Codici identificativi
    CIG: str = Field(default="", description="Codice CIG")
    CUP: str = Field(default="", description="Codice CUP")

    # Validatori per date
    @field_validator("Scadenza", "DataAggiudicazione", mode="before")
    def parse_and_standardize_date(cls, v):
        """Standardizza le date nel formato dd/mm/yyyy."""
        if not v or not isinstance(v, str):
            return ""

        v = str(v).lower().strip()

        if "non specificat" in v or v == "n/a" or v == "":
            return ""

        # Prova a parsare con vari formati
        formats = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%d.%m.%Y",
            "%Y/%m/%d",
            "%m/%d/%Y",
            "%d/%m/%y",
        ]

        for fmt in formats:
            try:
                date_obj = datetime.strptime(v[:10], fmt)
                return date_obj.strftime("%d/%m/%Y")
            except:
                continue

        return ""

    @field_validator("DurataAppalto", mode="before")
    def convert_duration_to_days(cls, v):
        """Converte la durata in giorni."""
        if not v or not isinstance(v, str):
            return ""

        v = str(v).lower().strip()

        try:
            # Cerca numeri nel testo
            numbers = re.findall(r"\d+", v)
            if not numbers:
                return ""

            num = int(numbers[0])

            if "giorn" in v:
                return str(num)
            elif "mes" in v:
                return str(num * 30)
            elif "ann" in v:
                return str(num * 365)
            else:
                return str(num)  # Assume giorni di default

        except:
            return ""

    @field_validator("OfferteRicevute", mode="before")
    def validate_offerte(cls, v):
        """Estrae solo il numero di offerte."""
        if not v:
            return ""
        return re.sub(r"\D", "", str(v))

    @field_validator("ImportoAggiudicazione", mode="before")
    def extract_amount(cls, v):
        """Estrae l'importo numerico."""
        if not v:
            return ""

        v = str(v).replace(",", ".")

        # Cerca pattern di numeri con decimali
        pattern = r"[\d]+\.?\d*"
        matches = re.findall(pattern, v)

        if matches:
            try:
                # Prendi il numero più grande (probabilmente l'importo principale)
                amounts = [float(m) for m in matches if float(m) > 100]
                if amounts:
                    return f"{max(amounts):.2f}"
            except:
                pass

        return ""

    @field_validator("CIG", "CUP", mode="before")
    def validate_codes(cls, v):
        """Valida e pulisce i codici CIG/CUP."""
        if not v:
            return ""

        v = str(v).strip().upper()

        if v in ["NON SPECIFICATO", "N/A", "NULL", "NONE"]:
            return ""

        # Rimuovi caratteri non alfanumerici
        v = re.sub(r"[^A-Z0-9]", "", v)

        return v


class GruppoLottiOCDS(BaseModel):
    """Gruppo di lotti per un singolo appalto."""

    Lotti: List[LottoOCDS]


class QuantiLottiOCDS(BaseModel):
    """Conteggio lotti."""

    NumeroLotti: int = Field(default=1)


# ============= FUNZIONI HELPER =============


def hash_text(text: str) -> str:
    """Genera hash SHA256 del testo."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def calculate_sconto_percentuale(importo: str, sconto: str) -> str:
    """Calcola la percentuale di sconto."""
    if not sconto or not importo:
        return ""

    try:
        # Se lo sconto contiene già %
        if "%" in str(sconto):
            return re.sub(r"[^\d.,]", "", sconto).replace(",", ".")

        # Altrimenti calcola la percentuale
        importo_val = float(str(importo).replace(",", "."))
        sconto_val = float(str(sconto).replace(",", "."))

        if importo_val > 0:
            percentuale = (sconto_val / importo_val) * 100
            return f"{percentuale:.2f}"

    except:
        pass

    return ""


def calculate_scadenza(data_aggiudicazione: str, durata: str) -> str:
    """Calcola la data di scadenza."""
    if not data_aggiudicazione or not durata:
        return ""

    try:
        data = datetime.strptime(data_aggiudicazione, "%d/%m/%Y")
        giorni = int(durata)
        scadenza = data + timedelta(days=giorni)
        return scadenza.strftime("%d/%m/%Y")
    except:
        return ""


def applica_filtro_categoria(testo: str) -> bool:
    """Applica filtri ampliati per catturare gare rilevanti."""
    if not testo:
        return False

    testo_lower = str(testo).lower()

    patterns = [
        r"illumin|lampioni|lampade|pubblica illuminazione|led",
        r"videosorveglian|telecamer|tvcc|sicurezza urbana",
        r"energ|elettric|termic|riscaldament|climatizzazion|fotovoltaic",
        r"edific|manutenzion|ristrutturazion|riqualificazion|scuol",
        r"colonnin|ricaric|e-mobility|mobilità elettrica",
        r"parchegg|sosta|mobilità|viabilità|traffico",
        r"smart city|smart|iot|sensori|monitoraggio",
        r"verde pubblic|irrigazion|parchi|giardini",
        r"strad|marciapiedi|asfalto|segnaletic",
        r"impianti sportiv|palestra|piscina",
        r"servizio pubblic|global service|facility",
        r"emergenza|protezione civile|antincendio",
        r"galleri|tunnel",
        r"acquedott|idric|fognatur",
        r"rifiuti|nettezza|raccolta differenziata",
    ]

    for pattern in patterns:
        if re.search(pattern, testo_lower):
            return True

    return False


# ============= FUNZIONI o3 =============


def extract_lot_count_o3(text: str) -> int:
    """Usa o3 per contare i lotti nel testo."""
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {
                    "role": "system",
                    "content": "Conta quanti lotti distinti sono presenti nel testo. Rispondi SOLO con un numero.",
                },
                {"role": "user", "content": text[:3000]},
            ],
            temperature=0,
            max_tokens=10,
        )

        count_text = response.choices[0].message.content.strip()
        count = (
            int(re.findall(r"\d+", count_text)[0])
            if re.findall(r"\d+", count_text)
            else 1
        )
        return min(count, 10)  # Limita a max 10 lotti

    except:
        return 1


def extract_single_lot_o3(
    text: str, buyer_name: str = "", amount: float = 0
) -> LottoOCDS:
    """Usa o3 per estrarre un singolo lotto."""

    prompt = f"""Analizza questo testo di appalto pubblico ed estrai le informazioni strutturate.

TESTO: {text[:3000]}
ENTE: {buyer_name}
IMPORTO: {amount}

Restituisci un JSON con TUTTI questi campi (usa stringa vuota "" se non trovato):
{{
  "Oggetto": "descrizione breve dell'oggetto",
  "Categoria": "uno tra: {[e.value for e in CategoriaLotto if e.value]}",
  "TipoIlluminazione": "uno tra: {[e.value for e in TipoIlluminazione if e.value][:10]}... o altro",
  "TipoEfficientamento": "uno tra: {[e.value for e in TipoEfficientamento if e.value][:10]}... o altro",
  "TipoAppalto": "uno tra: {[e.value for e in TipoAppalto if e.value]}",
  "TipoIntervento": "uno tra: {[e.value for e in TipoIntervento if e.value][:10]}... o altro",
  "TipoImpianto": "uno tra: {[e.value for e in TipoImpianto if e.value][:10]}... o altro",
  "TipoEnergia": "uno tra: {[e.value for e in TipoEnergia if e.value]}",
  "TipoOperazione": "uno tra: {[e.value for e in TipoOperazione if e.value]}",
  "Procedura": "tipo di procedura (es: Aperta, Ristretta, Negoziata)",
  "AmministrazioneAggiudicatrice": "nome ente",
  "OfferteRicevute": "numero offerte",
  "DurataAppalto": "durata in giorni",
  "DataAggiudicazione": "data formato dd/mm/yyyy",
  "Scadenza": "data scadenza formato dd/mm/yyyy",
  "ImportoAggiudicazione": "importo numerico",
  "Sconto": "sconto se presente",
  "Comune": "comune/località",
  "Aggiudicatario": "nome aggiudicatario",
  "CIG": "codice CIG se presente",
  "CUP": "codice CUP se presente"
}}

Rispondi SOLO con il JSON, nessun altro testo."""

    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {
                    "role": "system",
                    "content": "Sei un esperto di appalti pubblici. Estrai informazioni con precisione.",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
            max_tokens=800,
        )

        json_text = response.choices[0].message.content
        json_text = re.sub(r"```json\n?", "", json_text)
        json_text = re.sub(r"```\n?", "", json_text)

        data = json.loads(json_text)

        # Crea oggetto LottoOCDS con i dati estratti
        lotto = LottoOCDS(**data)

        # Aggiungi dati mancanti dai parametri
        if not lotto.AmministrazioneAggiudicatrice and buyer_name:
            lotto.AmministrazioneAggiudicatrice = buyer_name
        if not lotto.ImportoAggiudicazione and amount > 0:
            lotto.ImportoAggiudicazione = f"{amount:.2f}"

        return lotto

    except Exception as e:
        logger.debug(f"Errore estrazione o3: {e}")
        # Fallback con dati base
        return LottoOCDS(
            Oggetto=text[:200] if text else "",
            AmministrazioneAggiudicatrice=buyer_name,
            ImportoAggiudicazione=f"{amount:.2f}" if amount else "",
        )


def extract_multiple_lots_o3(
    text: str, lot_count: int, buyer_name: str = "", amount: float = 0
) -> List[LottoOCDS]:
    """Usa o3 per estrarre multipli lotti."""

    if lot_count <= 1:
        return [extract_single_lot_o3(text, buyer_name, amount)]

    prompt = f"""Dividi questo testo in {lot_count} lotti distinti ed estrai le informazioni per ciascuno.

TESTO: {text[:4000]}

Per OGNI lotto, crea un oggetto JSON con tutti i campi del modello LottoOCDS.
Restituisci un JSON con struttura: {{"Lotti": [...]}}

Campi richiesti per ogni lotto: Oggetto, Categoria, TipoIlluminazione, TipoEfficientamento, 
TipoAppalto, TipoIntervento, TipoImpianto, TipoEnergia, TipoOperazione, Procedura, 
AmministrazioneAggiudicatrice, OfferteRicevute, DurataAppalto, DataAggiudicazione, 
Scadenza, ImportoAggiudicazione, Sconto, Comune, Aggiudicatario, CIG, CUP.

Usa stringa vuota "" per campi non trovati. Rispondi SOLO con il JSON."""

    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {
                    "role": "system",
                    "content": "Estrai informazioni strutturate per ogni lotto identificato.",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
            max_tokens=2000,
        )

        json_text = response.choices[0].message.content
        json_text = re.sub(r"```json\n?", "", json_text)
        json_text = re.sub(r"```\n?", "", json_text)

        data = json.loads(json_text)
        lotti_data = data.get("Lotti", [])

        lotti = []
        for lotto_data in lotti_data:
            lotto = LottoOCDS(**lotto_data)
            if not lotto.AmministrazioneAggiudicatrice and buyer_name:
                lotto.AmministrazioneAggiudicatrice = buyer_name
            lotti.append(lotto)

        return lotti if lotti else [LottoOCDS()]

    except Exception as e:
        logger.debug(f"Errore estrazione multipla: {e}")
        return [extract_single_lot_o3(text, buyer_name, amount)]


# ============= CLASSE ANALYZER =============


class OCDSAnalyzerComplete:
    """
    Analizza file OCDS con categorizzazione completa usando o3.
    Replica tutti i campi del sistema originale.
    """

    def __init__(self, use_filter: bool = True, use_ai: bool = True):
        """
        Inizializza l'analizzatore.

        Args:
            use_filter: Se True, applica il filtro categorie
            use_ai: Se True, usa o3 per categorizzazione completa
        """
        self.use_filter = use_filter
        self.use_ai = use_ai
        self.checkpoint_manager = CheckpointManager()
        self.checkpoint_manager.create_session("ocds_analyzer_complete")

    def process_ocds_release(self, release: dict, file_name: str) -> List[dict]:
        """
        Processa un singolo release OCDS.

        Returns:
            Lista di record (uno per lotto)
        """
        records = []

        # Estrai dati base
        tender = release.get("tender", {})
        tender_title = tender.get("title", "")
        tender_description = tender.get("description", "")
        tender_value = tender.get("value", {}).get("amount", 0)
        tender_currency = tender.get("value", {}).get("currency", "EUR")

        buyer = release.get("buyer", {})
        buyer_name = buyer.get("name", "")
        buyer_id = buyer.get("id", "")

        # Combina testi
        testo_completo = f"{tender_title} {tender_description}"

        # Applica filtro se richiesto
        if self.use_filter and not applica_filtro_categoria(testo_completo):
            return []

        # Hash per gruppo
        unique_hash = hash_text(testo_completo)

        # Usa AI per estrarre lotti se richiesto
        if self.use_ai and testo_completo.strip():
            # Conta lotti
            lot_count = extract_lot_count_o3(testo_completo)

            # Estrai lotti
            lotti = extract_multiple_lots_o3(
                testo_completo, lot_count, buyer_name, tender_value
            )

            # Crea record per ogni lotto
            for i, lotto in enumerate(lotti):
                # Calcola campi derivati
                sconto_perc = calculate_sconto_percentuale(
                    lotto.ImportoAggiudicazione, lotto.Sconto
                )

                if (
                    not lotto.Scadenza
                    and lotto.DataAggiudicazione
                    and lotto.DurataAppalto
                ):
                    lotto.Scadenza = calculate_scadenza(
                        lotto.DataAggiudicazione, lotto.DurataAppalto
                    )

                record = {
                    # Metadati
                    "fonte": "OCDS",
                    "file": file_name,
                    "release_id": release.get("id", ""),
                    "release_date": release.get("date", ""),
                    "CodiceGruppo": unique_hash,
                    "Lotto": f"Lotto {i + 1}",
                    "NumeroLotti": lot_count,
                    # Campi strutturati dal modello
                    "Oggetto": lotto.Oggetto,
                    "Categoria": lotto.Categoria.value,
                    "TipoIlluminazione": lotto.TipoIlluminazione.value,
                    "TipoEfficientamento": lotto.TipoEfficientamento.value,
                    "TipoAppalto": lotto.TipoAppalto.value,
                    "TipoIntervento": lotto.TipoIntervento.value,
                    "TipoImpianto": lotto.TipoImpianto.value,
                    "TipoEnergia": lotto.TipoEnergia.value,
                    "TipoOperazione": lotto.TipoOperazione.value,
                    "Procedura": lotto.Procedura,
                    "AmministrazioneAggiudicatrice": lotto.AmministrazioneAggiudicatrice,
                    "OfferteRicevute": lotto.OfferteRicevute,
                    "DurataAppalto": lotto.DurataAppalto,
                    "DataAggiudicazione": lotto.DataAggiudicazione,
                    "Scadenza": lotto.Scadenza,
                    "ImportoAggiudicazione": lotto.ImportoAggiudicazione,
                    "Sconto": lotto.Sconto,
                    "Sconto %": sconto_perc,
                    "Comune": lotto.Comune,
                    "Aggiudicatario": lotto.Aggiudicatario,
                    "CIG": lotto.CIG,
                    "CUP": lotto.CUP,
                    # Campi aggiuntivi OCDS
                    "tender_status": tender.get("status", ""),
                    "tender_currency": tender_currency,
                    "buyer_id": buyer_id,
                    "initiationType": release.get("initiationType", ""),
                    "tag": ",".join(release.get("tag", [])),
                    # Placeholder per campi futuri
                    "VerbaleAggiudicazione": None,
                    "ServizioLuce": None,
                }

                records.append(record)
        else:
            # Senza AI, crea record base
            record = {
                "fonte": "OCDS",
                "file": file_name,
                "release_id": release.get("id", ""),
                "release_date": release.get("date", ""),
                "CodiceGruppo": unique_hash,
                "Lotto": "Lotto 1",
                "NumeroLotti": 1,
                "Oggetto": tender_title[:500],
                "Categoria": "",
                "TipoIlluminazione": "",
                "TipoEfficientamento": "",
                "TipoAppalto": "",
                "TipoIntervento": "",
                "TipoImpianto": "",
                "TipoEnergia": "",
                "TipoOperazione": "",
                "Procedura": tender.get("procurementMethod", ""),
                "AmministrazioneAggiudicatrice": buyer_name,
                "ImportoAggiudicazione": str(tender_value) if tender_value else "",
                "tender_status": tender.get("status", ""),
                "tender_currency": tender_currency,
            }
            records.append(record)

        return records

    def process_ocds_file(self, file_path: Path) -> pd.DataFrame:
        """
        Processa un file OCDS completo.

        Args:
            file_path: Path del file OCDS

        Returns:
            DataFrame con tutti i campi strutturati
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            all_records = []
            releases = data.get("releases", [])

            logger.info(f"Processamento {file_path.name}: {len(releases)} releases")

            for release in releases:
                records = self.process_ocds_release(release, file_path.name)
                all_records.extend(records)

            if all_records:
                df = pd.DataFrame(all_records)
                logger.info(f"  → {len(df)} record estratti da {file_path.name}")
                return df
            else:
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"Errore processamento {file_path}: {e}")
            return pd.DataFrame()

    @timer
    def run(self) -> None:
        """
        Esegue l'analisi completa di tutti i file OCDS.
        """
        logger.info("=" * 70)
        logger.info("ANALISI OCDS COMPLETA CON o3")
        logger.info("=" * 70)
        logger.info(f"Filtro categorie: {'ATTIVO' if self.use_filter else 'DISATTIVO'}")
        logger.info(
            f"Categorizzazione AI (o3): {'ATTIVA' if self.use_ai else 'DISATTIVA'}"
        )

        all_data = []

        # Trova file OCDS
        ocds_files = list(config.OCDS_DIR.glob("*.json"))
        logger.info(f"File OCDS trovati: {len(ocds_files)}")

        # Processa con ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {}

            for ocds_file in ocds_files:
                task_id = f"analyze_ocds_complete_{ocds_file.name}"

                if self.checkpoint_manager.should_skip(task_id):
                    logger.info(f"⏭️ Già analizzato: {ocds_file.name}")
                    continue

                future = executor.submit(self.process_ocds_file, ocds_file)
                futures[future] = (ocds_file, task_id)

            # Raccogli risultati
            for future in as_completed(futures):
                ocds_file, task_id = futures[future]
                try:
                    df = future.result()
                    if not df.empty:
                        all_data.append(df)
                        self.checkpoint_manager.mark_completed(
                            task_id, {"records": len(df)}
                        )
                    else:
                        self.checkpoint_manager.mark_completed(task_id, {"records": 0})

                except Exception as e:
                    logger.error(f"Errore {ocds_file}: {e}")
                    self.checkpoint_manager.mark_failed(task_id, str(e))

        # Combina e salva
        if all_data:
            df_combined = pd.concat(all_data, ignore_index=True)

            # Rimuovi duplicati per CIG se presente
            if "CIG" in df_combined.columns:
                # Mantieni solo CIG validi
                df_combined = df_combined[
                    (df_combined["CIG"].notna()) & (df_combined["CIG"] != "")
                    | (df_combined["CIG"].isna())
                ]
                df_combined.drop_duplicates(subset=["CIG"], keep="first", inplace=True)

            # Statistiche
            logger.info("\n" + "=" * 70)
            logger.info("STATISTICHE FINALI")
            logger.info("=" * 70)
            logger.info(f"Record totali: {len(df_combined):,}")
            logger.info(f"Colonne: {len(df_combined.columns)}")

            if "Categoria" in df_combined.columns:
                logger.info("\nTop 10 Categorie:")
                for cat, count in (
                    df_combined["Categoria"].value_counts().head(10).items()
                ):
                    if cat:  # Solo se non vuoto
                        logger.info(f"  {cat}: {count:,}")

            if "TipoIntervento" in df_combined.columns:
                logger.info("\nTop 5 Tipi Intervento:")
                for tipo, count in (
                    df_combined["TipoIntervento"].value_counts().head(5).items()
                ):
                    if tipo:
                        logger.info(f"  {tipo}: {count:,}")

            if "ImportoAggiudicazione" in df_combined.columns:
                # Converti in numerico per statistiche
                df_combined["ImportoNum"] = pd.to_numeric(
                    df_combined["ImportoAggiudicazione"], errors="coerce"
                )
                totale = df_combined["ImportoNum"].sum()
                if totale > 0:
                    logger.info(f"\nImporto totale: €{totale:,.2f}")

            # Salva risultato
            output_file = config.get_file_path("OCDS_Complete.xlsx", "output")

            if len(df_combined) > 1000000:
                csv_file = output_file.with_suffix(".csv")
                df_combined.to_csv(csv_file, index=False)
                logger.info(f"\n✅ Salvato in CSV: {csv_file}")

                # Salva sample Excel
                sample_file = output_file.parent / f"{output_file.stem}_sample.xlsx"
                df_combined.head(50000).to_excel(sample_file, index=False)
                logger.info(f"✅ Sample salvato: {sample_file}")
            else:
                df_combined.to_excel(output_file, index=False)
                logger.info(f"\n✅ Salvato: {output_file}")

            self.checkpoint_manager.mark_completed(
                "ocds_analysis_complete_final",
                {"total_records": len(df_combined), "output": str(output_file)},
            )

            logger.info("=" * 70)

        else:
            logger.warning("Nessun dato OCDS processato")
