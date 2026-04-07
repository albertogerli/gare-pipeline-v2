"""
Modelli Pydantic per i lotti di gara.

Questo modulo definisce i modelli per la strutturazione e validazione
dei dati relativi ai lotti di gara.
"""

import re
from datetime import datetime, timedelta
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator

from .enums import (
    CategoriaLotto,
    TipoAppalto,
    TipoEfficientamento,
    TipoEnergia,
    TipoIlluminazione,
    TipoImpianto,
    TipoIntervento,
    TipoOperazione,
)


class Lotto(BaseModel):
    """
    Modello per un singolo lotto di gara.

    Attributes:
        oggetto: Descrizione dell'oggetto del lotto
        categoria: Categoria principale del lotto
        tipo_illuminazione: Tipo specifico di illuminazione
        tipo_efficientamento: Tipo di efficientamento energetico
        tipo_appalto: Tipologia di appalto
        tipo_intervento: Tipo di intervento previsto
        tipo_impianto: Tipologia di impianto
        tipo_energia: Tipo di energia/fornitura
        tipo_operazione: Tipo di operazione
        procedura: Procedura di gara utilizzata
        amministrazione_aggiudicatrice: Ente aggiudicatore
        offerte_ricevute: Numero di offerte ricevute
        durata_appalto: Durata dell'appalto in giorni
        scadenza: Data di scadenza
        importo_aggiudicazione: Importo di aggiudicazione
        data_aggiudicazione: Data di aggiudicazione
        sconto: Percentuale di sconto
        comune: Comune di riferimento
        aggiudicatario: Nome dell'aggiudicatario
        cig: Codice Identificativo Gara
        cup: Codice Unico di Progetto
    """

    oggetto: str = Field(alias="Oggetto")
    categoria: CategoriaLotto = Field(alias="Categoria")
    tipo_illuminazione: TipoIlluminazione = Field(alias="TipoIlluminazione")
    tipo_efficientamento: TipoEfficientamento = Field(alias="TipoEfficientamento")
    tipo_appalto: TipoAppalto = Field(alias="TipoAppalto")
    tipo_intervento: TipoIntervento = Field(alias="TipoIntervento")
    tipo_impianto: TipoImpianto = Field(alias="TipoImpianto")
    tipo_energia: TipoEnergia = Field(alias="TipoEnergia")
    tipo_operazione: TipoOperazione = Field(alias="TipoOperazione")
    procedura: str = Field(alias="Procedura")
    amministrazione_aggiudicatrice: str = Field(alias="AmministrazioneAggiudicatrice")
    offerte_ricevute: str = Field(alias="OfferteRicevute")
    durata_appalto: str = Field(alias="DurataAppalto")
    scadenza: str = Field(alias="Scadenza")
    importo_aggiudicazione: str = Field(alias="ImportoAggiudicazione")
    data_aggiudicazione: str = Field(alias="DataAggiudicazione")
    sconto: str = Field(alias="Sconto")
    comune: str = Field(alias="Comune")
    aggiudicatario: str = Field(alias="Aggiudicatario")
    cig: str = Field(alias="CIG")
    cup: str = Field(alias="CUP")

    class Config:
        """Configurazione del modello."""

        populate_by_name = True
        str_strip_whitespace = True

    @field_validator("scadenza", "data_aggiudicazione", mode="before")
    def parse_and_standardize_date(cls, v: str) -> str:
        """
        Converte e standardizza le date in formato DD/MM/YYYY.

        Args:
            v: Stringa contenente la data

        Returns:
            Data standardizzata o stringa vuota se non valida
        """
        v = v.lower().strip()

        if "non specificat" in v:
            return ""

        # Mappa mesi italiani
        italian_months = {
            "gennaio": "01",
            "febbraio": "02",
            "marzo": "03",
            "aprile": "04",
            "maggio": "05",
            "giugno": "06",
            "luglio": "07",
            "agosto": "08",
            "settembre": "09",
            "ottobre": "10",
            "novembre": "11",
            "dicembre": "12",
        }

        for month, num in italian_months.items():
            v = re.sub(r"\b" + month + r"\b", num, v)

        # Formati di data supportati
        date_formats = [
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%d.%m.%Y",
            "%d/%m/%y",
            "%d-%m-%y",
            "%d.%m.%y",
            "%Y-%m-%d",
            "%Y/%m/%d",
        ]

        for fmt in date_formats:
            try:
                date_obj = datetime.strptime(v, fmt)
                return date_obj.strftime("%d/%m/%Y")
            except ValueError:
                continue

        return ""

    @field_validator("durata_appalto", mode="before")
    def convert_duration_to_days(cls, v: str) -> str:
        """
        Converte la durata dell'appalto in giorni.

        Args:
            v: Stringa contenente la durata

        Returns:
            Numero di giorni come stringa o stringa vuota
        """
        v = v.lower().strip()

        if "non specificat" in v:
            return ""

        try:
            # Gestione diretta di giorni, mesi, anni
            if "giorn" in v:
                days = int(re.findall(r"\d+", v)[0])
            elif "mes" in v:
                days = int(re.findall(r"\d+", v)[0]) * 30
            elif "ann" in v:
                days = int(re.findall(r"\d+", v)[0]) * 365
            elif "ventennale" in v:
                days = 20 * 365
            else:
                # Gestione date specifiche
                dates = re.findall(r"\d{2}[./-]\d{2}[./-]\d{4}", v)
                if len(dates) == 2:
                    start_date = datetime.strptime(
                        dates[0].replace("-", ".").replace("/", "."), "%d.%m.%Y"
                    )
                    end_date = datetime.strptime(
                        dates[1].replace("-", ".").replace("/", "."), "%d.%m.%Y"
                    )
                    days = (end_date - start_date).days
                else:
                    return ""

            return str(days)

        except (IndexError, ValueError):
            return ""

    @field_validator("offerte_ricevute", mode="before")
    def validate_offerte_ricevute(cls, v: str) -> str:
        """Estrae solo i numeri dalle offerte ricevute."""
        return re.sub(r"\D", "", v) if v else ""

    @field_validator("importo_aggiudicazione", mode="before")
    def extract_highest_number(cls, v: str) -> str:
        """
        Estrae l'importo più alto superiore a 100.

        Args:
            v: Stringa contenente l'importo

        Returns:
            Importo formattato o stringa vuota
        """
        v = v.strip()
        matches = re.findall(r"\d{1,3}(?:[\.\,]\d{3})*(?:,\d{2})?", v)
        highest_number = None

        for match in matches:
            number = match.replace(".", "").replace(",", ".")
            try:
                num_value = float(number)
                if num_value > 100:
                    if highest_number is None or num_value > highest_number:
                        highest_number = num_value
            except ValueError:
                continue

        return f"{highest_number:.2f}" if highest_number is not None else ""

    @field_validator(
        "oggetto",
        "procedura",
        "amministrazione_aggiudicatrice",
        "comune",
        "aggiudicatario",
        "cig",
        "cup",
        mode="before",
    )
    def strip_whitespace(cls, v: str) -> str:
        """Rimuove spazi bianchi in eccesso."""
        return v.strip() if isinstance(v, str) else v

    @field_validator("cig", "cup", mode="before")
    def validate_alphanumeric(cls, v: str) -> str:
        """
        Valida i codici CIG e CUP.

        Args:
            v: Codice da validare

        Returns:
            Codice validato in maiuscolo o stringa vuota
        """
        v = v.strip().lower()

        if v in ["non specificato", "non specificata", "n/a", ""]:
            return ""

        if not v.replace("-", "").isalnum():
            return ""

        return v.upper()

    @field_validator("sconto")
    def validate_discount(cls, v: str) -> str:
        """
        Valida il formato dello sconto.

        Args:
            v: Stringa contenente lo sconto

        Returns:
            Sconto validato o stringa vuota
        """
        discount_pattern = r"(?<!\d)(-?\d{1,3}(?:[.,]\d{3})*(?:[.,]\d+)?)\s*%?"
        discounts = re.findall(discount_pattern, v)

        return v if len(discounts) == 1 else ""


class GruppoLotti(BaseModel):
    """Modello per un gruppo di lotti."""

    lotti: List[Lotto] = Field(alias="Lotti")

    class Config:
        """Configurazione del modello."""

        populate_by_name = True


class QuantiLotti(BaseModel):
    """Modello per il conteggio dei lotti."""

    numero_lotti: int = Field(alias="NumeroLotti")

    class Config:
        """Configurazione del modello."""

        populate_by_name = True
