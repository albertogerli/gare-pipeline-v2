"""
Utilità per la gestione delle date.

Questo modulo fornisce funzioni per il parsing e la manipolazione
di date in vari formati.
"""

import re
from datetime import datetime, timedelta
from typing import Optional, Tuple

from dateutil import parser as date_parser

# Mappa mesi italiani
ITALIAN_MONTHS = {
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

# Formati di data comuni
DATE_FORMATS = [
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%d.%m.%Y",
    "%d/%m/%y",
    "%d-%m-%y",
    "%d.%m.%y",
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%Y.%m.%d",
    "%m/%d/%Y",
    "%m-%d-%Y",
    "%m.%d.%Y",
    "%Y%m%d",
]


def parse_date(date_str: str, dayfirst: bool = True) -> Optional[datetime]:
    """
    Converte una stringa in oggetto datetime.

    Args:
        date_str: Stringa contenente la data
        dayfirst: Se True, interpreta il primo numero come giorno

    Returns:
        Oggetto datetime o None se parsing fallisce

    Examples:
        >>> parse_date("15/03/2024")
        datetime.datetime(2024, 3, 15, 0, 0)
    """
    if not date_str:
        return None

    date_str = date_str.strip().lower()

    # Gestisci casi speciali
    if "non specificat" in date_str:
        return None

    # Sostituisci mesi italiani
    for month_it, month_num in ITALIAN_MONTHS.items():
        date_str = re.sub(r"\b" + month_it + r"\b", month_num, date_str)

    # Prova formati predefiniti
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    # Prova parsing generico
    try:
        return date_parser.parse(date_str, dayfirst=dayfirst)
    except (ValueError, TypeError):
        return None


def format_date(date_obj: Optional[datetime], format_str: str = "%d/%m/%Y") -> str:
    """
    Formatta un oggetto datetime in stringa.

    Args:
        date_obj: Oggetto datetime da formattare
        format_str: Formato di output desiderato

    Returns:
        Data formattata o stringa vuota

    Examples:
        >>> format_date(datetime(2024, 3, 15))
        '15/03/2024'
    """
    if not date_obj:
        return ""

    try:
        return date_obj.strftime(format_str)
    except (AttributeError, ValueError):
        return ""


def calculate_duration(start_date: str, end_date: str) -> Optional[int]:
    """
    Calcola la durata in giorni tra due date.

    Args:
        start_date: Data di inizio
        end_date: Data di fine

    Returns:
        Numero di giorni o None se calcolo fallisce

    Examples:
        >>> calculate_duration("01/01/2024", "31/01/2024")
        30
    """
    start = parse_date(start_date)
    end = parse_date(end_date)

    if not start or not end:
        return None

    delta = end - start
    return delta.days


def parse_duration_text(duration_text: str) -> Optional[int]:
    """
    Estrae la durata in giorni da un testo descrittivo.

    Args:
        duration_text: Testo contenente la durata

    Returns:
        Numero di giorni o None

    Examples:
        >>> parse_duration_text("3 anni")
        1095
        >>> parse_duration_text("6 mesi")
        180
    """
    if not duration_text:
        return None

    duration_text = duration_text.lower().strip()

    if "non specificat" in duration_text:
        return None

    try:
        # Gestione diretta di giorni, mesi, anni
        if "giorn" in duration_text:
            days = int(re.findall(r"\d+", duration_text)[0])
            return days
        elif "mes" in duration_text:
            months = int(re.findall(r"\d+", duration_text)[0])
            return months * 30
        elif "ann" in duration_text:
            years = int(re.findall(r"\d+", duration_text)[0])
            return years * 365
        elif "ventennale" in duration_text:
            return 20 * 365
        elif "decennale" in duration_text:
            return 10 * 365
        elif "quinquennale" in duration_text:
            return 5 * 365
        elif "triennale" in duration_text:
            return 3 * 365
        elif "biennale" in duration_text:
            return 2 * 365
        elif "annuale" in duration_text:
            return 365
        elif "semestrale" in duration_text:
            return 180
        elif "trimestrale" in duration_text:
            return 90
        elif "mensile" in duration_text:
            return 30
        elif "settimanale" in duration_text:
            return 7

        # Gestione range di date
        dates = re.findall(r"\d{2}[./-]\d{2}[./-]\d{4}", duration_text)
        if len(dates) == 2:
            return calculate_duration(dates[0], dates[1])

    except (IndexError, ValueError):
        pass

    return None


def add_days_to_date(date_str: str, days: int) -> str:
    """
    Aggiunge giorni a una data.

    Args:
        date_str: Data di partenza
        days: Numero di giorni da aggiungere

    Returns:
        Nuova data formattata

    Examples:
        >>> add_days_to_date("01/01/2024", 30)
        '31/01/2024'
    """
    date_obj = parse_date(date_str)

    if not date_obj:
        return ""

    new_date = date_obj + timedelta(days=days)
    return format_date(new_date)


def get_date_range(
    start_date: str, end_date: str
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Restituisce un range di date come tuple di datetime.

    Args:
        start_date: Data di inizio
        end_date: Data di fine

    Returns:
        Tupla (start, end) di oggetti datetime
    """
    return parse_date(start_date), parse_date(end_date)
