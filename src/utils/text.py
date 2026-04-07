"""
Utilità per la manipolazione del testo.

Questo modulo fornisce funzioni per la pulizia e l'elaborazione
di stringhe di testo.
"""

import hashlib
import re
from typing import Optional


def clean_text(text: str) -> str:
    """
    Pulisce e normalizza il testo rimuovendo spazi e caratteri speciali.

    Args:
        text: Testo da pulire

    Returns:
        Testo pulito e normalizzato

    Examples:
        >>> clean_text("  Testo\\n  con   spazi  ")
        'Testo con spazi'
    """
    if not text:
        return ""

    # Sostituisci newline letterali e reali
    text = text.replace("\\n", " ")
    text = text.replace("\n", " ")

    # Rimuovi caratteri non-breaking space
    text = text.replace("\xa0", " ")

    # Rimuovi spazi multipli
    text = re.sub(r"\s{2,}", " ", text)

    return text.strip()


def hash_text(input_text: str) -> str:
    """
    Genera un hash SHA-256 del testo fornito.

    Args:
        input_text: Testo da hashare

    Returns:
        Hash SHA-256 in formato esadecimale

    Examples:
        >>> len(hash_text("test"))
        64
    """
    if not input_text:
        return ""

    input_bytes = input_text.encode("utf-8")
    sha256 = hashlib.sha256()
    sha256.update(input_bytes)

    return sha256.hexdigest()


def normalize_text(text: str) -> str:
    """
    Normalizza il testo per confronti case-insensitive.

    Args:
        text: Testo da normalizzare

    Returns:
        Testo normalizzato in minuscolo
    """
    if not text:
        return ""

    return clean_text(text).lower()


def extract_numbers(text: str) -> Optional[float]:
    """
    Estrae il primo numero valido dal testo.

    Args:
        text: Testo contenente numeri

    Returns:
        Primo numero trovato o None

    Examples:
        >>> extract_numbers("Il prezzo è 123.45 euro")
        123.45
    """
    if not text:
        return None

    # Pattern per numeri con separatori italiani
    pattern = r"\d{1,3}(?:[.,]\d{3})*(?:[.,]\d+)?"
    matches = re.findall(pattern, text)

    for match in matches:
        # Converti formato italiano in formato standard
        number = match.replace(".", "").replace(",", ".")
        try:
            return float(number)
        except ValueError:
            continue

    return None


def truncate_text(text: str, max_length: int = 100, suffix: str = "...") -> str:
    """
    Tronca il testo alla lunghezza specificata.

    Args:
        text: Testo da troncare
        max_length: Lunghezza massima
        suffix: Suffisso da aggiungere se troncato

    Returns:
        Testo troncato

    Examples:
        >>> truncate_text("Testo molto lungo", 10)
        'Testo m...'
    """
    if not text or len(text) <= max_length:
        return text

    return text[: max_length - len(suffix)] + suffix


def remove_html_tags(text: str) -> str:
    """
    Rimuove tutti i tag HTML dal testo.

    Args:
        text: Testo contenente HTML

    Returns:
        Testo senza tag HTML

    Examples:
        >>> remove_html_tags("<p>Testo <b>bold</b></p>")
        'Testo bold'
    """
    if not text:
        return ""

    # Rimuovi tag HTML
    clean = re.sub("<.*?>", "", text)

    # Pulisci spazi residui
    return clean_text(clean)
