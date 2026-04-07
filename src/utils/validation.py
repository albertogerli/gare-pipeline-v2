"""
Utilità per la validazione dei dati.

Questo modulo fornisce funzioni per validare vari tipi di dati
utilizzati nel sistema.
"""

import re
from typing import Optional


def validate_cig(cig: str) -> Optional[str]:
    """
    Valida e normalizza un codice CIG.

    Il CIG (Codice Identificativo Gara) è un codice alfanumerico
    di 10 caratteri assegnato dall'ANAC.

    Args:
        cig: Codice CIG da validare

    Returns:
        CIG normalizzato o None se non valido

    Examples:
        >>> validate_cig("1234567890")
        '1234567890'
        >>> validate_cig("ZB12345678")
        'ZB12345678'
    """
    if not cig:
        return None

    cig = cig.strip().upper()

    # Rimuovi valori placeholder comuni
    if cig in ["NON SPECIFICATO", "NON SPECIFICATA", "N/A", "N.A.", "-", ""]:
        return None

    # CIG standard: 10 caratteri alfanumerici
    # CIG smart: Z + 9 caratteri alfanumerici o X + 9 caratteri
    if re.match(r"^[0-9A-F]{10}$", cig):
        return cig
    elif re.match(r"^[ZX][0-9A-F]{9}$", cig):
        return cig

    return None


def validate_cup(cup: str) -> Optional[str]:
    """
    Valida e normalizza un codice CUP.

    Il CUP (Codice Unico di Progetto) è un codice alfanumerico
    di 15 caratteri per progetti di investimento pubblico.

    Args:
        cup: Codice CUP da validare

    Returns:
        CUP normalizzato o None se non valido

    Examples:
        >>> validate_cup("B12F11000370004")
        'B12F11000370004'
    """
    if not cup:
        return None

    cup = cup.strip().upper()

    # Rimuovi valori placeholder comuni
    if cup in ["NON SPECIFICATO", "NON SPECIFICATA", "N/A", "N.A.", "-", ""]:
        return None

    # CUP: 15 caratteri alfanumerici (lettera + 14 caratteri)
    if re.match(r"^[A-Z][0-9A-Z]{14}$", cup):
        return cup

    return None


def validate_partita_iva(partita_iva: str) -> Optional[str]:
    """
    Valida una partita IVA italiana.

    Args:
        partita_iva: Partita IVA da validare

    Returns:
        Partita IVA normalizzata o None se non valida

    Examples:
        >>> validate_partita_iva("12345678901")
        '12345678901'
    """
    if not partita_iva:
        return None

    # Rimuovi spazi e caratteri non numerici
    partita_iva = re.sub(r"\D", "", partita_iva)

    # Partita IVA italiana: 11 cifre
    if len(partita_iva) != 11:
        return None

    # Algoritmo di validazione partita IVA
    if not _validate_partita_iva_checksum(partita_iva):
        return None

    return partita_iva


def _validate_partita_iva_checksum(partita_iva: str) -> bool:
    """
    Valida il checksum di una partita IVA.

    Args:
        partita_iva: Partita IVA da validare (11 cifre)

    Returns:
        True se il checksum è valido
    """
    if len(partita_iva) != 11 or not partita_iva.isdigit():
        return False

    odd_sum = sum(int(partita_iva[i]) for i in range(0, 10, 2))

    even_sum = 0
    for i in range(1, 10, 2):
        digit = int(partita_iva[i]) * 2
        even_sum += digit if digit < 10 else digit - 9

    total = odd_sum + even_sum
    check_digit = (10 - (total % 10)) % 10

    return check_digit == int(partita_iva[10])


def validate_codice_fiscale(codice_fiscale: str) -> Optional[str]:
    """
    Valida un codice fiscale italiano.

    Args:
        codice_fiscale: Codice fiscale da validare

    Returns:
        Codice fiscale normalizzato o None se non valido

    Examples:
        >>> validate_codice_fiscale("RSSMRA80A01H501U")
        'RSSMRA80A01H501U'
    """
    if not codice_fiscale:
        return None

    codice_fiscale = codice_fiscale.strip().upper()

    # Codice fiscale: 16 caratteri alfanumerici
    # o 11 cifre per persone giuridiche (uguale a partita IVA)
    if len(codice_fiscale) == 16:
        if re.match(
            r"^[A-Z]{6}[0-9]{2}[A-Z][0-9]{2}[A-Z][0-9]{3}[A-Z]$", codice_fiscale
        ):
            return codice_fiscale
    elif len(codice_fiscale) == 11:
        if codice_fiscale.isdigit():
            return validate_partita_iva(codice_fiscale)

    return None


def validate_email(email: str) -> Optional[str]:
    """
    Valida un indirizzo email.

    Args:
        email: Email da validare

    Returns:
        Email normalizzata o None se non valida

    Examples:
        >>> validate_email("test@example.com")
        'test@example.com'
    """
    if not email:
        return None

    email = email.strip().lower()

    # Pattern email semplificato
    pattern = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"

    if re.match(pattern, email):
        return email

    return None


def validate_importo(importo: str) -> Optional[float]:
    """
    Valida e converte un importo monetario.

    Args:
        importo: Stringa contenente l'importo

    Returns:
        Importo come float o None se non valido

    Examples:
        >>> validate_importo("1.234,56")
        1234.56
        >>> validate_importo("€ 1.000,00")
        1000.0
    """
    if not importo:
        return None

    # Rimuovi simboli di valuta e spazi
    importo = re.sub(r"[€$£¥\s]", "", str(importo))

    # Gestisci formato italiano (1.234,56) e internazionale (1,234.56)
    if "," in importo and "." in importo:
        # Determina quale è il separatore decimale
        if importo.rindex(",") > importo.rindex("."):
            # Formato italiano: punto per migliaia, virgola per decimali
            importo = importo.replace(".", "").replace(",", ".")
        else:
            # Formato internazionale: virgola per migliaia, punto per decimali
            importo = importo.replace(",", "")
    elif "," in importo:
        # Solo virgola: assumiamo sia il separatore decimale
        importo = importo.replace(",", ".")

    try:
        value = float(importo)
        return value if value >= 0 else None
    except ValueError:
        return None
