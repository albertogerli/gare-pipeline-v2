"""
Category filters for public procurement data.

Pure regex-based filtering (no LLM required).
Each category has a list of compiled patterns with specificity weights.
A record matches if ANY pattern in a category matches.

Confidence scoring: each pattern has a specificity weight (0.0-1.0).
More specific patterns (e.g. "illuminazione pubblica") get higher weight
than generic ones (e.g. "led"). The final confidence for a category is
computed from the number and specificity of matched patterns.
"""

import re
from typing import Optional

# ---------------------------------------------------------------------------
# Pattern definitions with specificity weights
# Each entry is (pattern_string, specificity_weight)
# ---------------------------------------------------------------------------
_ILLUMINAZIONE_PATTERNS: list[tuple[str, float]] = [
    (r"illuminazion[ei]\s+pubblic", 1.0),
    (r"pubblica\s+illuminazione", 1.0),
    (r"impianti?\s+di\s+illuminazione", 0.9),
    (r"corpi?\s+illuminant", 0.8),
    (r"lampion[ei]", 0.7),
    (r"armatur[ae]\s+stradal", 0.8),
    (r"relamping", 0.7),
    (r"punti?\s+luce", 0.7),
    (r"apparecchi?\s+illumin", 0.7),
    (r"\bled\b.*(?:pubblic|strad|comunal|illumin)", 0.6),
    (r"(?:pubblic|strad|comunal).*\bled\b", 0.6),
    (r"sostituzione\s+(?:lamp|corpi\s+illum)", 0.8),
    (r"efficientamento.*illumin", 0.9),
    (r"illumin.*efficientamento", 0.9),
    (r"gestione\s+(?:integrata\s+)?illumin", 0.9),
    (r"servizio\s+luce", 0.7),
    (r"global\s+service.*illumin", 0.8),
    (r"illumin.*global\s+service", 0.8),
    (r"manutenzione.*illumin", 0.8),
    (r"illumin.*manutenzione", 0.8),
    (r"smart\s*light", 0.7),
    (r"telegestione.*illumin", 0.8),
    (r"illumin.*telegestione", 0.8),
    (r"pali?\s+(?:di\s+)?illuminazione", 0.8),
    (r"torr[ei]\s+faro", 0.7),
    (r"illuminazione\s+(?:votiv|cimiteri|sportiv|architetton)", 0.8),
    (r"luce\s+(?:pubblica|stradale|urbana)", 0.8),
    (r"rinnov.*illumin", 0.7),
    (r"illumin.*rinnov", 0.7),
    (r"progett.*illumin", 0.6),
    (r"illumin.*progett", 0.6),
]

_VIDEOSORVEGLIANZA_PATTERNS: list[tuple[str, float]] = [
    (r"videosorveglian", 1.0),
    (r"telecamer[ae]", 0.8),
    (r"tvcc", 0.9),
    (r"sistema.*sorveglian", 0.7),
    (r"controllo\s+access[io]", 0.6),
    (r"antintrusione", 0.6),
    (r"sicurezza\s+(?:urbana|cittadina).*(?:video|telecam)", 0.9),
]

_SMART_CITY_PATTERNS: list[tuple[str, float]] = [
    (r"smart\s*city", 1.0),
    (r"citt[aà]\s+intelligent", 0.9),
    (r"\biot\b.*(?:pubblic|urban|città|comune)", 0.8),
    (r"fibra\s+ottic", 0.7),
    (r"digitalizzazione.*(?:servizi|infrastrutt)", 0.7),
    (r"telecontroll", 0.6),
    (r"telegestione", 0.5),
    (r"sensor[ei].*(?:pubblic|urban|città|comune)", 0.7),
    (r"infrastrutt.*digital", 0.7),
    (r"rete?\s+(?:dati|telecomunicazion)", 0.6),
]

_SMART_PARKING_PATTERNS: list[tuple[str, float]] = [
    (r"parcheggi?\s+intelligent", 1.0),
    (r"sosta\s+regolamentat", 0.8),
    (r"parcometr", 0.9),
    (r"parchimetr", 0.9),
    (r"parking\s+(?:smart|guid|sensor)", 0.9),
    (r"gestion.*parcheggi.*(?:automat|smart|sensor)", 0.8),
]

_ENERGIA_PATTERNS: list[tuple[str, float]] = [
    (r"fotovoltaic", 0.8),
    (r"pannell[oi]\s+solar[ei]", 0.8),
    (r"solare\s+termic", 0.8),
    (r"pompa?\s+(?:di\s+)?calor", 0.7),
    (r"biomass", 0.7),
    (r"geotermi[ac]", 0.8),
    (r"eolic[oa]", 0.8),
    (r"cogenerazion", 0.8),
    (r"trigenerazion", 0.8),
    (r"risparmio\s+energetic", 0.9),
    (r"efficien(?:tamento|za)\s+energetic", 1.0),
    (r"diagnosi\s+energetic", 0.8),
    (r"certificazione?\s+energetic", 0.7),
    (r"cappotto\s+termic", 0.7),
    (r"isolamento\s+termic", 0.7),
    (r"riqualificazione\s+energetic", 0.9),
    (r"produzione\s+energia", 0.7),
    (r"fornitura\s+(?:di\s+)?energia\s+elettric", 0.7),
    (r"energy\s+(?:service|performanc|saving)", 0.8),
    (r"esco\b", 0.6),
    (r"contabilizzazione\s+calor", 0.7),
    (r"termoregolazione", 0.7),
]

_EDIFICI_ENERGIA_PATTERNS: list[tuple[str, float]] = [
    (r"(?:scuol|ospedal|edific).*(?:riqualificazion|efficientament).*energetic", 1.0),
    (r"energetic.*(?:riqualificazion|efficientament).*(?:scuol|ospedal|edific)", 1.0),
    (r"impiant.*(?:termic|climatizzazion|riscaldament).*(?:edific|scuol|comunal)", 0.9),
    (r"(?:edific|scuol|comunal).*impiant.*(?:termic|climatizzazion|riscaldament)", 0.9),
    (r"adeguamento\s+impiant.*(?:edific|scuol|pubblic)", 0.8),
    (r"caldai.*(?:edific|scuol|pubblic|comunal)", 0.7),
]

_STRADE_INFRASTRUTTURE_PATTERNS: list[tuple[str, float]] = [
    (r"galleri[ae].*(?:impiant|illumin|ventilazion|sicurezz)", 0.8),
    (r"tunnel.*(?:impiant|illumin|ventilazion|sicurezz)", 0.8),
    (r"pist[ae]\s+ciclabil", 0.7),
    (r"rotatorie?\s+(?:stradal|urban)", 0.7),
    (r"infrastruttur.*(?:stradal|urban|viabilit)", 0.8),
    (r"manutenzion.*strad.*(?:illumin|segnaletic|semaf)", 0.8),
    (r"strad.*(?:illumin|segnaletic|semaf).*manutenzion", 0.8),
    (r"segnaletic.*stradal", 0.7),
]

_E_MOBILITY_PATTERNS: list[tuple[str, float]] = [
    (r"colonnin[ae]\s*(?:di\s*)?ricaric", 1.0),
    (r"ricarica\s*(?:per\s*)?(?:veicoli?\s*)?elettric", 0.9),
    (r"stazion[ei]\s*(?:di\s*)?ricaric", 0.8),
    (r"e[\-\s]?mobility", 0.9),
    (r"mobilit[àa]\s+elettric", 0.8),
    (r"veicol[oi]\s+elettric", 0.7),
    (r"auto\s+elettric", 0.6),
]

_ACQUA_FOGNATURE_PATTERNS: list[tuple[str, float]] = [
    (r"acquedott", 0.8),
    (r"rete?\s+idric", 0.8),
    (r"fognatur", 0.9),
    (r"depurator", 0.9),
    (r"collettore?\s+fognar", 0.9),
    (r"condott[ae].*(?:idric|acqua|fogn)", 0.8),
    (r"impianto?\s+(?:di\s+)?depurazion", 0.9),
]

_RIFIUTI_PATTERNS: list[tuple[str, float]] = [
    (r"raccolta\s+(?:differenziat|rifiut)", 0.9),
    (r"igiene\s+urban", 0.8),
    (r"discaric", 0.8),
    (r"compostaggio", 0.8),
    (r"termovalorizzator", 0.9),
    (r"gestione\s+(?:integrat\w+\s+)?rifiut", 0.9),
    (r"nettezza\s+urban", 0.8),
]

_VERDE_PUBBLICO_PATTERNS: list[tuple[str, float]] = [
    (r"verde\s+pubblic", 0.9),
    (r"arredo\s+urban", 0.7),
    (r"(?:parchi|giardini)\s+(?:pubblic|comunal|urban)", 0.9),
    (r"manutenzion.*(?:verde|parchi|giardini).*(?:pubblic|comunal)", 0.9),
]

# ---------------------------------------------------------------------------
# Build compiled patterns with weights
# ---------------------------------------------------------------------------
_CATEGORY_RAW: dict[str, list[tuple[str, float]]] = {
    "ILLUMINAZIONE": _ILLUMINAZIONE_PATTERNS,
    "VIDEOSORVEGLIANZA": _VIDEOSORVEGLIANZA_PATTERNS,
    "SMART_CITY": _SMART_CITY_PATTERNS,
    "SMART_PARKING": _SMART_PARKING_PATTERNS,
    "ENERGIA": _ENERGIA_PATTERNS,
    "EDIFICI_ENERGIA": _EDIFICI_ENERGIA_PATTERNS,
    "STRADE_INFRASTRUTTURE": _STRADE_INFRASTRUTTURE_PATTERNS,
    "E_MOBILITY": _E_MOBILITY_PATTERNS,
    "ACQUA_FOGNATURE": _ACQUA_FOGNATURE_PATTERNS,
    "RIFIUTI": _RIFIUTI_PATTERNS,
    "VERDE_PUBBLICO": _VERDE_PUBBLICO_PATTERNS,
}

# Compiled patterns: dict[category, list[tuple[compiled_pattern, weight]]]
_CATEGORY_COMPILED: dict[str, list[tuple[re.Pattern, float]]] = {
    cat: [(re.compile(p, re.IGNORECASE), w) for p, w in raw]
    for cat, raw in _CATEGORY_RAW.items()
}

# Backward-compatible flat dict (each value is list[Pattern])
CATEGORY_PATTERNS: dict[str, list[re.Pattern]] = {
    cat: [compiled for compiled, _w in entries]
    for cat, entries in _CATEGORY_COMPILED.items()
}

# ---------------------------------------------------------------------------
# Negative patterns to exclude false positives
# ---------------------------------------------------------------------------
NEGATIVE_PATTERNS = [
    re.compile(p, re.IGNORECASE) for p in [
        # Art / culture (not infrastructure)
        r"galleria?\s+d[\'\u2019]arte",
        r"galleria?\s+commercial[ei]",
        r"museo",
        r"mostra",
        r"MINISTERO\s+DELLA\s+CULTURA",
        r"via\s+galler",
        # Natural / solar light (not public lighting)
        r"luce\s+natural[ei]",
        r"luce\s+solar[ei]",
        # Temporary / decorative lighting (not infrastructure)
        r"illuminazione\s+nataliziz?a",
        r"illuminazione\s+festiv[ao]",
        r"luminari[ae]\s+(?:nataliziz?|festiv)",
        r"addobbi?\s+(?:nataliziz?|festiv|luminos)",
        # Furniture / interior (not public)
        r"lampad[ae]\s+da\s+(?:tavolo|ufficio|scrivania|interno)",
        r"lampad[ae]\s+(?:da\s+)?arredo",
        # "servizio luce" without illuminazione context -> handled by consip.py
        # Only exclude if it looks like a CONSIP framework reference
        r"servizio\s+luce\s+(?:\d|consip|convenzione|lotto\s+\d)",
    ]
]


def _check_negatives(text: str) -> bool:
    """
    Return True if text matches a negative pattern and should be excluded.

    Even with a negative match, we still allow through records that
    explicitly mention core ILLUMINAZIONE or VIDEOSORVEGLIANZA patterns.
    """
    for neg in NEGATIVE_PATTERNS:
        if neg.search(text):
            # Allow through if explicit core category match
            has_explicit = any(
                CATEGORY_PATTERNS[cat][0].search(text)
                for cat in ["ILLUMINAZIONE", "VIDEOSORVEGLIANZA"]
            )
            if not has_explicit:
                return True
    return False


# ===================================================================
# Public API
# ===================================================================

def match_categories_with_confidence(text: str) -> list[tuple[str, float]]:
    """
    Match text against all category patterns with confidence scoring.

    Confidence is computed as:
        base = max(specificity of matched patterns)
        bonus = 0.05 * (number_of_extra_matches - 1), capped at 0.15
        confidence = min(base + bonus, 1.0)

    This rewards texts that match multiple specific patterns.

    Args:
        text: Text to match (typically tender description).

    Returns:
        List of (category_name, confidence) tuples, sorted by confidence
        descending.  Empty list if no category matches.
    """
    if not text or not isinstance(text, str):
        return []

    text = text.strip()
    if len(text) < 5:
        return []

    # Check negative patterns
    if _check_negatives(text):
        return []

    results: list[tuple[str, float]] = []

    for category, weighted_patterns in _CATEGORY_COMPILED.items():
        matched_weights: list[float] = []
        for pat, weight in weighted_patterns:
            if pat.search(text):
                matched_weights.append(weight)

        if matched_weights:
            base = max(matched_weights)
            # Bonus for multiple pattern matches (up to +0.15)
            extra = len(matched_weights) - 1
            bonus = min(extra * 0.05, 0.15)
            confidence = min(base + bonus, 1.0)
            results.append((category, round(confidence, 3)))

    # Sort by confidence descending
    results.sort(key=lambda x: x[1], reverse=True)
    return results


def match_categories(text: str) -> list[str]:
    """
    Match text against all category patterns.

    Args:
        text: Text to match (typically tender description).

    Returns:
        List of matched category names (may be empty).
    """
    return [cat for cat, _conf in match_categories_with_confidence(text)]


def get_primary_category(text: str) -> Optional[str]:
    """
    Get the primary (highest-confidence) category for a text.

    Args:
        text: Text to categorize.

    Returns:
        Category name or None.
    """
    results = match_categories_with_confidence(text)
    return results[0][0] if results else None


def passes_filter(text: str) -> bool:
    """
    Check if text matches at least one category.

    Args:
        text: Text to check.

    Returns:
        True if at least one category matches.
    """
    return len(match_categories_with_confidence(text)) > 0
