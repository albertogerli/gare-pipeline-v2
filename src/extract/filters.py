"""
Category filters for public procurement data.

Pure regex-based filtering (no LLM required).
Each category has a list of compiled patterns.
A record matches if ANY pattern in a category matches.
"""

import re
from typing import Optional

# Pre-compiled patterns per category
CATEGORY_PATTERNS: dict[str, list[re.Pattern]] = {
    "ILLUMINAZIONE": [
        re.compile(p, re.IGNORECASE) for p in [
            r"illuminazion[ei]\s+pubblic",
            r"pubblica\s+illuminazione",
            r"impianti?\s+di\s+illuminazione",
            r"corpi?\s+illuminant",
            r"lampion[ei]",
            r"armatur[ae]\s+stradal",
            r"relamping",
            r"punti?\s+luce",
            r"apparecchi?\s+illumin",
            r"\bled\b.*(?:pubblic|strad|comunal|illumin)",
            r"(?:pubblic|strad|comunal).*\bled\b",
            r"sostituzione\s+(?:lamp|corpi\s+illum)",
            r"efficientamento.*illumin",
            r"illumin.*efficientamento",
            r"gestione\s+(?:integrata\s+)?illumin",
            r"servizio\s+luce",
            r"global\s+service.*illumin",
            r"illumin.*global\s+service",
            r"manutenzione.*illumin",
            r"illumin.*manutenzione",
            r"smart\s*light",
            r"telegestione.*illumin",
            r"illumin.*telegestione",
            r"pali?\s+(?:di\s+)?illuminazione",
            r"torr[ei]\s+faro",
            r"illuminazione\s+(?:votiv|cimiteri|sportiv|architetton)",
            r"luce\s+(?:pubblica|stradale|urbana)",
            r"rinnov.*illumin",
            r"illumin.*rinnov",
            r"progett.*illumin",
            r"illumin.*progett",
        ]
    ],
    "VIDEOSORVEGLIANZA": [
        re.compile(p, re.IGNORECASE) for p in [
            r"videosorveglian",
            r"telecamer[ae]",
            r"tvcc",
            r"sistema.*sorveglian",
            r"controllo\s+access[io]",
            r"antintrusione",
            r"sicurezza\s+(?:urbana|cittadina).*(?:video|telecam)",
        ]
    ],
    "SMART_CITY": [
        re.compile(p, re.IGNORECASE) for p in [
            r"smart\s*city",
            r"citt[aà]\s+intelligent",
            r"\biot\b.*(?:pubblic|urban|città|comune)",
            r"fibra\s+ottic",
            r"digitalizzazione.*(?:servizi|infrastrutt)",
            r"telecontroll",
            r"telegestione",
            r"sensor[ei].*(?:pubblic|urban|città|comune)",
            r"infrastrutt.*digital",
            r"rete?\s+(?:dati|telecomunicazion)",
        ]
    ],
    "SMART_PARKING": [
        re.compile(p, re.IGNORECASE) for p in [
            r"parcheggi?\s+intelligent",
            r"sosta\s+regolamentat",
            r"parcometr",
            r"parchimetr",
            r"parking\s+(?:smart|guid|sensor)",
            r"gestion.*parcheggi.*(?:automat|smart|sensor)",
        ]
    ],
    "ENERGIA": [
        re.compile(p, re.IGNORECASE) for p in [
            r"fotovoltaic",
            r"pannell[oi]\s+solar[ei]",
            r"solare\s+termic",
            r"pompa?\s+(?:di\s+)?calor",
            r"biomass",
            r"geotermi[ac]",
            r"eolic[oa]",
            r"cogenerazion",
            r"trigenerazion",
            r"risparmio\s+energetic",
            r"efficien(?:tamento|za)\s+energetic",
            r"diagnosi\s+energetic",
            r"certificazione?\s+energetic",
            r"cappotto\s+termic",
            r"isolamento\s+termic",
            r"riqualificazione\s+energetic",
            r"produzione\s+energia",
            r"fornitura\s+(?:di\s+)?energia\s+elettric",
            r"energy\s+(?:service|performanc|saving)",
            r"esco\b",
            r"contabilizzazione\s+calor",
            r"termoregolazione",
        ]
    ],
    "EDIFICI_ENERGIA": [
        re.compile(p, re.IGNORECASE) for p in [
            r"(?:scuol|ospedal|edific).*(?:riqualificazion|efficientament).*energetic",
            r"energetic.*(?:riqualificazion|efficientament).*(?:scuol|ospedal|edific)",
            r"impiant.*(?:termic|climatizzazion|riscaldament).*(?:edific|scuol|comunal)",
            r"(?:edific|scuol|comunal).*impiant.*(?:termic|climatizzazion|riscaldament)",
            r"adeguamento\s+impiant.*(?:edific|scuol|pubblic)",
            r"caldai.*(?:edific|scuol|pubblic|comunal)",
        ]
    ],
    "STRADE_INFRASTRUTTURE": [
        re.compile(p, re.IGNORECASE) for p in [
            r"galleri[ae].*(?:impiant|illumin|ventilazion|sicurezz)",
            r"tunnel.*(?:impiant|illumin|ventilazion|sicurezz)",
            r"pist[ae]\s+ciclabil",
            r"rotatorie?\s+(?:stradal|urban)",
            r"infrastruttur.*(?:stradal|urban|viabilit)",
            r"manutenzion.*strad.*(?:illumin|segnaletic|semaf)",
            r"strad.*(?:illumin|segnaletic|semaf).*manutenzion",
            r"segnaletic.*stradal",
        ]
    ],
    "E_MOBILITY": [
        re.compile(p, re.IGNORECASE) for p in [
            r"colonnin[ae]\s*(?:di\s*)?ricaric",
            r"ricarica\s*(?:per\s*)?(?:veicoli?\s*)?elettric",
            r"stazion[ei]\s*(?:di\s*)?ricaric",
            r"e[\-\s]?mobility",
            r"mobilit[àa]\s+elettric",
            r"veicol[oi]\s+elettric",
            r"auto\s+elettric",
        ]
    ],
    "ACQUA_FOGNATURE": [
        re.compile(p, re.IGNORECASE) for p in [
            r"acquedott",
            r"rete?\s+idric",
            r"fognatur",
            r"depurator",
            r"collettore?\s+fognar",
            r"condott[ae].*(?:idric|acqua|fogn)",
            r"impianto?\s+(?:di\s+)?depurazion",
        ]
    ],
    "RIFIUTI": [
        re.compile(p, re.IGNORECASE) for p in [
            r"raccolta\s+(?:differenziat|rifiut)",
            r"igiene\s+urban",
            r"discaric",
            r"compostaggio",
            r"termovalorizzator",
            r"gestione\s+(?:integrat\w+\s+)?rifiut",
            r"nettezza\s+urban",
        ]
    ],
    "VERDE_PUBBLICO": [
        re.compile(p, re.IGNORECASE) for p in [
            r"verde\s+pubblic",
            r"arredo\s+urban",
            r"(?:parchi|giardini)\s+(?:pubblic|comunal|urban)",
            r"manutenzion.*(?:verde|parchi|giardini).*(?:pubblic|comunal)",
        ]
    ],
}

# Negative patterns to exclude false positives
NEGATIVE_PATTERNS = [
    re.compile(p, re.IGNORECASE) for p in [
        r"galleria?\s+d[\'\']arte",
        r"museo",
        r"mostra",
        r"MINISTERO\s+DELLA\s+CULTURA",
        r"via\s+galler",
    ]
]


def match_categories(text: str) -> list[str]:
    """
    Match text against all category patterns.

    Args:
        text: Text to match (typically tender description).

    Returns:
        List of matched category names (may be empty).
    """
    if not text or not isinstance(text, str):
        return []

    text = text.strip()
    if len(text) < 5:
        return []

    # Check negative patterns first
    for neg in NEGATIVE_PATTERNS:
        if neg.search(text):
            # Still allow if explicitly mentions illuminazione/videosorveglianza
            has_explicit = any(
                CATEGORY_PATTERNS[cat][0].search(text)
                for cat in ["ILLUMINAZIONE", "VIDEOSORVEGLIANZA"]
            )
            if not has_explicit:
                return []

    matched = []
    for category, patterns in CATEGORY_PATTERNS.items():
        for pat in patterns:
            if pat.search(text):
                matched.append(category)
                break

    return matched


def get_primary_category(text: str) -> Optional[str]:
    """
    Get the primary (first matched) category for a text.

    Args:
        text: Text to categorize.

    Returns:
        Category name or None.
    """
    cats = match_categories(text)
    return cats[0] if cats else None


def passes_filter(text: str) -> bool:
    """
    Check if text matches at least one category.

    Args:
        text: Text to check.

    Returns:
        True if at least one category matches.
    """
    return len(match_categories(text)) > 0
