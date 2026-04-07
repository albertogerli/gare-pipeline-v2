"""
Modulo di utilità per funzioni comuni.

Questo modulo contiene funzioni di utilità riutilizzabili
in tutto il progetto.
"""

from .date import calculate_duration, parse_date
from .performance import profile_memory, timer
from .text import clean_text, hash_text
from .validation import validate_cig, validate_cup

__all__ = [
    "clean_text",
    "hash_text",
    "parse_date",
    "calculate_duration",
    "validate_cig",
    "validate_cup",
    "timer",
    "profile_memory",
]
