"""
Modelli Pydantic per la validazione e strutturazione dei dati.

Questo modulo contiene tutti i modelli di dati utilizzati nell'applicazione
per garantire la validazione e la coerenza dei dati.
"""

from .categorization import CategLotto
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
from .lotto import GruppoLotti, Lotto, QuantiLotti

__all__ = [
    # Enums
    "CategoriaLotto",
    "TipoIlluminazione",
    "TipoEfficientamento",
    "TipoAppalto",
    "TipoIntervento",
    "TipoImpianto",
    "TipoEnergia",
    "TipoOperazione",
    # Models
    "Lotto",
    "GruppoLotti",
    "QuantiLotti",
    "CategLotto",
]
