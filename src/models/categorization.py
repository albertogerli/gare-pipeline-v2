"""
Modelli per la categorizzazione dei lotti.

Questo modulo contiene i modelli utilizzati per la categorizzazione
automatica dei lotti tramite AI.
"""

from typing import Optional

from pydantic import BaseModel, Field

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


class CategLotto(BaseModel):
    """
    Modello per la categorizzazione di un lotto.

    Utilizzato per l'analisi AI dei testi dei bandi per
    l'estrazione automatica delle categorie.
    """

    categoria: Optional[CategoriaLotto] = Field(
        None, alias="Categoria", description="Categoria principale del lotto"
    )

    tipo_illuminazione: Optional[TipoIlluminazione] = Field(
        None, alias="TipoIlluminazione", description="Tipo specifico di illuminazione"
    )

    tipo_efficientamento: Optional[TipoEfficientamento] = Field(
        None,
        alias="TipoEfficientamento",
        description="Tipo di efficientamento energetico",
    )

    tipo_appalto: Optional[TipoAppalto] = Field(
        None, alias="TipoAppalto", description="Tipologia di appalto"
    )

    tipo_intervento: Optional[TipoIntervento] = Field(
        None, alias="TipoIntervento", description="Tipo di intervento previsto"
    )

    tipo_impianto: Optional[TipoImpianto] = Field(
        None, alias="TipoImpianto", description="Tipologia di impianto"
    )

    tipo_energia: Optional[TipoEnergia] = Field(
        None, alias="TipoEnergia", description="Tipo di energia/fornitura"
    )

    tipo_operazione: Optional[TipoOperazione] = Field(
        None, alias="TipoOperazione", description="Tipo di operazione"
    )

    class Config:
        """Configurazione del modello."""

        populate_by_name = True
        use_enum_values = True

    def to_dict(self) -> dict:
        """
        Converte il modello in dizionario con valori stringa.

        Returns:
            Dizionario con le categorie come stringhe
        """
        return {
            "Categoria": self.categoria.value if self.categoria else None,
            "TipoIlluminazione": self.tipo_illuminazione.value
            if self.tipo_illuminazione
            else None,
            "TipoEfficientamento": self.tipo_efficientamento.value
            if self.tipo_efficientamento
            else None,
            "TipoAppalto": self.tipo_appalto.value if self.tipo_appalto else None,
            "TipoIntervento": self.tipo_intervento.value
            if self.tipo_intervento
            else None,
            "TipoImpianto": self.tipo_impianto.value if self.tipo_impianto else None,
            "TipoEnergia": self.tipo_energia.value if self.tipo_energia else None,
            "TipoOperazione": self.tipo_operazione.value
            if self.tipo_operazione
            else None,
        }
