"""
Test per i modelli Pydantic.

Questo file contiene i test per i modelli di dati.
"""

import pytest
from src.models.enums import (
    CategoriaLotto, TipoIlluminazione, TipoEfficientamento,
    TipoAppalto, TipoIntervento, TipoImpianto, TipoEnergia, TipoOperazione
)
from src.models.lotto import Lotto, GruppoLotti, QuantiLotti
from src.models.categorization import CategLotto


class TestEnums:
    """Test per le enumerazioni."""
    
    def test_categoria_lotto_values(self):
        """Test valori enum CategoriaLotto."""
        assert CategoriaLotto.ILLUMINAZIONE.value == "Illuminazione"
        assert CategoriaLotto.VIDEOSORVEGLIANZA.value == "Videosorveglianza"
        assert CategoriaLotto.ALTRO.value == ""
    
    def test_tipo_illuminazione_values(self):
        """Test valori enum TipoIlluminazione."""
        assert TipoIlluminazione.PUBBLICA.value == "Pubblica"
        assert TipoIlluminazione.STRADALE.value == "Stradale"
        assert TipoIlluminazione.SMART_STRADALE.value == "Stradale Intelligente"
    
    def test_tipo_appalto_values(self):
        """Test valori enum TipoAppalto."""
        assert TipoAppalto.SERVIZIO.value == "Servizio"
        assert TipoAppalto.FORNITURA.value == "Fornitura"
        assert TipoAppalto.PROJECT_FINANCING.value == "Project Financing"


class TestLottoModel:
    """Test per il modello Lotto."""
    
    def test_lotto_creation(self, sample_lotto_data):
        """Test creazione di un lotto valido."""
        lotto = Lotto(**sample_lotto_data)
        
        assert lotto.oggetto == "Servizio di illuminazione pubblica"
        assert lotto.categoria == CategoriaLotto.ILLUMINAZIONE
        assert lotto.cig == "1234567890"
        assert lotto.importo_aggiudicazione == "100000.00"
    
    def test_lotto_date_parsing(self):
        """Test parsing delle date nel lotto."""
        data = {
            "Oggetto": "Test",
            "Categoria": "Illuminazione",
            "TipoIlluminazione": "Pubblica",
            "TipoEfficientamento": "Energetico",
            "TipoAppalto": "Servizio",
            "TipoIntervento": "Efficientamento Energetico",
            "TipoImpianto": "Pubblica Illuminazione",
            "TipoEnergia": "Energia Elettrica",
            "TipoOperazione": "Gestione",
            "Procedura": "Test",
            "AmministrazioneAggiudicatrice": "Test",
            "OfferteRicevute": "1",
            "DurataAppalto": "365",
            "Scadenza": "31 dicembre 2024",
            "ImportoAggiudicazione": "1000",
            "DataAggiudicazione": "1 gennaio 2024",
            "Sconto": "10%",
            "Comune": "Test",
            "Aggiudicatario": "Test",
            "CIG": "1234567890",
            "CUP": ""
        }
        
        lotto = Lotto(**data)
        assert lotto.scadenza == "31/12/2024"
        assert lotto.data_aggiudicazione == "01/01/2024"
    
    def test_lotto_duration_conversion(self):
        """Test conversione durata appalto."""
        data = {
            "Oggetto": "Test",
            "Categoria": "Illuminazione",
            "TipoIlluminazione": "Pubblica",
            "TipoEfficientamento": "Energetico",
            "TipoAppalto": "Servizio",
            "TipoIntervento": "Efficientamento Energetico",
            "TipoImpianto": "Pubblica Illuminazione",
            "TipoEnergia": "Energia Elettrica",
            "TipoOperazione": "Gestione",
            "Procedura": "Test",
            "AmministrazioneAggiudicatrice": "Test",
            "OfferteRicevute": "1",
            "DurataAppalto": "3 anni",
            "Scadenza": "",
            "ImportoAggiudicazione": "1000",
            "DataAggiudicazione": "",
            "Sconto": "",
            "Comune": "Test",
            "Aggiudicatario": "Test",
            "CIG": "TEST123",
            "CUP": ""
        }
        
        lotto = Lotto(**data)
        assert lotto.durata_appalto == "1095"
    
    def test_lotto_importo_extraction(self):
        """Test estrazione importo."""
        data = {
            "Oggetto": "Test",
            "Categoria": "Illuminazione",
            "TipoIlluminazione": "Pubblica",
            "TipoEfficientamento": "Energetico",
            "TipoAppalto": "Servizio",
            "TipoIntervento": "Efficientamento Energetico",
            "TipoImpianto": "Pubblica Illuminazione",
            "TipoEnergia": "Energia Elettrica",
            "TipoOperazione": "Gestione",
            "Procedura": "Test",
            "AmministrazioneAggiudicatrice": "Test",
            "OfferteRicevute": "1",
            "DurataAppalto": "",
            "Scadenza": "",
            "ImportoAggiudicazione": "€ 1.234.567,89",
            "DataAggiudicazione": "",
            "Sconto": "",
            "Comune": "Test",
            "Aggiudicatario": "Test",
            "CIG": "TEST123",
            "CUP": ""
        }
        
        lotto = Lotto(**data)
        assert lotto.importo_aggiudicazione == "1234567.89"


class TestGruppoLottiModel:
    """Test per il modello GruppoLotti."""
    
    def test_gruppo_lotti_creation(self, sample_lotto_data):
        """Test creazione gruppo di lotti."""
        lotto1 = Lotto(**sample_lotto_data)
        lotto2 = Lotto(**sample_lotto_data)
        
        gruppo = GruppoLotti(Lotti=[lotto1, lotto2])
        assert len(gruppo.lotti) == 2
        assert gruppo.lotti[0].cig == "1234567890"


class TestCategLottoModel:
    """Test per il modello CategLotto."""
    
    def test_categ_lotto_creation(self):
        """Test creazione categorizzazione."""
        categ = CategLotto(
            Categoria=CategoriaLotto.ILLUMINAZIONE,
            TipoIlluminazione=TipoIlluminazione.PUBBLICA,
            TipoAppalto=TipoAppalto.SERVIZIO
        )
        
        assert categ.categoria == CategoriaLotto.ILLUMINAZIONE
        assert categ.tipo_illuminazione == TipoIlluminazione.PUBBLICA
        assert categ.tipo_appalto == TipoAppalto.SERVIZIO
    
    def test_categ_lotto_to_dict(self):
        """Test conversione a dizionario."""
        categ = CategLotto(
            Categoria=CategoriaLotto.ILLUMINAZIONE,
            TipoIlluminazione=TipoIlluminazione.PUBBLICA
        )
        
        result = categ.to_dict()
        assert result["Categoria"] == "Illuminazione"
        assert result["TipoIlluminazione"] == "Pubblica"
        assert result["TipoAppalto"] is None