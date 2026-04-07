# Domain Model - Gare Appalti

## Bounded Contexts

Il sistema Gare Appalti è organizzato in bounded contexts che rappresentano aree di competenza specifiche:

### 1. Procurement Context (Core Business)
**Responsabilità**: Gestione del ciclo di vita delle gare e dei lotti

#### Entities
```python
# Gara: Aggregato root
class Gara:
    def __init__(self, gara_id: GaraId, titolo: str, ente: Ente):
        self._id = gara_id
        self._titolo = titolo  
        self._ente = ente
        self._lotti = []
        self._stato = StatoGara.BOZZA
        self._events = []
    
    def aggiungi_lotto(self, lotto: Lotto) -> None:
        self._lotti.append(lotto)
        self._events.append(LottoAggiunto(self._id, lotto.id))
    
    def pubblica(self) -> None:
        if not self._lotti:
            raise DomainException("Cannot publish gara without lotti")
        self._stato = StatoGara.PUBBLICATA
        self._events.append(GaraPubblicata(self._id))

# Lotto: Entity
class Lotto:
    def __init__(self, lotto_id: LottoId, oggetto: str, importo: Money):
        self._id = lotto_id
        self._oggetto = oggetto
        self._importo = importo
        self._categoria = None
        self._aggiudicazione = None
        
    def categorizza(self, categoria: CategoriaLotto) -> None:
        self._categoria = categoria
        
    def aggiudica(self, aggiudicazione: Aggiudicazione) -> None:
        if self._aggiudicazione is not None:
            raise DomainException("Lotto already assigned")
        self._aggiudicazione = aggiudicazione
```

#### Value Objects
```python
# Money: Value Object per importi
class Money:
    def __init__(self, amount: Decimal, currency: Currency = Currency.EUR):
        if amount < 0:
            raise ValueError("Amount cannot be negative")
        self._amount = amount
        self._currency = currency
    
    @property
    def amount(self) -> Decimal:
        return self._amount
    
    def add(self, other: 'Money') -> 'Money':
        if self._currency != other._currency:
            raise ValueError("Cannot add different currencies")
        return Money(self._amount + other._amount, self._currency)

# CategoriaLotto: Value Object
class CategoriaLotto:
    def __init__(self, 
                 categoria_principale: TipoCategoria,
                 sottocategorie: List[str] = None,
                 confidence: float = 1.0):
        self._categoria_principale = categoria_principale
        self._sottocategorie = sottocategorie or []
        self._confidence = confidence
        
    def is_illuminazione(self) -> bool:
        return self._categoria_principale == TipoCategoria.ILLUMINAZIONE

# Periodo: Value Object per range temporali
class Periodo:
    def __init__(self, inizio: datetime, fine: datetime):
        if inizio >= fine:
            raise ValueError("Start date must be before end date")
        self._inizio = inizio
        self._fine = fine
    
    def contiene(self, data: datetime) -> bool:
        return self._inizio <= data <= self._fine
    
    def durata_giorni(self) -> int:
        return (self._fine - self._inizio).days
```

#### Domain Services
```python
# CategorizationService: Domain Service
class CategorizationService:
    def __init__(self, analyzer: TextAnalyzer):
        self._analyzer = analyzer
    
    def categorizza_lotto(self, lotto: Lotto) -> CategoriaLotto:
        """Determina la categoria di un lotto basandosi sull'oggetto"""
        analisi = self._analyzer.analyze_text(lotto.oggetto)
        
        # Business rules per categorizzazione
        if self._is_illuminazione(analisi):
            return self._create_illuminazione_category(analisi)
        elif self._is_energia(analisi):
            return self._create_energia_category(analisi)
        # ... altre categorie
        
        return CategoriaLotto(TipoCategoria.ALTRO)
    
    def _is_illuminazione(self, analisi: TextAnalysis) -> bool:
        keywords = ['illuminazione', 'lampade', 'led', 'pubblica illuminazione']
        return any(keyword in analisi.tokens for keyword in keywords)

# ValidationService: Domain Service
class ValidationService:
    def valida_lotto(self, lotto: Lotto) -> ValidationResult:
        """Valida la completezza e correttezza di un lotto"""
        errors = []
        warnings = []
        
        # Required fields validation
        if not lotto.oggetto.strip():
            errors.append("Oggetto del lotto è obbligatorio")
        
        if lotto.importo.amount <= 0:
            errors.append("Importo deve essere maggiore di zero")
        
        # Business rules validation
        if lotto.importo.amount > 1000000 and not lotto.categoria:
            warnings.append("Lotti sopra 1M€ dovrebbero essere categorizzati")
        
        return ValidationResult(errors, warnings)
```

### 2. Data Acquisition Context
**Responsabilità**: Acquisizione dati da fonti esterne (Gazzetta, OCDS)

#### Entities
```python
# ScrapingSession: Entity per tracciare sessioni di scraping
class ScrapingSession:
    def __init__(self, session_id: SessionId, fonte: FonteDati, periodo: Periodo):
        self._id = session_id
        self._fonte = fonte
        self._periodo = periodo
        self._stato = StatoScraping.INIZIALIZZATA
        self._items_processati = 0
        self._errori = []
        self._inizio = None
        self._fine = None
    
    def inizia(self) -> None:
        self._stato = StatoScraping.IN_CORSO
        self._inizio = datetime.utcnow()
    
    def registra_item_processato(self) -> None:
        self._items_processati += 1
    
    def registra_errore(self, errore: ScrapingError) -> None:
        self._errori.append(errore)
    
    def completa(self) -> None:
        self._stato = StatoScraping.COMPLETATA
        self._fine = datetime.utcnow()

# DataSource: Entity per configurazione sorgenti
class DataSource:
    def __init__(self, source_id: SourceId, nome: str, base_url: str):
        self._id = source_id
        self._nome = nome
        self._base_url = base_url
        self._configurazione = {}
        self._ultima_sincronizzazione = None
    
    def aggiorna_configurazione(self, config: dict) -> None:
        self._configurazione.update(config)
    
    def marca_sincronizzata(self) -> None:
        self._ultima_sincronizzazione = datetime.utcnow()
```

#### Value Objects
```python
# ScrapingConfig: Configurazione per scraping
class ScrapingConfig:
    def __init__(self, 
                 delay: int = 1,
                 timeout: int = 30,
                 retry_attempts: int = 3,
                 user_agent: str = None):
        self._delay = delay
        self._timeout = timeout 
        self._retry_attempts = retry_attempts
        self._user_agent = user_agent or self._default_user_agent()
    
    @property
    def delay(self) -> int:
        return self._delay

# URLPattern: Value Object per pattern URL
class URLPattern:
    def __init__(self, template: str, parameters: List[str]):
        self._template = template
        self._parameters = parameters
    
    def build_url(self, **kwargs) -> str:
        missing_params = set(self._parameters) - set(kwargs.keys())
        if missing_params:
            raise ValueError(f"Missing parameters: {missing_params}")
        return self._template.format(**kwargs)
```

### 3. Analysis Context
**Responsabilità**: Analisi e processamento dei dati acquisiti

#### Entities
```python
# AnalysisJob: Entity per job di analisi
class AnalysisJob:
    def __init__(self, job_id: JobId, tipo: TipoAnalisi, input_data: List[RawData]):
        self._id = job_id
        self._tipo = tipo
        self._input_data = input_data
        self._stato = StatoAnalisi.PENDING
        self._risultati = []
        self._started_at = None
        self._completed_at = None
    
    def inizia(self) -> None:
        self._stato = StatoAnalisi.RUNNING
        self._started_at = datetime.utcnow()
    
    def aggiungi_risultato(self, risultato: AnalysisResult) -> None:
        self._risultati.append(risultato)
    
    def completa(self) -> None:
        self._stato = StatoAnalisi.COMPLETED
        self._completed_at = datetime.utcnow()

# TextAnalysisSession: Entity per sessioni di analisi testo
class TextAnalysisSession:
    def __init__(self, session_id: str, llm_config: LLMConfig):
        self._id = session_id
        self._llm_config = llm_config
        self._requests_count = 0
        self._tokens_used = 0
        self._cache = {}
    
    def analyze_text(self, text: str) -> TextAnalysis:
        # Check cache first
        text_hash = hash(text)
        if text_hash in self._cache:
            return self._cache[text_hash]
        
        # Perform analysis
        result = self._perform_llm_analysis(text)
        self._requests_count += 1
        self._tokens_used += result.tokens_used
        
        # Cache result
        self._cache[text_hash] = result
        return result
```

#### Value Objects
```python
# TextAnalysis: Risultato dell'analisi di testo
class TextAnalysis:
    def __init__(self,
                 original_text: str,
                 tokens: List[str],
                 keywords: List[str],
                 categoria_suggerita: Optional[CategoriaLotto],
                 confidence: float,
                 tokens_used: int = 0):
        self._original_text = original_text
        self._tokens = tokens
        self._keywords = keywords
        self._categoria_suggerita = categoria_suggerita
        self._confidence = confidence
        self._tokens_used = tokens_used
    
    @property
    def tokens(self) -> List[str]:
        return self._tokens.copy()
    
    def has_keyword(self, keyword: str) -> bool:
        return keyword.lower() in [k.lower() for k in self._keywords]

# LLMConfig: Configurazione per LLM
class LLMConfig:
    def __init__(self,
                 model_name: str,
                 temperature: float = 0.1,
                 max_tokens: int = 1000,
                 timeout: int = 30):
        self._model_name = model_name
        self._temperature = temperature
        self._max_tokens = max_tokens
        self._timeout = timeout
        
    def to_dict(self) -> dict:
        return {
            'model': self._model_name,
            'temperature': self._temperature,
            'max_tokens': self._max_tokens
        }
```

### 4. Reporting Context
**Responsabilità**: Generazione di report e dashboard

#### Entities
```python
# Report: Entity per report generati
class Report:
    def __init__(self, report_id: ReportId, tipo: TipoReport, parametri: ReportParams):
        self._id = report_id
        self._tipo = tipo
        self._parametri = parametri
        self._stato = StatoReport.DRAFT
        self._contenuto = None
        self._generato_il = None
        self._formato = None
    
    def genera(self, generator: ReportGenerator) -> None:
        self._contenuto = generator.generate(self._parametri)
        self._stato = StatoReport.GENERATO
        self._generato_il = datetime.utcnow()
        self._formato = generator.formato
    
    def esporta(self, formato: FormatoExport) -> bytes:
        if self._stato != StatoReport.GENERATO:
            raise DomainException("Cannot export non-generated report")
        
        exporter = ExporterFactory.create(formato)
        return exporter.export(self._contenuto)
```

#### Value Objects
```python
# ReportParams: Parametri per generazione report
class ReportParams:
    def __init__(self,
                 periodo: Periodo,
                 filtri: Dict[str, Any] = None,
                 raggruppamento: List[str] = None,
                 metriche: List[str] = None):
        self._periodo = periodo
        self._filtri = filtri or {}
        self._raggruppamento = raggruppamento or []
        self._metriche = metriche or ['count', 'total_amount']
    
    def applica_filtro(self, campo: str, valore: Any) -> 'ReportParams':
        new_filtri = self._filtri.copy()
        new_filtri[campo] = valore
        return ReportParams(
            self._periodo,
            new_filtri,
            self._raggruppamento,
            self._metriche
        )

# ChartConfig: Configurazione per grafici
class ChartConfig:
    def __init__(self,
                 tipo_chart: TipoChart,
                 asse_x: str,
                 asse_y: str,
                 colori: List[str] = None):
        self._tipo_chart = tipo_chart
        self._asse_x = asse_x
        self._asse_y = asse_y
        self._colori = colori or ['#1f77b4', '#ff7f0e', '#2ca02c']
```

## Domain Events

```python
# Base Domain Event
class DomainEvent:
    def __init__(self):
        self._occurred_on = datetime.utcnow()
        self._id = str(uuid.uuid4())
    
    @property
    def occurred_on(self) -> datetime:
        return self._occurred_on
    
    @property
    def id(self) -> str:
        return self._id

# Specific Events
class LottoCreated(DomainEvent):
    def __init__(self, lotto_id: LottoId, gara_id: GaraId):
        super().__init__()
        self.lotto_id = lotto_id
        self.gara_id = gara_id

class LottoCategorized(DomainEvent):
    def __init__(self, lotto_id: LottoId, categoria: CategoriaLotto):
        super().__init__()
        self.lotto_id = lotto_id
        self.categoria = categoria

class ScrapingCompleted(DomainEvent):
    def __init__(self, session_id: SessionId, items_count: int):
        super().__init__()
        self.session_id = session_id
        self.items_count = items_count

class AnalysisCompleted(DomainEvent):
    def __init__(self, job_id: JobId, results_count: int):
        super().__init__()
        self.job_id = job_id
        self.results_count = results_count

class ReportGenerated(DomainEvent):
    def __init__(self, report_id: ReportId, formato: FormatoExport):
        super().__init__()
        self.report_id = report_id
        self.formato = formato
```

## Repository Interfaces

```python
# Repository per Procurement Context
class GaraRepository(ABC):
    @abstractmethod
    def save(self, gara: Gara) -> None:
        pass
    
    @abstractmethod
    def find_by_id(self, gara_id: GaraId) -> Optional[Gara]:
        pass
    
    @abstractmethod
    def find_by_periodo(self, periodo: Periodo) -> List[Gara]:
        pass

class LottoRepository(ABC):
    @abstractmethod
    def save(self, lotto: Lotto) -> None:
        pass
    
    @abstractmethod
    def find_by_id(self, lotto_id: LottoId) -> Optional[Lotto]:
        pass
    
    @abstractmethod
    def find_by_categoria(self, categoria: CategoriaLotto) -> List[Lotto]:
        pass
    
    @abstractmethod
    def find_by_importo_range(self, min_amount: Money, max_amount: Money) -> List[Lotto]:
        pass

# Repository per Data Acquisition Context
class ScrapingSessionRepository(ABC):
    @abstractmethod
    def save(self, session: ScrapingSession) -> None:
        pass
    
    @abstractmethod
    def find_active_sessions(self) -> List[ScrapingSession]:
        pass

class DataSourceRepository(ABC):
    @abstractmethod
    def save(self, source: DataSource) -> None:
        pass
    
    @abstractmethod
    def find_all(self) -> List[DataSource]:
        pass

# Repository per Analysis Context  
class AnalysisJobRepository(ABC):
    @abstractmethod
    def save(self, job: AnalysisJob) -> None:
        pass
    
    @abstractmethod
    def find_pending_jobs(self) -> List[AnalysisJob]:
        pass

# Repository per Reporting Context
class ReportRepository(ABC):
    @abstractmethod
    def save(self, report: Report) -> None:
        pass
    
    @abstractmethod
    def find_by_tipo(self, tipo: TipoReport) -> List[Report]:
        pass
```

## Ubiquitous Language

### Procurement Domain
- **Gara**: Una procedura di appalto pubblico
- **Lotto**: Una suddivisione di una gara per oggetto specifico
- **Aggiudicazione**: Assegnazione di un lotto a un'azienda
- **Categorizzazione**: Classificazione di un lotto per tipo di intervento
- **Importo**: Valore economico di un lotto

### Data Acquisition Domain  
- **Scraping**: Estrazione automatica di dati da siti web
- **Fonte**: Sorgente di dati (Gazzetta Ufficiale, OCDS)
- **Sessione**: Un'istanza di raccolta dati con parametri specifici
- **Checkpoint**: Punto di ripristino per scraping interrotti

### Analysis Domain
- **Analisi**: Processamento di testi per estrarre informazioni
- **Token**: Unità lessicale di un testo
- **Confidence**: Livello di certezza di una categorizzazione
- **LLM**: Large Language Model per analisi di testo

### Reporting Domain
- **Report**: Documento generato con dati aggregati
- **Dashboard**: Vista interattiva dei dati
- **Metrica**: Indicatore quantitativo (count, sum, avg)
- **Esportazione**: Conversione in formato specifico (Excel, PDF)

## Business Rules

### Procurement Rules
1. Una gara deve avere almeno un lotto per essere pubblicata
2. Un lotto può essere aggiudicato una sola volta
3. L'importo di un lotto deve essere positivo
4. La categorizzazione è obbligatoria per lotti > 100k€

### Data Acquisition Rules
1. Il delay tra richieste deve essere >= 1 secondo
2. Massimo 3 tentativi per richiesta fallita
3. Sessioni attive < 5 contemporaneamente
4. Checkpoint ogni 100 items processati

### Analysis Rules
1. Cache dei risultati per 24 ore
2. Confidence < 0.7 richiede revisione manuale
3. Timeout analisi LLM: 30 secondi
4. Batch size massimo: 50 items

### Reporting Rules
1. Report generati sono immutabili
2. Periodo massimo: 1 anno
3. Cache report per 1 ora
4. Esportazioni limitate a 10MB

Questo domain model fornisce una base solida per l'implementazione dell'architettura, garantendo chiarezza nel linguaggio di business e separazione netta delle responsabilità tra i diversi bounded contexts.