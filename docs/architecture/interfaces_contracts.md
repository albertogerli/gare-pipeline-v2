# Interface Contracts - Gare Appalti

## Repository Interfaces

### Core Domain Repositories

```python
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from datetime import datetime

from ..core.entities.lotto import Lotto, LottoId
from ..core.entities.gara import Gara, GaraId
from ..core.value_objects.money import Money
from ..core.value_objects.categorization import CategoriaLotto
from ..core.value_objects.periodo import Periodo

class LottoRepository(ABC):
    """Repository interface for Lotto aggregate management."""
    
    @abstractmethod
    def save(self, lotto: Lotto) -> None:
        """Persiste un lotto nel repository.
        
        Args:
            lotto: Il lotto da salvare
            
        Raises:
            RepositoryError: Se il salvataggio fallisce
        """
        pass
    
    @abstractmethod
    def find_by_id(self, lotto_id: LottoId) -> Optional[Lotto]:
        """Trova un lotto per ID.
        
        Args:
            lotto_id: L'identificativo del lotto
            
        Returns:
            Il lotto se trovato, None altrimenti
        """
        pass
    
    @abstractmethod
    def find_by_criteria(self, criteria: 'LottoCriteria') -> List[Lotto]:
        """Trova lotti che soddisfano i criteri specificati.
        
        Args:
            criteria: Criteri di ricerca
            
        Returns:
            Lista di lotti che soddisfano i criteri
        """
        pass
    
    @abstractmethod
    def find_by_importo_range(self, min_amount: Money, max_amount: Money) -> List[Lotto]:
        """Trova lotti in un range di importo.
        
        Args:
            min_amount: Importo minimo (incluso)
            max_amount: Importo massimo (incluso)
            
        Returns:
            Lista di lotti nel range specificato
        """
        pass
    
    @abstractmethod
    def find_by_categoria(self, categoria: CategoriaLotto) -> List[Lotto]:
        """Trova lotti per categoria.
        
        Args:
            categoria: La categoria da cercare
            
        Returns:
            Lista di lotti della categoria specificata
        """
        pass
    
    @abstractmethod
    def find_uncategorized(self) -> List[Lotto]:
        """Trova lotti non ancora categorizzati.
        
        Returns:
            Lista di lotti senza categoria
        """
        pass
    
    @abstractmethod
    def count_by_categoria(self) -> Dict[CategoriaLotto, int]:
        """Conta lotti per categoria.
        
        Returns:
            Dizionario categoria -> count
        """
        pass
    
    @abstractmethod
    def delete(self, lotto_id: LottoId) -> bool:
        """Elimina un lotto.
        
        Args:
            lotto_id: ID del lotto da eliminare
            
        Returns:
            True se eliminato, False se non trovato
        """
        pass

class GaraRepository(ABC):
    """Repository interface for Gara aggregate management."""
    
    @abstractmethod
    def save(self, gara: Gara) -> None:
        """Persiste una gara nel repository."""
        pass
    
    @abstractmethod
    def find_by_id(self, gara_id: GaraId) -> Optional[Gara]:
        """Trova una gara per ID."""
        pass
    
    @abstractmethod
    def find_by_periodo(self, periodo: Periodo) -> List[Gara]:
        """Trova gare in un periodo specifico."""
        pass
    
    @abstractmethod
    def find_by_ente(self, ente: str) -> List[Gara]:
        """Trova gare per ente appaltante."""
        pass

# Criteria Classes
class LottoCriteria:
    """Criteri di ricerca per lotti."""
    
    def __init__(self):
        self.periodo: Optional[Periodo] = None
        self.categoria: Optional[CategoriaLotto] = None
        self.importo_min: Optional[Money] = None
        self.importo_max: Optional[Money] = None
        self.ente: Optional[str] = None
        self.keywords: List[str] = []
        self.only_uncategorized: bool = False
        self.only_categorized: bool = False
    
    def with_periodo(self, periodo: Periodo) -> 'LottoCriteria':
        self.periodo = periodo
        return self
    
    def with_categoria(self, categoria: CategoriaLotto) -> 'LottoCriteria':
        self.categoria = categoria
        return self
    
    def with_importo_range(self, min_amount: Money, max_amount: Money) -> 'LottoCriteria':
        self.importo_min = min_amount
        self.importo_max = max_amount
        return self
    
    def with_keywords(self, keywords: List[str]) -> 'LottoCriteria':
        self.keywords = keywords
        return self
    
    def only_uncategorized_lotti(self) -> 'LottoCriteria':
        self.only_uncategorized = True
        self.only_categorized = False
        return self
```

## Service Interfaces

### Scraping Services

```python
from abc import ABC, abstractmethod
from typing import List, Iterator, Optional
from datetime import datetime

from ..core.value_objects.periodo import Periodo
from ..infrastructure.external.models.raw_data import RawData

class BaseScraper(ABC):
    """Interface base per tutti gli scrapers."""
    
    @abstractmethod
    def scrape(self, periodo: Periodo, **kwargs) -> Iterator[RawData]:
        """Scraping dati per il periodo specificato.
        
        Args:
            periodo: Periodo temporale da scrapare
            **kwargs: Parametri aggiuntivi specifici del scraper
            
        Yields:
            RawData: Dati grezzi estratti
            
        Raises:
            ScrapingError: Se il scraping fallisce
        """
        pass
    
    @abstractmethod
    def validate_connection(self) -> bool:
        """Valida la connessione alla fonte dati.
        
        Returns:
            True se la connessione è valida, False altrimenti
        """
        pass
    
    @abstractmethod
    def get_last_update(self) -> Optional[datetime]:
        """Restituisce la data dell'ultimo aggiornamento disponibile.
        
        Returns:
            Datetime dell'ultimo aggiornamento o None se non disponibile
        """
        pass
    
    @property
    @abstractmethod
    def source_name(self) -> str:
        """Nome identificativo della fonte dati."""
        pass

class GazzettaScraper(BaseScraper):
    """Scraper specifico per Gazzetta Ufficiale."""
    
    @abstractmethod
    def scrape_anno(self, anno: int) -> Iterator[RawData]:
        """Scraping per anno specifico."""
        pass
    
    @abstractmethod
    def scrape_regione(self, regione: str, periodo: Periodo) -> Iterator[RawData]:
        """Scraping per regione specifica."""
        pass

class OCDSScraper(BaseScraper):
    """Scraper specifico per dati OCDS."""
    
    @abstractmethod
    def scrape_releases(self, periodo: Periodo) -> Iterator[RawData]:
        """Scraping releases OCDS."""
        pass
    
    @abstractmethod
    def scrape_records(self, periodo: Periodo) -> Iterator[RawData]:
        """Scraping records OCDS."""
        pass
```

### Analysis Services

```python
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any

from ..core.value_objects.categorization import CategoriaLotto
from ..infrastructure.external.models.text_analysis import TextAnalysis

class BaseAnalyzer(ABC):
    """Interface base per tutti gli analyzers."""
    
    @abstractmethod
    def analyze(self, raw_data: RawData) -> 'AnalysisResult':
        """Analizza dati grezzi e estrae informazioni strutturate.
        
        Args:
            raw_data: Dati grezzi da analizzare
            
        Returns:
            Risultato dell'analisi con informazioni estratte
            
        Raises:
            AnalysisError: Se l'analisi fallisce
        """
        pass
    
    @abstractmethod
    def batch_analyze(self, raw_data_list: List[RawData]) -> List['AnalysisResult']:
        """Analizza un batch di dati grezzi.
        
        Args:
            raw_data_list: Lista di dati grezzi
            
        Returns:
            Lista di risultati analisi
        """
        pass
    
    @property
    @abstractmethod
    def analyzer_type(self) -> str:
        """Tipo di analyzer."""
        pass

class TextAnalyzer(ABC):
    """Interface per analisi di testi."""
    
    @abstractmethod
    def analyze_text(self, text: str) -> TextAnalysis:
        """Analizza un testo e estrae informazioni.
        
        Args:
            text: Testo da analizzare
            
        Returns:
            Risultato dell'analisi testuale
        """
        pass
    
    @abstractmethod
    def extract_keywords(self, text: str, max_keywords: int = 10) -> List[str]:
        """Estrae keywords da un testo.
        
        Args:
            text: Testo da analizzare
            max_keywords: Numero massimo di keywords
            
        Returns:
            Lista di keywords estratte
        """
        pass

class CategorizationAnalyzer(ABC):
    """Interface per categorizzazione automatica."""
    
    @abstractmethod
    def categorize(self, text: str) -> CategoriaLotto:
        """Categorizza un testo.
        
        Args:
            text: Testo da categorizzare
            
        Returns:
            Categoria assegnata
        """
        pass
    
    @abstractmethod
    def categorize_with_confidence(self, text: str) -> tuple[CategoriaLotto, float]:
        """Categorizza con livello di confidenza.
        
        Args:
            text: Testo da categorizzare
            
        Returns:
            Tupla (categoria, confidence_score)
        """
        pass
```

### LLM Client Interface

```python
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

@dataclass
class LLMRequest:
    """Request per servizi LLM."""
    prompt: str
    model: str = "o3-mini"
    temperature: float = 0.1
    max_tokens: int = 1000
    system_prompt: Optional[str] = None
    context: Optional[Dict[str, Any]] = None

@dataclass
class LLMResponse:
    """Response da servizi LLM."""
    content: str
    tokens_used: int
    model_used: str
    response_time: float
    confidence: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None

class LLMClient(ABC):
    """Interface per client LLM."""
    
    @abstractmethod
    def complete(self, request: LLMRequest) -> LLMResponse:
        """Completa un prompt usando LLM.
        
        Args:
            request: Richiesta LLM
            
        Returns:
            Risposta del modello
            
        Raises:
            LLMError: Se la richiesta fallisce
        """
        pass
    
    @abstractmethod
    def batch_complete(self, requests: List[LLMRequest]) -> List[LLMResponse]:
        """Completa multipli prompt in batch.
        
        Args:
            requests: Lista di richieste
            
        Returns:
            Lista di risposte
        """
        pass
    
    @abstractmethod
    def categorize_text(self, text: str) -> CategoriaLotto:
        """Categorizza testo usando LLM.
        
        Args:
            text: Testo da categorizzare
            
        Returns:
            Categoria identificata
        """
        pass
    
    @abstractmethod
    def extract_structured_data(self, text: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Estrae dati strutturati da testo.
        
        Args:
            text: Testo sorgente
            schema: Schema dei dati da estrarre
            
        Returns:
            Dati estratti secondo lo schema
        """
        pass
    
    @abstractmethod
    def get_usage_stats(self) -> Dict[str, Any]:
        """Statistiche di utilizzo del client.
        
        Returns:
            Statistiche (tokens, requests, costs, etc.)
        """
        pass
```

## Application Service Interfaces

### Command Interfaces

```python
from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Any
from dataclasses import dataclass

T = TypeVar('T')
R = TypeVar('R')

class Command(ABC):
    """Base class for all commands."""
    pass

class CommandHandler(Generic[T, R], ABC):
    """Interface for command handlers."""
    
    @abstractmethod
    def handle(self, command: T) -> R:
        """Handle a command.
        
        Args:
            command: The command to handle
            
        Returns:
            Command result
            
        Raises:
            CommandError: If command handling fails
        """
        pass

# Specific Commands
@dataclass
class ScrapeGazzettaCommand(Command):
    """Command per scraping Gazzetta Ufficiale."""
    periodo: Periodo
    regioni: Optional[List[str]] = None
    enti: Optional[List[str]] = None
    force_refresh: bool = False

@dataclass
class AnalyzeLottiCommand(Command):
    """Command per analisi lotti."""
    lotto_ids: Optional[List[LottoId]] = None
    criteria: Optional[LottoCriteria] = None
    force_reanalysis: bool = False
    batch_size: int = 50

@dataclass
class GenerateReportCommand(Command):
    """Command per generazione report."""
    tipo_report: 'TipoReport'
    periodo: Periodo
    filtri: Optional[Dict[str, Any]] = None
    formato: 'FormatoExport' = 'EXCEL'
    output_path: Optional[str] = None

# Command Handlers
class ScrapeGazzettaHandler(CommandHandler[ScrapeGazzettaCommand, 'ScrapingResult']):
    """Handler per comando scraping Gazzetta."""
    
    def __init__(self, 
                 scraper: GazzettaScraper,
                 lotto_repo: LottoRepository,
                 analyzer: 'GazzettaAnalyzer'):
        self._scraper = scraper
        self._lotto_repo = lotto_repo
        self._analyzer = analyzer
    
    @abstractmethod
    def handle(self, command: ScrapeGazzettaCommand) -> 'ScrapingResult':
        pass

class AnalyzeLottiHandler(CommandHandler[AnalyzeLottiCommand, 'AnalysisResult']):
    """Handler per comando analisi lotti."""
    
    def __init__(self,
                 lotto_repo: LottoRepository,
                 categorization_service: 'CategorizationService',
                 llm_client: LLMClient):
        self._lotto_repo = lotto_repo
        self._categorization_service = categorization_service
        self._llm_client = llm_client
    
    @abstractmethod
    def handle(self, command: AnalyzeLottiCommand) -> 'AnalysisResult':
        pass
```

### Query Interfaces

```python
from abc import ABC, abstractmethod
from typing import Generic, TypeVar, List, Optional, Dict, Any
from dataclasses import dataclass

Q = TypeVar('Q')
R = TypeVar('R')

class Query(ABC):
    """Base class for all queries."""
    pass

class QueryHandler(Generic[Q, R], ABC):
    """Interface for query handlers."""
    
    @abstractmethod
    def handle(self, query: Q) -> R:
        """Handle a query.
        
        Args:
            query: The query to handle
            
        Returns:
            Query result
        """
        pass

# Specific Queries
@dataclass
class GetLottiQuery(Query):
    """Query per recuperare lotti."""
    criteria: Optional[LottoCriteria] = None
    page: int = 1
    page_size: int = 100
    sort_by: str = 'data_pubblicazione'
    sort_order: str = 'desc'

@dataclass
class GetLottoByIdQuery(Query):
    """Query per recuperare lotto specifico."""
    lotto_id: LottoId

@dataclass
class GetStatisticsQuery(Query):
    """Query per statistiche."""
    periodo: Periodo
    raggruppa_per: List[str] = None  # ['categoria', 'regione', 'ente']
    metriche: List[str] = None  # ['count', 'sum', 'avg']

@dataclass
class GetReportsQuery(Query):
    """Query per recuperare report."""
    tipo_report: Optional['TipoReport'] = None
    periodo: Optional[Periodo] = None
    stato: Optional['StatoReport'] = None

# Query Handlers
class GetLottiHandler(QueryHandler[GetLottiQuery, 'PagedResult[Lotto]']):
    """Handler per query lotti."""
    
    def __init__(self, lotto_repo: LottoRepository):
        self._lotto_repo = lotto_repo
    
    @abstractmethod
    def handle(self, query: GetLottiQuery) -> 'PagedResult[Lotto]':
        pass

class GetStatisticsHandler(QueryHandler[GetStatisticsQuery, 'StatisticsResult']):
    """Handler per query statistiche."""
    
    def __init__(self, lotto_repo: LottoRepository):
        self._lotto_repo = lotto_repo
    
    @abstractmethod
    def handle(self, query: GetStatisticsQuery) -> 'StatisticsResult':
        pass
```

## Event Interfaces

### Event Handling

```python
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, List
from datetime import datetime
import uuid

class DomainEvent(ABC):
    """Base class for domain events."""
    
    def __init__(self):
        self.event_id = str(uuid.uuid4())
        self.occurred_on = datetime.utcnow()
        self.version = 1
    
    @property
    @abstractmethod
    def event_type(self) -> str:
        """Type identifier for this event."""
        pass

T = TypeVar('T', bound=DomainEvent)

class EventHandler(Generic[T], ABC):
    """Interface for event handlers."""
    
    @abstractmethod
    def handle(self, event: T) -> None:
        """Handle a domain event.
        
        Args:
            event: The event to handle
        """
        pass
    
    @property
    @abstractmethod
    def handled_event_types(self) -> List[str]:
        """List of event types this handler can handle."""
        pass

class EventBus(ABC):
    """Interface for event bus."""
    
    @abstractmethod
    def publish(self, event: DomainEvent) -> None:
        """Publish an event.
        
        Args:
            event: Event to publish
        """
        pass
    
    @abstractmethod
    def subscribe(self, handler: EventHandler) -> None:
        """Subscribe a handler to events.
        
        Args:
            handler: Event handler to subscribe
        """
        pass
    
    @abstractmethod
    def unsubscribe(self, handler: EventHandler) -> None:
        """Unsubscribe a handler.
        
        Args:
            handler: Event handler to unsubscribe
        """
        pass

# Specific Events
class LottoCreatedEvent(DomainEvent):
    """Event fired when a lotto is created."""
    
    def __init__(self, lotto_id: LottoId, gara_id: GaraId):
        super().__init__()
        self.lotto_id = lotto_id
        self.gara_id = gara_id
    
    @property
    def event_type(self) -> str:
        return "lotto.created"

class LottoCategorizedEvent(DomainEvent):
    """Event fired when a lotto is categorized."""
    
    def __init__(self, lotto_id: LottoId, categoria: CategoriaLotto, confidence: float):
        super().__init__()
        self.lotto_id = lotto_id
        self.categoria = categoria
        self.confidence = confidence
    
    @property
    def event_type(self) -> str:
        return "lotto.categorized"

class ScrapingCompletedEvent(DomainEvent):
    """Event fired when scraping is completed."""
    
    def __init__(self, session_id: str, items_processed: int, errors: int):
        super().__init__()
        self.session_id = session_id
        self.items_processed = items_processed
        self.errors = errors
    
    @property
    def event_type(self) -> str:
        return "scraping.completed"
```

## Configuration Interfaces

```python
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Type, TypeVar

T = TypeVar('T')

class ConfigurationProvider(ABC):
    """Interface for configuration providers."""
    
    @abstractmethod
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        pass
    
    @abstractmethod
    def get_typed(self, key: str, value_type: Type[T], default: Optional[T] = None) -> T:
        """Get typed configuration value.
        
        Args:
            key: Configuration key
            value_type: Expected type
            default: Default value
            
        Returns:
            Configuration value of specified type
            
        Raises:
            ConfigurationError: If value cannot be converted to specified type
        """
        pass
    
    @abstractmethod
    def set(self, key: str, value: Any) -> None:
        """Set configuration value.
        
        Args:
            key: Configuration key
            value: Configuration value
        """
        pass
    
    @abstractmethod
    def has(self, key: str) -> bool:
        """Check if configuration key exists.
        
        Args:
            key: Configuration key
            
        Returns:
            True if key exists, False otherwise
        """
        pass
    
    @abstractmethod
    def get_section(self, section: str) -> Dict[str, Any]:
        """Get entire configuration section.
        
        Args:
            section: Section name
            
        Returns:
            Dictionary with section configuration
        """
        pass

# Specific Configuration Interfaces
class DatabaseConfig(ABC):
    """Database configuration interface."""
    
    @property
    @abstractmethod
    def connection_string(self) -> str:
        pass
    
    @property
    @abstractmethod
    def pool_size(self) -> int:
        pass
    
    @property
    @abstractmethod
    def timeout(self) -> int:
        pass

class ScrapingConfig(ABC):
    """Scraping configuration interface."""
    
    @property
    @abstractmethod
    def delay_between_requests(self) -> float:
        pass
    
    @property
    @abstractmethod
    def timeout(self) -> int:
        pass
    
    @property
    @abstractmethod
    def max_retries(self) -> int:
        pass
    
    @property
    @abstractmethod
    def user_agent(self) -> str:
        pass

class LLMConfig(ABC):
    """LLM configuration interface."""
    
    @property
    @abstractmethod
    def api_key(self) -> str:
        pass
    
    @property
    @abstractmethod
    def model_name(self) -> str:
        pass
    
    @property
    @abstractmethod
    def temperature(self) -> float:
        pass
    
    @property
    @abstractmethod
    def max_tokens(self) -> int:
        pass
    
    @property
    @abstractmethod
    def timeout(self) -> int:
        pass
```

## Exception Interfaces

```python
from abc import ABC
from typing import Optional, Dict, Any

class DomainError(Exception, ABC):
    """Base class for domain errors."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}
        self.error_code = self._get_error_code()
    
    @property
    def error_code(self) -> str:
        return self._error_code
    
    @error_code.setter
    def error_code(self, value: str):
        self._error_code = value
    
    def _get_error_code(self) -> str:
        return f"{self.__class__.__module__}.{self.__class__.__name__}"

class InfrastructureError(Exception, ABC):
    """Base class for infrastructure errors."""
    
    def __init__(self, message: str, cause: Optional[Exception] = None):
        super().__init__(message)
        self.message = message
        self.cause = cause

class ApplicationError(Exception, ABC):
    """Base class for application errors."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

# Specific Error Types
class LottoNotFoundError(DomainError):
    """Raised when a lotto is not found."""
    
    def __init__(self, lotto_id: LottoId):
        super().__init__(f"Lotto not found: {lotto_id}", {"lotto_id": str(lotto_id)})
        self.lotto_id = lotto_id

class InvalidCategorizationError(DomainError):
    """Raised when categorization is invalid."""
    
    def __init__(self, text: str, reason: str):
        super().__init__(f"Invalid categorization: {reason}", {"text": text, "reason": reason})
        self.text = text
        self.reason = reason

class ScrapingError(InfrastructureError):
    """Raised when scraping fails."""
    
    def __init__(self, url: str, status_code: Optional[int] = None, cause: Optional[Exception] = None):
        message = f"Scraping failed for URL: {url}"
        if status_code:
            message += f" (status: {status_code})"
        super().__init__(message, cause)
        self.url = url
        self.status_code = status_code

class LLMError(InfrastructureError):
    """Raised when LLM requests fail."""
    
    def __init__(self, request: LLMRequest, response_code: Optional[str] = None, cause: Optional[Exception] = None):
        message = f"LLM request failed for model: {request.model}"
        if response_code:
            message += f" (code: {response_code})"
        super().__init__(message, cause)
        self.request = request
        self.response_code = response_code

class CommandHandlingError(ApplicationError):
    """Raised when command handling fails."""
    
    def __init__(self, command_type: str, reason: str):
        super().__init__(f"Command handling failed: {command_type} - {reason}", 
                        {"command_type": command_type, "reason": reason})
        self.command_type = command_type
        self.reason = reason
```

## Contract Testing

```python
from abc import ABC, abstractmethod
from typing import Type, TypeVar, Generic

T = TypeVar('T')

class ContractTest(Generic[T], ABC):
    """Base class for contract testing."""
    
    def __init__(self, implementation: T):
        self.implementation = implementation
    
    @abstractmethod
    def test_contract(self) -> None:
        """Test that implementation satisfies contract."""
        pass

class RepositoryContractTest(ContractTest[LottoRepository]):
    """Contract test for LottoRepository implementations."""
    
    def test_contract(self) -> None:
        # Test save and find_by_id
        lotto = self._create_test_lotto()
        self.implementation.save(lotto)
        found = self.implementation.find_by_id(lotto.id)
        assert found is not None
        assert found.id == lotto.id
        
        # Test find_by_criteria
        criteria = LottoCriteria().with_categoria(lotto.categoria)
        results = self.implementation.find_by_criteria(criteria)
        assert len(results) >= 1
        assert any(r.id == lotto.id for r in results)
        
        # Test delete
        deleted = self.implementation.delete(lotto.id)
        assert deleted is True
        found_after_delete = self.implementation.find_by_id(lotto.id)
        assert found_after_delete is None
    
    def _create_test_lotto(self) -> Lotto:
        # Create test lotto instance
        pass

class ScraperContractTest(ContractTest[BaseScraper]):
    """Contract test for BaseScraper implementations."""
    
    def test_contract(self) -> None:
        # Test validate_connection
        is_valid = self.implementation.validate_connection()
        assert isinstance(is_valid, bool)
        
        # Test source_name property
        name = self.implementation.source_name
        assert isinstance(name, str)
        assert len(name) > 0
        
        # Test scrape method (with small date range)
        periodo = Periodo(
            datetime(2024, 1, 1),
            datetime(2024, 1, 2)
        )
        
        results = list(self.implementation.scrape(periodo))
        assert isinstance(results, list)
        # Results can be empty, but should not raise exception
```

Questi contratti di interfaccia forniscono una base solida per l'implementazione del sistema, garantendo:

1. **Consistency**: Interfacce coerenti tra tutti i layer
2. **Testability**: Contratti chiari per il testing
3. **Maintainability**: Separazione netta delle responsabilità
4. **Extensibility**: Facile aggiunta di nuove implementazioni
5. **Documentation**: Documentazione integrata nel codice

Ogni interfaccia definisce chiaramente:
- **Responsabilità**: Cosa fa il componente
- **Contratti**: Input/output attesi
- **Eccezioni**: Condizioni di errore
- **Esempi**: Come usare l'interfaccia

Questo approccio garantisce che tutte le implementazioni rispettino i contratti stabiliti e facilita il testing e la manutenzione del sistema.