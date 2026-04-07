# Gare Appalti - Nuova Architettura

## Executive Summary

Questo documento definisce la nuova architettura per il progetto Gare Appalti, basata sui principi SOLID e Domain-Driven Design (DDD). La ristrutturazione mira a migliorare manutenibilità, testabilità e scalabilità del sistema.

## Problemi Attuali Identificati

### Struttura Attuale
- File Python sparsi nella root (50+ file)
- Duplicazione di logiche (gazzetta_scraper*, download_ocds*)
- Mancanza di separazione delle responsabilità
- Dipendenze circolari tra moduli
- Configurazione hardcoded
- Test non organizzati

### Analisi dei Domini

Dopo l'analisi del codice, sono emersi questi bounded contexts principali:

1. **Procurement Domain**: Core business logic per gare e lotti
2. **Scraping Domain**: Acquisizione dati da fonti esterne
3. **Analysis Domain**: Elaborazione e analisi dei dati
4. **Reporting Domain**: Generazione report e dashboard
5. **Infrastructure**: Servizi condivisi e infrastruttura

## Nuova Struttura Proposta

```
gare_appalti/
├── pyproject.toml              # Project configuration
├── README.md                   # Project documentation
├── .env.example               # Environment template
├── .gitignore                 # Git ignore rules
│
├── src/
│   └── gare_appalti/          # Main package
│       ├── __init__.py
│       ├── main.py            # Application entry point
│       │
│       ├── core/              # Core domain (DDD)
│       │   ├── __init__.py
│       │   ├── entities/      # Domain entities
│       │   │   ├── __init__.py
│       │   │   ├── lotto.py
│       │   │   ├── gara.py
│       │   │   └── aggiudicazione.py
│       │   ├── value_objects/ # Value objects
│       │   │   ├── __init__.py
│       │   │   ├── categorization.py
│       │   │   ├── money.py
│       │   │   └── periodo.py
│       │   ├── repositories/  # Repository interfaces
│       │   │   ├── __init__.py
│       │   │   ├── lotto_repository.py
│       │   │   └── gara_repository.py
│       │   └── services/      # Domain services
│       │       ├── __init__.py
│       │       ├── categorization_service.py
│       │       └── validation_service.py
│       │
│       ├── application/       # Application layer (Use Cases)
│       │   ├── __init__.py
│       │   ├── commands/      # Command handlers
│       │   │   ├── __init__.py
│       │   │   ├── scrape_gazzetta.py
│       │   │   ├── analyze_lotti.py
│       │   │   └── generate_report.py
│       │   ├── queries/       # Query handlers
│       │   │   ├── __init__.py
│       │   │   ├── get_lotti.py
│       │   │   └── get_statistics.py
│       │   └── services/      # Application services
│       │       ├── __init__.py
│       │       ├── orchestrator.py
│       │       └── workflow_manager.py
│       │
│       ├── infrastructure/    # Infrastructure layer
│       │   ├── __init__.py
│       │   ├── persistence/   # Data access
│       │   │   ├── __init__.py
│       │   │   ├── repositories/
│       │   │   │   ├── __init__.py
│       │   │   │   ├── file_lotto_repository.py
│       │   │   │   └── json_gara_repository.py
│       │   │   └── models/     # Persistence models
│       │   │       ├── __init__.py
│       │   │       └── database_models.py
│       │   ├── external/      # External services
│       │   │   ├── __init__.py
│       │   │   ├── scrapers/
│       │   │   │   ├── __init__.py
│       │   │   │   ├── base_scraper.py
│       │   │   │   ├── gazzetta_scraper.py
│       │   │   │   └── ocds_scraper.py
│       │   │   ├── analyzers/
│       │   │   │   ├── __init__.py
│       │   │   │   ├── base_analyzer.py
│       │   │   │   ├── gazzetta_analyzer.py
│       │   │   │   └── ocds_analyzer.py
│       │   │   └── llm/
│       │   │       ├── __init__.py
│       │   │       ├── client.py
│       │   │       └── prompt_templates.py
│       │   └── config/        # Configuration
│       │       ├── __init__.py
│       │       ├── settings.py
│       │       └── logging_config.py
│       │
│       ├── presentation/      # Presentation layer
│       │   ├── __init__.py
│       │   ├── cli/           # Command line interface
│       │   │   ├── __init__.py
│       │   │   ├── main.py
│       │   │   └── commands.py
│       │   ├── api/           # Future REST API
│       │   │   ├── __init__.py
│       │   │   └── endpoints.py
│       │   └── reports/       # Report generation
│       │       ├── __init__.py
│       │       ├── excel_generator.py
│       │       └── dashboard.py
│       │
│       └── shared/            # Shared utilities
│           ├── __init__.py
│           ├── utils/
│           │   ├── __init__.py
│           │   ├── text_processing.py
│           │   ├── date_utils.py
│           │   └── file_utils.py
│           ├── exceptions/
│           │   ├── __init__.py
│           │   ├── domain_exceptions.py
│           │   └── infrastructure_exceptions.py
│           └── types/
│               ├── __init__.py
│               └── common_types.py
│
├── tests/                     # Test structure mirroring src
│   ├── __init__.py
│   ├── unit/                  # Unit tests
│   │   ├── __init__.py
│   │   ├── core/
│   │   ├── application/
│   │   └── infrastructure/
│   ├── integration/           # Integration tests
│   │   ├── __init__.py
│   │   ├── test_scrapers.py
│   │   └── test_analyzers.py
│   ├── end_to_end/           # E2E tests
│   │   ├── __init__.py
│   │   └── test_workflows.py
│   ├── fixtures/             # Test data
│   │   ├── __init__.py
│   │   ├── sample_data.json
│   │   └── mock_responses.py
│   └── conftest.py           # Pytest configuration
│
├── data/                     # Data directory (unchanged)
│   ├── cig/
│   ├── ocds/
│   ├── output/
│   ├── logs/
│   ├── temp/
│   └── checkpoints/
│
├── docs/                     # Documentation
│   ├── architecture/
│   │   ├── README.md
│   │   ├── domain_model.md
│   │   ├── api_design.md
│   │   └── deployment.md
│   ├── user_guide/
│   │   ├── installation.md
│   │   ├── usage.md
│   │   └── configuration.md
│   └── developer_guide/
│       ├── contributing.md
│       ├── testing.md
│       └── architecture.md
│
├── scripts/                  # Utility scripts
│   ├── migrate.py           # Migration script
│   ├── setup_dev.py         # Development setup
│   └── clean_data.py        # Data cleaning utilities
│
└── deployment/              # Deployment configurations
    ├── docker/
    │   ├── Dockerfile
    │   └── docker-compose.yml
    ├── k8s/
    │   └── manifests/
    └── terraform/
        └── infrastructure.tf
```

## Principi Architetturali Applicati

### Domain-Driven Design (DDD)

1. **Bounded Contexts**: Separazione chiara dei domini
2. **Entities**: Oggetti con identità (Lotto, Gara)
3. **Value Objects**: Oggetti immutabili (Categorization, Money)
4. **Repositories**: Astrazione per persistenza
5. **Domain Services**: Logiche di business complesse

### SOLID Principles

1. **Single Responsibility**: Ogni classe ha una sola responsabilità
2. **Open/Closed**: Estensibili senza modifiche
3. **Liskov Substitution**: Le implementazioni sono sostituibili
4. **Interface Segregation**: Interfacce specifiche e piccole
5. **Dependency Inversion**: Dipendenze da astrazioni

### Clean Architecture

1. **Dependency Rule**: Dipendenze verso l'interno
2. **Layer Separation**: Separazione netta tra layer
3. **Interface Abstraction**: Uso di interfacce per il disaccoppiamento

## Vantaggi della Nuova Struttura

### Manutenibilità
- Separazione chiara delle responsabilità
- Dipendenze esplicite e gestibili
- Codice più leggibile e comprensibile

### Testabilità
- Dependency injection facilita il mocking
- Struttura test che rispecchia il codice
- Isolamento delle unità di test

### Scalabilità
- Architettura modulare
- Facile aggiunta di nuovi scrapers/analyzers
- Supporto per future API REST

### Riusabilità
- Componenti ben definiti e riutilizzabili
- Interfacce chiare tra moduli
- Separazione tra logica di business e infrastruttura

## Pattern Implementati

### Repository Pattern
```python
from abc import ABC, abstractmethod

class LottoRepository(ABC):
    @abstractmethod
    def save(self, lotto: Lotto) -> None:
        pass
        
    @abstractmethod
    def find_by_id(self, lotto_id: str) -> Optional[Lotto]:
        pass
```

### Command Pattern
```python
class ScrapeLottiCommand:
    def __init__(self, scraper: GazzettaScraper):
        self._scraper = scraper
    
    def execute(self) -> List[Lotto]:
        return self._scraper.scrape_lotti()
```

### Factory Pattern
```python
class ScraperFactory:
    def create_scraper(self, source: str) -> BaseScraper:
        if source == "gazzetta":
            return GazzettaScraper()
        elif source == "ocds":
            return OCDSScraper()
        raise ValueError(f"Unknown source: {source}")
```

### Dependency Injection
```python
class AnalysisService:
    def __init__(self, 
                 lotto_repo: LottoRepository,
                 categorization_service: CategorizationService):
        self._lotto_repo = lotto_repo
        self._categorization_service = categorization_service
```

## Benefici Immediati

1. **Eliminazione duplicazioni**: Un solo scraper per fonte
2. **Test coverage**: Struttura che favorisce il testing
3. **Configurazione centralizzata**: Settings in un unico posto
4. **Logging strutturato**: Sistema di logging unificato
5. **Error handling**: Gestione errori centralizzata

## Prossimi Passi

1. Implementazione graduale dei moduli core
2. Migrazione dei file esistenti
3. Refactoring delle dipendenze
4. Aggiornamento dei test
5. Documentazione delle interfacce
6. Validazione con stakeholder

Questa architettura fornisce una base solida per il futuro sviluppo del sistema, garantendo manutenibilità e scalabilità a lungo termine.