# System Architecture Diagrams - Gare Appalti

## C4 Model - System Context

```mermaid
C4Context
    title System Context - Gare Appalti Platform
    
    Person(user, "System User", "Analista che utilizza il sistema per analizzare gare")
    Person(admin, "System Admin", "Amministratore del sistema")
    
    System(gare_system, "Gare Appalti System", "Sistema per analisi e categorizzazione gare pubbliche")
    
    System_Ext(gazzetta, "Gazzetta Ufficiale", "Portale gare pubbliche italiane")
    System_Ext(ocds, "OCDS Data", "Open Contracting Data Standard")
    System_Ext(openai, "OpenAI API", "Large Language Model per analisi testi")
    System_Ext(excel, "Microsoft Excel", "Tool per visualizzazione report")
    
    Rel(user, gare_system, "Utilizza per analizzare gare")
    Rel(admin, gare_system, "Configura e monitora")
    
    Rel(gare_system, gazzetta, "Scraping dati gare", "HTTPS")
    Rel(gare_system, ocds, "Download dati OCDS", "HTTPS")
    Rel(gare_system, openai, "Analisi categorizzazione", "REST API")
    Rel(gare_system, excel, "Esporta report", "XLSX files")
```

## C4 Model - Container Diagram

```mermaid
C4Container
    title Container Diagram - Gare Appalti System
    
    Person(user, "User")
    
    Container_Boundary(gare_system, "Gare Appalti System") {
        Container(cli, "CLI Application", "Python Click", "Interface a riga di comando")
        Container(core, "Core Domain", "Python", "Logica di business e modelli")
        Container(app_layer, "Application Layer", "Python", "Use cases e orchestrazione")
        Container(infra, "Infrastructure", "Python", "Servizi esterni e persistenza")
        
        ContainerDb(file_storage, "File Storage", "JSON/Excel/CSV", "Persistenza dati")
        ContainerDb(cache, "Cache", "In-Memory", "Cache risultati LLM")
    }
    
    System_Ext(gazzetta, "Gazzetta Ufficiale")
    System_Ext(ocds, "OCDS Data")
    System_Ext(openai, "OpenAI API")
    
    Rel(user, cli, "Esegue comandi")
    Rel(cli, app_layer, "Invoca use cases")
    Rel(app_layer, core, "Utilizza domain logic")
    Rel(app_layer, infra, "Accede a servizi")
    
    Rel(infra, file_storage, "Legge/scrive dati")
    Rel(infra, cache, "Cache gestione")
    Rel(infra, gazzetta, "Scraping")
    Rel(infra, ocds, "Download")
    Rel(infra, openai, "LLM requests")
```

## C4 Model - Component Diagram (Core Domain)

```mermaid
C4Component
    title Component Diagram - Core Domain
    
    Container_Boundary(core, "Core Domain") {
        Component(entities, "Entities", "Python Classes", "Gara, Lotto, Aggiudicazione")
        Component(value_objects, "Value Objects", "Python Classes", "Money, CategoriaLotto, Periodo")
        Component(repositories, "Repository Interfaces", "Python ABC", "Contratti persistenza")
        Component(domain_services, "Domain Services", "Python Classes", "CategorizationService, ValidationService")
        Component(events, "Domain Events", "Python Classes", "Eventi di dominio")
    }
    
    Container_Boundary(app, "Application Layer") {
        Component(commands, "Command Handlers", "Python Classes", "Gestione comandi")
        Component(queries, "Query Handlers", "Python Classes", "Gestione query")
        Component(app_services, "Application Services", "Python Classes", "Orchestrazione workflow")
    }
    
    Container_Boundary(infra, "Infrastructure") {
        Component(repo_impl, "Repository Implementations", "Python Classes", "Persistenza concreta")
        Component(scrapers, "Scrapers", "Python Classes", "Data acquisition")
        Component(analyzers, "Analyzers", "Python Classes", "Text analysis")
    }
    
    Rel(commands, entities, "Crea e modifica")
    Rel(commands, domain_services, "Utilizza")
    Rel(queries, repositories, "Query dati")
    Rel(domain_services, value_objects, "Crea")
    Rel(app_services, commands, "Orchestr")
    Rel(repo_impl, entities, "Persiste")
    Rel(analyzers, domain_services, "Utilizza")
```

## Deployment Diagram

```mermaid
deployment
    title Deployment Architecture
    
    node "Development Machine" {
        component "Python 3.11+" as python
        component "CLI Application" as cli
        database "Local File System" as files {
            folder "data/" as data_folder
            folder "logs/" as logs_folder
            folder "checkpoints/" as checkpoints
        }
    }
    
    cloud "External Services" {
        component "Gazzetta Ufficiale" as gazzetta
        component "OCDS Portal" as ocds  
        component "OpenAI API" as openai_api
    }
    
    cli --> python
    python --> files
    python --> gazzetta : HTTPS/Selenium
    python --> ocds : HTTPS/requests
    python --> openai_api : HTTPS/REST
```

## Data Flow Diagram

```mermaid
graph TD
    A["Utente avvia scraping"] --> B["CLI riceve comando"]
    B --> C["Application Service"]
    C --> D["Scraping Command Handler"]
    
    D --> E["Gazzetta Scraper"]
    D --> F["OCDS Scraper"]
    
    E --> G["Raw HTML Data"]
    F --> H["Raw JSON Data"]
    
    G --> I["Gazzetta Analyzer"]
    H --> J["OCDS Analyzer"]
    
    I --> K["Text Analysis (LLM)"]
    J --> K
    
    K --> L["Categorization Service"]
    L --> M["Lotto Entity Creation"]
    
    M --> N["Repository Save"]
    N --> O["File Persistence"]
    
    O --> P["Report Generation"]
    P --> Q["Excel Export"]
    Q --> R["Output Files"]
    
    style A fill:#e1f5fe
    style R fill:#c8e6c9
    style K fill:#fff3e0
    style L fill:#fff3e0
```

## Sequence Diagram - Scraping Workflow

```mermaid
sequenceDiagram
    participant U as User
    participant CLI as CLI Interface
    participant AS as Application Service
    participant CH as Command Handler
    participant S as Scraper
    participant A as Analyzer
    participant CS as Categorization Service
    participant R as Repository
    participant FS as File Storage
    
    U->>CLI: scrape-gazzetta --start-date 2024-01-01
    CLI->>AS: create_scraping_workflow()
    AS->>CH: handle(ScrapeCommand)
    
    CH->>S: scrape_data(date_range)
    S->>S: navigate_pages()
    S->>S: extract_html_content()
    S-->>CH: raw_data_list
    
    loop For each raw_data
        CH->>A: analyze(raw_data)
        A->>A: extract_text_info()
        A->>CS: categorize(text)
        CS->>CS: apply_business_rules()
        CS-->>A: categoria_lotto
        A-->>CH: analyzed_lotto
        
        CH->>R: save(lotto)
        R->>FS: persist(lotto_data)
        FS-->>R: success
        R-->>CH: saved
    end
    
    CH-->>AS: scraping_completed
    AS-->>CLI: workflow_result
    CLI-->>U: "Scraping completato: N lotti processati"
```

## Class Diagram - Core Entities

```mermaid
classDiagram
    class Gara {
        -GaraId id
        -string titolo
        -Ente ente
        -List~Lotto~ lotti
        -StatoGara stato
        -List~DomainEvent~ events
        +aggiungi_lotto(Lotto)
        +pubblica()
        +get_importo_totale() Money
    }
    
    class Lotto {
        -LottoId id
        -string oggetto
        -Money importo
        -CategoriaLotto categoria
        -Aggiudicazione aggiudicazione
        +categorizza(CategoriaLotto)
        +aggiudica(Aggiudicazione)
        +is_categorized() bool
    }
    
    class Money {
        -Decimal amount
        -Currency currency
        +add(Money) Money
        +subtract(Money) Money
        +multiply(int) Money
        +is_zero() bool
    }
    
    class CategoriaLotto {
        -TipoCategoria categoria_principale
        -List~string~ sottocategorie
        -float confidence
        +is_illuminazione() bool
        +is_energia() bool
        +has_alta_confidence() bool
    }
    
    class Periodo {
        -datetime inizio
        -datetime fine
        +contiene(datetime) bool
        +durata_giorni() int
        +overlap(Periodo) bool
    }
    
    class Aggiudicazione {
        -AggiudicazioneId id
        -string impresa
        -Money importo_aggiudicazione
        -datetime data_aggiudicazione
        +is_valida() bool
    }
    
    Gara ||--o{ Lotto : contains
    Lotto ||--|| Money : has
    Lotto ||--o| CategoriaLotto : categorized_as
    Lotto ||--o| Aggiudicazione : awarded_to
    Gara ||--|| Periodo : valid_in
```

## Architecture Decision Records (ADRs)

### ADR-001: Clean Architecture with DDD

**Status**: Accepted

**Context**: Il sistema attuale ha problemi di manutenibilità dovuti a:
- Logica di business sparsa
- Dipendenze circolari
- Test difficili da scrivere
- Codice duplicato

**Decision**: Adottare Clean Architecture con Domain-Driven Design

**Consequences**:
- **Positive**: Separazione chiara responsabilità, testabilità, manutenibilità
- **Negative**: Maggiore complessità iniziale, più file da gestire
- **Neutral**: Necessità di formazione team su DDD

### ADR-002: File-Based Persistence

**Status**: Accepted

**Context**: Il sistema attuale utilizza file JSON/Excel per persistenza

**Decision**: Mantenere file-based storage con repository pattern

**Consequences**:
- **Positive**: Semplicità, no database setup, portabilità
- **Negative**: Performance limitate, no transazioni ACID
- **Neutral**: Possibilità futura migrazione a DB

### ADR-003: Command Query Responsibility Segregation (CQRS)

**Status**: Accepted

**Context**: Il sistema ha pattern di lettura e scrittura differenti

**Decision**: Separare command handlers da query handlers

**Consequences**:
- **Positive**: Ottimizzazione separata read/write, scalabilità
- **Negative**: Maggiore complessità implementazione
- **Neutral**: Preparazione per future ottimizzazioni

### ADR-004: Dependency Injection Container

**Status**: Accepted

**Context**: Gestione dipendenze complesse tra layer

**Decision**: Utilizzare dependency-injector library

**Consequences**:
- **Positive**: Loose coupling, testabilità, configurabilità
- **Negative**: Learning curve, setup iniziale
- **Neutral**: Standard industry practice

## Performance Considerations

### Bottleneck Analysis

1. **LLM API Calls**
   - **Issue**: Latenza alta, rate limiting
   - **Solution**: Caching, batch processing, async calls
   - **Metrics**: Response time, tokens/minute

2. **Web Scraping**
   - **Issue**: Network latency, anti-bot protection
   - **Solution**: Respectful delays, session management
   - **Metrics**: Pages/minute, success rate

3. **File I/O Operations**
   - **Issue**: Large datasets, frequent writes
   - **Solution**: Batch writes, compression, async I/O
   - **Metrics**: MB/s throughput, IOPS

### Scalability Patterns

```mermaid
graph TD
    A["Request"] --> B["Load Balancer"]
    B --> C["Worker Pool"]
    C --> D["Queue Manager"]
    D --> E["Async Workers"]
    E --> F["Cache Layer"]
    F --> G["Storage Layer"]
    
    H["Monitoring"] --> C
    H --> D
    H --> E
    
    style H fill:#ffecb3
    style F fill:#e8f5e8
```

### Memory Management

- **Strategy**: Lazy loading, streaming processing
- **Patterns**: Generator functions, context managers
- **Monitoring**: Memory profiling, garbage collection metrics
- **Limits**: Max 4GB RAM usage, auto-cleanup after batch

## Security Considerations

### Data Protection
- **Encryption**: Sensitive config in environment variables
- **Access Control**: File permissions, API key rotation
- **Audit Trail**: Operation logging, error tracking
- **Privacy**: No PII storage, anonymized logs

### External Service Security
- **API Keys**: Secure storage, rotation schedule
- **Rate Limiting**: Respect service limits, exponential backoff
- **Input Validation**: Sanitize scraped data, validate configs
- **Network Security**: HTTPS only, certificate validation

## Monitoring and Observability

### Metrics Collection
```python
class MetricsCollector:
    def track_scraping_session(self, session: ScrapingSession):
        # Durata sessione
        # Items processati
        # Errori incontrati
        # Bandwidth utilizzo
        pass
    
    def track_llm_usage(self, request: LLMRequest, response: LLMResponse):
        # Tokens utilizzati
        # Latenza request
        # Costo request
        # Success rate
        pass
```

### Health Checks
```python
class HealthChecker:
    def check_external_services(self) -> HealthStatus:
        # Gazzetta availability
        # OCDS endpoint status  
        # OpenAI API status
        # File system space
        pass
```

### Alerting Rules
- **Error Rate**: > 5% in 10 minutes
- **Response Time**: > 30s average
- **Disk Space**: < 10% available
- **API Quota**: > 80% utilized

Questa documentazione architettural fornisce una visione completa del sistema, dalle decisioni di alto livello ai dettagli implementativi, garantendo che tutti gli stakeholder abbiano una comprensione chiara della struttura e dei principi guida del nuovo sistema.