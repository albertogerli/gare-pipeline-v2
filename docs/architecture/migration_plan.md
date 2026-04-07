# Piano di Migrazione - Gare Appalti

## Panoramica

Questo documento descrive il piano dettagliato per la migrazione dalla struttura attuale alla nuova architettura basata su principi SOLID e Domain-Driven Design.

## Analisi dei File Attuali

### File Root da Migrare (47 file Python)

#### Categoria: Scrapers
- `gazzetta_scraper.py` → `src/gare_appalti/infrastructure/external/scrapers/gazzetta_scraper.py`
- `gazzetta_scraper_*.py` → Da consolidare nel nuovo scraper
- `download_ocds*.py` → `src/gare_appalti/infrastructure/external/scrapers/ocds_scraper.py`
- `download_cigs.py` → `src/gare_appalti/infrastructure/external/scrapers/cig_scraper.py`

#### Categoria: Analyzers
- `gazzetta_analyzer.py` → `src/gare_appalti/infrastructure/external/analyzers/gazzetta_analyzer.py`
- `servizio_luce.py` → `src/gare_appalti/infrastructure/external/analyzers/servizio_luce_analyzer.py`
- `verbali.py` → `src/gare_appalti/infrastructure/external/analyzers/verbali_analyzer.py`

#### Categoria: Processors
- `concatenate.py` → `src/gare_appalti/application/commands/concatenate_data.py`
- `json_to_excel.py` → `src/gare_appalti/presentation/reports/excel_generator.py`
- `transformer.py` → `src/gare_appalti/infrastructure/persistence/models/transformers.py`

#### Categoria: Configuration
- `config.py` → `src/gare_appalti/infrastructure/config/settings.py`
- `config_optimized.py` → Da merge in settings.py

#### Categoria: Main Applications
- `main.py` → `src/gare_appalti/main.py`
- `run_*.py` → `src/gare_appalti/presentation/cli/commands.py`

#### Categoria: Testing
- `test_*.py` → `tests/integration/` o `tests/unit/`
- `debug_*.py` → `scripts/debug/`

#### Categoria: Utilities
- `check_checkpoint.py` → `src/gare_appalti/shared/utils/checkpoint_utils.py`
- `fix_resume.py` → `scripts/maintenance/`
- `force_resume_*.py` → `scripts/maintenance/`
- `verify_and_fix_ocds.py` → `scripts/data_quality/`

### File src/ Esistenti da Riorganizzare

#### Already Well-Placed (Keep)
- `src/models/` → Mantenere come core entities
- `src/utils/` → Spostare in shared/utils

#### Need Reorganization
- `src/analyzers/` → Consolidare in infrastructure/external/analyzers
- `src/scrapers/` → Consolidare in infrastructure/external/scrapers
- `src/processors/` → Dividere tra application layer e infrastructure

## Fasi di Migrazione

### Fase 1: Preparazione (Settimana 1)

#### Step 1.1: Backup e Branching
```bash
# Creare branch per migrazione
git checkout -b migration/new-architecture

# Backup dei file attuali
mkdir -p migration_backup
cp *.py migration_backup/
```

#### Step 1.2: Creazione Struttura
```bash
# Creare la nuova struttura directory
mkdir -p src/gare_appalti/{core,application,infrastructure,presentation,shared}
mkdir -p src/gare_appalti/core/{entities,value_objects,repositories,services}
mkdir -p src/gare_appalti/application/{commands,queries,services}
mkdir -p src/gare_appalti/infrastructure/{persistence,external,config}
mkdir -p src/gare_appalti/infrastructure/external/{scrapers,analyzers,llm}
mkdir -p src/gare_appalti/infrastructure/persistence/{repositories,models}
mkdir -p src/gare_appalti/presentation/{cli,api,reports}
mkdir -p src/gare_appalti/shared/{utils,exceptions,types}
```

#### Step 1.3: Dependency Analysis
- Mappare tutte le dipendenze tra file
- Identificare dipendenze circolari
- Pianificare ordine di migrazione

### Fase 2: Core Domain (Settimana 2)

#### Step 2.1: Migrate Domain Models
```python
# Da: src/models/lotto.py
# A: src/gare_appalti/core/entities/lotto.py

# Refactor per separare:
# - Entities: Identità e comportamenti
# - Value Objects: Dati immutabili
# - Domain Services: Logiche di business complesse
```

#### Step 2.2: Create Repository Interfaces
```python
# src/gare_appalti/core/repositories/lotto_repository.py
from abc import ABC, abstractmethod
from typing import List, Optional
from ..entities.lotto import Lotto

class LottoRepository(ABC):
    @abstractmethod
    def save(self, lotto: Lotto) -> None:
        pass
    
    @abstractmethod
    def find_by_id(self, lotto_id: str) -> Optional[Lotto]:
        pass
    
    @abstractmethod
    def find_by_criteria(self, criteria: dict) -> List[Lotto]:
        pass
```

#### Step 2.3: Domain Services
```python
# src/gare_appalti/core/services/categorization_service.py
class CategorizationService:
    def categorize_lotto(self, lotto: Lotto) -> CategoriaLotto:
        # Logica di categorizzazione
        pass
```

### Fase 3: Application Layer (Settimana 3)

#### Step 3.1: Command Handlers
```python
# src/gare_appalti/application/commands/scrape_gazzetta.py
class ScrapeGazzettaCommand:
    def __init__(self, date_range: DateRange):
        self.date_range = date_range

class ScrapeGazzettaHandler:
    def __init__(self, scraper: GazzettaScraper, repo: LottoRepository):
        self._scraper = scraper
        self._repo = repo
    
    def handle(self, command: ScrapeGazzettaCommand) -> List[Lotto]:
        # Implementation
        pass
```

#### Step 3.2: Query Handlers
```python
# src/gare_appalti/application/queries/get_lotti.py
class GetLottiQuery:
    def __init__(self, filters: dict):
        self.filters = filters

class GetLottiHandler:
    def __init__(self, repo: LottoRepository):
        self._repo = repo
    
    def handle(self, query: GetLottiQuery) -> List[Lotto]:
        return self._repo.find_by_criteria(query.filters)
```

### Fase 4: Infrastructure Layer (Settimana 4-5)

#### Step 4.1: Migrate Scrapers
```python
# Consolidare tutti i gazzetta_scraper_*.py in:
# src/gare_appalti/infrastructure/external/scrapers/gazzetta_scraper.py

class GazzettaScraper(BaseScraper):
    def __init__(self, config: ScrapingConfig):
        self._config = config
    
    def scrape_lotti(self, date_range: DateRange) -> List[Lotto]:
        # Implementazione unificata
        pass
```

#### Step 4.2: Migrate Analyzers
```python
# Consolidare analyzer logic in:
# src/gare_appalti/infrastructure/external/analyzers/

class GazzettaAnalyzer(BaseAnalyzer):
    def __init__(self, llm_client: LLMClient):
        self._llm_client = llm_client
    
    def analyze_text(self, text: str) -> AnalysisResult:
        # Implementazione
        pass
```

#### Step 4.3: Repository Implementations
```python
# src/gare_appalti/infrastructure/persistence/repositories/file_lotto_repository.py
class FileLottoRepository(LottoRepository):
    def __init__(self, data_path: Path):
        self._data_path = data_path
    
    def save(self, lotto: Lotto) -> None:
        # Implementazione file-based
        pass
```

### Fase 5: Presentation Layer (Settimana 6)

#### Step 5.1: CLI Interface
```python
# src/gare_appalti/presentation/cli/commands.py
import click
from ...application.commands.scrape_gazzetta import ScrapeGazzettaCommand, ScrapeGazzettaHandler

@click.command()
@click.option('--start-date')
@click.option('--end-date')
def scrape_gazzetta(start_date, end_date):
    # Implementazione CLI
    pass
```

#### Step 5.2: Report Generation
```python
# src/gare_appalti/presentation/reports/excel_generator.py
class ExcelReportGenerator:
    def generate_lotti_report(self, lotti: List[Lotto], output_path: Path):
        # Migrazione da json_to_excel.py
        pass
```

### Fase 6: Configuration and Utilities (Settimana 7)

#### Step 6.1: Configuration Management
```python
# src/gare_appalti/infrastructure/config/settings.py
from pydantic import BaseSettings

class Settings(BaseSettings):
    # Consolidare config.py e config_optimized.py
    openai_api_key: str
    scraping_delay: int = 1
    
    class Config:
        env_file = ".env"
```

#### Step 6.2: Shared Utilities
```python
# src/gare_appalti/shared/utils/text_processing.py
# Migrazione delle utility da src/utils/text.py

# src/gare_appalti/shared/exceptions/domain_exceptions.py
class LottoNotFound(Exception):
    pass

class InvalidCategorizationError(Exception):
    pass
```

### Fase 7: Testing Migration (Settimana 8)

#### Step 7.1: Test Structure
```
tests/
├── unit/
│   ├── core/
│   │   ├── test_entities.py
│   │   ├── test_value_objects.py
│   │   └── test_services.py
│   ├── application/
│   │   ├── test_commands.py
│   │   └── test_queries.py
│   └── infrastructure/
│       ├── test_scrapers.py
│       └── test_repositories.py
├── integration/
│   ├── test_scraping_workflow.py
│   └── test_analysis_workflow.py
└── end_to_end/
    └── test_complete_pipeline.py
```

#### Step 7.2: Migrate Existing Tests
```python
# Migrare test_*.py files mantenendo la copertura
# Aggiornare per nuova architettura
# Aggiungere mock per dependency injection
```

### Fase 8: Dependency Injection Setup (Settimana 9)

#### Step 8.1: Container Setup
```python
# src/gare_appalti/infrastructure/di_container.py
from dependency_injector import containers, providers

class Container(containers.DeclarativeContainer):
    # Configuration
    config = providers.Configuration()
    
    # Infrastructure
    lotto_repository = providers.Factory(
        FileLottoRepository,
        data_path=config.data_path
    )
    
    # Application Services
    scrape_gazzetta_handler = providers.Factory(
        ScrapeGazzettaHandler,
        scraper=providers.Factory(GazzettaScraper),
        repo=lotto_repository
    )
```

## Mappatura File Dettagliata

### Root Files Migration Map

| File Corrente | Destinazione | Azione | Priorità |
|---------------|--------------|--------|---------|
| `gazzetta_analyzer.py` | `infrastructure/external/analyzers/gazzetta_analyzer.py` | Refactor + Move | High |
| `gazzetta_scraper.py` | `infrastructure/external/scrapers/gazzetta_scraper.py` | Refactor + Move | High |
| `main.py` | `main.py` + `presentation/cli/main.py` | Split + Move | High |
| `config.py` | `infrastructure/config/settings.py` | Consolidate | High |
| `concatenate.py` | `application/commands/concatenate_data.py` | Refactor + Move | Medium |
| `json_to_excel.py` | `presentation/reports/excel_generator.py` | Refactor + Move | Medium |
| `transformer.py` | `infrastructure/persistence/transformers.py` | Move | Medium |
| `verbali.py` | `infrastructure/external/analyzers/verbali_analyzer.py` | Move | Low |
| `servizio_luce.py` | `infrastructure/external/analyzers/servizio_luce_analyzer.py` | Move | Low |

### Duplicate Files Consolidation

| Pattern | Files | Action |
|---------|-------|--------|
| `gazzetta_scraper_*` | 5 files | Merge into single scraper with config |
| `download_ocds_*` | 8 files | Merge into single OCDS scraper |
| `test_*` | 10 files | Organize by test type (unit/integration) |
| `run_*` | 6 files | Convert to CLI commands |

## Script di Migrazione

### Automated Migration Script

```python
# scripts/migrate.py
import os
import shutil
from pathlib import Path

class MigrationTool:
    def __init__(self, source_dir: Path, target_dir: Path):
        self.source_dir = source_dir
        self.target_dir = target_dir
        self.migration_map = self._load_migration_map()
    
    def migrate_file(self, source_file: str, target_path: str):
        """Migrate a single file with basic refactoring"""
        source_path = self.source_dir / source_file
        target_full_path = self.target_dir / target_path
        
        # Create target directory if not exists
        target_full_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Copy and refactor file
        self._copy_with_refactoring(source_path, target_full_path)
    
    def _copy_with_refactoring(self, source: Path, target: Path):
        """Copy file with basic import refactoring"""
        with open(source, 'r') as f:
            content = f.read()
        
        # Basic import refactoring
        content = self._update_imports(content)
        
        with open(target, 'w') as f:
            f.write(content)
    
    def _update_imports(self, content: str) -> str:
        """Update import statements for new structure"""
        import_map = {
            'import config': 'from gare_appalti.infrastructure.config import settings',
            'from models': 'from gare_appalti.core.entities',
            # Add more mappings
        }
        
        for old_import, new_import in import_map.items():
            content = content.replace(old_import, new_import)
        
        return content

# Usage
migrator = MigrationTool(Path('.'), Path('src/gare_appalti'))
migrator.migrate_all()
```

### Validation Script

```python
# scripts/validate_migration.py
class MigrationValidator:
    def validate_structure(self):
        """Validate new directory structure"""
        required_dirs = [
            'src/gare_appalti/core/entities',
            'src/gare_appalti/application/commands',
            'src/gare_appalti/infrastructure/external/scrapers',
            # ... more directories
        ]
        
        for dir_path in required_dirs:
            assert Path(dir_path).exists(), f"Missing directory: {dir_path}"
    
    def validate_imports(self):
        """Validate that all imports are resolved"""
        # Check for import errors in migrated files
        pass
    
    def validate_tests(self):
        """Run tests to ensure functionality is preserved"""
        import subprocess
        result = subprocess.run(['python', '-m', 'pytest'], capture_output=True)
        assert result.returncode == 0, "Tests failed after migration"

# Run validation
validator = MigrationValidator()
validator.validate_all()
```

## Rollback Plan

In caso di problemi:

1. **Immediate Rollback**: Ripristino dal backup
2. **Partial Rollback**: Ripristino di specifici moduli
3. **Forward Fix**: Correzione dei problemi in-place

```bash
# Emergency rollback
git checkout main
# or
cp -r migration_backup/* .
```

## Timeline e Milestones

### Settimana 1-2: Foundation
- [ ] Creazione struttura directory
- [ ] Migrazione core domain models
- [ ] Setup dependency injection

### Settimana 3-4: Business Logic
- [ ] Migrazione scrapers
- [ ] Migrazione analyzers  
- [ ] Application services

### Settimana 5-6: Infrastructure
- [ ] Repository implementations
- [ ] Configuration management
- [ ] External service integrations

### Settimana 7-8: Presentation & Testing
- [ ] CLI interface
- [ ] Report generation
- [ ] Test migration
- [ ] End-to-end testing

### Settimana 9: Finalization
- [ ] Performance testing
- [ ] Documentation update
- [ ] Deployment preparation
- [ ] Team training

## Success Criteria

1. **Functional Parity**: Tutte le funzionalità esistenti preserved
2. **Test Coverage**: >= 80% code coverage
3. **Performance**: No performance degradation
4. **Code Quality**: Improved maintainability metrics
5. **Documentation**: Complete API and architecture docs

## Risk Mitigation

### High Risk
- **Data Loss**: Backup before migration + staged rollout
- **Functionality Breaking**: Comprehensive testing at each stage
- **Performance Regression**: Performance benchmarks

### Medium Risk  
- **Import Errors**: Automated import checking
- **Configuration Issues**: Environment-specific testing

### Low Risk
- **Documentation Gaps**: Documentation review process
- **Training Needs**: Gradual team onboarding

Questo piano di migrazione garantisce una transizione sicura e graduale verso la nuova architettura, minimizzando i rischi e preservando tutte le funzionalità esistenti.