# Refactoring Plan for Gare_new Repository

## Overview
This document provides a machine-readable refactoring plan to consolidate duplicate files while maintaining identical outputs and functionality.

## Phase 1: High Priority - Core Duplicates (Breaking Changes)

### 1.1 Gazzetta Scraper Consolidation
```yaml
source_files:
  - gazzetta_scraper.py (DELETE)
  - gazzetta_scraper_correct.py (DELETE)  
  - gazzetta_scraper_fixed.py (DELETE)
  - gazzetta_scraper_full.py (DELETE)
  - gazzetta_scraper_final.py (CANONICAL)

target_file: src/scrapers/gazzetta_scraper.py

actions:
  - copy: gazzetta_scraper_final.py -> src/scrapers/gazzetta_scraper.py
  - update_imports:
      - "from gazzetta_scraper_final import" -> "from src.scrapers.gazzetta_scraper import"
      - Add proper module structure imports
  - delete_source_files: 
      - gazzetta_scraper.py
      - gazzetta_scraper_correct.py
      - gazzetta_scraper_fixed.py
      - gazzetta_scraper_full.py
      - gazzetta_scraper_final.py

import_updates:
  files_to_update:
    - run_pipeline_*.py
    - tests/integration/test_gazzetta_*.py
    - Any scripts importing from gazzetta_scraper_*
  old_patterns:
    - "import gazzetta_scraper_final"
    - "from gazzetta_scraper_final import"
    - "import gazzetta_scraper"
  new_patterns:
    - "from src.scrapers.gazzetta_scraper import"

rationale: "gazzetta_scraper_final.py has the most complete implementation with proper filtering logic and 390 lines of functionality"
```

### 1.2 OCDS Downloader Consolidation  
```yaml
source_files:
  - download_ocds.py (DELETE)
  - download_ocds_simple.py (DELETE)
  - download_ocds_fixed.py (DELETE) 
  - download_ocds_progress.py (DELETE)
  - download_ocds_recent.py (DELETE)
  - download_ocds_specific.py (DELETE)
  - download_ocds_correct.py (CANONICAL)

target_file: src/scrapers/ocds_downloader.py

actions:
  - copy: download_ocds_correct.py -> src/scrapers/ocds_downloader.py
  - merge_features:
      progress_tracking: from download_ocds_progress.py
      recent_only_mode: from download_ocds_recent.py  
      specific_periods: from download_ocds_specific.py
  - update_class_name: "download_ocds" -> "OCDSDownloader"
  - add_strategy_pattern: for different download modes
  - delete_source_files: all download_ocds_*.py

import_updates:
  files_to_update:
    - run_pipeline_*.py
    - src/main.py
    - Any scripts calling download functions
  old_patterns:
    - "import download_ocds_correct"
    - "from download_ocds_correct import download_ocds"
    - "import download_ocds"
  new_patterns:
    - "from src.scrapers.ocds_downloader import OCDSDownloader"

rationale: "download_ocds_correct.py has proper URL handling and error management (275 lines), with features from other variants merged in"
```

### 1.3 Root vs Src Structure Cleanup
```yaml
duplicate_pairs:
  - source: concatenate.py
    target: src/analyzers/concatenate.py  
    action: DELETE_ROOT_KEEP_SRC
    
  - source: transformer.py
    target: src/processors/transformer.py
    action: DELETE_ROOT_KEEP_SRC
    
  - source: verbali.py  
    target: src/analyzers/verbali.py
    action: DELETE_ROOT_KEEP_SRC
    
  - source: servizio_luce.py
    target: src/analyzers/servizio_luce.py
    action: DELETE_ROOT_KEEP_SRC

import_updates:
  old_patterns:
    - "import concatenate"
    - "import transformer" 
    - "import verbali"
    - "import servizio_luce"
  new_patterns:
    - "from src.analyzers.concatenate import"
    - "from src.processors.transformer import"
    - "from src.analyzers.verbali import"
    - "from src.analyzers.servizio_luce import"
```

## Phase 2: Medium Priority - Analyzer Variants

### 2.1 Gazzetta Analyzer Consolidation
```yaml
source_files:
  - gazzetta_analyzer.py (ROOT, DELETE)
  - src/analyzers/gazzetta_analyzer.py (CANONICAL)
  - src/analyzers/gazzetta_analyzer_optimized.py (MERGE_OPTIMIZATIONS)

target_file: src/analyzers/gazzetta_analyzer.py

actions:
  - keep: src/analyzers/gazzetta_analyzer.py
  - merge_optimizations_from: src/analyzers/gazzetta_analyzer_optimized.py
  - delete: gazzetta_analyzer.py (root)
  - delete: src/analyzers/gazzetta_analyzer_optimized.py
  - add_performance_mode_flag: to enable optimizations

optimization_features_to_merge:
  - batch_processing_logic
  - memory_efficient_operations
  - parallel_processing_improvements
  - caching_mechanisms
```

### 2.2 OCDS Analyzer Consolidation
```yaml
source_files:
  - src/analyzers/ocds_analyzer.py (CANONICAL)
  - src/analyzers/ocds_analyzer_optimized.py (MERGE_OPTIMIZATIONS)
  - src/analyzers/ocds_analyzer_complete.py (MERGE_FEATURES)

target_file: src/analyzers/ocds_analyzer.py

actions:
  - keep: src/analyzers/ocds_analyzer.py
  - merge_complete_features_from: src/analyzers/ocds_analyzer_complete.py
  - merge_optimizations_from: src/analyzers/ocds_analyzer_optimized.py
  - delete: src/analyzers/ocds_analyzer_optimized.py
  - delete: src/analyzers/ocds_analyzer_complete.py
  - add_mode_flags: complete_analysis, optimized_mode
```

## Phase 3: Low Priority - Pipeline and Configuration

### 3.1 Pipeline Runner Consolidation
```yaml
source_files:
  - run_pipeline_complete.py (DELETE)
  - run_pipeline_custom.py (DELETE)
  - run_pipeline_final.py (CANONICAL)

target_file: src/pipeline/runner.py

actions:
  - copy: run_pipeline_final.py -> src/pipeline/runner.py
  - create: src/pipeline/__init__.py
  - merge_customization_features_from: run_pipeline_custom.py
  - add_cli_interface: with argparse for different modes
  - delete_source_files: all run_pipeline_*.py

features_to_preserve:
  from_complete:
    - full_pipeline_execution
    - comprehensive_error_handling
  from_custom:  
    - parameter_customization
    - selective_step_execution
  from_final:
    - integrated_feature_set
    - final_optimizations
```

### 3.2 Configuration Consolidation
```yaml
source_files:
  - config.py (DELETE)
  - config_optimized.py (DELETE) 
  - config/settings.py (CANONICAL)

target_file: config/settings.py

actions:
  - keep: config/settings.py
  - merge_optimizations_from: config_optimized.py
  - merge_basic_settings_from: config.py (if any unique)
  - delete: config.py
  - delete: config_optimized.py
  - ensure_env_variable_support: from optimized version

critical_features_to_preserve:
  - environment_variable_loading
  - api_key_management
  - model_configuration
  - performance_settings
```

## Phase 4: Cleanup - Obsolete Scripts

### 4.1 Remove Obsolete Scripts
```yaml
files_to_remove:
  - run_old_gazzetta_scraper.py: "Explicitly marked as old version"
  - force_resume_34000.py: "Very specific one-time fix script"
  
files_to_move_to_scripts:
  - debug_gazzetta_real.py -> scripts/debug/gazzetta_debug.py
  - check_checkpoint.py -> scripts/utils/check_checkpoint.py  
  - fix_resume.py -> scripts/utils/fix_resume.py
  - verify_and_fix_ocds.py -> scripts/utils/verify_ocds.py

files_to_consolidate:
  - run_optimized_analyzer.py: merge functionality into main runner
```

## Phase 5: Import and Reference Updates

### 5.1 Systematic Import Updates
```yaml
update_strategy: "regex_based_replacement"

search_and_replace_patterns:
  - pattern: "import gazzetta_scraper(_\w+)?"
    replacement: "from src.scrapers.gazzetta_scraper import GazzettaScraper" 
    files: "**/*.py"
    
  - pattern: "import download_ocds(_\w+)?"
    replacement: "from src.scrapers.ocds_downloader import OCDSDownloader"
    files: "**/*.py"
    
  - pattern: "^import (concatenate|transformer|verbali|servizio_luce)$"
    replacement: "from src.{analyzers|processors}.\\1 import"
    files: "**/*.py"
    
  - pattern: "from gazzetta_analyzer import"  
    replacement: "from src.analyzers.gazzetta_analyzer import"
    files: "**/*.py"
```

### 5.2 Test File Updates
```yaml
test_files_needing_updates:
  - tests/integration/test_gazzetta_*.py: Update all gazzetta scraper imports
  - tests/integration/test_ocds_*.py: Update OCDS downloader imports  
  - tests/integration/test_optimized_analyzer.py: Update analyzer imports
  - tests/unit/test_*.py: Update all modular imports

update_actions:
  - fix_import_paths: to use src.* structure
  - update_test_data_paths: if any hardcoded paths exist
  - verify_test_compatibility: ensure tests still pass
```

## Phase 6: Validation and Testing

### 6.1 Output Validation Strategy
```yaml
validation_approach: "before_after_comparison"

steps:
  1. baseline_run:
      - execute: current pipeline with existing duplicates
      - capture: all output files and their checksums
      - store: in data/validation/baseline/
      
  2. incremental_refactor:
      - apply: one phase at a time
      - test: after each phase
      - compare: outputs with baseline
      - rollback: if outputs differ
      
  3. final_validation:
      - full_pipeline_run: with refactored code
      - output_comparison: byte-by-byte where possible
      - semantic_comparison: for files that may have minor formatting differences
      - performance_comparison: ensure no degradation

critical_outputs_to_validate:
  - data/output/Final.xlsx
  - data/output/Gare.xlsx  
  - data/output/Lotti_*.xlsx
  - All generated CSV files
  - Log files consistency
```

### 6.2 Performance Impact Assessment
```yaml
metrics_to_track:
  - execution_time: overall pipeline duration
  - memory_usage: peak memory consumption  
  - disk_io: file operation efficiency
  - error_rates: any increase in failures

acceptable_changes:
  - execution_time: ±5% variation allowed
  - memory_usage: ±10% variation allowed
  - disk_io: improvement preferred, no degradation
  - error_rates: no increase acceptable
```

## Implementation Order

1. **Phase 1 (Day 1-2)**: Core duplicates - highest risk, highest impact
2. **Phase 2 (Day 3)**: Analyzer consolidation - medium risk
3. **Phase 3 (Day 4)**: Pipeline and config - lower risk
4. **Phase 4 (Day 5)**: Cleanup - lowest risk
5. **Phase 5-6 (Day 6-7)**: Import updates and validation

## Risk Mitigation

- **Backup**: Complete repository backup before starting
- **Branch Strategy**: Each phase in separate branch with validation
- **Rollback Plan**: Automated rollback if validation fails
- **Incremental Testing**: Test after each file move/consolidation
- **Output Verification**: Checksum validation of critical output files

## Success Criteria

✅ **Functional**: All existing functionality preserved  
✅ **Output Identical**: Generated files remain byte-identical  
✅ **Performance**: No degradation in execution time or memory usage  
✅ **Maintainability**: Single canonical version for each component  
✅ **Structure**: Proper modular organization in src/ directory  
✅ **Tests**: All existing tests continue to pass  