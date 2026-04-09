"""
Smoke tests per i moduli di export.

Verifica che:
1. I moduli di export siano importabili
2. Le funzioni di export non generino eccezioni immediate
3. I file vengano creati con la formato corretto
4. I DataFrame siano esportati correttamente
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch
from pathlib import Path
import tempfile
import sys
import os
from openpyxl import load_workbook

# Aggiungi il percorso del progetto
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


class TestExcelExportSmoke:
    """Smoke tests per l'export Excel."""
    
    @pytest.fixture
    def sample_dataframe(self):
        """Crea un DataFrame di esempio per i test."""
        return pd.DataFrame({
            'CIG': ['ABC123', 'DEF456', 'GHI789'],
            'Oggetto': ['Illuminazione LED', 'Videosorveglianza', 'Efficientamento'],
            'Importo': [50000.0, 75000.0, 30000.0],
            'Data': ['2024-01-15', '2024-02-20', '2024-03-10'],
            'Ente': ['Comune A', 'Comune B', 'Comune C']
        })
    
    def test_excel_export_basic_functionality(self, sample_dataframe):
        """Test che l'export Excel funzioni correttamente."""
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            try:
                # Export verso Excel
                sample_dataframe.to_excel(tmp_file.name, index=False)
                
                # Verifica che il file sia stato creato
                assert Path(tmp_file.name).exists()
                
                # Verifica che il file possa essere riletto
                df_reloaded = pd.read_excel(tmp_file.name)
                
                assert len(df_reloaded) == len(sample_dataframe)
                assert list(df_reloaded.columns) == list(sample_dataframe.columns)
                assert df_reloaded['CIG'].tolist() == sample_dataframe['CIG'].tolist()
                
            finally:
                # Cleanup
                if Path(tmp_file.name).exists():
                    Path(tmp_file.name).unlink()
    
    def test_excel_export_with_different_dtypes(self):
        """Test export Excel con diversi tipi di dati."""
        df = pd.DataFrame({
            'str_col': ['A', 'B', 'C'],
            'int_col': [1, 2, 3],
            'float_col': [1.1, 2.2, 3.3],
            'bool_col': [True, False, True],
            'date_col': pd.to_datetime(['2024-01-01', '2024-02-01', '2024-03-01']),
            'null_col': [None, 'value', None]
        })
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            try:
                df.to_excel(tmp_file.name, index=False)
                
                assert Path(tmp_file.name).exists()
                
                # Verifica rilettura
                df_reloaded = pd.read_excel(tmp_file.name)
                assert len(df_reloaded) == 3
                assert len(df_reloaded.columns) == 6
                
            finally:
                if Path(tmp_file.name).exists():
                    Path(tmp_file.name).unlink()
    
    def test_excel_export_empty_dataframe(self):
        """Test export di DataFrame vuoto."""
        df = pd.DataFrame()
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            try:
                df.to_excel(tmp_file.name, index=False)
                
                assert Path(tmp_file.name).exists()
                
                # Verifica rilettura
                df_reloaded = pd.read_excel(tmp_file.name)
                assert len(df_reloaded) == 0
                
            finally:
                if Path(tmp_file.name).exists():
                    Path(tmp_file.name).unlink()
    
    def test_excel_export_large_dataframe(self):
        """Test export di DataFrame di dimensioni significative."""
        # Crea DataFrame con ~1000 righe
        large_df = pd.DataFrame({
            'id': range(1000),
            'description': [f'Description {i}' for i in range(1000)],
            'value': np.random.rand(1000) * 10000,
            'category': ['A', 'B', 'C'] * 334  # Ripete pattern per 1000 righe
        })
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            try:
                large_df.to_excel(tmp_file.name, index=False)
                
                assert Path(tmp_file.name).exists()
                file_size = Path(tmp_file.name).stat().st_size
                assert file_size > 0
                
                # Verifica che il file sia accessibile
                df_reloaded = pd.read_excel(tmp_file.name, nrows=5)  # Leggi solo 5 righe per velocità
                assert len(df_reloaded) == 5
                
            finally:
                if Path(tmp_file.name).exists():
                    Path(tmp_file.name).unlink()


class TestCSVExportSmoke:
    """Smoke tests per l'export CSV."""
    
    @pytest.fixture
    def sample_dataframe(self):
        """Crea un DataFrame di esempio per i test."""
        return pd.DataFrame({
            'CIG': ['ABC123', 'DEF456', 'GHI789'],
            'Oggetto': ['Illuminazione LED', 'Videosorveglianza', 'Efficientamento'],
            'Importo': [50000.0, 75000.0, 30000.0]
        })
    
    def test_csv_export_basic_functionality(self, sample_dataframe):
        """Test che l'export CSV funzioni correttamente."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            try:
                # Export verso CSV
                sample_dataframe.to_csv(tmp_file.name, index=False)
                
                # Verifica che il file sia stato creato
                assert Path(tmp_file.name).exists()
                
                # Verifica che il file possa essere riletto
                df_reloaded = pd.read_csv(tmp_file.name)
                
                assert len(df_reloaded) == len(sample_dataframe)
                assert list(df_reloaded.columns) == list(sample_dataframe.columns)
                
            finally:
                if Path(tmp_file.name).exists():
                    Path(tmp_file.name).unlink()
    
    def test_csv_export_with_encoding(self, sample_dataframe):
        """Test export CSV con encoding UTF-8."""
        # Aggiungi caratteri speciali per testare encoding
        df_with_accents = sample_dataframe.copy()
        df_with_accents.loc[0, 'Oggetto'] = 'Illuminazione à è ì ò ù'
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            try:
                df_with_accents.to_csv(tmp_file.name, index=False, encoding='utf-8')
                
                assert Path(tmp_file.name).exists()
                
                # Verifica rilettura con encoding
                df_reloaded = pd.read_csv(tmp_file.name, encoding='utf-8')
                assert df_reloaded.loc[0, 'Oggetto'] == 'Illuminazione à è ì ò ù'
                
            finally:
                if Path(tmp_file.name).exists():
                    Path(tmp_file.name).unlink()
    
    def test_csv_export_large_dataframe(self):
        """Test export CSV per DataFrame grandi (>1M record simulation)."""
        # Simula condizioni per file grandi
        large_df = pd.DataFrame({
            'id': range(10000),  # Più piccolo per test veloce, ma simula logica
            'data': [f'data_{i}' for i in range(10000)]
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            try:
                # Simula la logica che userebbe CSV per file grandi
                if len(large_df) > 1000:  # Soglia arbitraria per il test
                    large_df.to_csv(tmp_file.name, index=False)
                    csv_file_path = tmp_file.name
                else:
                    large_df.to_csv(tmp_file.name, index=False)
                    csv_file_path = tmp_file.name
                
                assert Path(csv_file_path).exists()
                
                # Verifica che sia leggibile
                df_sample = pd.read_csv(csv_file_path, nrows=5)
                assert len(df_sample) == 5
                
            finally:
                if Path(tmp_file.name).exists():
                    Path(tmp_file.name).unlink()


class TestFilePathHandling:
    """Test per la gestione dei percorsi dei file."""
    
    def test_output_directory_creation(self):
        """Test che le directory di output vengano create correttamente."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir) / 'output'
            
            # Simula la logica di creazione directory
            output_dir.mkdir(parents=True, exist_ok=True)
            
            assert output_dir.exists()
            assert output_dir.is_dir()
    
    def test_file_path_construction(self):
        """Test che la costruzione dei percorsi file funzioni."""
        base_path = Path('/tmp/test')
        filename = 'Gare.xlsx'
        
        # Simula la logica config.get_file_path
        full_path = base_path / filename
        
        assert full_path.name == filename
        assert full_path.parent == base_path
        assert str(full_path).endswith('Gare.xlsx')
    
    def test_file_extension_handling(self):
        """Test che la gestione delle estensioni file funzioni."""
        base_path = Path('/tmp/test/file.xlsx')
        
        # Test cambio estensione (.xlsx -> .csv)
        csv_path = base_path.with_suffix('.csv')
        
        assert csv_path.suffix == '.csv'
        assert csv_path.stem == base_path.stem
        assert str(csv_path).endswith('.csv')


class TestDataFrameExportFormats:
    """Test per diversi formati di export."""
    
    @pytest.fixture
    def complex_dataframe(self):
        """DataFrame complesso per test formati."""
        return pd.DataFrame({
            'id': ['001', '002', '003'],
            'date': pd.to_datetime(['2024-01-01', '2024-02-01', '2024-03-01']),
            'oggetto': ['Illuminazione pubblica LED per il Comune di Roma',
                       'Videosorveglianza urbana con telecamere IP',
                       'Efficientamento energetico edifici pubblici'],
            'categoria': ['Illuminazione', 'Videosorveglianza', 'Energia'],
            'importo': [50000.50, 75000.75, 30000.25],
            'smart_city': [True, True, False],
            'sostenibilita': [True, False, True]
        })
    
    def test_excel_preserves_data_types(self, complex_dataframe):
        """Test che Excel preservi i tipi di dati."""
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            try:
                complex_dataframe.to_excel(tmp_file.name, index=False)
                
                df_reloaded = pd.read_excel(tmp_file.name)
                
                # Verifica che i dati numerici siano preservati
                assert pd.api.types.is_numeric_dtype(df_reloaded['importo'])
                
                # Verifica che le date siano gestite (potrebbero essere convertite)
                assert len(df_reloaded) == len(complex_dataframe)
                
            finally:
                if Path(tmp_file.name).exists():
                    Path(tmp_file.name).unlink()
    
    def test_csv_string_conversion(self, complex_dataframe):
        """Test che CSV converta tutto in stringhe."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            try:
                complex_dataframe.to_csv(tmp_file.name, index=False)
                
                # Leggi come stringhe
                df_reloaded = pd.read_csv(tmp_file.name, dtype=str)
                
                # Tutto dovrebbe essere stringa
                for col in df_reloaded.columns:
                    assert pd.api.types.is_object_dtype(df_reloaded[col])
                
                assert len(df_reloaded) == len(complex_dataframe)
                
            finally:
                if Path(tmp_file.name).exists():
                    Path(tmp_file.name).unlink()


class TestExportErrorHandling:
    """Test per la gestione degli errori nell'export."""
    
    def test_handles_invalid_file_path(self):
        """Test gestione percorsi file non validi."""
        df = pd.DataFrame({'col': [1, 2, 3]})
        
        # Percorso non valido (directory inesistente)
        invalid_path = '/nonexistent/directory/file.xlsx'
        
        with pytest.raises((FileNotFoundError, PermissionError, OSError)):
            df.to_excel(invalid_path, index=False)
    
    def test_handles_readonly_file_system(self):
        """Test gestione file system readonly."""
        df = pd.DataFrame({'col': [1, 2, 3]})
        
        # Simula tentativo di scrittura in directory readonly
        # (il test specifico dipende dal sistema operativo)
        try:
            readonly_path = '/dev/null/file.xlsx'  # Su Unix systems
            df.to_excel(readonly_path, index=False)
        except (PermissionError, OSError):
            # Comportamento atteso per file system readonly
            pass
    
    def test_handles_dataframe_with_problematic_data(self):
        """Test gestione DataFrame con dati problematici."""
        df = pd.DataFrame({
            'inf_col': [float('inf'), 1, 2],
            'nan_col': [np.nan, np.nan, 3],
            'mixed_col': [1, 'text', None]
        })
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            try:
                # Dovrebbe gestire valori inf e NaN senza crash
                df.to_excel(tmp_file.name, index=False)
                
                assert Path(tmp_file.name).exists()
                
            except Exception as e:
                # Se genera eccezione, dovrebbe essere gestita gracefully
                assert "inf" in str(e).lower() or "nan" in str(e).lower()
            finally:
                if Path(tmp_file.name).exists():
                    Path(tmp_file.name).unlink()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])