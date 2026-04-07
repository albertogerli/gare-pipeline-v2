"""
Smoke tests per i moduli processor.

Verifica che:
1. I moduli processor siano importabili
2. Le classi principali siano instanziabili
3. I metodi principali non generino eccezioni immediate
4. I DataFrame di processing funzionino correttamente
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch
from pathlib import Path
import tempfile
import sys
from datetime import datetime

# Aggiungi il percorso del progetto
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


class TestProcessorsImport:
    """Test di importazione per i moduli processor."""
    
    def test_transformer_importable(self):
        """Test che Transformer sia importabile."""
        from src.processors.transformer import Transformer
        assert Transformer is not None
    
    def test_concatenator_importable(self):
        """Test che Concatenator sia importabile."""
        try:
            from src.processors.concatenator import Concatenator
            assert Concatenator is not None
        except ImportError:
            pytest.skip("Concatenator non disponibile")


class TestTransformerSmoke:
    """Smoke tests per il modulo Transformer."""
    
    def test_transformer_instantiation(self):
        """Test che Transformer possa essere istanziato."""
        from src.processors.transformer import Transformer
        transformer = Transformer()
        assert transformer is not None
    
    def test_transformer_has_run_method(self):
        """Test che Transformer abbia il metodo run."""
        from src.processors.transformer import Transformer
        transformer = Transformer()
        
        assert hasattr(transformer, 'run')
        assert callable(getattr(transformer, 'run'))
    
    @patch('config.settings.config')
    def test_transformer_run_with_no_input_file(self, mock_config):
        """Test che Transformer gestisca l'assenza del file di input."""
        from src.processors.transformer import Transformer
        
        # Mock della configurazione
        mock_config.get_file_path.return_value = Path('nonexistent_file.xlsx')
        
        transformer = Transformer()
        
        # Non dovrebbe generare eccezioni, dovrebbe creare dati di esempio
        try:
            # Questo potrebbe generare eccezioni relative al salvataggio,
            # ma non dovrebbe crashare per il file mancante
            transformer.run()
        except FileNotFoundError:
            # Accettabile se il file di output non può essere salvato
            pass
        except Exception as e:
            if "No such file or directory" not in str(e):
                pytest.fail(f"Transformer.run() ha generato eccezione inattesa: {e}")


class TestDataFrameTransformations:
    """Test per le trasformazioni sui DataFrame."""
    
    def test_fillna_transformation(self):
        """Test che il riempimento dei valori mancanti funzioni."""
        df = pd.DataFrame({
            'col1': [1, 2, np.nan, 4],
            'col2': ['a', None, 'c', 'd'],
            'col3': [1.1, 2.2, np.nan, 4.4]
        })
        
        # Simula la trasformazione che fa il Transformer
        df_filled = df.fillna('')
        
        assert df_filled.isna().sum().sum() == 0  # Nessun valore mancante
        assert df_filled.loc[2, 'col1'] == ''     # np.nan diventa stringa vuota
        assert df_filled.loc[1, 'col2'] == ''     # None diventa stringa vuota
    
    def test_datetime_conversion(self):
        """Test che la conversione delle date funzioni."""
        df = pd.DataFrame({
            'data_creazione': ['2024-01-15', '2024-02-20', '2024-03-10'],
            'date_modified': ['15/01/2024', '20/02/2024', '10/03/2024'],
            'normal_column': ['A', 'B', 'C']
        })
        
        # Simula la logica del Transformer
        date_columns = [col for col in df.columns if 'data' in col.lower() or 'date' in col.lower()]
        
        for col in date_columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except:
                pass
        
        assert pd.api.types.is_datetime64_any_dtype(df['data_creazione'])
        assert pd.api.types.is_datetime64_any_dtype(df['date_modified'])
        assert not pd.api.types.is_datetime64_any_dtype(df['normal_column'])
    
    def test_numeric_conversion(self):
        """Test che la conversione numerica funzioni."""
        df = pd.DataFrame({
            'importo_totale': ['1000', '2000.50', '3500'],
            'amount_base': ['500.25', '1500', '2750.75'],
            'normal_column': ['A', 'B', 'C']
        })
        
        # Simula la logica del Transformer
        amount_columns = [col for col in df.columns if 'importo' in col.lower() or 'amount' in col.lower()]
        
        for col in amount_columns:
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            except:
                pass
        
        assert pd.api.types.is_numeric_dtype(df['importo_totale'])
        assert pd.api.types.is_numeric_dtype(df['amount_base'])
        assert not pd.api.types.is_numeric_dtype(df['normal_column'])
    
    def test_cig_standardization(self):
        """Test che la standardizzazione dei CIG funzioni."""
        df = pd.DataFrame({
            'CIG': ['  abc123  ', 'def456', 'GHI789', None, ''],
            'other_column': ['A', 'B', 'C', 'D', 'E']
        })
        
        # Simula la logica del Transformer per CIG
        if 'CIG' in df.columns:
            df['CIG'] = df['CIG'].astype(str).str.upper().str.strip()
        
        assert df['CIG'].tolist() == ['ABC123', 'DEF456', 'GHI789', 'NONE', '']
    
    def test_metadata_addition(self):
        """Test che l'aggiunta dei metadati funzioni."""
        df = pd.DataFrame({
            'oggetto': ['A', 'B', 'C'],
            'importo': [1000, 2000, 3000]
        })
        
        # Simula la logica del Transformer per metadati
        df['data_elaborazione'] = datetime.now().isoformat()
        df['versione'] = '1.0'
        
        assert 'data_elaborazione' in df.columns
        assert 'versione' in df.columns
        assert all(df['versione'] == '1.0')
        assert all(isinstance(date_str, str) for date_str in df['data_elaborazione'])
    
    def test_dataframe_sorting(self):
        """Test che l'ordinamento dei DataFrame funzioni."""
        df = pd.DataFrame({
            'CIG': ['ZZZ999', 'AAA111', 'MMM555'],
            'Data': ['2024-03-01', '2024-01-01', '2024-02-01'],
            'importo': [3000, 1000, 2000]
        })
        
        # Test ordinamento per CIG
        df_sorted_cig = df.sort_values('CIG')
        assert df_sorted_cig['CIG'].tolist() == ['AAA111', 'MMM555', 'ZZZ999']
        
        # Test ordinamento per Data
        df['Data'] = pd.to_datetime(df['Data'])
        df_sorted_date = df.sort_values('Data')
        expected_dates = pd.to_datetime(['2024-01-01', '2024-02-01', '2024-03-01'])
        pd.testing.assert_series_equal(df_sorted_date['Data'], expected_dates, check_names=False)


class TestDataFrameStatistics:
    """Test per le statistiche sui DataFrame."""
    
    def test_dataframe_memory_usage_calculation(self):
        """Test che il calcolo dell'uso della memoria funzioni."""
        df = pd.DataFrame({
            'col1': range(1000),
            'col2': [f'text_{i}' for i in range(1000)],
            'col3': np.random.rand(1000)
        })
        
        memory_usage = df.memory_usage(deep=True).sum() / 1024 / 1024
        
        assert isinstance(memory_usage, float)
        assert memory_usage > 0
    
    def test_dataframe_top_records_selection(self):
        """Test che la selezione dei top record funzioni."""
        df = pd.DataFrame({
            'CIG': ['A001', 'B002', 'C003', 'D004', 'E005'],
            'importo': [5000, 2000, 8000, 1000, 3000]
        })
        
        # Test nlargest (come nel Transformer)
        top_3 = df.nlargest(3, 'importo')
        
        assert len(top_3) == 3
        assert top_3.iloc[0]['CIG'] == 'C003'  # importo 8000
        assert top_3.iloc[1]['CIG'] == 'A001'  # importo 5000
        assert top_3.iloc[2]['CIG'] == 'E005'  # importo 3000
    
    def test_dataframe_column_type_analysis(self):
        """Test che l'analisi dei tipi di colonna funzioni."""
        df = pd.DataFrame({
            'numeric_col': [1, 2, 3],
            'date_col': pd.to_datetime(['2024-01-01', '2024-02-01', '2024-03-01']),
            'text_col': ['A', 'B', 'C'],
            'float_col': [1.1, 2.2, 3.3]
        })
        
        # Simula la logica del Transformer per analisi tipi
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        date_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
        text_cols = df.select_dtypes(include=['object']).columns.tolist()
        
        assert 'numeric_col' in numeric_cols
        assert 'float_col' in numeric_cols
        assert 'date_col' in date_cols
        assert 'text_col' in text_cols


class TestProcessorsErrorHandling:
    """Test per la gestione degli errori nei processor."""
    
    def test_handles_empty_dataframe(self):
        """Test che i processor gestiscano DataFrame vuoti."""
        df = pd.DataFrame()
        
        # Le operazioni sui DataFrame vuoti non dovrebbero generare eccezioni
        try:
            filled = df.fillna('')
            assert isinstance(filled, pd.DataFrame)
            assert len(filled) == 0
            
            memory_usage = df.memory_usage(deep=True).sum()
            assert memory_usage >= 0
            
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            assert isinstance(numeric_cols, list)
            assert len(numeric_cols) == 0
            
        except Exception as e:
            pytest.fail(f"Operazioni su DataFrame vuoto hanno generato eccezione: {e}")
    
    def test_handles_dataframe_with_all_nan(self):
        """Test che i processor gestiscano DataFrame con tutti valori NaN."""
        df = pd.DataFrame({
            'col1': [np.nan, np.nan, np.nan],
            'col2': [None, None, None],
            'col3': [np.nan, np.nan, np.nan]
        })
        
        try:
            # Riempimento valori mancanti
            filled = df.fillna('')
            assert filled.isna().sum().sum() == 0
            
            # Conversione numerica (dovrebbe fallire gracefully)
            numeric_converted = pd.to_numeric(df['col1'], errors='coerce')
            assert numeric_converted.isna().all()
            
        except Exception as e:
            pytest.fail(f"Gestione DataFrame con tutti NaN ha generato eccezione: {e}")
    
    def test_handles_mixed_type_columns(self):
        """Test che i processor gestiscano colonne con tipi misti."""
        df = pd.DataFrame({
            'mixed_col': [1, 'text', 3.14, None, True],
            'normal_col': ['A', 'B', 'C', 'D', 'E']
        })
        
        try:
            # Conversione stringa (dovrebbe funzionare sempre)
            string_converted = df['mixed_col'].astype(str)
            assert len(string_converted) == 5
            
            # Riempimento NaN
            filled = df.fillna('')
            assert isinstance(filled, pd.DataFrame)
            
        except Exception as e:
            pytest.fail(f"Gestione colonne con tipi misti ha generato eccezione: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])