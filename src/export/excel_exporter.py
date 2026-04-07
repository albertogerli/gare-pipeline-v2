"""Excel export utilities."""

import os
from typing import Any, Dict, List, Optional

import pandas as pd

from ..config.config import Config


class ExcelExporter:
    """Unified Excel export functionality."""

    def __init__(self, config: Optional[Config] = None):
        """Initialize exporter with configuration.

        Args:
            config: Configuration object, defaults to global config
        """
        self.config = config or Config()

    def export_dataframe(
        self,
        df: pd.DataFrame,
        filename: str,
        directory: Optional[str] = None,
        index: bool = False,
        **kwargs,
    ) -> str:
        """Export DataFrame to Excel file.

        Args:
            df: DataFrame to export
            filename: Output filename
            directory: Output directory (defaults to config temp dir)
            index: Whether to include index
            **kwargs: Additional arguments for pandas.to_excel()

        Returns:
            Full path to exported file
        """
        directory = directory or self.config.TEMP_DIR

        # Ensure directory exists
        os.makedirs(directory, exist_ok=True)

        output_path = os.path.join(directory, filename)

        # Remove existing file
        if os.path.exists(output_path):
            os.remove(output_path)

        # Export to Excel
        df.to_excel(output_path, index=index, engine="openpyxl", **kwargs)

        print(f"Exported {len(df)} records to {output_path}")
        return output_path

    def concatenate_files(
        self,
        file_paths: List[str],
        output_filename: str,
        output_directory: Optional[str] = None,
    ) -> str:
        """Concatenate multiple Excel files.

        Args:
            file_paths: List of Excel file paths to concatenate
            output_filename: Output filename
            output_directory: Output directory (defaults to config temp dir)

        Returns:
            Path to concatenated file
        """
        output_directory = output_directory or self.config.TEMP_DIR
        output_path = os.path.join(output_directory, output_filename)

        # Remove existing file
        if os.path.exists(output_path):
            os.remove(output_path)

        dataframes = []
        for file_path in file_paths:
            if os.path.exists(file_path):
                df = pd.read_excel(file_path)
                dataframes.append(df)
                print(f"Loaded {len(df)} records from {file_path}")
            else:
                print(f"Warning: File {file_path} not found")

        if not dataframes:
            raise ValueError("No valid files found to concatenate")

        # Concatenate all DataFrames
        combined_df = pd.concat(dataframes, ignore_index=True, sort=False)

        # Export result
        return self.export_dataframe(combined_df, output_filename, output_directory)

    @staticmethod
    def concatenate_lotti():
        """Concatenate Gazzetta and OCDS lotti files."""
        config = Config()
        exporter = ExcelExporter(config)

        file_paths = [
            os.path.join(config.TEMP_DIR, config.LOTTI_GAZZETTA),
            os.path.join(config.TEMP_DIR, config.LOTTI_OCDS),
        ]

        return exporter.concatenate_files(
            file_paths, config.LOTTI_MERGED, config.TEMP_DIR
        )

    @staticmethod
    def concatenate_all():
        """Concatenate all final files."""
        config = Config()
        exporter = ExcelExporter(config)

        file_paths = [
            os.path.join(config.TEMP_DIR, config.SERVIZIO_LUCE_CONSIP_CIG),
            os.path.join(config.TEMP_DIR, config.VERBALI),
        ]

        return exporter.concatenate_files(file_paths, config.FINAL, config.TEMP_DIR)


# For backward compatibility
class Concatenator(ExcelExporter):
    """Backward compatibility class."""

    @staticmethod
    def concat_lotti():
        """Legacy method for concatenating lotti."""
        return ExcelExporter.concatenate_lotti()

    @staticmethod
    def concat_all():
        """Legacy method for concatenating all files."""
        return ExcelExporter.concatenate_all()


if __name__ == "__main__":
    # ExcelExporter.concatenate_lotti()
    ExcelExporter.concatenate_all()
