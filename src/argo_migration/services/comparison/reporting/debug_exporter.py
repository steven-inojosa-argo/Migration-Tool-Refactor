"""
Debug file export utilities for data comparison debugging.

This module provides functionality to export comparison tables as CSV files
with metadata for detailed debugging and analysis.
"""

import os
import re
import logging
from typing import List
import pandas as pd

from ....utils.file_logger import get_file_logger


class DebugExporter:
    """Export comparison tables and metadata for debugging."""
    
    def __init__(self):
        """Initialize debug exporter."""
        self.logger = logging.getLogger("DebugExporter")
        self.file_logger = get_file_logger()
    
    def export_comparison_tables(self, domo_df: pd.DataFrame, sf_df: pd.DataFrame, 
                               domo_dataset_id: str, snowflake_table: str, 
                               key_columns: List[str]) -> None:
        """
        Export the tables that will be compared with datacompy as CSV files for debugging.
        
        Args:
            domo_df: Domo DataFrame
            sf_df: Snowflake DataFrame  
            domo_dataset_id: Domo dataset ID
            snowflake_table: Snowflake table name
            key_columns: Key columns for comparison
        """
        # Get current timestamp for directory structure
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        
        # Create debug directory with timestamp folder
        debug_dir = f"results/debug/{timestamp}"
        os.makedirs(debug_dir, exist_ok=True)
        
        # Clean table name for filename
        safe_table_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(snowflake_table).strip()) or "unknown_table"
        
        # Create descriptive filenames
        domo_filename = f"{safe_table_name}_domo_data_{timestamp}.csv"
        snowflake_filename = f"{safe_table_name}_snowflake_data_{timestamp}.csv"
        info_filename = f"{safe_table_name}_comparison_info_{timestamp}.txt"
        
        # Export DataFrames to CSV
        domo_filepath = os.path.join(debug_dir, domo_filename)
        snowflake_filepath = os.path.join(debug_dir, snowflake_filename)
        info_filepath = os.path.join(debug_dir, info_filename)
        
        try:
            # Save Domo data
            domo_df.to_csv(domo_filepath, index=False, encoding='utf-8')
            self.logger.info(f"💾 Exported Domo data: {domo_filepath}")
            
            # Save Snowflake data
            sf_df.to_csv(snowflake_filepath, index=False, encoding='utf-8')
            self.logger.info(f"💾 Exported Snowflake data: {snowflake_filepath}")
            
            # Create info file with metadata
            with open(info_filepath, 'w', encoding='utf-8') as f:
                f.write("COMPARISON DEBUG INFO\n")
                f.write("="*50 + "\n")
                f.write(f"Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Domo Dataset ID: {domo_dataset_id}\n")
                f.write(f"Snowflake Table: {snowflake_table}\n")
                f.write(f"Key Columns: {', '.join(key_columns)}\n")
                f.write(f"Domo Shape: {domo_df.shape}\n")
                f.write(f"Snowflake Shape: {sf_df.shape}\n")
                f.write("\nColumn Comparison:\n")
                f.write("-" * 30 + "\n")
                f.write(f"Domo columns ({len(domo_df.columns)}): {list(domo_df.columns)}\n")
                f.write(f"Snowflake columns ({len(sf_df.columns)}): {list(sf_df.columns)}\n")
                
                # Key column sample values
                f.write("\nKey Column Sample Values (First 10):\n")
                f.write("-" * 30 + "\n")
                for key_col in key_columns:
                    if key_col in domo_df.columns:
                        domo_key_values = domo_df[key_col].head(10).tolist()
                        f.write(f"Domo '{key_col}': {domo_key_values}\n")
                    if key_col in sf_df.columns:
                        sf_key_values = sf_df[key_col].head(10).tolist()
                        f.write(f"Snowflake '{key_col}': {sf_key_values}\n")
                
                f.write("\nFiles Generated:\n")
                f.write("-" * 15 + "\n")
                f.write(f"Domo CSV: {domo_filename}\n")
                f.write(f"Snowflake CSV: {snowflake_filename}\n")
                f.write(f"Info file: {info_filename}\n")
            
            self.logger.info(f"💾 Exported comparison info: {info_filepath}")
            self.logger.info(f"🗂 Debug files saved in: {debug_dir}/")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to export debug tables: {str(e)}")
            # Log to file logger as well
            self.file_logger.log_error("Debug Export", "Failed to save debug tables", str(e))
