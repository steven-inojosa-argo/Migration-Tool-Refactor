"""
Data comparison utilities using datacompy.

This module provides comprehensive data comparison functionality including
sampling coordination, column transformation, and datacompy integration.
"""

import os
import re
import logging
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
import datacompy

from .sampling.sampler import SmartSampler
from .sampling.query_builder import escape_domo_column_list, normalize_snowflake_column_list
from .reporting.debug_exporter import DebugExporter
from ...utils.common import transform_column_name
from ...api.domo import DomoHandler
from ...api.snowflake import SnowflakeHandler


class DataComparator:
    """Compare data samples between Domo and Snowflake using datacompy."""
    
    def __init__(self, domo_handler: DomoHandler, snowflake_handler: SnowflakeHandler):
        """
        Initialize data comparator.
        
        Args:
            domo_handler: Initialized Domo handler
            snowflake_handler: Initialized Snowflake handler
        """
        self.domo_handler = domo_handler
        self.snowflake_handler = snowflake_handler
        self.logger = logging.getLogger("DataComparator")
        self.sampler = SmartSampler(domo_handler, snowflake_handler)
        self.debug_exporter = DebugExporter()
        self.errors = []
        self.session_timestamp = None
    
    def set_session_timestamp(self, timestamp: str):
        """Set the session timestamp for this comparator."""
        self.session_timestamp = timestamp
    
    def compare_data_samples(self, domo_dataset_id: str, snowflake_table: str, 
                           key_columns: List[str], sample_size: Optional[int] = None, 
                           transform_names: bool = False, schema_comparison: Dict[str, Any] = None, 
                           sampling_method: str = "random", export_debug_tables: bool = False,
                           domo_column_mapping: dict = None, use_intelligent_mapping: bool = False,
                           intelligent_mapping: dict = None) -> Dict[str, Any]:
        """
        Compare data samples using datacompy.
        
        Args:
            domo_dataset_id: Domo dataset ID
            snowflake_table: Snowflake table name
            key_columns: List of key columns for comparison
            sample_size: Number of rows to sample (auto-calculated if None)
            transform_names: Whether to apply column name transformation
            schema_comparison: Schema comparison results to include in report
            sampling_method: Sampling method ('random' for smart random with fallback, 'ordered' for direct ordered)
            export_debug_tables: If True, export the comparison tables as CSV files to results/debug/ for debugging
            domo_column_mapping: Mapping from normalized to original Domo column names
            use_intelligent_mapping: Whether to use intelligent column mapping
            intelligent_mapping: Intelligent column mapping dictionary
            
        Returns:
            Dictionary with data comparison results
        """
        self.logger.info("🔍 Comparing data samples...")
        
        # Always normalize key columns for comparison (regardless of transform_names setting)
        # This ensures compatibility between Domo and Snowflake column names
        normalized_key_columns = [transform_column_name(col) for col in key_columns]
        self.logger.info(f"🔄 Key columns normalized for comparison: {key_columns} → {normalized_key_columns}")
        
        # Use provided mapping or create empty one
        domo_column_mapping = domo_column_mapping or {}
        
        # Map normalized key columns back to original Domo names for queries
        domo_key_columns = []
        self.logger.info(f"🔍 Debugging column mapping:")
        self.logger.info(f"  Normalized key columns: {normalized_key_columns}")
        self.logger.info(f"  Domo column mapping available: {bool(domo_column_mapping)}")
        if domo_column_mapping:
            self.logger.info(f"  _domo_original_columns keys: {list(domo_column_mapping.keys())}")
            self.logger.info(f"  _domo_original_columns values: {list(domo_column_mapping.values())}")
        
        for normalized_key in normalized_key_columns:
            self.logger.info(f"🔍 Looking for '{normalized_key}' in domo_column_mapping...")
            self.logger.info(f"  Available keys: {list(domo_column_mapping.keys())}")
            if normalized_key in domo_column_mapping:
                domo_key_columns.append(domo_column_mapping[normalized_key])
                self.logger.info(f"  ✅ '{normalized_key}' → '{domo_column_mapping[normalized_key]}'")
            else:
                # Fallback: assume the key column name is already in Domo format
                domo_key_columns.append(normalized_key)
                self.logger.info(f"  ⚠️ '{normalized_key}' → '{normalized_key}' (no mapping found)")
        
        self.logger.info(f"🔄 Key columns mapped back to Domo format: {normalized_key_columns} → {domo_key_columns}")
        
        # Get total count and calculate sample size if needed
        try:
            # Use DomoHandler for count query
            domo_count_query = "SELECT COUNT(*) as row_count FROM table"
            domo_result = self.domo_handler.query_dataset(domo_dataset_id, domo_count_query)
            total_domo_rows = domo_result['rows'][0][0] if domo_result['rows'] else 0
            
            if sample_size is None:
                sample_size = self.sampler.calculate_sample_size(total_domo_rows)
                
            self.logger.info(f"Total rows: {total_domo_rows:,}, Sample size: {sample_size:,}")
            
        except Exception as e:
            self._add_error("Sample Size Calculation", "Could not get total row count", str(e))
            sample_size = sample_size or 1000
        
        # Get synchronized samples based on chosen sampling method
        if sampling_method == "ordered":
            # Direct ordered sampling - skip random sampling entirely
            self.logger.info("🎯 Using direct ordered sampling (no random attempt)")
            actual_sampling_method = "Ordered Sampling"
            try:
                domo_df, sf_df = self.sampler.get_ordered_samples(
                    domo_dataset_id, snowflake_table, key_columns, sample_size, domo_column_mapping
                )
            except Exception as ordered_error:
                self._add_error("Data Sampling", "Ordered sampling failed", str(ordered_error))
                return {'error': f"Ordered sampling failed: {ordered_error}"}
        else:
            # Random sampling with fallback (default behavior)
            actual_sampling_method = "Random Sampling"
            try:
                # Use the new smart random sampling approach
                domo_df, sf_df = self.sampler.get_smart_random_samples(
                    domo_dataset_id, snowflake_table, key_columns, sample_size, domo_column_mapping
                )
                
            except Exception as e:
                self.logger.error(f"❌ Smart random sampling failed: {e}")
                self.logger.info("🔄 Falling back to ordered sampling...")
                actual_sampling_method = "Ordered Sampling"
                
                # Fallback to original deterministic sampling
                try:
                    domo_df, sf_df = self.sampler.get_ordered_samples(
                        domo_dataset_id, snowflake_table, key_columns, sample_size, domo_column_mapping
                    )
                    self.logger.info("✅ Fallback sampling completed")
                    
                except Exception as fallback_error:
                    self._add_error("Sample Extraction", "Both smart and fallback sampling failed", str(fallback_error))
                    return self._get_error_data_result(sample_size)
        
        # Apply column transformation if enabled (applies to both sampling methods)
        if transform_names:
            self.logger.info("🔄 Applying full column name transformation...")
            
            # Transform Domo columns
            original_domo_columns = domo_df.columns.tolist()
            transformed_domo_columns = [transform_column_name(col) for col in original_domo_columns]
            domo_df.columns = transformed_domo_columns
            
            # Transform Snowflake columns (may have different case)
            original_sf_columns = sf_df.columns.tolist()
            transformed_sf_columns = [transform_column_name(col) for col in original_sf_columns]
            sf_df.columns = transformed_sf_columns
            
            # Use normalized key columns (already transformed above)
            key_columns_for_comparison = normalized_key_columns
            
            self.logger.info(f"🔄 Full column transformation applied to both DataFrames")
        else:
            # Even without full transformation, we need to use normalized key columns
            key_columns_for_comparison = normalized_key_columns
            self.logger.info(f"🔄 Using normalized key columns without full column transformation")


        self.logger.info("\n\n\n\n\n")
        # Print data types for both tables before datacompy comparison
        self.logger.info("🔍 DATA TYPES BEFORE DATACOMPY COMPARISON:")
        self.logger.info(f"📊 DOMO TABLE ({domo_dataset_id}):")
        for col in domo_df.columns:
            dtype = str(domo_df[col].dtype)
            self.logger.info(f"   {col}: {dtype}")
        
        self.logger.info(f"📊 SNOWFLAKE TABLE ({snowflake_table}):")
        for col in sf_df.columns:
            dtype = str(sf_df[col].dtype)
            self.logger.info(f"   {col}: {dtype}")
        
        self.logger.info("🔍 END DATA TYPES")
        self.logger.info("\n\n\n\n\n")


        # Use datacompy for comparison
        try:
            self.logger.info(f"📊 Domo DataFrame shape: {domo_df.shape}, columns: {list(domo_df.columns)}")
            self.logger.info(f"📊 Snowflake DataFrame shape: {sf_df.shape}, columns: {list(sf_df.columns)}")
            self.logger.info(f"📊 Key columns for comparison: {key_columns_for_comparison}")
            
            # Normalize data types for key columns to ensure compatibility
            for col in key_columns_for_comparison:
                # Find matching columns case-insensitively
                domo_col = None
                sf_col = None
                
                # Find column in Domo DataFrame (case-insensitive)
                for domo_column in domo_df.columns:
                    if domo_column.lower() == col.lower():
                        domo_col = domo_column
                        break
                
                # Find column in Snowflake DataFrame (case-insensitive)
                for sf_column in sf_df.columns:
                    if sf_column.lower() == col.lower():
                        sf_col = sf_column
                        break
                
                if domo_col and sf_col:
                    domo_dtype = str(domo_df[domo_col].dtype)
                    sf_dtype = str(sf_df[sf_col].dtype)
                    
                    self.logger.info(f"Key column '{col}': Domo '{domo_col}'={domo_dtype}, Snowflake '{sf_col}'={sf_dtype}")
                    
                    # If types are different, convert both to string for compatibility
                    if domo_dtype != sf_dtype:
                        self.logger.info(f"Converting column '{col}' to string for compatibility")
                        domo_df[domo_col] = domo_df[domo_col].astype(str)
                        sf_df[sf_col] = sf_df[sf_col].astype(str)
                    
                    # Rename columns to match key_columns for datacompy compatibility
                    if domo_col != col:
                        domo_df = domo_df.rename(columns={domo_col: col})
                        self.logger.info(f"Renamed Domo column '{domo_col}' to '{col}'")
                    if sf_col != col:
                        sf_df = sf_df.rename(columns={sf_col: col})
                        self.logger.info(f"Renamed Snowflake column '{sf_col}' to '{col}'")
                else:
                    # Handle column not found cases
                    self._handle_missing_key_column(domo_df, sf_df, col, key_columns)
            
            # Apply intelligent column mapping if enabled
            if use_intelligent_mapping and intelligent_mapping:
                self.logger.info("🧠 Applying intelligent column mapping to DataFrames...")
                self.logger.info(f"🧠 Intelligent mapping available: {len(intelligent_mapping)} mappings")
                self.logger.info(f"🧠 Domo columns before mapping: {list(domo_df.columns)}")
                self.logger.info(f"🧠 Snowflake columns before mapping: {list(sf_df.columns)}")
                domo_df, sf_df = self._apply_intelligent_mapping(domo_df, sf_df, intelligent_mapping)
                self.logger.info(f"🧠 Domo columns after mapping: {list(domo_df.columns)}")
                self.logger.info(f"🧠 Snowflake columns after mapping: {list(sf_df.columns)}")
            else:
                self.logger.info("🧠 Intelligent mapping NOT enabled or not available")
                if not use_intelligent_mapping:
                    self.logger.info("🧠 use_intelligent_mapping = False")
                if not intelligent_mapping:
                    self.logger.info("🧠 intelligent_mapping = None or empty")
            
            # Debug: Export tables if requested
            if export_debug_tables:
                self.debug_exporter.export_comparison_tables(
                    domo_df, sf_df, domo_dataset_id, snowflake_table, key_columns_for_comparison
                )
            print("\n\n\n\n\n")
            print("\n\n\n\n\n")
            # Print data types for both tables before datacompy comparison
            self.logger.info("🔍 DATA TYPES BEFORE DATACOMPY COMPARISON:")
            self.logger.info(f"📊 DOMO TABLE ({domo_dataset_id}):")
            for col in domo_df.columns:
                dtype = str(domo_df[col].dtype)
                self.logger.info(f"   {col}: {dtype}")
            
            self.logger.info(f"📊 SNOWFLAKE TABLE ({snowflake_table}):")
            for col in sf_df.columns:
                dtype = str(sf_df[col].dtype)
                self.logger.info(f"   {col}: {dtype}")
            
            self.logger.info("🔍 END DATA TYPES")
            print("\n\n\n\n\n")
            comparison = datacompy.Compare(
                domo_df,
                sf_df,
                join_columns=key_columns_for_comparison,
                df1_name='Domo',
                df2_name='Snowflake'
            )
            
            # Save detailed report
            report_filename = self._save_detailed_report(
                comparison, domo_dataset_id, snowflake_table, key_columns_for_comparison, 
                transform_names, schema_comparison
            )
            
            return {
                'sample_size': sample_size,
                'domo_sample_rows': len(domo_df),
                'snowflake_sample_rows': len(sf_df),
                'data_match': comparison.matches(),
                'missing_in_snowflake': len(comparison.df1_unq_rows),
                'extra_in_snowflake': len(comparison.df2_unq_rows),
                'rows_with_differences': self._count_differing_rows(comparison),
                'transform_applied': transform_names,
                'report_file': report_filename,
                'comparison_object': comparison,  # Add comparison object for executive summary
                'sampling_method': actual_sampling_method  # Track which sampling method was actually used
            }
            
        except Exception as e:
            self._add_error("Data Comparison", "Error using datacompy", str(e))
            return self._get_error_data_result(sample_size, len(domo_df), len(sf_df))
    
    def _apply_intelligent_mapping(self, domo_df: pd.DataFrame, sf_df: pd.DataFrame, 
                                 intelligent_mapping: dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Apply intelligent column mapping to align Domo and Snowflake column names.
        
        Args:
            domo_df: Domo DataFrame
            sf_df: Snowflake DataFrame
            intelligent_mapping: Intelligent column mapping dictionary
            
        Returns:
            Tuple of (domo_df, sf_df) with aligned column names
        """
        self.logger.info("🔄 Applying intelligent column mapping...")
        self.logger.info(f"🔄 Intelligent mapping keys: {list(intelligent_mapping.keys())}")
        
        # Create mapping from Domo to Snowflake column names
        domo_to_sf_mapping = {}
        sf_to_domo_mapping = {}
        
        for domo_col, mapping_info in intelligent_mapping.items():
            self.logger.info(f"🔄 Processing mapping for '{domo_col}': {mapping_info}")
            if mapping_info.get('auto_apply', False) and mapping_info.get('confidence', 0) >= 0.8:
                snowflake_col = mapping_info.get('snowflake_column')
                if snowflake_col:
                    domo_to_sf_mapping[domo_col] = snowflake_col
                    sf_to_domo_mapping[snowflake_col] = domo_col
                    self.logger.info(f"✅ AUTO '{domo_col}' → '{snowflake_col}' (confidence: {mapping_info.get('confidence', 0):.2f})")
                else:
                    self.logger.warning(f"⚠️ No snowflake_column found for '{domo_col}'")
            else:
                self.logger.info(f"⚠️ Skipping '{domo_col}' - auto_apply: {mapping_info.get('auto_apply', False)}, confidence: {mapping_info.get('confidence', 0)}")
        
        self.logger.info(f"🔄 Final mappings - Domo to SF: {domo_to_sf_mapping}")
        self.logger.info(f"🔄 Final mappings - SF to Domo: {sf_to_domo_mapping}")
        
        # Rename Domo columns to match Snowflake
        domo_df_renamed = domo_df.copy()
        for domo_col, sf_col in domo_to_sf_mapping.items():
            if domo_col in domo_df_renamed.columns:
                domo_df_renamed = domo_df_renamed.rename(columns={domo_col: sf_col})
                self.logger.info(f"🔄 Renamed Domo column '{domo_col}' → '{sf_col}'")
            else:
                self.logger.warning(f"⚠️ Domo column '{domo_col}' not found in DataFrame")
        
        # Rename Snowflake columns to match Domo (for consistency)
        sf_df_renamed = sf_df.copy()
        for sf_col, domo_col in sf_to_domo_mapping.items():
            if sf_col in sf_df_renamed.columns:
                sf_df_renamed = sf_df_renamed.rename(columns={sf_col: domo_col})
                self.logger.info(f"🔄 Renamed Snowflake column '{sf_col}' → '{domo_col}'")
            else:
                self.logger.warning(f"⚠️ Snowflake column '{sf_col}' not found in DataFrame")
        
        self.logger.info(f"📊 Intelligent mapping applied: {len(domo_to_sf_mapping)} columns mapped")
        return domo_df_renamed, sf_df_renamed
    
    def _handle_missing_key_column(self, domo_df: pd.DataFrame, sf_df: pd.DataFrame, 
                                 col: str, key_columns: List[str]):
        """Handle cases where key column is not found in DataFrames."""
        # If column not found, try to find it with the original name (before normalization)
        original_col_name = None
        for original_key in key_columns:
            if transform_column_name(original_key) == col:
                original_col_name = original_key
                break
        
        if original_col_name:
            self.logger.info(f"Trying to find original column name '{original_col_name}' for normalized key '{col}'")
            
            # Try to find the original column name in Domo
            domo_col = None
            for domo_column in domo_df.columns:
                if domo_column.lower() == original_col_name.lower():
                    domo_col = domo_column
                    break
            
            # Try to find the normalized column name in Snowflake
            sf_col = None
            for sf_column in sf_df.columns:
                if sf_column.lower() == col.lower():
                    sf_col = sf_column
                    break
            
            if domo_col and sf_col:
                self.logger.info(f"Found columns with original/normalized names: Domo '{domo_col}', Snowflake '{sf_col}'")
                
                # Rename both columns to the normalized name for datacompy
                domo_df = domo_df.rename(columns={domo_col: col})
                sf_df = sf_df.rename(columns={sf_col: col})
                
                self.logger.info(f"Renamed columns for datacompy compatibility: '{domo_col}' → '{col}', '{sf_col}' → '{col}'")
            else:
                self.logger.warning(f"Could not find matching columns for key '{col}' (original: '{original_col_name}'). Domo: {domo_col}, Snowflake: {sf_col}")
        else:
            self.logger.warning(f"Key column '{col}' not found in both DataFrames")
    
    def _save_detailed_report(self, comparison: datacompy.Compare, domo_dataset_id: str, 
                            snowflake_table: str, key_columns: List[str], 
                            transform_names: bool, schema_comparison: Dict[str, Any] = None) -> str:
        """Save detailed comparison report to file."""
        # Use session timestamp if available, otherwise create new one
        timestamp = self.session_timestamp or pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        
        # Use the provided Snowflake table name as base (now expected to be the Model Name)
        safe_base = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(snowflake_table).strip()) or "report"
        
        # Create comparison directory structure with timestamp folder
        comparison_dir = f"results/comparison/{timestamp}"
        os.makedirs(comparison_dir, exist_ok=True)
        
        # Create report filename with timestamp
        report_filename = f"{comparison_dir}/{safe_base}_{timestamp}.txt"
        
        with open(report_filename, 'w', encoding='utf-8') as f:
            f.write(f"COMPARISON REPORT\n")
            f.write(f"Domo Dataset: {domo_dataset_id}\n")
            f.write(f"Snowflake Table: {snowflake_table}\n")
            f.write(f"Key Columns: {', '.join(key_columns)}\n")
            f.write(f"Transform Applied: {transform_names}\n")
            f.write(f"Timestamp: {pd.Timestamp.now().isoformat()}\n")
            f.write("="*80 + "\n")
            
            # Get the datacompy report and modify it to include column names
            datacompy_report = comparison.report()
            
            # Add column names information to the Column Summary section
            if schema_comparison and not schema_comparison.get('error'):
                # Get the actual column names from the DataFrames being compared
                domo_cols_set = set(comparison.df1.columns)
                sf_cols_set = set(comparison.df2.columns)
                
                # Calculate the actual differences based on the DataFrames being compared
                missing_in_sf = list(domo_cols_set - sf_cols_set)
                extra_in_sf = list(sf_cols_set - domo_cols_set)
                
                # Debug: print what we have
                self.logger.info(f"DataFrame comparison debug:")
                self.logger.info(f"  Domo columns: {list(domo_cols_set)}")
                self.logger.info(f"  Snowflake columns: {list(sf_cols_set)}")
                self.logger.info(f"  Missing in Snowflake: {missing_in_sf}")
                self.logger.info(f"  Extra in Snowflake: {extra_in_sf}")
                
                # Find the Column Summary section and add column names
                lines = datacompy_report.split('\n')
                modified_lines = []
                
                for i, line in enumerate(lines):
                    modified_lines.append(line)
                    
                    # After "Number of columns in Domo but not in Snowflake" line, add the column names
                    if "Number of columns in Domo but not in Snowflake:" in line and missing_in_sf:
                        missing_cols = ', '.join(missing_in_sf)
                        modified_lines.append(f"Missing columns: {missing_cols}")
                    
                    # After "Number of columns in Snowflake but not in Domo" line, add the column names
                    elif "Number of columns in Snowflake but not in Domo:" in line and extra_in_sf:
                        extra_cols = ', '.join(extra_in_sf)
                        modified_lines.append(f"Extra columns: {extra_cols}")
                
                # Write the modified report
                f.write('\n'.join(modified_lines))
            else:
                # Write the original report if no schema comparison available
                f.write(datacompy_report)
        
        self.logger.info(f"📄 Detailed report saved to: {report_filename}")
        return report_filename
    
    def _count_differing_rows(self, comparison: datacompy.Compare) -> int:
        """Count rows with differences from datacompy comparison."""
        try:
            if hasattr(comparison, 'column_stats') and comparison.column_stats is not None:
                # Count rows where any column has differences
                differing_columns = comparison.column_stats[comparison.column_stats['matches'] == False]
                return len(differing_columns)
            else:
                return 0
        except:
            return 0
    
    def _get_error_data_result(self, sample_size: int, domo_rows: int = 0, 
                              sf_rows: int = 0) -> Dict[str, Any]:
        """Get error result for data comparison."""
        return {
            'sample_size': sample_size,
            'domo_sample_rows': domo_rows,
            'snowflake_sample_rows': sf_rows,
            'data_match': False,
            'missing_in_snowflake': 0,
            'extra_in_snowflake': 0,
            'rows_with_differences': 0,
            'error': True
        }
    
    def _add_error(self, section: str, error: str, details: str = ""):
        """Add error to the error list."""
        self.errors.append({
            'section': section,
            'error': error,
            'details': details
        })
        self.logger.error(f"Error in {section}: {error}")
        if details:
            self.logger.error(f"Details: {details}")
