"""
Main dataset comparator coordinator.

This module provides the main DatasetComparator class that coordinates
all comparison activities using specialized components.
"""

import logging
from typing import List, Dict, Any, Optional

from .schema_comparator import SchemaComparator
from .row_count_comparator import RowCountComparator
from .data_comparator import DataComparator
from .reporting.report_generator import ReportGenerator
from .bulk_operations.spreadsheet_runner import SpreadsheetComparisonRunner
from .bulk_operations.inventory_runner import InventoryComparisonRunner
from ...utils.common import setup_dual_connections
from ...api.domo import DomoHandler
from ...api.snowflake import SnowflakeHandler
from ...utils.file_logger import get_file_logger, setup_file_logging, start_logging_session, end_logging_session, get_current_session_timestamp
import pandas as pd

class DatasetComparator:
    """Main class to compare Domo datasets with Snowflake tables using datacompy."""
    
    def __init__(self):
        """Initialize the comparator with handlers for Domo and Snowflake."""
        # Import handlers here to avoid circular imports
        self.domo_handler = DomoHandler()
        self.snowflake_handler = SnowflakeHandler()
        self.errors = []
        self._domo_connected = False
        self._snowflake_connected = False
        self.logger = logging.getLogger("DatasetComparator")
        
        # Setup file logging
        self.file_logger = get_file_logger()
        self.general_file_logger, self.error_file_logger = setup_file_logging()
        
        # Initialize specialized components (lazy initialization)
        self._schema_comparator = None
        self._row_count_comparator = None
        self._data_comparator = None
        self._report_generator = None
        self._spreadsheet_runner = None
        self._inventory_runner = None
    
    @property
    def schema_comparator(self) -> SchemaComparator:
        """Get schema comparator instance."""
        if self._schema_comparator is None:
            self._schema_comparator = SchemaComparator(self.domo_handler, self.snowflake_handler)
        return self._schema_comparator
    
    @property
    def row_count_comparator(self) -> RowCountComparator:
        """Get row count comparator instance."""
        if self._row_count_comparator is None:
            self._row_count_comparator = RowCountComparator(self.domo_handler, self.snowflake_handler)
        return self._row_count_comparator
    
    @property
    def data_comparator(self) -> DataComparator:
        """Get data comparator instance."""
        if self._data_comparator is None:
            self._data_comparator = DataComparator(self.domo_handler, self.snowflake_handler)
        return self._data_comparator
    
    @property
    def report_generator(self) -> ReportGenerator:
        """Get report generator instance."""
        if self._report_generator is None:
            self._report_generator = ReportGenerator()
        return self._report_generator
    
    @property
    def spreadsheet_runner(self) -> SpreadsheetComparisonRunner:
        """Get spreadsheet runner instance."""
        if self._spreadsheet_runner is None:
            self._spreadsheet_runner = SpreadsheetComparisonRunner(self)
        return self._spreadsheet_runner
    
    @property
    def inventory_runner(self) -> InventoryComparisonRunner:
        """Get inventory runner instance."""
        if self._inventory_runner is None:
            self._inventory_runner = InventoryComparisonRunner(self)
        return self._inventory_runner
    
    def setup_connections(self) -> bool:
        """Setup connections to both Domo and Snowflake."""
        success, domo_handler, snowflake_handler = setup_dual_connections(
            self.domo_handler, self.snowflake_handler
        )
        
        if success:
            self.domo_handler = domo_handler
            self.snowflake_handler = snowflake_handler
            self._domo_connected = True
            self._snowflake_connected = True
            
            # Update handlers in specialized components
            if self._schema_comparator:
                self._schema_comparator.domo_handler = domo_handler
                self._schema_comparator.snowflake_handler = snowflake_handler
            if self._row_count_comparator:
                self._row_count_comparator.domo_handler = domo_handler
                self._row_count_comparator.snowflake_handler = snowflake_handler
            if self._data_comparator:
                self._data_comparator.domo_handler = domo_handler
                self._data_comparator.snowflake_handler = snowflake_handler
        
        return success
    
    def add_error(self, section: str, error: str, details: str = ""):
        """Add error to the error list."""
        self.errors.append({
            'section': section,
            'error': error,
            'details': details
        })
        self.logger.error(f"Error in {section}: {error}")
        if details:
            self.logger.error(f"Details: {details}")
        
        # Log to file
        self.file_logger.log_error(section, error, details or "No additional details")
    
    def generate_report(self, domo_dataset_id: str, snowflake_table: str, 
                       key_columns: List[str], sample_size: Optional[int] = None,
                       transform_names: bool = False, sampling_method: str = "random", 
                       use_session_logging: bool = True, export_debug_tables: bool = False,
                       use_intelligent_mapping: bool = False) -> Dict[str, Any]:
        """
        Generate complete comparison report.
        
        Args:
            domo_dataset_id: Domo dataset ID
            snowflake_table: Snowflake table name
            key_columns: List of key columns for comparison
            sample_size: Number of rows to sample
            transform_names: Whether to apply column name transformation
            sampling_method: Sampling method ('random' or 'ordered')
            use_session_logging: Whether to start/end logging session (set to False when called from other methods)
            export_debug_tables: If True, export the comparison tables as CSV files to results/debug/ for debugging
            use_intelligent_mapping: Whether to use intelligent column mapping with Levenshtein
            
        Returns:
            Complete comparison report dictionary
        """
        session_timestamp = None
        if use_session_logging:
            # Start logging session for single comparison
            session_timestamp = start_logging_session("single_comparison")
        
        try:
            self.logger.info(f"🚀 Starting comparison: {domo_dataset_id} vs {snowflake_table}")
            if session_timestamp:
                self.logger.info(f"📅 Session: {session_timestamp}")
            
            # Log to file
            self.file_logger.log_comparison_start(
                domo_dataset_id, snowflake_table, key_columns, transform_names
            )
        
            # Clear previous errors
            self.errors = []
            
            # Setup connections if needed
            if not self._domo_connected or not self._snowflake_connected:
                if not self.setup_connections():
                    return self.report_generator.get_connection_error_report(
                        domo_dataset_id, snowflake_table, key_columns, transform_names
                    )
            
            # Perform comparisons using specialized components
            schema_comparison = self.schema_comparator.compare_schemas(
                domo_dataset_id, snowflake_table, transform_names, use_intelligent_mapping
            )
            
            row_count_comparison = self.row_count_comparator.compare_row_counts(
                domo_dataset_id, snowflake_table
            )
            
            # Get column mapping from schema comparator for data comparison
            domo_column_mapping = self.schema_comparator.domo_original_columns
            
            # Set session timestamp in data comparator
            # Use current session timestamp if available, even when use_session_logging is False
            current_timestamp = session_timestamp or get_current_session_timestamp()
            if current_timestamp:
                self.data_comparator.set_session_timestamp(current_timestamp)
            
            # Get intelligent mapping from schema comparator if available
            intelligent_mapping = None
            if use_intelligent_mapping and hasattr(self.schema_comparator, 'intelligent_mapping'):
                intelligent_mapping = self.schema_comparator.intelligent_mapping
            
            data_comparison = self.data_comparator.compare_data_samples(
                domo_dataset_id, snowflake_table, key_columns, sample_size, 
                transform_names, schema_comparison, sampling_method, export_debug_tables,
                domo_column_mapping, use_intelligent_mapping, intelligent_mapping
            )
            
            # Determine overall match
            overall_match = False
            if not schema_comparison.get('error') and not data_comparison.get('error'):
                row_count_ok = (row_count_comparison['match'] or 
                              row_count_comparison.get('negligible_analysis', {}).get('is_negligible', False))
                
                overall_match = (schema_comparison['schema_match'] and 
                               row_count_ok and 
                               data_comparison.get('data_match', False))
            
            # Build result
            result = {
                'domo_dataset_id': domo_dataset_id,
                'snowflake_table': snowflake_table,
                'key_columns': key_columns,
                'overall_match': overall_match,
                'schema_comparison': schema_comparison,
                'row_count_comparison': row_count_comparison,
                'data_comparison': data_comparison,
                'errors': self.errors + self.data_comparator.errors,  # Combine errors from all components
                'timestamp': pd.Timestamp.now().isoformat(),
                'transform_applied': transform_names,
                'use_intelligent_mapping': use_intelligent_mapping
            }
            
            # Log result to file
            self.file_logger.log_comparison_result(result)
            
            return result
            
        finally:
            # End logging session if it was started by this method
            if use_session_logging:
                end_logging_session()
    
    # Backward compatibility methods - delegate to specialized components
    def compare_schemas(self, domo_dataset_id: str, snowflake_table: str, 
                       transform_names: bool = False) -> Dict[str, Any]:
        """Compare schemas between Domo and Snowflake."""
        return self.schema_comparator.compare_schemas(domo_dataset_id, snowflake_table, transform_names)
    
    def compare_row_counts(self, domo_dataset_id: str, snowflake_table: str) -> Dict[str, Any]:
        """Compare row counts between Domo and Snowflake."""
        return self.row_count_comparator.compare_row_counts(domo_dataset_id, snowflake_table)
    
    def compare_data_samples(self, domo_dataset_id: str, snowflake_table: str, 
                           key_columns: List[str], sample_size: Optional[int] = None, 
                           transform_names: bool = False, schema_comparison: Dict[str, Any] = None, 
                           sampling_method: str = "random", export_debug_tables: bool = False,
                           use_intelligent_mapping: bool = False) -> Dict[str, Any]:
        """Compare data samples using datacompy."""
        # Get column mapping from schema comparator
        domo_column_mapping = self.schema_comparator.domo_original_columns
        
        # Get intelligent mapping from schema comparator if available
        intelligent_mapping = None
        if use_intelligent_mapping and hasattr(self.schema_comparator, 'intelligent_mapping'):
            intelligent_mapping = self.schema_comparator.intelligent_mapping
        
        return self.data_comparator.compare_data_samples(
            domo_dataset_id, snowflake_table, key_columns, sample_size, 
            transform_names, schema_comparison, sampling_method, export_debug_tables,
            domo_column_mapping, use_intelligent_mapping, intelligent_mapping
        )
    
    def print_report(self, report: Dict[str, Any]):
        """Print comparison report in a readable format."""
        self.report_generator.print_report(report)
    
    def compare_from_spreadsheet(self, spreadsheet_id: str, sheet_name: str = None,
                                credentials_path: str = None, sampling_method: str = "random", 
                                export_debug_tables: bool = False) -> Dict[str, Any]:
        """Compare multiple datasets from Google Sheets configuration."""
        return self.spreadsheet_runner.run_comparisons(
            spreadsheet_id, sheet_name, credentials_path, sampling_method, export_debug_tables
        )
    
    def compare_from_inventory(self, credentials_path: str = None, sampling_method: str = "random", 
                              export_debug_tables: bool = False) -> Dict[str, Any]:
        """Compare datasets from the existing inventory spreadsheet."""
        return self.inventory_runner.run_comparisons(credentials_path, sampling_method, export_debug_tables)
    
    def cleanup(self):
        """Clean up resources."""
        if self.snowflake_handler:
            self.snowflake_handler.cleanup()
        
        # Close file logging
        self.file_logger.close_loggers()
