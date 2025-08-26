"""
Inventory-based bulk comparison runner.

This module provides functionality to execute multiple dataset comparisons
based on the existing inventory spreadsheet system.
"""

import os
import logging
from typing import Dict, Any
import pandas as pd

from ....utils.common import get_env_config
from ....utils.gsheets import GoogleSheets, READ_WRITE_SCOPES
from ....utils.file_logger import start_logging_session, end_logging_session


class InventoryComparisonRunner:
    """Run multiple comparisons based on inventory spreadsheet."""
    
    def __init__(self, comparator):
        """
        Initialize inventory runner.
        
        Args:
            comparator: DatasetComparator instance
        """
        self.comparator = comparator
        self.logger = logging.getLogger("InventoryRunner")
    
    def run_comparisons(self, credentials_path: str = None, sampling_method: str = "random", 
                       export_debug_tables: bool = False) -> Dict[str, Any]:
        """
        Compare datasets from the existing inventory spreadsheet.
        
        Uses the same spreadsheet and sheet as the inventory system with columns:
        - Output ID: Domo dataset ID
        - Model Name: Snowflake table name  
        - Key Columns: Comma-separated list of key columns
        
        Args:
            credentials_path: Path to Google Sheets credentials file
            sampling_method: Sampling method ('random' or 'ordered')
            export_debug_tables: If True, export the comparison tables as CSV files to results/debug/ for debugging
            
        Returns:
            Dictionary with comparison results summary
        """
        # Start logging session for this execution
        session_timestamp = start_logging_session("inventory_comparison")
        
        try:
            # Get spreadsheet configuration from environment
            env_config = get_env_config()
            spreadsheet_id = env_config.get("MIGRATION_SPREADSHEET_ID")
            sheet_name = env_config.get("INTERMEDIATE_MODELS_SHEET_NAME", "Inventory")
            
            if not spreadsheet_id:
                raise Exception("MIGRATION_SPREADSHEET_ID environment variable not set")
            
            self.logger.info(f"🚀 Starting inventory-based comparisons...")
            self.logger.info(f"📋 Using inventory spreadsheet: {spreadsheet_id}")
            self.logger.info(f"📄 Using inventory sheet: {sheet_name}")
            self.logger.info(f"📅 Session: {session_timestamp}")
            
            # Setup Google Sheets client
            gsheets_client = self._setup_gsheets_client(credentials_path)
            
            # Read and parse inventory data
            df, column_mappings = self._read_inventory_data(gsheets_client, spreadsheet_id, sheet_name)
            
            # Filter valid entries
            valid_df = self._filter_valid_entries(df, column_mappings)
            
            if len(valid_df) == 0:
                self.logger.info("✅ No valid entries found for comparison")
                return {"success": 0, "failed": 0, "total": 0, "errors": []}
            
            # Setup connections once
            if not self.comparator.setup_connections():
                raise Exception("Failed to setup connections")
            
            # Process comparisons
            results = self._process_inventory_comparisons(
                valid_df, column_mappings, sampling_method, export_debug_tables
            )
            
            # Log summary
            self._log_summary(results)
            
            return results
            
        except Exception as e:
            self.logger.error(f"❌ Inventory comparison failed: {e}")
            return {
                "success": 0,
                "failed": 0,
                "total": 0,
                "errors": [str(e)]
            }
        finally:
            # End logging session
            end_logging_session()
    
    def _setup_gsheets_client(self, credentials_path: str = None) -> GoogleSheets:
        """Setup Google Sheets client."""
        if not credentials_path:
            credentials_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
        
        if not credentials_path:
            raise Exception("Google Sheets credentials not provided")
        
        if not os.path.exists(credentials_path):
            raise Exception(f"Google Sheets credentials file not found: {credentials_path}")
        
        return GoogleSheets(
            credentials_path=credentials_path,
            scopes=READ_WRITE_SCOPES
        )
    
    def _read_inventory_data(self, gsheets_client: GoogleSheets, 
                           spreadsheet_id: str, sheet_name: str) -> tuple:
        """Read and parse inventory data."""
        # Read inventory data
        self.logger.info(f"📖 Reading inventory data from {sheet_name}...")
        data = gsheets_client.read_range(spreadsheet_id, f"{sheet_name}!A:Z")
        
        if not data or len(data) < 2:
            raise Exception(f"No data found in sheet '{sheet_name}' or missing headers")
        
        # Convert to DataFrame
        headers = data[0]
        rows = data[1:]
        
        # Normalize row lengths to match header length
        num_cols = len(headers)
        normalized_rows = []
        for row in rows:
            # Ensure each row has the same number of columns as headers
            normalized_row = row + [''] * (num_cols - len(row)) if len(row) < num_cols else row[:num_cols]
            normalized_rows.append(normalized_row)
        
        df = pd.DataFrame(normalized_rows, columns=headers)
        
        # Remove empty rows
        df = df.dropna(how='all')
        
        self.logger.info(f"📊 Found {len(df)} inventory entries")
        
        # Find column mappings
        column_mappings = self._find_inventory_column_mappings(df)
        
        return df, column_mappings
    
    def _find_inventory_column_mappings(self, df: pd.DataFrame) -> Dict[str, str]:
        """Find column mappings in inventory spreadsheet."""
        column_mappings = {}
        
        # Find Output ID column (Domo dataset ID)
        possible_dataset_id_columns = ['Output ID', 'output_id', 'Dataset ID', 'dataset_id', 'Domo Dataset ID']
        for col in possible_dataset_id_columns:
            if col in df.columns:
                column_mappings['dataset_id'] = col
                break
        
        # Find Model Name column (Snowflake table name)
        possible_table_columns = ['Model Name', 'model_name', 'Table Name', 'table_name', 'Snowflake Table']
        for col in possible_table_columns:
            if col in df.columns:
                column_mappings['table_name'] = col
                break
        
        # Find Key Columns column
        possible_key_columns = ['Key Columns', 'key_columns', 'Keys', 'keys']
        for col in possible_key_columns:
            if col in df.columns:
                column_mappings['key_columns'] = col
                break
        
        # Validate required columns
        required_columns = ['dataset_id', 'table_name', 'key_columns']
        for req_col in required_columns:
            if req_col not in column_mappings:
                raise Exception(f"Required column '{req_col}' not found in inventory spreadsheet")
        
        return column_mappings
    
    def _filter_valid_entries(self, df: pd.DataFrame, column_mappings: Dict[str, str]) -> pd.DataFrame:
        """Filter out empty/invalid entries."""
        dataset_id_column = column_mappings['dataset_id']
        table_name_column = column_mappings['table_name']
        key_columns_column = column_mappings['key_columns']
        
        # Filter out empty entries
        valid_df = df[
            df[dataset_id_column].notna() & 
            df[table_name_column].notna() & 
            df[key_columns_column].notna() &
            (df[dataset_id_column].astype(str).str.strip() != '') &
            (df[table_name_column].astype(str).str.strip() != '') &
            (df[key_columns_column].astype(str).str.strip() != '')
        ]
        
        self.logger.info(f"📋 Found {len(valid_df)} valid entries for comparison (with Output ID, Model Name, and Key Columns)")
        
        return valid_df
    
    def _process_inventory_comparisons(self, valid_df: pd.DataFrame, column_mappings: Dict[str, str],
                                     sampling_method: str, export_debug_tables: bool) -> Dict[str, Any]:
        """Process inventory comparison entries."""
        successful_comparisons = []
        failed_comparisons = []
        errors = []
        
        dataset_id_column = column_mappings['dataset_id']
        table_name_column = column_mappings['table_name']
        key_columns_column = column_mappings['key_columns']
        
        for index, row in valid_df.iterrows():
            dataset_id = str(row[dataset_id_column]).strip()
            table_name = str(row[table_name_column]).strip()
            key_columns_str = str(row[key_columns_column]).strip()
            
            # Clean table name - remove .sql extension if present
            if table_name and table_name.lower().endswith('.sql'):
                table_name = table_name[:-4]  # Remove last 4 characters (.sql)
            
            # Parse key columns (comma-separated)
            key_columns = [col.strip() for col in key_columns_str.split(',') if col.strip()]
            
            if not key_columns:
                self.logger.info(f"⏭️  Skipping row {index + 2}: Invalid Key Columns format")
                continue
            
            self.logger.info(f"🔄 Comparing dataset {dataset_id} vs table {table_name}")
            self.logger.info(f"🔑 Using key columns: {', '.join(key_columns)}")
            
            try:
                # Generate comparison report
                report = self.comparator.generate_report(
                    domo_dataset_id=dataset_id,
                    snowflake_table=table_name,
                    key_columns=key_columns,
                    sample_size=None,  # Use auto-calculation
                    transform_names=False,  # Don't transform by default for inventory
                    sampling_method=sampling_method,
                    use_session_logging=False,  # Session logging handled by runner
                    export_debug_tables=export_debug_tables
                )
                
                # Check if comparison was successful
                if report.get('errors'):
                    error_msg = f"Dataset {dataset_id}: Comparison failed with errors"
                    self.logger.error(f"❌ {error_msg}")
                    errors.extend([f"Dataset {dataset_id}: {err['error']}" for err in report['errors']])
                    failed_comparisons.append(dataset_id)
                else:
                    success_msg = f"Dataset {dataset_id}: Comparison completed"
                    if report.get('overall_match'):
                        self.logger.info(f"✅ {success_msg} - Perfect match!")
                    else:
                        self.logger.warning(f"⚠️  {success_msg} - Discrepancies found")
                    successful_comparisons.append(dataset_id)
                
            except Exception as e:
                error_msg = f"Dataset {dataset_id}: {str(e)}"
                self.logger.error(f"❌ {error_msg}")
                errors.append(error_msg)
                failed_comparisons.append(dataset_id)
        
        # Return results
        total_comparisons = len(successful_comparisons) + len(failed_comparisons)
        
        return {
            "success": len(successful_comparisons),
            "failed": len(failed_comparisons),
            "total": total_comparisons,
            "errors": errors,
            "successful_datasets": successful_comparisons,
            "failed_datasets": failed_comparisons
        }
    
    def _log_summary(self, results: Dict[str, Any]):
        """Log comparison summary."""
        total_comparisons = results['total']
        successful_comparisons = results['success']
        failed_comparisons = results['failed']
        errors = results['errors']
        
        self.logger.info("="*80)
        self.logger.info("📊 INVENTORY COMPARISON SUMMARY")
        self.logger.info("="*80)
        self.logger.info(f"✅ Successful comparisons: {successful_comparisons}")
        self.logger.info(f"❌ Failed comparisons: {failed_comparisons}")
        self.logger.info(f"📋 Total comparisons: {total_comparisons}")
        
        if successful_comparisons:
            self.logger.info(f"📈 Success rate: {successful_comparisons/total_comparisons*100:.1f}%")
        
        if errors:
            self.logger.error("\n❌ Errors encountered:")
            for error in errors[:10]:  # Show first 10 errors
                self.logger.error(f"   • {error}")
            if len(errors) > 10:
                self.logger.error(f"   ... and {len(errors) - 10} more errors")
