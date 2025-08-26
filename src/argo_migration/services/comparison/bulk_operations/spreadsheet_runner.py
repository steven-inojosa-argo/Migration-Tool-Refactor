"""
Spreadsheet-based bulk comparison runner.

This module provides functionality to execute multiple dataset comparisons
based on configurations stored in Google Sheets.
"""

import os
import logging
from typing import Dict, Any, Optional
import pandas as pd

from ....utils.common import get_env_config
from ....utils.gsheets import GoogleSheets, READ_WRITE_SCOPES
from ....utils.file_logger import start_logging_session, end_logging_session


class SpreadsheetComparisonRunner:
    """Run multiple comparisons based on Google Sheets configuration."""
    
    def __init__(self, comparator):
        """
        Initialize spreadsheet runner.
        
        Args:
            comparator: DatasetComparator instance
        """
        self.comparator = comparator
        self.logger = logging.getLogger("SpreadsheetRunner")
    
    def run_comparisons(self, spreadsheet_id: str, sheet_name: str = None,
                       credentials_path: str = None, sampling_method: str = "random", 
                       export_debug_tables: bool = False) -> Dict[str, Any]:
        """
        Compare multiple datasets from Google Sheets configuration.
        
        Expected columns in spreadsheet:
        - Output ID: Domo dataset ID
        - Table Name: Snowflake table name
        - Key Columns: Comma-separated list of key columns
        - Sample Size: (Optional) Number of rows to sample
        - Transform Columns: (Optional) True/False for column transformation
        - Status: (Optional) Track comparison status
        
        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            sheet_name: Sheet name containing comparison configurations (uses COMPARISON_SHEET_NAME env var if None)
            credentials_path: Path to Google Sheets credentials file
            sampling_method: Sampling method ('random' or 'ordered')
            export_debug_tables: If True, export the comparison tables as CSV files to results/debug/ for debugging
            
        Returns:
            Dictionary with comparison results summary
        """
        # Start logging session for this execution
        session_timestamp = start_logging_session("spreadsheet_comparison")
        
        try:
            # Get sheet name from environment if not provided
            if sheet_name is None:
                env_config = get_env_config()
                sheet_name = env_config.get("COMPARISON_SHEET_NAME", "QA - Test")
            
            self.logger.info(f"🚀 Starting spreadsheet-based comparisons...")
            self.logger.info(f"📋 Spreadsheet ID: {spreadsheet_id}")
            self.logger.info(f"📄 Sheet name: {sheet_name}")
            self.logger.info(f"📅 Session: {session_timestamp}")
            
            # Setup Google Sheets client
            gsheets_client = self._setup_gsheets_client(credentials_path)
            
            # Read and parse spreadsheet data
            df, column_mappings = self._read_spreadsheet_config(gsheets_client, spreadsheet_id, sheet_name)
            
            # Filter for testing entries
            testing_df = self._filter_testing_entries(df, column_mappings)
            
            if len(testing_df) == 0:
                self.logger.info("✅ No comparisons in 'Testing' status")
                return {"success": 0, "failed": 0, "total": 0, "errors": []}
            
            # Setup connections once
            if not self.comparator.setup_connections():
                raise Exception("Failed to setup connections")
            
            # Process comparisons
            results = self._process_comparisons(
                testing_df, column_mappings, sampling_method, export_debug_tables, 
                gsheets_client, spreadsheet_id, sheet_name
            )
            
            # Log summary
            self._log_summary(results)
            
            return results
            
        except Exception as e:
            self.logger.error(f"❌ Spreadsheet comparison failed: {e}")
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
        
        # Initialize Google Sheets client
        return GoogleSheets(
            credentials_path=credentials_path,
            scopes=READ_WRITE_SCOPES
        )
    
    def _read_spreadsheet_config(self, gsheets_client: GoogleSheets, 
                               spreadsheet_id: str, sheet_name: str) -> tuple:
        """Read and parse spreadsheet configuration."""
        # Read comparison configurations
        self.logger.info(f"📖 Reading comparison configurations from {sheet_name}...")
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
        
        self.logger.info(f"📊 Found {len(df)} comparison configurations")
        
        # Find column mappings
        column_mappings = self._find_column_mappings(df)
        
        return df, column_mappings
    
    def _find_column_mappings(self, df: pd.DataFrame) -> Dict[str, str]:
        """Find the column mappings in the spreadsheet."""
        column_mappings = {}
        
        # Find Output ID column
        possible_dataset_id_columns = ['Output ID', 'output_id', 'Dataset ID', 'dataset_id', 'Domo Dataset ID', 'domo_dataset_id', 'ID', 'id']
        for col in possible_dataset_id_columns:
            if col in df.columns:
                column_mappings['dataset_id'] = col
                break
        
        # Find Table Name column (accept 'Model Name' as preferred)
        possible_table_columns = ['Model Name', 'model_name']
        for col in possible_table_columns:
            if col in df.columns:
                column_mappings['table_name'] = col
                break
        
        # Find Key Columns column
        possible_key_columns = ['Key Columns', 'key_columns', 'Keys', 'keys', 'Join Columns', 'join_columns']
        for col in possible_key_columns:
            if col in df.columns:
                column_mappings['key_columns'] = col
                break
        
        # Find Sample Size column (optional)
        possible_sample_columns = ['Sample Size', 'sample_size', 'Sample', 'sample']
        for col in possible_sample_columns:
            if col in df.columns:
                column_mappings['sample_size'] = col
                break
        
        # Find Transform Columns column (optional)
        possible_transform_columns = ['Transform Columns', 'transform_columns', 'Transform', 'transform']
        for col in possible_transform_columns:
            if col in df.columns:
                column_mappings['transform_columns'] = col
                break
        
        # Find Status column (optional)
        possible_status_columns = ['Status', 'status', 'comparison_status', 'Comparison Status', 'state']
        for col in possible_status_columns:
            if col in df.columns:
                column_mappings['status'] = col
                break
        
        # Find Notes column (optional)
        possible_notes_columns = ['Notes', 'notes', 'Note', 'note', 'Comments', 'comments']
        for col in possible_notes_columns:
            if col in df.columns:
                column_mappings['notes'] = col
                break
        
        # Validate required columns
        required_columns = ['dataset_id', 'table_name', 'key_columns']
        for req_col in required_columns:
            if req_col not in column_mappings:
                raise Exception(f"Required column '{req_col}' not found in spreadsheet")
        
        return column_mappings
    
    def _filter_testing_entries(self, df: pd.DataFrame, column_mappings: Dict[str, str]) -> pd.DataFrame:
        """Filter entries that are in 'Testing' status."""
        status_column = column_mappings.get('status')
        
        # Filter rows where Status is "Testing" (if status column exists)
        if status_column and status_column in df.columns:
            df[status_column] = df[status_column].fillna('Pending')
            df[status_column] = df[status_column].astype(str)
            testing_df = df[df[status_column].str.contains('Testing', case=False, na=False)]
            self.logger.info(f"📋 Found {len(testing_df)} comparisons in 'Testing' status")
        else:
            testing_df = df
            self.logger.info(f"📋 No status column found, processing all {len(testing_df)} comparisons")
        
        return testing_df
    
    def _process_comparisons(self, testing_df: pd.DataFrame, column_mappings: Dict[str, str],
                           sampling_method: str, export_debug_tables: bool,
                           gsheets_client: GoogleSheets, spreadsheet_id: str, sheet_name: str) -> Dict[str, Any]:
        """Process all comparison entries."""
        successful_comparisons = []
        failed_comparisons = []
        errors = []
        
        dataset_id_column = column_mappings['dataset_id']
        table_name_column = column_mappings['table_name']
        key_columns_column = column_mappings['key_columns']
        sample_size_column = column_mappings.get('sample_size')
        transform_columns_column = column_mappings.get('transform_columns')
        notes_column = column_mappings.get('notes')
        
        for index, row in testing_df.iterrows():
            dataset_id = row[dataset_id_column]
            table_name = row[table_name_column]
            key_columns_str = row[key_columns_column]
            
            # Clean table name - remove .sql extension if present
            if table_name and str(table_name).strip().lower().endswith('.sql'):
                table_name = str(table_name).strip()[:-4]  # Remove last 4 characters (.sql)
            
            # Validate required fields - skip incomplete rows instead of treating as errors
            if pd.isna(dataset_id) or str(dataset_id).strip() == '':
                self.logger.info(f"⏭️  Skipping row {index + 2}: Empty Output ID")
                continue
            
            if pd.isna(table_name) or str(table_name).strip() == '':
                self.logger.info(f"⏭️  Skipping row {index + 2}: Empty Table Name")
                continue
            
            if pd.isna(key_columns_str) or str(key_columns_str).strip() == '':
                self.logger.info(f"⏭️  Skipping row {index + 2}: Empty Key Columns")
                continue
            
            # Parse key columns (comma-separated)
            key_columns = [col.strip() for col in str(key_columns_str).split(',') if col.strip()]
            
            # Parse optional fields
            sample_size = None
            if sample_size_column and not pd.isna(row.get(sample_size_column)):
                try:
                    sample_size = int(row[sample_size_column])
                except (ValueError, TypeError):
                    self.logger.warning(f"⚠️  Row {index + 2}: Invalid sample size, using auto-calculation")
            
            transform_columns = False
            if transform_columns_column and not pd.isna(row.get(transform_columns_column)):
                transform_value = str(row[transform_columns_column]).lower()
                transform_columns = transform_value in ['true', '1', 'yes', 'y', 'enabled']
            
            self.logger.info(f"🔄 Comparing dataset {dataset_id} vs table {table_name}")
            
            try:
                # Generate comparison report
                report = self.comparator.generate_report(
                    domo_dataset_id=str(dataset_id),
                    snowflake_table=str(table_name),
                    key_columns=key_columns,
                    sample_size=sample_size,
                    transform_names=transform_columns,
                    sampling_method=sampling_method,
                    use_session_logging=False,  # Session logging handled by runner
                    export_debug_tables=export_debug_tables
                )
                
                # Check if comparison was successful
                if report.get('errors'):
                    error_msg = f"Dataset {dataset_id}: Comparison failed with errors"
                    self.logger.error(f"❌ {error_msg}")
                    errors.extend([f"Dataset {dataset_id}: {err['error']}" for err in report['errors']])
                    failed_comparisons.append(str(dataset_id))
                else:
                    success_msg = f"Dataset {dataset_id}: Comparison completed"
                    if report.get('overall_match'):
                        self.logger.info(f"✅ {success_msg} - Perfect match!")
                    else:
                        self.logger.warning(f"⚠️  {success_msg} - Discrepancies found")
                    successful_comparisons.append(str(dataset_id))
                
                # Always update notes in spreadsheet if notes column exists
                if notes_column:
                    self._update_spreadsheet_notes(
                        gsheets_client, spreadsheet_id, sheet_name, index, 
                        notes_column, testing_df, dataset_id, table_name, report
                    )
                
            except Exception as e:
                error_msg = f"Dataset {dataset_id}: {str(e)}"
                self.logger.error(f"❌ {error_msg}")
                errors.append(error_msg)
                failed_comparisons.append(str(dataset_id))
        
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
    
    def _update_spreadsheet_notes(self, gsheets_client: GoogleSheets, spreadsheet_id: str,
                                sheet_name: str, index: int, notes_column: str, 
                                testing_df: pd.DataFrame, dataset_id: str, table_name: str,
                                report: Dict[str, Any]):
        """Update notes in the spreadsheet."""
        try:
            from ..reporting.executive_summary import ExecutiveSummaryGenerator
            summary_generator = ExecutiveSummaryGenerator()
            
            # Generate executive summary or error summary based on comparison result
            if report.get('errors'):
                # Generate error summary for failed comparisons
                timestamp = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
                error_summary = f"❌ COMPARISON FAILED [{timestamp}]\n"
                error_summary += f"Dataset: {dataset_id} → {table_name}\n"
                error_summary += f"❌ Errors encountered:\n"
                for error in report.get('errors', []):
                    error_summary += f"  • {error.get('error', str(error))}\n"
                error_summary += f"📄 Check detailed logs for more information"
                executive_summary = error_summary
            else:
                # Generate normal executive summary for successful comparisons
                comparison_obj = report.get('data_comparison', {}).get('comparison_object')
                executive_summary = summary_generator.generate_executive_summary(report, comparison_obj)
            
            # Get current notes content
            notes_cell_range = f"{sheet_name}!{chr(65 + testing_df.columns.get_loc(notes_column))}{index + 2}"
            current_notes_result = gsheets_client.read_range(spreadsheet_id, notes_cell_range)
            current_notes = ""
            if current_notes_result and len(current_notes_result) > 0 and len(current_notes_result[0]) > 0:
                current_notes = str(current_notes_result[0][0]).strip()
            
            # Append executive summary to existing notes
            if current_notes:
                updated_notes = f"{current_notes}\n\n{executive_summary}"
            else:
                updated_notes = executive_summary
            
            # Update the notes cell
            gsheets_client.write_range(spreadsheet_id, notes_cell_range, [[updated_notes]])
            self.logger.info(f"📝 Updated notes for dataset {dataset_id}")
            
        except Exception as e:
            self.logger.warning(f"⚠️  Could not update notes for row {index + 2}: {e}")
    
    def _log_summary(self, results: Dict[str, Any]):
        """Log comparison summary."""
        total_comparisons = results['total']
        successful_comparisons = results['success']
        failed_comparisons = results['failed']
        errors = results['errors']
        
        self.logger.info("="*80)
        self.logger.info("📊 SPREADSHEET COMPARISON SUMMARY")
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
