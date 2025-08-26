"""
Executive summary generation for comparison results.

This module provides intelligent analysis and executive-level summaries
of data comparison results with visual indicators and key insights.
"""

import logging
from typing import Dict, Any, Optional, List
import pandas as pd
import datacompy

from ....utils.common import get_env_config


class ExecutiveSummaryGenerator:
    """Generate executive-level summaries of comparison results."""
    
    def __init__(self):
        """Initialize executive summary generator."""
        self.logger = logging.getLogger("ExecutiveSummaryGenerator")
    
    def generate_executive_summary(self, report: Dict[str, Any], comparison: Optional[datacompy.Compare] = None) -> str:
        """
        Generate detailed executive summary for comparison results.
        
        Args:
            report: Complete comparison report dictionary
            comparison: Optional datacompy Compare object for direct data access
            
        Returns:
            String with detailed executive summary or error information
        """
        timestamp = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
        dataset_id = report.get('domo_dataset_id', 'N/A')
        table_name = report.get('snowflake_table', 'N/A')
        
        # Get database and schema from environment configuration
        env_config = get_env_config()
        database = env_config.get('SNOWFLAKE_DATABASE', 'UNKNOWN_DB')
        schema = env_config.get('SNOWFLAKE_SCHEMA', 'UNKNOWN_SCHEMA')
        
        # Build full table path with real database and schema
        database_schema_table = f"{database.lower()}.{schema.lower()}.{table_name}"
        sampling_method = report.get('data_comparison', {}).get('sampling_method', 'Unknown')
        
        # Track failure reasons for debugging
        failure_reasons = []
        
        # Debug: Check if comparison object is available
        self.logger.info(f"🔍 Executive Summary Debug - Comparison object: {comparison is not None}")
        if comparison:
            self.logger.info(f"🔍 Has column_stats: {hasattr(comparison, 'column_stats')}")
            if hasattr(comparison, 'column_stats'):
                self.logger.info(f"🔍 Column_stats is not None: {comparison.column_stats is not None}")
                if comparison.column_stats is not None:
                    self.logger.info(f"🔍 Column_stats type: {type(comparison.column_stats)}")
                    if isinstance(comparison.column_stats, list):
                        self.logger.info(f"🔍 Column_stats length: {len(comparison.column_stats)}")
                    elif hasattr(comparison.column_stats, 'shape'):
                        self.logger.info(f"🔍 Column_stats shape: {comparison.column_stats.shape}")
        else:
            failure_reasons.append("DataComPy comparison object not available")
        
        # Check for major errors
        if report.get('errors'):
            error_summary = f"[{timestamp}] {sampling_method} - COMPARISON FAILED:\n"
            error_summary += f"{dataset_id} → {database_schema_table}\n"
            error_summary += f"❌ Errors encountered:\n"
            for error in report.get('errors', []):
                error_summary += f"  • {error}\n"
            if failure_reasons:
                error_summary += f"🔍 Debug info:\n"
                for reason in failure_reasons:
                    error_summary += f"  • {reason}\n"
            error_summary += f"📄 Check detailed logs for more information"
            return error_summary
        
        # Overall status
        if report.get('overall_match'):
            status = "PERFECT MATCH"
        else:
            status = "DISCREPANCIES"
        
        # Wrap the entire summary generation in try-catch
        try:
            summary_lines = [
                f"[{timestamp}] {sampling_method} - {status}:",
                f"{dataset_id} → {database_schema_table}"
            ]
            
            # Duplicate keys analysis
            duplicate_keys_info = self._analyze_duplicate_keys(comparison)
            if duplicate_keys_info:
                summary_lines.append(duplicate_keys_info)

            # Row count analysis
            row_analysis = self._analyze_row_counts(report)
            if row_analysis:
                summary_lines.append(row_analysis)
            else:
                failure_reasons.append("Row count analysis failed")
                summary_lines.append(f"❌ Could not compare row counts")
            
            # Schema analysis
            schema_analysis = self._analyze_schema(report, comparison)
            summary_lines.extend(schema_analysis)
            
            # Data comparison analysis
            data_analysis = self._analyze_data_differences(report, comparison)
            summary_lines.extend(data_analysis)
            
            # Add report file reference
            report_data = report.get('data_comparison', {})
            if report_data.get('report_file'):
                summary_lines.append(f"📄 Detailed Report: {report_data['report_file']}")
            
            return '\n'.join(summary_lines)
        
        except Exception as e:
            # If anything fails during summary generation, return error info
            failure_summary = f"[{timestamp}] {sampling_method} - SUMMARY GENERATION FAILED:\n"
            failure_summary += f"{dataset_id} → {database_schema_table}\n"
            failure_summary += f"💥 Unexpected error: {str(e)}\n"
            failure_summary += f"🔍 Error type: {type(e).__name__}\n"
            if failure_reasons:
                failure_summary += f"⚠️ Previous issues detected:\n"
                for reason in failure_reasons:
                    failure_summary += f"  • {reason}\n"
            failure_summary += f"📊 Comparison object available: {comparison is not None}\n"
            failure_summary += f"📊 Report keys: {list(report.keys()) if report else 'None'}\n"
            failure_summary += f"📄 Check logs and detailed report file for more information"
            
            self.logger.error(f"❌ Executive summary generation failed: {e}")
            return failure_summary
    
    def _analyze_duplicate_keys(self, comparison: Optional[datacompy.Compare]) -> str:
        """Analyze duplicate keys in the comparison."""
        duplicate_keys_info = ""
        if comparison and hasattr(comparison, 'df1') and hasattr(comparison, 'df2'):
            try:
                # Get join columns from the comparison object
                join_columns = getattr(comparison, 'join_columns', [])
                
                if join_columns:
                    # Check for duplicate keys in Domo data (df1)
                    domo_duplicates = comparison.df1.duplicated(subset=join_columns, keep=False).sum()
                    # Check for duplicate keys in Snowflake data (df2)  
                    sf_duplicates = comparison.df2.duplicated(subset=join_columns, keep=False).sum()
                    
                    if domo_duplicates > 0 or sf_duplicates > 0:
                        duplicate_keys_info = f"❌ Duplicate keys detected"
                        self.logger.info(f"🔍 Duplicate keys found - Domo: {domo_duplicates}, Snowflake: {sf_duplicates}")
                    else:
                        duplicate_keys_info = "✅ Duplicate keys: None found"
                        
            except Exception as e:
                self.logger.warning(f"Could not check for duplicate keys: {e}")
                duplicate_keys_info = "❌ Duplicate keys: Could not determine"
        
        return duplicate_keys_info
    
    def _analyze_row_counts(self, report: Dict[str, Any]) -> Optional[str]:
        """Analyze row count differences."""
        rows = report.get('row_count_comparison', {})
        if rows and not rows.get('error'):
            domo_rows = rows.get('domo_rows', 0)
            sf_rows = rows.get('snowflake_rows', 0)
            
            if domo_rows > 0:
                error_pct = abs(sf_rows - domo_rows) / domo_rows * 100
                
                # Add visual indicator based on percentage difference
                if error_pct <= 1.0:
                    row_indicator = "✅"
                elif error_pct <= 5.0:
                    row_indicator = "⚠️"
                else:
                    row_indicator = "❌"
                
                return f"{row_indicator} Rows: Domo {domo_rows:,} vs Snowflake {sf_rows:,} ({error_pct:.2f}% difference)"
            else:
                # Handle zero rows case
                if sf_rows == 0:
                    return f"✅ Rows: Domo {domo_rows:,} vs Snowflake {sf_rows:,}"
                else:
                    return f"❌ Rows: Domo {domo_rows:,} vs Snowflake {sf_rows:,}"
        
        return None
    
    def _analyze_schema(self, report: Dict[str, Any], comparison: Optional[datacompy.Compare]) -> List[str]:
        """Analyze schema differences."""
        schema_lines = []
        
        # Schema analysis - use datacompy comparison object if available, otherwise fallback to schema comparison
        if comparison and hasattr(comparison, 'df1') and hasattr(comparison, 'df2'):
            # Extract column information directly from datacompy comparison
            domo_cols = len(comparison.df1.columns)
            sf_cols = len(comparison.df2.columns)
            
            # Get missing and extra columns directly from dataframes first
            domo_cols_set = set(comparison.df1.columns)
            sf_cols_set = set(comparison.df2.columns)
            missing_cols = list(domo_cols_set - sf_cols_set)
            extra_cols = list(sf_cols_set - domo_cols_set)
            
            # Special logic for batch columns (technical metadata columns)
            batch_columns = {'_batch_last_run_', '_batch_id_'}
            missing_cols_set = set(missing_cols)
            
            # Check if missing columns are only batch columns
            only_batch_missing = missing_cols_set.issubset(batch_columns)
            has_non_batch_missing = bool(missing_cols_set - batch_columns)
            
            # Add visual indicator for column count comparison with special logic
            if domo_cols == sf_cols:
                col_indicator = "✅"
            elif len(missing_cols) <= 2 and only_batch_missing:
                # Special case: only batch columns missing (≤2)
                col_indicator = "✅"
            else:
                col_indicator = "⚠️"
            
            schema_lines.append(f"{col_indicator} Columns: Domo {domo_cols} vs Snowflake {sf_cols}")
            
            # Missing columns with special batch column logic
            if missing_cols:
                # Choose indicator based on whether non-batch columns are missing
                missing_indicator = "❌" if has_non_batch_missing else "✅"
                
                if len(missing_cols) <= 5:
                    schema_lines.append(f"{missing_indicator} Missing Columns in Snowflake: {', '.join(missing_cols)}")
                else:
                    schema_lines.append(f"{missing_indicator} Missing Columns in Snowflake: {', '.join(missing_cols[:5])}")
                    schema_lines.append(f"... and {len(missing_cols) - 5} more missing columns")
            
            # Extra columns
            if extra_cols:
                if len(extra_cols) <= 5:
                    schema_lines.append(f"⚠️ Extra Columns in Snowflake: {', '.join(extra_cols)}")
                else:
                    schema_lines.append(f"⚠️ Extra Columns in Snowflake: {', '.join(extra_cols[:5])}")
                    schema_lines.append(f"... and {len(extra_cols) - 5} more extra columns")
        
        else:
            # Fallback to schema comparison from report
            schema = report.get('schema_comparison', {})
            if not schema.get('error'):
                domo_cols = schema.get('domo_columns', 0)
                sf_cols = schema.get('snowflake_columns', 0)
            
                # Get missing columns from schema report for fallback logic
                missing_cols = schema.get('missing_in_snowflake', [])
                
                # Special logic for batch columns (technical metadata columns)
                batch_columns = {'_batch_last_run_', '_batch_id_'}
                missing_cols_set = set(missing_cols)
                
                # Check if missing columns are only batch columns
                only_batch_missing = missing_cols_set.issubset(batch_columns)
                has_non_batch_missing = bool(missing_cols_set - batch_columns)
                
                # Add visual indicator for column count comparison with special logic
                if domo_cols == sf_cols:
                    col_indicator = "✅"
                elif len(missing_cols) <= 2 and only_batch_missing:
                    # Special case: only batch columns missing (≤2)
                    col_indicator = "✅"
                else:
                    col_indicator = "⚠️"
                
                if domo_cols > 0:
                    col_error_pct = abs(sf_cols - domo_cols) / domo_cols * 100
                    schema_lines.append(f"{col_indicator} Columns: Domo {domo_cols} vs Snowflake {sf_cols} ({col_error_pct:.2f}% difference)")
                else:
                    schema_lines.append(f"{col_indicator} Columns: Domo {domo_cols} vs Snowflake {sf_cols}")
                
                # Data type errors
                type_mismatches = schema.get('type_mismatches', [])
                if type_mismatches:
                    schema_lines.append(f"❌ Data Type Errors: {len(type_mismatches)} columns")
                    mismatch_details = []
                    for mismatch in type_mismatches[:5]:  # Show first 5
                        col_name = mismatch.get('column', 'unknown')
                        domo_type = mismatch.get('domo_type', 'unknown')
                        sf_type = mismatch.get('snowflake_type', 'unknown')
                        mismatch_details.append(f"{col_name} (Domo: {domo_type} vs SF: {sf_type})")
                    
                    schema_lines.append(f"Type Mismatches: {', '.join(mismatch_details)}")
                    if len(type_mismatches) > 5:
                        schema_lines.append(f"... and {len(type_mismatches) - 5} more type mismatches")
                
                # Missing columns with special batch column logic
                if missing_cols:
                    # Choose indicator based on whether non-batch columns are missing
                    missing_indicator = "❌" if has_non_batch_missing else "✅"
                    
                    if len(missing_cols) <= 5:
                        schema_lines.append(f"{missing_indicator} Missing Columns in Snowflake: {', '.join(missing_cols)}")
                    else:
                        schema_lines.append(f"{missing_indicator} Missing Columns in Snowflake: {', '.join(missing_cols[:5])}")
                        schema_lines.append(f"... and {len(missing_cols) - 5} more missing columns")
                
                # Extra columns
                extra_cols = schema.get('extra_in_snowflake', [])
                if extra_cols:
                    if len(extra_cols) <= 5:
                        schema_lines.append(f"⚠️ Extra Columns in Snowflake: {', '.join(extra_cols)}")
                    else:
                        schema_lines.append(f"⚠️ Extra Columns in Snowflake: {', '.join(extra_cols[:5])}")
                        schema_lines.append(f"... and {len(extra_cols) - 5} more extra columns")
        
        return schema_lines
    
    def _analyze_data_differences(self, report: Dict[str, Any], comparison: Optional[datacompy.Compare]) -> List[str]:
        """Analyze data value differences."""
        data_lines = []
        
        # Data comparison analysis - simplified approach
        if comparison:
            try:
                # Get basic row differences
                missing_in_sf = len(comparison.df1_unq_rows) if hasattr(comparison, 'df1_unq_rows') else 0
                extra_in_sf = len(comparison.df2_unq_rows) if hasattr(comparison, 'df2_unq_rows') else 0
                
                if missing_in_sf > 0:
                    data_lines.append(f"❌ Rows Missing in Snowflake: {missing_in_sf}")
                if extra_in_sf > 0:
                    data_lines.append(f"⚠️ Extra Rows in Snowflake: {extra_in_sf}")
                
                # Get columns with different values - more specific approach
                columns_with_diffs = []
                self.logger.info(f"🔍 Starting column difference analysis...")
                try:
                    # Use column_stats if available, but be more specific about what we count
                    if hasattr(comparison, 'column_stats') and comparison.column_stats is not None:
                        self.logger.info(f"🔍 Column_stats available, type: {type(comparison.column_stats)}")
                        if isinstance(comparison.column_stats, list):
                            self.logger.info(f"🔍 Processing list with {len(comparison.column_stats)} elements")
                            
                            # Debug: Show first few entries
                            if len(comparison.column_stats) > 0:
                                self.logger.info(f"🔍 First column_stat entry: {comparison.column_stats[0]}")
                            
                            # Get common columns
                            common_cols = set(comparison.df1.columns) & set(comparison.df2.columns)
                            self.logger.info(f"🔍 Common columns count: {len(common_cols)}")
                            
                            # Only count columns that have actual value differences, not just type mismatches
                            # Filter for columns that have real data differences (not just type differences)
                            columns_with_diffs = []
                            for stat in comparison.column_stats:
                                col_name = stat.get('column')
                                unequal_cnt = stat.get('unequal_cnt', 0)
                                max_diff = stat.get('max_diff', 0)
                                null_diff = stat.get('null_diff', 0)
                                
                                # Only include if:
                                # 1. Column is in common columns
                                # 2. Has unequal values > 0 
                                # 3. AND either has max_diff > 0 OR null_diff > 0 (real value differences)
                                if (col_name in common_cols and 
                                    unequal_cnt > 0 and 
                                    (max_diff > 0 or null_diff > 0)):
                                    columns_with_diffs.append(col_name)
                                    self.logger.info(f"🔍 Column '{col_name}': unequal={unequal_cnt}, max_diff={max_diff}, null_diff={null_diff}")
                                elif col_name in common_cols and unequal_cnt > 0:
                                    self.logger.info(f"🔍 SKIPPED '{col_name}': unequal={unequal_cnt}, max_diff={max_diff}, null_diff={null_diff} (type-only difference)")
                            
                            # Log more details about what we found
                            all_unequal_cols = [s for s in comparison.column_stats if s.get('unequal_cnt', 0) > 0]
                            type_only_diffs = len(all_unequal_cols) - len(columns_with_diffs)
                            
                            self.logger.info(f"🔍 SUMMARY:")
                            self.logger.info(f"  📊 Total columns with unequal_cnt > 0: {len(all_unequal_cols)}")
                            self.logger.info(f"  📊 Columns with type-only differences: {type_only_diffs}")
                            self.logger.info(f"  📊 Columns with real value differences: {len(columns_with_diffs)}")
                            self.logger.info(f"🔍 Final filtered columns with differences: {columns_with_diffs}")
                            
                        elif hasattr(comparison.column_stats, 'shape'):  # DataFrame case
                            self.logger.info(f"🔍 Processing DataFrame with shape: {comparison.column_stats.shape}")
                            # Filter for common columns only and real value differences
                            common_cols = set(comparison.df1.columns) & set(comparison.df2.columns)
                            mask = (
                                (comparison.column_stats['unequal_cnt'] > 0) & 
                                (comparison.column_stats['column'].isin(common_cols)) &
                                ((comparison.column_stats['max_diff'] > 0) | (comparison.column_stats['null_diff'] > 0))
                            )
                            columns_with_diffs = comparison.column_stats[mask]['column'].tolist()
                            self.logger.info(f"🔍 DataFrame filtered columns with real differences: {len(columns_with_diffs)}")
                    else:
                        self.logger.info(f"🔍 Column_stats not available or None")
                except Exception as e:
                    self.logger.error(f"❌ Error analyzing column differences: {e}")
                    import traceback
                    self.logger.error(f"❌ Traceback: {traceback.format_exc()}")
                    pass  # Ignore errors, will use fallback
                
                # Show column differences - check case 0 first
                if len(columns_with_diffs) == 0:
                    # Case when len(columns_with_diffs) == 0
                    data_lines.append("✅ All values matched")
                elif len(columns_with_diffs) <= 5:
                    data_lines.append(f"⚠️ Columns with Different Values: {', '.join(columns_with_diffs)}")
                else:
                    data_lines.append(f"⚠️ Columns with Different Values: {', '.join(columns_with_diffs[:5])}")
                    data_lines.append(f"... and {len(columns_with_diffs) - 5} more columns with differences")
                
            except Exception as e:
                data_lines.append(f"❌ Could not analyze data differences: {str(e)}")
        
        # Fallback: use report data if comparison object failed
        if not data_lines or len([line for line in data_lines if 'Different Values' in line or 'Missing' in line or 'Extra' in line or 'All values matched' in line]) == 0:
            data = report.get('data_comparison', {})
            if data and not data.get('error'):
                if data.get('missing_in_snowflake', 0) > 0:
                    data_lines.append(f"❌ Rows Missing in Snowflake: {data['missing_in_snowflake']}")
                
                if data.get('extra_in_snowflake', 0) > 0:
                    data_lines.append(f"⚠️ Extra Rows in Snowflake: {data['extra_in_snowflake']}")
                
                if data.get('rows_with_differences', 0) > 0:
                    data_lines.append(f"❌ Rows with Different Values: {data['rows_with_differences']} (column details unavailable)")
        
        return data_lines
