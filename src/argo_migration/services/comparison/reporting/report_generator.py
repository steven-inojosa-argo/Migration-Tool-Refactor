"""
Report generation utilities for comparison results.

This module provides comprehensive report generation and presentation
for data comparison results.
"""

import logging
from typing import Dict, Any, List
import pandas as pd


class ReportGenerator:
    """Generate comprehensive comparison reports."""
    
    def __init__(self):
        """Initialize report generator."""
        self.logger = logging.getLogger("ReportGenerator")
    
    def print_report(self, report: Dict[str, Any]):
        """Print comparison report in a readable format."""
        print("\n" + "="*80)
        print("DOMO vs SNOWFLAKE COMPARISON REPORT")
        print("="*80)
        
        print(f"📊 Domo Dataset: {report['domo_dataset_id']}")
        print(f"❄️  Snowflake Table: {report['snowflake_table']}")
        print(f"🔑 Key Columns: {', '.join(report['key_columns'])}")
        print(f"⏰ Timestamp: {report['timestamp']}")
        print(f"🔄 Column Transformation: {'Applied' if report.get('transform_applied') else 'Not Applied'}")
        
        # Show errors
        if report.get('errors'):
            print(f"\n⚠️  ERRORS ({len(report['errors'])}):")
            for i, error in enumerate(report['errors'], 1):
                print(f"   {i}. {error['section']}: {error['error']}")
                if error.get('details'):
                    print(f"      Details: {error['details']}")
        
        # Overall status
        if report.get('errors'):
            print(f"\n🎯 OVERALL STATUS: ❌ ERRORS FOUND")
        elif report['overall_match']:
            print(f"\n🎯 OVERALL STATUS: ✅ PERFECT MATCH")
        else:
            print(f"\n🎯 OVERALL STATUS: ❌ DISCREPANCIES FOUND")
        
        # Schema comparison
        schema = report['schema_comparison']
        print(f"\n📋 SCHEMA COMPARISON:")
        if schema.get('error'):
            print("   ❌ Error getting schemas")
        else:
            print(f"   Domo columns: {schema['domo_columns']}")
            print(f"   Snowflake columns: {schema['snowflake_columns']}")
            print(f"   Common columns: {schema['common_columns']}")
            
            if schema['missing_in_snowflake']:
                print(f"   ❌ Missing in Snowflake: {schema['missing_in_snowflake']}")
            if schema['extra_in_snowflake']:
                print(f"   ⚠️  Extra in Snowflake: {schema['extra_in_snowflake']}")
            if schema['type_mismatches']:
                print(f"   🔄 Type mismatches: {len(schema['type_mismatches'])}")
            
            if schema['schema_match']:
                print(f"   ✅ Schema matches")
            else:
                print(f"   ❌ Schema differences found")
        
        # Row count comparison
        rows = report['row_count_comparison']
        print(f"\n📊 ROW COUNT COMPARISON:")
        print(f"   Domo rows: {rows['domo_rows']:,}")
        print(f"   Snowflake rows: {rows['snowflake_rows']:,}")
        print(f"   Difference: {rows['difference']:,}")
        
        negligible = rows.get('negligible_analysis', {})
        if negligible:
            if negligible.get('is_negligible'):
                print(f"   ✅ Difference is negligible: {negligible.get('reason')}")
            else:
                print(f"   ❌ Significant difference: {negligible.get('reason')}")
        
        # Data comparison
        data = report['data_comparison']
        print(f"\n🔍 DATA COMPARISON:")
        if data.get('error'):
            print("   ❌ Error comparing data")
        else:
            print(f"   Sample size: {data['sample_size']:,}")
            print(f"   Domo sample rows: {data['domo_sample_rows']:,}")
            print(f"   Snowflake sample rows: {data['snowflake_sample_rows']:,}")
            
            if data.get('missing_in_snowflake', 0) > 0:
                print(f"   ❌ Missing in Snowflake: {data['missing_in_snowflake']}")
            if data.get('extra_in_snowflake', 0) > 0:
                print(f"   ⚠️  Extra in Snowflake: {data['extra_in_snowflake']}")
            if data.get('rows_with_differences', 0) > 0:
                print(f"   🔄 Rows with differences: {data['rows_with_differences']}")
            
            if data.get('data_match'):
                print(f"   ✅ Data samples match")
            else:
                print(f"   ❌ Data differences found")
            
            if data.get('report_file'):
                print(f"   📄 Detailed report: {data['report_file']}")
        
        print("="*80)
    
    def get_connection_error_report(self, domo_dataset_id: str, snowflake_table: str, 
                                  key_columns: List[str], transform_names: bool) -> Dict[str, Any]:
        """Get error report for connection failures."""
        return {
            'domo_dataset_id': domo_dataset_id,
            'snowflake_table': snowflake_table,
            'key_columns': key_columns,
            'overall_match': False,
            'errors': [{'section': 'Connection', 'error': 'Failed to setup connections', 'details': ''}],
            'timestamp': pd.Timestamp.now().isoformat(),
            'transform_applied': transform_names
        }
