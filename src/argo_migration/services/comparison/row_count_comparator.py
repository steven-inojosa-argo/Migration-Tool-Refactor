"""
Row count comparison utilities for Domo and Snowflake.

This module provides row count validation and statistical analysis
of differences between datasets.
"""

import logging
from typing import Dict, Any

from ...api.domo import DomoHandler
from ...api.snowflake import SnowflakeHandler


class RowCountComparator:
    """Compare row counts between Domo and Snowflake datasets."""
    
    def __init__(self, domo_handler: DomoHandler, snowflake_handler: SnowflakeHandler):
        """
        Initialize row count comparator.
        
        Args:
            domo_handler: Initialized Domo handler
            snowflake_handler: Initialized Snowflake handler
        """
        self.domo_handler = domo_handler
        self.snowflake_handler = snowflake_handler
        self.logger = logging.getLogger("RowCountComparator")
    
    def compare_row_counts(self, domo_dataset_id: str, snowflake_table: str) -> Dict[str, Any]:
        """Compare row counts between Domo and Snowflake."""
        self.logger.info("📊 Comparing row counts...")
        
        # Get Domo count using DomoHandler for simple queries
        try:
            domo_count_query = "SELECT COUNT(*) as row_count FROM table"
            domo_result = self.domo_handler.query_dataset(domo_dataset_id, domo_count_query)
            domo_count = domo_result['rows'][0][0] if domo_result['rows'] else 0
        except Exception as e:
            self.logger.error(f"❌ Could not get row count from Domo: {e}")
            domo_count = 0
        
        # Get Snowflake count
        try:
            sf_count_query = f"SELECT COUNT(*) as row_count FROM {snowflake_table}"
            sf_result = self.snowflake_handler.execute_query(sf_count_query)
            if sf_result is not None:
                sf_count = int(sf_result.iloc[0]['ROW_COUNT'])  # Already pandas DataFrame
            else:
                raise Exception("Failed to get Snowflake count")
        except Exception as e:
            self.logger.error(f"❌ Could not get row count from Snowflake: {e}")
            sf_count = 0
        
        # Analyze if difference is negligible
        negligible_analysis = self._analyze_row_count_difference(domo_count, sf_count)
        
        return {
            'domo_rows': domo_count,
            'snowflake_rows': sf_count,
            'difference': sf_count - domo_count,
            'match': domo_count == sf_count,
            'negligible_analysis': negligible_analysis
        }
    
    def _analyze_row_count_difference(self, domo_count: int, snowflake_count: int) -> Dict[str, Any]:
        """Determine if the difference in row counts is statistically negligible."""
        if domo_count == 0 and snowflake_count == 0:
            return {'is_negligible': True, 'reason': 'Both datasets are empty', 'percentage': 0.0}
        
        if domo_count == 0 or snowflake_count == 0:
            return {
                'is_negligible': False,
                'reason': 'One dataset is empty',
                'percentage': 100.0
            }
        
        difference = abs(snowflake_count - domo_count)
        percentage = (difference / max(domo_count, snowflake_count)) * 100
        
        # Determine negligible thresholds
        if difference <= 10:
            return {
                'is_negligible': True,
                'reason': f'Very small absolute difference ({difference} rows)',
                'percentage': percentage
            }
        elif percentage <= 0.1:
            return {
                'is_negligible': True,
                'reason': f'Very small percentage difference ({percentage:.3f}%)',
                'percentage': percentage
            }
        elif percentage <= 1.0 and max(domo_count, snowflake_count) >= 10000:
            return {
                'is_negligible': True,
                'reason': f'Small percentage difference for large dataset ({percentage:.3f}%)',
                'percentage': percentage
            }
        else:
            return {
                'is_negligible': False,
                'reason': f'Significant difference: {difference} rows ({percentage:.3f}%)',
                'percentage': percentage
            }
