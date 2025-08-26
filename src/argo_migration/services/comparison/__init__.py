"""
Comparison module for Domo to Snowflake data validation.

This module provides modular components for comparing datasets between Domo and Snowflake,
including schema comparison, data sampling, and comprehensive reporting.
"""

# Main exports for backward compatibility
from .dataset_comparator import DatasetComparator

# Individual component exports for advanced usage
from .schema_comparator import SchemaComparator
from .row_count_comparator import RowCountComparator  
from .data_comparator import DataComparator

__all__ = [
    'DatasetComparator',
    'SchemaComparator',
    'RowCountComparator',
    'DataComparator',
]
