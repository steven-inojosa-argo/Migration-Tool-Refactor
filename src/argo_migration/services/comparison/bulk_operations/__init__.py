"""
Bulk comparison operations for multiple datasets.

This package provides runners for executing multiple comparisons from different sources
like Google Sheets and inventory systems.
"""

from .spreadsheet_runner import SpreadsheetComparisonRunner
from .inventory_runner import InventoryComparisonRunner

__all__ = [
    'SpreadsheetComparisonRunner',
    'InventoryComparisonRunner',
]
