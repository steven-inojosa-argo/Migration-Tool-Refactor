"""
Simplified Domo API module.

Usage:
    from argo_migration.api.domo import DomoHandler
    
    # Initialize and authenticate
    domo = DomoHandler()
    domo.authenticate()
    
    # Extract data
    df = domo.extract_data("dataset_id")
    
    # Get all datasets
    datasets = domo.get_all_datasets()
    
    # Get lineage information
    dataflows = domo.get_all_dataflows(["dataset1", "dataset2"])
"""

# Main interface
from .handler import DomoHandler

# Individual modules for advanced usage
from .auth import DomoAuth
from .data_extractor import DomoDataExtractor
from .dataset_manager import DomoDatasetManager
from .lineage_crawler import DomoLineageCrawler
from .utils import clean_dataframe

__all__ = [
    'DomoHandler',
    'DomoAuth',
    'DomoDataExtractor', 
    'DomoDatasetManager',
    'DomoLineageCrawler',
    'clean_dataframe'
]
