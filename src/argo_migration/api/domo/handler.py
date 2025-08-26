"""Main Domo handler - simplified and clean."""

import logging
from typing import Optional, List, Dict, Any
import pandas as pd

from .auth import DomoAuth
from .data_extractor import DomoDataExtractor
from .dataset_manager import DomoDatasetManager
from .lineage_crawler import DomoLineageCrawler

logger = logging.getLogger(__name__)


class DomoHandler:
    """Simple and clean Domo handler for all operations."""
    
    def __init__(self):
        """Initialize the handler."""
        self._auth = DomoAuth()
        self._data_extractor = None
        self._dataset_manager = None
        self._lineage_crawler = DomoLineageCrawler()
        self._authenticated = False
    
    def authenticate(self):
        """Authenticate with Domo and initialize components."""
        self._auth.authenticate()
        
        if self._auth.is_authenticated:
            self._data_extractor = DomoDataExtractor(self._auth.dataset_api)
            self._dataset_manager = DomoDatasetManager(self._auth.dataset_api)
            self._authenticated = True
            logger.info("✅ DomoHandler ready")
        else:
            raise ValueError("❌ Authentication failed")
    
    @property
    def is_authenticated(self) -> bool:
        """Check if handler is authenticated and ready."""
        return self._authenticated
    
    # Data Extraction
    def extract_data(self, dataset_id: str, query: Optional[str] = None, 
                    chunk_size: int = 1000000, auto_convert_types: bool = False) -> Optional[pd.DataFrame]:
        """Extract data from a Domo dataset."""
        self._ensure_authenticated()
        return self._data_extractor.extract_data(dataset_id, query, chunk_size, auto_convert_types)
    
    def query_dataset(self, dataset_id: str, query: str) -> Dict[str, Any]:
        """Execute SQL query on a dataset."""
        self._ensure_authenticated()
        return self._data_extractor.query_dataset(dataset_id, query)
    
    # Dataset Management
    def get_all_datasets(self, batch_size: int = 500) -> List[Dict[str, Any]]:
        """Get all datasets from Domo."""
        self._ensure_authenticated()
        return self._dataset_manager.get_all_datasets(batch_size)
    
    def get_dataset_info(self, dataset_id: str) -> Dict[str, Any]:
        """Get information about a specific dataset."""
        self._ensure_authenticated()
        return self._dataset_manager.get_dataset_info(dataset_id)
    
    def get_dataset_schema(self, dataset_id: str) -> List[Dict[str, Any]]:
        """Get schema information for a specific dataset."""
        self._ensure_authenticated()
        return self._dataset_manager.get_dataset_schema(dataset_id)
    
    # Lineage
    def get_all_dataflows(self, dataset_id_list: List[str]) -> pd.DataFrame:
        """Get all dataflows connected to the provided datasets."""
        return self._lineage_crawler.get_all_dataflows(dataset_id_list)
    
    def _ensure_authenticated(self):
        """Ensure the handler is authenticated."""
        if not self._authenticated:
            raise ValueError("❌ Not authenticated. Call authenticate() first.")
