"""Domo data extraction module."""

import logging
from typing import Optional
import pandas as pd
from domo_utils.utils.pandas_utils import to_dataframe
from .utils import clean_dataframe

logger = logging.getLogger(__name__)


class DomoDataExtractor:
    """Handles Domo data extraction operations."""
    
    def __init__(self, dataset_api):
        self.dataset_api = dataset_api
    
    def extract_data(self, dataset_id: str, query: Optional[str] = None, 
                    chunk_size: int = 1000000, auto_convert_types: bool = False) -> Optional[pd.DataFrame]:
        """Extract data from Domo dataset."""
        try:
            # Build query
            if query:
                sql_query = query
            else:
                sql_query = f"SELECT * FROM table LIMIT {chunk_size}"
            
            logger.info(f"📥 Extracting data from dataset {dataset_id}")
            
            # Execute query
            result = self.dataset_api.query(dataset_id, sql_query)
            df = to_dataframe(result)
            
            if df is not None and not df.empty:
                cleaned_df = clean_dataframe(df, auto_convert_types)
                logger.info(f"✅ Extracted {len(cleaned_df)} rows")
                return cleaned_df
            
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"❌ Failed to extract data: {e}")
            return None
    
    def query_dataset(self, dataset_id: str, query: str) -> dict:
        """Execute SQL query and return structured result."""
        df = self.extract_data(dataset_id, query, chunk_size=1000)
        
        if df is None or df.empty:
            return {"datasource": "", "columns": [], "rows": []}
        
        return {
            "datasource": dataset_id,
            "columns": df.columns.tolist(),
            "rows": df.values.tolist()
        }
