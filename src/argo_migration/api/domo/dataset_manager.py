"""Domo dataset management module."""

import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class DomoDatasetManager:
    """Handles Domo dataset management operations."""
    
    def __init__(self, dataset_api):
        self.dataset_api = dataset_api
    
    def get_all_datasets(self, batch_size: int = 500) -> List[Dict[str, Any]]:
        """Get all datasets from Domo using pagination."""
        logger.info(f"🔍 Fetching all datasets (batch size: {batch_size})")
        
        all_datasets = []
        offset = 0
        
        while True:
            try:
                # Get batch of datasets
                search_results = self.dataset_api.search(
                    limit=batch_size, 
                    offset=offset,
                    filters=[],
                    sort=None
                )
                
                if not search_results:
                    break
                
                # Extract metadata
                for dataset in search_results:
                    dataset_info = {
                        'id': dataset.id,
                        'name': dataset.name,
                        'description': getattr(dataset, 'description', ''),
                        'created': getattr(dataset, 'created', ''),
                        'last_updated': getattr(dataset, 'last_updated', ''),
                        'row_count': getattr(dataset, 'row_count', 0),
                        'column_count': getattr(dataset, 'column_count', 0),
                        'owner': getattr(dataset.owner, 'name', '') if hasattr(dataset, 'owner') and dataset.owner else ''
                    }
                    all_datasets.append(dataset_info)
                
                if len(search_results) < batch_size:
                    break
                    
                offset += batch_size
                
            except Exception as e:
                logger.error(f"❌ Error fetching datasets: {e}")
                break
        
        logger.info(f"✅ Fetched {len(all_datasets)} datasets")
        return all_datasets
    
    def get_dataset_info(self, dataset_id: str) -> Dict[str, Any]:
        """Get detailed information about a specific dataset."""
        try:
            dataset_info = self.dataset_api.get(dataset_id)
            return {
                'id': dataset_info.id,
                'name': dataset_info.name,
                'description': getattr(dataset_info, 'description', ''),
                'row_count': getattr(dataset_info, 'row_count', 0),
                'column_count': getattr(dataset_info, 'column_count', 0)
            }
        except Exception as e:
            logger.error(f"❌ Error getting dataset info: {e}")
            return {}
    
    def get_dataset_schema(self, dataset_id: str) -> List[Dict[str, Any]]:
        """Get schema information for a specific dataset."""
        try:
            dataset_info = self.dataset_api.get(dataset_id)
            schema = []
            
            # Try to get schema from dataset info
            if hasattr(dataset_info, 'schema') and dataset_info.schema:
                for column in dataset_info.schema:
                    schema.append({
                        'name': column.name,
                        'type': column.type,
                        'description': getattr(column, 'description', '')
                    })
            else:
                # Fallback: create basic schema info
                logger.warning(f"⚠️ No schema found for dataset {dataset_id}, using fallback")
                schema = [
                    {'name': 'column_1', 'type': 'STRING', 'description': 'Unknown column'}
                ]
            
            return schema
            
        except Exception as e:
            logger.error(f"❌ Error getting dataset schema: {e}")
            return []
