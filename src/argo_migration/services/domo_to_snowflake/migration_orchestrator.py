"""Migration orchestrator for Domo to Snowflake data transfer."""

import logging
from typing import Dict, List, Optional, Tuple
import pandas as pd

from .table_utils import sanitize_table_name, validate_table_name

logger = logging.getLogger(__name__)


class MigrationOrchestrator:
    """Orchestrates the migration process from Domo to Snowflake."""
    
    def __init__(self, domo_handler, snowflake_handler):
        """
        Initialize the migration orchestrator.
        
        Args:
            domo_handler: Authenticated Domo handler
            snowflake_handler: Connected Snowflake handler
        """
        self.domo = domo_handler
        self.snowflake = snowflake_handler
    
    def migrate_dataset(self, dataset_id: str, dataset_name: str = None, 
                       table_name: str = None, chunk_size: int = None) -> bool:
        """
        Migrate a single dataset from Domo to Snowflake.
        
        Args:
            dataset_id: Domo dataset ID
            dataset_name: Dataset name (optional)
            table_name: Custom table name (optional)
            chunk_size: Data extraction chunk size
            
        Returns:
            bool: True if migration successful, False otherwise
        """
        try:
            logger.info(f"🚀 Starting migration for dataset {dataset_id}")
            
            # Generate table name if not provided
            if not table_name:
                table_name = sanitize_table_name(dataset_id, dataset_name)
            
            # Validate table name
            if not validate_table_name(table_name):
                logger.error(f"❌ Invalid table name: {table_name}")
                return False
            
            # Extract data from Domo
            logger.info(f"📥 Extracting data from Domo...")
            df = self.domo.extract_data(dataset_id, chunk_size=chunk_size or 1000000)
            
            if df is None:
                logger.error(f"❌ Failed to extract data from dataset {dataset_id}")
                return False
            
            if df.empty:
                logger.warning(f"⚠️  Dataset {dataset_id} is empty")
                return True  # Empty dataset is not an error
            
            logger.info(f"✅ Extracted {len(df)} rows, {len(df.columns)} columns")
            
            # Upload to Snowflake
            logger.info(f"📤 Uploading to Snowflake table {table_name}...")
            upload_success = self.snowflake.upload_data(df, table_name, if_exists='replace')
            
            if not upload_success:
                logger.error(f"❌ Failed to upload data to Snowflake")
                return False
            
            # Verify upload
            logger.info(f"🔍 Verifying upload...")
            verify_success = self.snowflake.verify_upload(table_name, len(df))
            
            if verify_success:
                logger.info(f"🎉 Migration completed successfully for {dataset_id}")
                return True
            else:
                logger.error(f"❌ Upload verification failed for {dataset_id}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Migration failed for dataset {dataset_id}: {e}")
            return False
    
    def migrate_multiple_datasets(self, dataset_configs: List[Dict]) -> Dict[str, bool]:
        """
        Migrate multiple datasets from Domo to Snowflake.
        
        Args:
            dataset_configs: List of dataset configuration dictionaries
                Each should contain: {'dataset_id': str, 'dataset_name': str, ...}
                
        Returns:
            Dict[str, bool]: Results for each dataset ID
        """
        results = {}
        total_datasets = len(dataset_configs)
        
        logger.info(f"🚀 Starting batch migration for {total_datasets} datasets")
        
        for i, config in enumerate(dataset_configs, 1):
            dataset_id = config.get('dataset_id')
            dataset_name = config.get('dataset_name', '')
            
            if not dataset_id:
                logger.error(f"❌ Missing dataset_id in config {i}")
                results[f"config_{i}"] = False
                continue
            
            logger.info(f"📊 Processing dataset {i}/{total_datasets}: {dataset_id}")
            
            # Extract configuration
            table_name = config.get('table_name')
            chunk_size = config.get('chunk_size')
            
            # Perform migration
            success = self.migrate_dataset(
                dataset_id=dataset_id,
                dataset_name=dataset_name,
                table_name=table_name,
                chunk_size=chunk_size
            )
            
            results[dataset_id] = success
            
            if success:
                logger.info(f"✅ Dataset {i}/{total_datasets} completed successfully")
            else:
                logger.error(f"❌ Dataset {i}/{total_datasets} failed")
        
        # Summary
        successful = sum(1 for success in results.values() if success)
        failed = total_datasets - successful
        
        logger.info(f"📊 Batch migration summary:")
        logger.info(f"   ✅ Successful: {successful}")
        logger.info(f"   ❌ Failed: {failed}")
        logger.info(f"   📈 Success rate: {successful/total_datasets*100:.1f}%")
        
        return results
    
    def get_migration_summary(self, results: Dict[str, bool]) -> Dict[str, any]:
        """
        Generate a summary of migration results.
        
        Args:
            results: Dictionary of dataset_id -> success_boolean
            
        Returns:
            Dict: Summary statistics
        """
        total = len(results)
        successful = sum(1 for success in results.values() if success)
        failed = total - successful
        success_rate = (successful / total * 100) if total > 0 else 0
        
        successful_datasets = [dataset_id for dataset_id, success in results.items() if success]
        failed_datasets = [dataset_id for dataset_id, success in results.items() if not success]
        
        return {
            'total_datasets': total,
            'successful_count': successful,
            'failed_count': failed,
            'success_rate_percent': round(success_rate, 1),
            'successful_datasets': successful_datasets,
            'failed_datasets': failed_datasets
        }
    
    def test_connections(self) -> Tuple[bool, bool]:
        """
        Test both Domo and Snowflake connections.
        
        Returns:
            Tuple[bool, bool]: (domo_connected, snowflake_connected)
        """
        # Test Domo connection
        domo_connected = False
        try:
            if hasattr(self.domo, 'is_authenticated'):
                domo_connected = self.domo.is_authenticated
            else:
                # Fallback test
                datasets = self.domo.get_all_datasets(batch_size=1)
                domo_connected = isinstance(datasets, list)
        except Exception as e:
            logger.error(f"❌ Domo connection test failed: {e}")
        
        # Test Snowflake connection
        snowflake_connected = False
        try:
            if hasattr(self.snowflake, 'is_connected'):
                snowflake_connected = self.snowflake.is_connected
            else:
                # Fallback test
                result = self.snowflake.execute_query("SELECT 1")
                snowflake_connected = result is not None
        except Exception as e:
            logger.error(f"❌ Snowflake connection test failed: {e}")
        
        return domo_connected, snowflake_connected
