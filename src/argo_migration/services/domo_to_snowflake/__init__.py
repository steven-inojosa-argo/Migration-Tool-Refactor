"""
Domo to Snowflake migration service.

This module provides tools for migrating data from Domo to Snowflake including:
- Table name utilities and sanitization
- Migration orchestration and batch processing
- Connection testing and validation

Usage:
    from argo_migration.services.domo_to_snowflake import MigrationOrchestrator
    from argo_migration.api.domo import DomoHandler
    from argo_migration.api.snowflake import SnowflakeHandler
    
    # Setup handlers
    domo = DomoHandler()
    domo.authenticate()
    
    with SnowflakeHandler() as sf:
        # Create orchestrator
        orchestrator = MigrationOrchestrator(domo, sf)
        
        # Migrate single dataset
        success = orchestrator.migrate_dataset('dataset_id')
        
        # Migrate multiple datasets
        configs = [
            {'dataset_id': 'id1', 'dataset_name': 'Dataset 1'},
            {'dataset_id': 'id2', 'dataset_name': 'Dataset 2'}
        ]
        results = orchestrator.migrate_multiple_datasets(configs)
"""

# Main interface
from .migration_orchestrator import MigrationOrchestrator

# Utilities
from .table_utils import sanitize_table_name, validate_table_name, generate_table_name_variants

__all__ = [
    'MigrationOrchestrator',
    'sanitize_table_name',
    'validate_table_name', 
    'generate_table_name_variants'
]
