#!/usr/bin/env python3
"""
Simple CLI for Argo Migration Tools - Works with modular structure.

This is a simplified CLI that uses the new modular API structure.
"""

import os
import sys
import argparse
import logging
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_domo_connection():
    """Test Domo connection."""
    try:
        from api.domo import DomoHandler
        
        logger.info("🧪 Testing Domo connection...")
        domo = DomoHandler()
        domo.authenticate()
        
        if domo.is_authenticated:
            logger.info("✅ Domo connection successful")
            return True
        else:
            logger.error("❌ Domo authentication failed")
            return False
    except Exception as e:
        logger.error(f"❌ Domo connection test failed: {e}")
        return False


def test_snowflake_connection():
    """Test Snowflake connection."""
    try:
        from api.snowflake import SnowflakeHandler
        
        logger.info("🧪 Testing Snowflake connection...")
        with SnowflakeHandler() as sf:
            if sf.is_connected:
                logger.info("✅ Snowflake connection successful")
                return True
            else:
                logger.error("❌ Snowflake connection failed")
                return False
    except Exception as e:
        logger.error(f"❌ Snowflake connection test failed: {e}")
        return False


def migrate_single_dataset(dataset_id: str, table_name: str):
    """Migrate a single dataset from Domo to Snowflake."""
    try:
        from api.domo import DomoHandler
        from api.snowflake import SnowflakeHandler
        from services.domo_to_snowflake import MigrationOrchestrator
        
        logger.info(f"🚀 Starting migration: {dataset_id} -> {table_name}")
        
        # Setup handlers
        domo = DomoHandler()
        domo.authenticate()
        
        if not domo.is_authenticated:
            logger.error("❌ Domo authentication failed")
            return False
        
        with SnowflakeHandler() as sf:
            if not sf.is_connected:
                logger.error("❌ Snowflake connection failed")
                return False
            
            # Create orchestrator and migrate
            orchestrator = MigrationOrchestrator(domo, sf)
            success = orchestrator.migrate_dataset(dataset_id, table_name=table_name)
            
            if success:
                logger.info("🎉 Migration completed successfully!")
                return True
            else:
                logger.error("❌ Migration failed!")
                return False
                
    except Exception as e:
        logger.error(f"❌ Migration failed: {e}")
        return False


def list_datasets(batch_size: int = 50):
    """List Domo datasets."""
    try:
        from api.domo import DomoHandler
        
        logger.info("📋 Fetching Domo datasets...")
        
        domo = DomoHandler()
        domo.authenticate()
        
        if not domo.is_authenticated:
            logger.error("❌ Domo authentication failed")
            return False
        
        datasets = domo.get_all_datasets(batch_size=batch_size)
        
        if not datasets:
            logger.warning("⚠️ No datasets found")
            return True
        
        logger.info(f"📊 Found {len(datasets)} datasets:")
        
        for i, dataset in enumerate(datasets, 1):
            dataset_id = dataset.get('id', 'N/A')
            dataset_name = dataset.get('name', 'N/A')
            row_count = dataset.get('rowCount', 0) or dataset.get('row_count', 0)
            
            logger.info(f"   {i:3d}. {dataset_name}")
            logger.info(f"        ID: {dataset_id}")
            logger.info(f"        Rows: {row_count:,}")
            
            if i % 5 == 0:  # Add spacing every 5 datasets
                logger.info("")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to list datasets: {e}")
        return False


def generate_stg_files(database: str, schema: str = "TEMP_ARGO_RAW", output_dir: str = "sql/stg", dry_run: bool = False):
    """Generate STG files for datasets."""
    try:
        from api.domo import DomoHandler
        from services.stg_handler import StgFileGenerator
        
        logger.info("🚀 Starting STG file generation...")
        
        config = {
            "Database": database,
            "Schema": schema,
            "Output Directory": output_dir,
            "Dry Run": dry_run
        }
        
        logger.info("🔧 Configuration:")
        for key, value in config.items():
            logger.info(f"   {key}: {value}")
        
        # Setup Domo handler
        domo = DomoHandler()
        domo.authenticate()
        
        if not domo.is_authenticated:
            logger.error("❌ Domo authentication failed")
            return False
        
        # Initialize STG generator
        generator = StgFileGenerator(output_dir=output_dir)
        
        # Get datasets
        logger.info("📊 Fetching datasets...")
        datasets = domo.get_all_datasets(batch_size=10)  # Start small
        
        if not datasets:
            logger.warning("⚠️ No datasets found")
            return True
        
        if dry_run:
            logger.info(f"🧪 Dry run - would generate STG files for {len(datasets)} datasets:")
            for i, dataset in enumerate(datasets[:5], 1):  # Show first 5
                dataset_name = dataset.get('name', 'Unknown')
                logger.info(f"   {i}. stg_{_sanitize_name(dataset_name)}.sql")
            
            if len(datasets) > 5:
                logger.info(f"   ... and {len(datasets) - 5} more files")
            
            logger.info("🧪 Use without --dry-run to actually generate files")
            return True
        
        # Generate STG files
        configs = []
        for dataset in datasets:
            # Create basic schema for demo
            basic_schema = [
                {'name': 'id', 'type': 'STRING'},
                {'name': 'created_date', 'type': 'DATE'},
                {'name': 'value', 'type': 'NUMBER'}
            ]
            
            configs.append({
                'dataset_id': dataset.get('id'),
                'dataset_name': dataset.get('name', ''),
                'schema': basic_schema
            })
        
        logger.info(f"📄 Generating STG files for {len(configs)} datasets...")
        results = generator.generate_batch_stg_files(configs)
        
        # Generate sources.yml
        logger.info("📄 Generating sources.yml...")
        sources_success = generator.generate_sources_yml(configs)
        
        # Summary
        successful = sum(1 for success in results.values() if success)
        failed = len(results) - successful
        
        if failed == 0 and sources_success:
            logger.info("🎉 All STG files generated successfully!")
            return True
        else:
            logger.error(f"❌ Generation completed with issues:")
            logger.error(f"   STG files: {successful} successful, {failed} failed")
            logger.error(f"   sources.yml: {'✅' if sources_success else '❌'}")
            return False
            
    except Exception as e:
        logger.error(f"❌ STG generation failed: {e}")
        return False


def _sanitize_name(name: str) -> str:
    """Sanitize a name for file naming."""
    if not name:
        return "unknown"
    
    import re
    sanitized = re.sub(r'[^\w\s-]', '_', name.lower())
    sanitized = re.sub(r'[-\s]+', '_', sanitized)
    sanitized = re.sub(r'_+', '_', sanitized)
    sanitized = sanitized.strip('_')
    
    return sanitized if sanitized else "unknown"


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Simple Argo Migration CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Test connections
    python cli_simple.py test-domo
    python cli_simple.py test-snowflake
    
    # List datasets
    python cli_simple.py list-datasets
    python cli_simple.py list-datasets --batch-size 20
    
    # Migrate dataset
    python cli_simple.py migrate --dataset-id 12345 --table-name my_table
    
    # Generate STG files
    python cli_simple.py generate-stg --database DW_REPORTS
    python cli_simple.py generate-stg --database DW_REPORTS --dry-run

Environment Variables Required:
    DOMO_DEVELOPER_TOKEN: Your Domo API token
    DOMO_INSTANCE: Your Domo instance name
    SNOWFLAKE_ACCOUNT: Snowflake account identifier
    SNOWFLAKE_USER: Snowflake username  
    SNOWFLAKE_PASSWORD: Snowflake password
    SNOWFLAKE_DATABASE: Snowflake database name
    SNOWFLAKE_SCHEMA: Snowflake schema name
    SNOWFLAKE_WAREHOUSE: Snowflake warehouse name
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Test commands
    subparsers.add_parser('test-domo', help='Test Domo connection')
    subparsers.add_parser('test-snowflake', help='Test Snowflake connection')
    
    # List datasets
    list_parser = subparsers.add_parser('list-datasets', help='List Domo datasets')
    list_parser.add_argument('--batch-size', type=int, default=50, help='Batch size for fetching')
    
    # Migration
    migrate_parser = subparsers.add_parser('migrate', help='Migrate single dataset')
    migrate_parser.add_argument('--dataset-id', required=True, help='Domo dataset ID')
    migrate_parser.add_argument('--table-name', required=True, help='Target Snowflake table')
    
    # STG generation
    stg_parser = subparsers.add_parser('generate-stg', help='Generate STG files')
    stg_parser.add_argument('--database', required=True, help='Snowflake database')
    stg_parser.add_argument('--schema', default='TEMP_ARGO_RAW', help='Snowflake schema')
    stg_parser.add_argument('--output-dir', default='sql/stg', help='Output directory')
    stg_parser.add_argument('--dry-run', action='store_true', help='Show what would be generated')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    logger.info("🚀 Argo Migration Tools - Simple CLI")
    
    try:
        # Route commands
        if args.command == 'test-domo':
            success = test_domo_connection()
            return 0 if success else 1
            
        elif args.command == 'test-snowflake':
            success = test_snowflake_connection()
            return 0 if success else 1
            
        elif args.command == 'list-datasets':
            success = list_datasets(args.batch_size)
            return 0 if success else 1
            
        elif args.command == 'migrate':
            success = migrate_single_dataset(args.dataset_id, args.table_name)
            return 0 if success else 1
            
        elif args.command == 'generate-stg':
            success = generate_stg_files(
                database=args.database,
                schema=args.schema,
                output_dir=args.output_dir,
                dry_run=args.dry_run
            )
            return 0 if success else 1
            
        else:
            logger.error(f"❌ Unknown command: {args.command}")
            return 1
            
    except KeyboardInterrupt:
        logger.info("⚠️  Operation cancelled by user")
        return 1
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
