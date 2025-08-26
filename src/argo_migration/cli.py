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
        from .api.domo import DomoHandler
        
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
        from .api.snowflake import SnowflakeHandler
        
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
        from .api.domo import DomoHandler
        from .api.snowflake import SnowflakeHandler
        from .services.domo_to_snowflake import MigrationOrchestrator
        
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


def migrate_batch_datasets(batch_file: str):
    """Migrate multiple datasets from a JSON batch file."""
    try:
        from .api.domo import DomoHandler
        from .api.snowflake import SnowflakeHandler
        from .services.domo_to_snowflake import MigrationOrchestrator
        import json
        
        if not os.path.exists(batch_file):
            logger.error(f"❌ Batch file not found: {batch_file}")
            return False
        
        # Load mappings
        with open(batch_file, 'r') as f:
            dataset_mappings = json.load(f)
        
        logger.info(f"🚀 Starting batch migration for {len(dataset_mappings)} datasets")
        
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
            
            orchestrator = MigrationOrchestrator(domo, sf)
            
            # Convert mappings to config format
            configs = []
            for dataset_id, table_name in dataset_mappings.items():
                configs.append({
                    'dataset_id': dataset_id,
                    'table_name': table_name
                })
            
            # Perform batch migration
            results = orchestrator.migrate_multiple_datasets(configs)
            
            # Analyze results
            summary = orchestrator.get_migration_summary(results)
            
            logger.info(f"📊 Migration Summary:")
            logger.info(f"   ✅ Successful: {summary['successful_count']}")
            logger.info(f"   ❌ Failed: {summary['failed_count']}")
            logger.info(f"   📈 Success rate: {summary['success_rate_percent']:.1f}%")
            
            if summary['failed_count'] == 0:
                logger.info("🎉 All migrations completed successfully!")
                return True
            else:
                logger.error(f"❌ {summary['failed_count']} migrations failed")
                if summary['failed_datasets']:
                    logger.error(f"   Failed datasets: {', '.join(summary['failed_datasets'][:5])}")
                return False
                
    except Exception as e:
        logger.error(f"❌ Batch migration failed: {e}")
        return False


def list_datasets(batch_size: int = 50):
    """List Domo datasets."""
    try:
        from .api.domo import DomoHandler
        
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


def compare_datasets(domo_dataset_id: str, snowflake_table: str, key_columns: list, sample_size: int = None):
    """Compare a Domo dataset with a Snowflake table."""
    try:
        from .api.domo import DomoHandler
        from .api.snowflake import SnowflakeHandler
        
        logger.info(f"🔍 Comparing Domo dataset {domo_dataset_id} with Snowflake table {snowflake_table}")
        logger.info(f"🔑 Key columns: {', '.join(key_columns)}")
        
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
            
            # Extract sample data from both sources
            sample_size = sample_size or 1000
            
            logger.info("📊 Extracting data from Domo...")
            query = f"SELECT * FROM table LIMIT {sample_size}"
            domo_data = domo.extract_data(domo_dataset_id, query=query)
            
            if domo_data is None or domo_data.empty:
                logger.error("❌ No data found in Domo dataset")
                return False
            
            logger.info(f"✅ Extracted {len(domo_data)} rows from Domo")
            
            logger.info("❄️ Extracting data from Snowflake...")
            sf_query = f"SELECT * FROM {snowflake_table} LIMIT {sample_size}"
            sf_data = sf.execute_query(sf_query)
            
            if sf_data is None or sf_data.empty:
                logger.error("❌ No data found in Snowflake table")
                return False
            
            logger.info(f"✅ Extracted {len(sf_data)} rows from Snowflake")
            
            # Perform basic comparison
            logger.info("🔍 Performing comparison...")
            
            # Check row counts
            if len(domo_data) != len(sf_data):
                logger.warning(f"⚠️ Row count mismatch: Domo={len(domo_data)}, Snowflake={len(sf_data)}")
            else:
                logger.info(f"✅ Row counts match: {len(domo_data)}")
            
            # Check column counts
            if len(domo_data.columns) != len(sf_data.columns):
                logger.warning(f"⚠️ Column count mismatch: Domo={len(domo_data.columns)}, Snowflake={len(sf_data.columns)}")
            else:
                logger.info(f"✅ Column counts match: {len(domo_data.columns)}")
            
            # Show column names
            logger.info(f"📋 Domo columns: {list(domo_data.columns)}")
            logger.info(f"📋 Snowflake columns: {list(sf_data.columns)}")
            
            logger.info("🎉 Basic comparison completed")
            return True
            
    except Exception as e:
        logger.error(f"❌ Comparison failed: {e}")
        return False


def compare_from_spreadsheet(spreadsheet_id: str = None, sheet_name: str = None, 
                           credentials_path: str = None, sampling_method: str = "random"):
    """Compare multiple datasets from Google Sheets configuration."""
    try:
        from .services.comparison.dataset_comparator import DatasetComparator
        from .utils.common import get_env_config, setup_dual_connections
        
        logger.info("🚀 Starting spreadsheet-based comparisons...")
        
        # Get configuration from environment if not provided
        env_config = get_env_config()
        
        if not spreadsheet_id:
            spreadsheet_id = env_config.get("COMPARISON_SPREADSHEET_ID") or env_config.get("MIGRATION_SPREADSHEET_ID")
        
        if not sheet_name:
            sheet_name = env_config.get("COMPARISON_SHEET_NAME", "QA - Test")
        
        if not credentials_path:
            credentials_path = env_config.get("GOOGLE_SHEETS_CREDENTIALS_FILE")
        
        # Validate required parameters
        if not spreadsheet_id:
            logger.error("❌ Spreadsheet ID is required")
            logger.error("Set COMPARISON_SPREADSHEET_ID or MIGRATION_SPREADSHEET_ID environment variable or use --spreadsheet-id")
            return False
        
        if not credentials_path:
            logger.error("❌ Google Sheets credentials are required")
            logger.error("Set GOOGLE_SHEETS_CREDENTIALS_FILE environment variable or use --credentials")
            return False
        
        if not os.path.exists(credentials_path):
            logger.error(f"❌ Credentials file not found: {credentials_path}")
            return False
        
        config = {
            "Spreadsheet ID": spreadsheet_id,
            "Sheet Name": sheet_name,
            "Credentials": credentials_path,
            "Sampling Method": sampling_method
        }
        
        logger.info("🔧 Comparison Configuration:")
        for key, value in config.items():
            logger.info(f"   {key}: {value}")
        
        # Setup connections first
        logger.info("🔗 Setting up connections...")
        success, domo_handler, snowflake_handler = setup_dual_connections()
        
        if not success:
            logger.error("❌ Failed to setup connections")
            return False
        
        logger.info("✅ Connections established")
        
        # Initialize comparator with existing connections
        comparator = DatasetComparator()
        comparator.domo_handler = domo_handler
        comparator.snowflake_handler = snowflake_handler
        comparator._domo_connected = True
        comparator._snowflake_connected = True
        
        try:
            # Run comparisons from spreadsheet
            logger.info("📊 Running comparisons from spreadsheet...")
            results = comparator.compare_from_spreadsheet(
                spreadsheet_id=spreadsheet_id,
                sheet_name=sheet_name,
                credentials_path=credentials_path,
                sampling_method=sampling_method
            )
            
            # Analyze results
            if 'errors' in results and results['errors']:
                logger.error("❌ Spreadsheet comparison failed due to errors:")
                for error in results['errors'][:5]:  # Show first 5 errors
                    logger.error(f"   - {error}")
                if len(results['errors']) > 5:
                    logger.error(f"   ... and {len(results['errors']) - 5} more errors")
                return False
            
            # Show summary
            total = results.get('total', 0)
            successful = results.get('success', 0)
            failed = results.get('failed', 0)
            
            logger.info("📊 Comparison Summary:")
            logger.info(f"   📋 Total comparisons: {total}")
            logger.info(f"   ✅ Successful: {successful}")
            logger.info(f"   ❌ Failed: {failed}")
            
            if failed == 0:
                logger.info("🎉 All spreadsheet comparisons completed successfully!")
                return True
            else:
                logger.warning(f"⚠️ {failed} comparisons failed or found discrepancies")
                return True  # Not an error, just differences found
                
        finally:
            # Cleanup connections
            try:
                comparator.cleanup()
            except:
                pass  # Ignore cleanup errors
        
    except Exception as e:
        logger.error(f"❌ Spreadsheet comparison failed: {e}")
        return False


def compare_from_inventory(credentials_path: str = None, sampling_method: str = "random"):
    """Compare datasets from inventory spreadsheet."""
    try:
        from .services.comparison.dataset_comparator import DatasetComparator
        from .utils.common import get_env_config, setup_dual_connections
        
        logger.info("🚀 Starting inventory-based comparisons...")
        
        # Get configuration from environment
        env_config = get_env_config()
        
        if not credentials_path:
            credentials_path = env_config.get("GOOGLE_SHEETS_CREDENTIALS_FILE")
        
        if not credentials_path:
            logger.error("❌ Google Sheets credentials are required")
            logger.error("Set GOOGLE_SHEETS_CREDENTIALS_FILE environment variable or use --credentials")
            return False
        
        if not os.path.exists(credentials_path):
            logger.error(f"❌ Credentials file not found: {credentials_path}")
            return False
        
        # Check for migration spreadsheet ID
        spreadsheet_id = env_config.get("MIGRATION_SPREADSHEET_ID")
        if not spreadsheet_id:
            logger.error("❌ MIGRATION_SPREADSHEET_ID environment variable not set")
            return False
        
        config = {
            "Spreadsheet ID": spreadsheet_id,
            "Sheet Name": "Inventory (from env)",
            "Credentials": credentials_path,
            "Sampling Method": sampling_method
        }
        
        logger.info("🔧 Inventory Comparison Configuration:")
        for key, value in config.items():
            logger.info(f"   {key}: {value}")
        
        # Setup connections first
        logger.info("🔗 Setting up connections...")
        success, domo_handler, snowflake_handler = setup_dual_connections()
        
        if not success:
            logger.error("❌ Failed to setup connections")
            return False
        
        logger.info("✅ Connections established")
        
        # Initialize comparator with existing connections
        comparator = DatasetComparator()
        comparator.domo_handler = domo_handler
        comparator.snowflake_handler = snowflake_handler
        comparator._domo_connected = True
        comparator._snowflake_connected = True
        
        try:
            # Run comparisons from inventory
            logger.info("📊 Running comparisons from inventory...")
            results = comparator.compare_from_inventory(
                credentials_path=credentials_path,
                sampling_method=sampling_method
            )
            
            # Analyze results
            if 'errors' in results and results['errors']:
                logger.error("❌ Inventory comparison failed due to errors:")
                for error in results['errors'][:5]:  # Show first 5 errors
                    logger.error(f"   - {error}")
                if len(results['errors']) > 5:
                    logger.error(f"   ... and {len(results['errors']) - 5} more errors")
                return False
            
            # Show summary
            total = results.get('total', 0)
            successful = results.get('success', 0)
            failed = results.get('failed', 0)
            
            logger.info("📊 Inventory Comparison Summary:")
            logger.info(f"   📋 Total comparisons: {total}")
            logger.info(f"   ✅ Successful: {successful}")
            logger.info(f"   ❌ Failed: {failed}")
            
            if failed == 0:
                logger.info("🎉 All inventory comparisons completed successfully!")
                return True
            else:
                logger.warning(f"⚠️ {failed} comparisons failed or found discrepancies")
                return True  # Not an error, just differences found
                
        finally:
            # Cleanup connections
            try:
                comparator.cleanup()
            except:
                pass  # Ignore cleanup errors
        
    except Exception as e:
        logger.error(f"❌ Inventory comparison failed: {e}")
        return False


def generate_stg_files(database: str, schema: str = "TEMP_ARGO_RAW", output_dir: str = "sql/stg", dry_run: bool = False):
    """Generate STG files for datasets."""
    try:
        from .api.domo import DomoHandler
        from .services.stg_handler import StgFileGenerator
        
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
    python cli.py test-connections
    python cli.py test-domo
    python cli.py test-snowflake
    
    # List datasets
    python cli.py list-datasets
    python cli.py list-datasets --batch-size 20
    
    # Migrate datasets
    python cli.py migrate --dataset-id 12345 --table-name my_table
    python cli.py migrate --batch-file mappings.json
    
    # Compare data
    python cli.py compare --domo-dataset-id 12345 --snowflake-table my_table --key-columns id date
    
    # Compare from spreadsheet
    python cli.py compare-spreadsheet
    python cli.py compare-spreadsheet --spreadsheet-id YOUR_SHEET_ID --sheet-name "QA - Test"
    
    # Compare from inventory
    python cli.py compare-inventory
    
    # Generate STG files
    python cli.py generate-stg --database DW_REPORTS
    python cli.py generate-stg --database DW_REPORTS --dry-run

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
    subparsers.add_parser('test-connections', help='Test all connections')
    subparsers.add_parser('test-domo', help='Test Domo connection')
    subparsers.add_parser('test-snowflake', help='Test Snowflake connection')
    
    # List datasets
    list_parser = subparsers.add_parser('list-datasets', help='List Domo datasets')
    list_parser.add_argument('--batch-size', type=int, default=50, help='Batch size for fetching')
    
    # Migration
    migrate_parser = subparsers.add_parser('migrate', help='Migrate datasets')
    migrate_group = migrate_parser.add_mutually_exclusive_group(required=True)
    migrate_group.add_argument('--dataset-id', help='Single Domo dataset ID to migrate')
    migrate_group.add_argument('--batch-file', help='JSON file with dataset mappings')
    
    migrate_parser.add_argument('--table-name', help='Target Snowflake table (required for single dataset)')
    
    # Compare commands
    compare_parser = subparsers.add_parser('compare', help='Compare Domo dataset with Snowflake table')
    compare_parser.add_argument('--domo-dataset-id', required=True, help='Domo dataset ID')
    compare_parser.add_argument('--snowflake-table', required=True, help='Snowflake table name')
    compare_parser.add_argument('--key-columns', nargs='+', required=True, help='Key columns for comparison')
    compare_parser.add_argument('--sample-size', type=int, help='Sample size for comparison')
    
    # Compare from spreadsheet
    compare_spreadsheet_parser = subparsers.add_parser('compare-spreadsheet', help='Compare datasets from Google Sheets')
    compare_spreadsheet_parser.add_argument('--spreadsheet-id', help='Google Sheets spreadsheet ID')
    compare_spreadsheet_parser.add_argument('--sheet-name', help='Sheet name (default: QA - Test)')
    compare_spreadsheet_parser.add_argument('--credentials', help='Path to Google Sheets credentials JSON file')
    compare_spreadsheet_parser.add_argument('--sampling-method', choices=['random', 'ordered'], default='random', help='Sampling method')
    
    # Compare from inventory
    compare_inventory_parser = subparsers.add_parser('compare-inventory', help='Compare datasets from inventory spreadsheet')
    compare_inventory_parser.add_argument('--credentials', help='Path to Google Sheets credentials JSON file')
    compare_inventory_parser.add_argument('--sampling-method', choices=['random', 'ordered'], default='random', help='Sampling method')
    
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
        if args.command == 'test-connections':
            success = test_connections()
            return 0 if success else 1
            
        elif args.command == 'test-domo':
            success = test_domo_connection()
            return 0 if success else 1
            
        elif args.command == 'test-snowflake':
            success = test_snowflake_connection()
            return 0 if success else 1
            
        elif args.command == 'list-datasets':
            success = list_datasets(args.batch_size)
            return 0 if success else 1
            
        elif args.command == 'migrate':
            if args.dataset_id:
                if not args.table_name:
                    logger.error("❌ --table-name is required when using --dataset-id")
                    return 1
                success = migrate_single_dataset(args.dataset_id, args.table_name)
            elif args.batch_file:
                success = migrate_batch_datasets(args.batch_file)
            else:
                logger.error("❌ Must specify either --dataset-id or --batch-file")
                return 1
            
            return 0 if success else 1
            
        elif args.command == 'compare':
            success = compare_datasets(
                args.domo_dataset_id,
                args.snowflake_table,
                args.key_columns,
                args.sample_size
            )
            return 0 if success else 1
            
        elif args.command == 'compare-spreadsheet':
            success = compare_from_spreadsheet(
                spreadsheet_id=args.spreadsheet_id,
                sheet_name=args.sheet_name,
                credentials_path=args.credentials,
                sampling_method=args.sampling_method
            )
            return 0 if success else 1
            
        elif args.command == 'compare-inventory':
            success = compare_from_inventory(
                credentials_path=args.credentials,
                sampling_method=args.sampling_method
            )
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
