"""
Common utilities and shared functions for data migration tools.

This module contains utility functions and type definitions that are used
across multiple components in the migration pipeline.
"""

import os
import re
import logging
from typing import TypedDict, List, Optional, Dict
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logger
logger = logging.getLogger(__name__)


class ColumnMetadata(TypedDict):
    """Metadata for a Domo column."""
    colLabel: str  # Usually same as name
    colFormat: str  # Usually empty ''
    isEncrypted: bool  # Always false


class Columns(TypedDict):
    """Definition of a Domo column."""
    type: str
    name: str
    id: str  # Usually same as name
    visible: bool  # Always true
    metadata: ColumnMetadata


class Schema(TypedDict):
    """Domo schema definition."""
    columns: list[Columns]


class QueryResult(TypedDict):
    """Result from a Domo query operation."""
    datasource: str
    columns: list[str]
    rows: list[list]


def transform_column_name(column_name: str) -> str:
    """
    Transform Domo column names to match Snowflake naming conventions.
    
    Args:
        column_name: Original column name from Domo
        
    Returns:
        Transformed column name for Snowflake compatibility
        
    Examples:
        >>> transform_column_name("Product Name")
        'PRODUCT_NAME'
        >>> transform_column_name("Sales $")
        'SALES'
        >>> transform_column_name("Date Created")
        'DATE_CREATED'
    """
    # Convert to lowercase for processing
    name = column_name.lower()
    
    # Replace special characters with words
    name = re.sub(r"\bno\.\s*", "number_", name)
    name = name.replace("#", "number").replace("&", "and")
    
    # Remove/replace special characters
    name = name.replace("(", "_").replace(")", "_")
    name = name.replace(" ", "_").replace("-", "_")
    name = name.replace("/", "_").replace(".", "_").replace("?", "_")
    
    # Clean up consecutive underscores
    name = re.sub(r"_+", "_", name).strip("_")
    
    return name.upper()


def check_env_vars(required_vars: Optional[List[str]] = None) -> bool:
    """
    Check if required environment variables are set.
    
    Args:
        required_vars: List of required variable names. If None, uses default set.
        
    Returns:
        True if all required variables are set, False otherwise
    """
    
    if required_vars is None:
        required_vars = [
            'DOMO_INSTANCE', 'DOMO_DEVELOPER_TOKEN',
            'SNOWFLAKE_USER', 'SNOWFLAKE_ACCOUNT',
            'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA'
        ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
            logger.warning(f"❌ {var}: NOT FOUND")
        else:
            # Show masked value for security
            value = os.getenv(var)
            masked_value = value[:3] + '*' * (len(value) - 6) + value[-3:] if len(value) > 6 else '***'
            logger.info(f"✅ {var}: {masked_value}")
    
    if missing_vars:
        logger.error(f"Missing required variables: {', '.join(missing_vars)}")
        return False
    
    logger.info("✅ All required environment variables are configured")
    return True


def combine_schemas(schema_a: Schema, schema_b: Schema) -> Schema:
    """
    Combines two Domo schemas, adding columns from schema_b to schema_a
    without replacing existing columns if names conflict.
    New columns from schema_b are added to the end.

    Args:
        schema_a: The base schema
        schema_b: The schema with columns to add

    Returns:
        A new schema with combined columns
    """
    combined_columns = list(
        schema_a["columns"]
    )  # Start with a copy of schema_a's columns
    existing_column_names = {col["name"] for col in combined_columns}

    for col_b in schema_b["columns"]:
        if col_b["name"] not in existing_column_names:
            combined_columns.append(col_b)
            existing_column_names.add(col_b["name"])

    return Schema(columns=combined_columns)


def get_snowflake_table_full_name(table_name: str) -> str:
    """
    Get the fully qualified Snowflake table name.
    
    Args:
        table_name: The table name
        
    Returns:
        Fully qualified table name in format: "DATABASE"."SCHEMA"."TABLE"
    """
    db = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    return f'"{db}"."{schema}"."{table_name}"'


def mask_sensitive_value(value: str, visible_chars: int = 2) -> str:
    """
    Mask sensitive values for logging.
    
    Args:
        value: The sensitive value to mask
        visible_chars: Number of characters to show at the beginning
        
    Returns:
        Masked string
    """
    if not value or len(value) <= visible_chars:
        return '***'
    
    return value[:visible_chars] + '*' * (len(value) - visible_chars)


def get_env_config() -> Dict[str, Optional[str]]:
    """
    Get all environment configuration in one place.
    
    Returns:
        Dictionary with all environment variables
    """
    return {
        # Snowflake configuration
        'SNOWFLAKE_USER': os.getenv('SNOWFLAKE_USER'),
        'SNOWFLAKE_ACCOUNT': os.getenv('SNOWFLAKE_ACCOUNT'),
        'SNOWFLAKE_WAREHOUSE': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'SNOWFLAKE_DATABASE': os.getenv('SNOWFLAKE_DATABASE'),
        'SNOWFLAKE_SCHEMA': os.getenv('SNOWFLAKE_SCHEMA'),
        'SNOWFLAKE_PASSWORD': os.getenv('SNOWFLAKE_PASSWORD'),
        'SNOWFLAKE_PASSCODE': os.getenv('SNOWFLAKE_PASSCODE'),
        'SNOWFLAKE_PRIVATE_KEY_PATH': os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH'),
        'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE': os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'),
        'SNOWFLAKE_AUTHENTICATOR': os.getenv('SNOWFLAKE_AUTHENTICATOR'),
        
        # Domo configuration
        'DOMO_DEVELOPER_TOKEN': os.getenv('DOMO_DEVELOPER_TOKEN'),
        'DOMO_INSTANCE': os.getenv('DOMO_INSTANCE'),
        'DOMO_CLIENT_ID': os.getenv('DOMO_CLIENT_ID'),
        'DOMO_CLIENT_SECRET': os.getenv('DOMO_CLIENT_SECRET'),
        'DOMO_TABLE_PREFIX': os.getenv('DOMO_TABLE_PREFIX', 'DOMO_'),
        
        # Google Sheets configuration
        'GOOGLE_SHEETS_CREDENTIALS_FILE': os.getenv('GOOGLE_SHEETS_CREDENTIALS_FILE'),
        'MIGRATION_SPREADSHEET_ID': os.getenv('MIGRATION_SPREADSHEET_ID'),
        'COMPARISON_SPREADSHEET_ID': os.getenv('COMPARISON_SPREADSHEET_ID'),
        'MIGRATION_SHEET_NAME': os.getenv('MIGRATION_SHEET_NAME', 'Migration'),
        'COMPARISON_SHEET_NAME': os.getenv('COMPARISON_SHEET_NAME', 'QA - Test'),
        'INTERMEDIATE_MODELS_SHEET_NAME': os.getenv('INTERMEDIATE_MODELS_SHEET_NAME', 'Inventory'),
        
        # Comparison configuration
        'TRANSFORM_COLUMNS': os.getenv('TRANSFORM_COLUMNS'),
    }


def get_transform_columns_setting(cli_arg: Optional[bool] = None) -> bool:
    """
    Determine whether to transform column names based on priority:
    1. CLI argument (if provided)
    2. Environment variable TRANSFORM_COLUMNS
    3. Default: False
    
    Args:
        cli_arg: Value from CLI argument (True if --transform-columns passed, None if not)
        
    Returns:
        bool: Whether to transform column names
    """
    # Priority 1: CLI argument
    if cli_arg is not None:
        return cli_arg
    
    # Priority 2: Environment variable
    env_value = os.getenv('TRANSFORM_COLUMNS')
    if env_value:
        env_value_lower = env_value.lower()
        return env_value_lower in ['true', '1', 'yes', 'y', 'enabled']
    
    # Priority 3: Default
    return False


def setup_dual_connections(domo_handler=None, snowflake_handler=None) -> tuple[bool, Optional[object], Optional[object]]:
    """
    Shared connection setup for Domo and Snowflake handlers.
    
    Args:
        domo_handler: Optional existing DomoHandler instance
        snowflake_handler: Optional existing SnowflakeHandler instance
        
    Returns:
        Tuple of (success: bool, domo_handler, snowflake_handler)
    """
    try:
        logger.info("🔧 Setting up dual connections...")
        
        # Setup Domo if not provided
        if domo_handler is None:
            from ..api.domo import DomoHandler
            domo_handler = DomoHandler()
        
        try:
            domo_handler.authenticate()
            if not domo_handler.is_authenticated:
                logger.error("❌ Failed to connect to Domo")
                return False, None, None
        except Exception as e:
            logger.error(f"❌ Failed to connect to Domo: {e}")
            return False, None, None
        
        # Setup Snowflake if not provided
        if snowflake_handler is None:
            from ..api.snowflake import SnowflakeHandler
            snowflake_handler = SnowflakeHandler()
        
        if not snowflake_handler.setup_connection():
            logger.error("❌ Failed to connect to Snowflake")
            return False, domo_handler, None
        
        logger.info("✅ All connections established successfully")
        return True, domo_handler, snowflake_handler
        
    except Exception as e:
        logger.error(f"❌ Failed to setup connections: {e}")
        return False, None, None


def show_mfa_debug_info():
    """
    Show MFA/TOTP debug information for Snowflake authentication.
    """
    env_config = get_env_config()
    passcode = env_config.get('SNOWFLAKE_PASSCODE')
    
    if passcode:
        masked_passcode = mask_sensitive_value(passcode, 2)
        logger.info(f"📱 Current TOTP passcode: {masked_passcode}")
        
        # Show TOTP debug info if available
        try:
            from ..api.snowflake import show_current_totp_debug
            logger.info("🔐 Using MFA authentication - TOTP Debug Info:")
            show_current_totp_debug()
        except ImportError:
            logger.warning("TOTP debug not available")
    else:
        logger.info("📱 No TOTP passcode found")


def reload_environment():
    """
    Reload environment variables and show debug info.
    """
    logger.info("🔄 Reloading environment variables...")
    
    try:
        from ..api.snowflake import reload_env_vars
        reload_env_vars()
        show_mfa_debug_info()
    except ImportError:
        logger.warning("Environment reload utilities not available") 