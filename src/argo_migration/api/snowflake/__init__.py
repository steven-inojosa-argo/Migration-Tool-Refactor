"""
Snowflake API module for data transfer operations.

This module provides a modular approach to Snowflake operations including:
- Authentication and connection management
- Data upload and querying
- Table operations and verification

Usage:
    from argo_migration.api.snowflake import SnowflakeHandler
    
    # Using context manager (recommended)
    with SnowflakeHandler() as sf:
        df = pd.DataFrame({'col1': [1, 2, 3]})
        sf.upload_data(df, 'my_table')
    
    # Manual management
    sf = SnowflakeHandler()
    if sf.setup_connection():
        sf.upload_data(df, 'my_table')
        sf.cleanup()
"""

# Main interface
from .handler import SnowflakeHandler

# Individual modules for advanced usage
from .auth import SnowflakeAuth, show_current_totp_debug, reload_env_vars
from .data_handler import SnowflakeDataHandler

__all__ = [
    'SnowflakeHandler',
    'SnowflakeAuth',
    'SnowflakeDataHandler',
    'show_current_totp_debug',
    'reload_env_vars'
]
