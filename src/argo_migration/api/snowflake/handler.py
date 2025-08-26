"""Main Snowflake handler - orchestrates all Snowflake operations."""

import logging
from typing import Optional, List, Dict, Any
import pandas as pd

from .auth import SnowflakeAuth
from .data_handler import SnowflakeDataHandler

logger = logging.getLogger(__name__)


class SnowflakeHandler:
    """Main handler for all Snowflake operations."""
    
    def __init__(self):
        """Initialize the Snowflake handler."""
        self._auth = SnowflakeAuth()
        self._data_handler = None
        self._connected = False
    
    def setup_connection(self) -> bool:
        """Setup Snowflake connection and initialize components."""
        if self._auth.setup_connection():
            self._data_handler = SnowflakeDataHandler(self._auth.get_connection())
            self._connected = True
            logger.info("✅ SnowflakeHandler ready")
            return True
        else:
            logger.error("❌ Failed to setup Snowflake connection")
            return False
    
    @property
    def is_connected(self) -> bool:
        """Check if handler is connected and ready."""
        return self._connected and self._auth.is_connected()
    
    # Data Operations
    def upload_data(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace', chunk_size: int = None) -> bool:
        """Upload DataFrame to Snowflake table."""
        self._ensure_connected()
        return self._data_handler.upload_data(df, table_name, if_exists, chunk_size)
    
    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        """Execute SQL query and return results."""
        self._ensure_connected()
        return self._data_handler.execute_query(query)
    
    def verify_upload(self, table_name: str, expected_rows: int) -> bool:
        """Verify that data was uploaded correctly."""
        self._ensure_connected()
        return self._data_handler.verify_upload(table_name, expected_rows)
    
    def get_table_columns(self, database: str, schema: str, table_name: str, role: str = "DBT_ROLE", warehouse: str = None) -> List[Dict[str, Any]]:
        """Get column information for a table."""
        self._ensure_connected()
        return self._data_handler.get_table_columns(database, schema, table_name, role, warehouse)
    
    # Connection Management
    def cleanup(self):
        """Close connection and cleanup resources."""
        if self._auth:
            self._auth.close_connection()
        self._connected = False
        logger.info("✅ SnowflakeHandler cleanup completed")
    
    def _ensure_connected(self):
        """Ensure the handler is connected."""
        if not self._connected:
            raise ValueError("❌ Not connected to Snowflake. Call setup_connection() first.")
    
    # Context manager support
    def __enter__(self):
        """Enter context manager."""
        if not self.setup_connection():
            raise ConnectionError("Failed to setup Snowflake connection")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager."""
        self.cleanup()
