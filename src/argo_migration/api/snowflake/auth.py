"""Snowflake authentication module."""

import os
import getpass
import logging
from typing import Optional, Dict, Any
from dotenv import load_dotenv

try:
    import snowflake.connector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    snowflake = None

logger = logging.getLogger(__name__)


class SnowflakeAuth:
    """Handles Snowflake authentication operations."""
    
    def __init__(self):
        self.connection = None
        self.connection_params = {}
    
    def setup_connection(self) -> bool:
        """Setup Snowflake connection with environment variables."""
        if not SNOWFLAKE_AVAILABLE:
            logger.error("❌ Snowflake connector not available. Install with: pip install snowflake-connector-python")
            return False
        
        try:
            # Reload environment variables
            self._reload_env_vars()
            
            # Get connection parameters
            self.connection_params = self._get_connection_params()
            
            # Test connection
            self.connection = snowflake.connector.connect(**self.connection_params)
            
            # Test the connection
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            
            logger.info("✅ Snowflake connection established successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to Snowflake: {e}")
            return False
    
    def _reload_env_vars(self):
        """Reload environment variables from .env file."""
        load_dotenv(override=True)
        logger.info("🔄 Environment variables reloaded")
    
    def _get_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters from environment variables."""
        # Required parameters
        required_params = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'role': os.getenv('SNOWFLAKE_ROLE')
        }
        
        # Check for missing required parameters
        missing = [k for k, v in required_params.items() if not v]
        if missing:
            raise ValueError(f"Missing required Snowflake environment variables: {missing}")
        
        # Handle TOTP passcode
        passcode = os.getenv('SNOWFLAKE_PASSCODE')
        if passcode:
            if passcode == "MANUAL":
                # Interactive TOTP input
                passcode = getpass.getpass("🔐 Enter TOTP passcode: ")
            required_params['passcode'] = passcode
        
        # Optional parameters
        optional_params = {
            'region': os.getenv('SNOWFLAKE_REGION'),
            'autocommit': True,
            'timeout': 300
        }
        
        # Combine parameters
        connection_params = {**required_params, **optional_params}
        
        # Remove None values
        return {k: v for k, v in connection_params.items() if v is not None}
    
    def get_connection(self):
        """Get the active Snowflake connection."""
        return self.connection
    
    def is_connected(self) -> bool:
        """Check if connection is active."""
        try:
            if self.connection:
                cursor = self.connection.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                return True
        except:
            pass
        return False
    
    def close_connection(self):
        """Close the Snowflake connection."""
        if self.connection:
            try:
                self.connection.close()
                logger.info("✅ Snowflake connection closed")
            except Exception as e:
                logger.error(f"❌ Error closing connection: {e}")
            finally:
                self.connection = None


def show_current_totp_debug():
    """Show current TOTP passcode for debugging purposes."""
    passcode = os.getenv('SNOWFLAKE_PASSCODE')
    if passcode:
        if passcode == "MANUAL":
            print("📱 TOTP mode: MANUAL (interactive input)")
            print("💡 You will be prompted to enter TOTP code when connecting")
        else:
            masked_passcode = passcode[:2] + '*' * (len(passcode) - 2) if len(passcode) > 2 else '***'
            print(f"📱 Current TOTP passcode: {masked_passcode}")
            print("💡 Remember: TOTP codes expire every 30 seconds")
    else:
        print("📱 No TOTP passcode found in environment variables")


def reload_env_vars():
    """Reload environment variables from .env file."""
    load_dotenv(override=True)
    logger.info("🔄 Environment variables reloaded from .env file")
