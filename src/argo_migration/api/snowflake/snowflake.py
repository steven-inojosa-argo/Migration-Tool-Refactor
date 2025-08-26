"""
Snowflake utilities for data transfer operations.

This module handles all Snowflake-related operations including:
- Connection setup with multiple authentication methods
- Data upload to Snowflake tables
- Table creation and verification
"""

import os
import logging
import time
import getpass
from typing import Optional
import pandas as pd
import numpy as np
from dotenv import load_dotenv

try:
    import snowflake.connector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    snowflake = None  # type: ignore

logger = logging.getLogger(__name__)

def reload_env_vars():
    """Reload environment variables from .env file"""
    load_dotenv(override=True)  # override=True forces reload of existing variables
    logger.info("🔄 Environment variables reloaded from .env file")

def show_current_totp_debug():
    """Show current TOTP passcode for debugging purposes"""
    passcode = os.getenv('SNOWFLAKE_PASSCODE')
    if passcode:
        if passcode == "MANUAL":
            print(f"📱 TOTP mode: MANUAL (interactive input)")
            print(f"💡 You will be prompted to enter TOTP code when connecting")
        else:
            masked_passcode = passcode[:2] + '*' * (len(passcode) - 2) if len(passcode) > 2 else '***'
            print(f"📱 Current TOTP passcode: {masked_passcode}")
            print(f"⏰ Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"💡 Remember: TOTP codes expire every 30 seconds")
    else:
        print("📱 No TOTP passcode found in environment variables")


class SnowflakeHandler:
    """Handles all Snowflake operations including connection and data upload."""
    
    def __init__(self):
        """Initialize the Snowflake handler."""
        self.conn = None
        
    def setup_connection(self) -> bool:
        """
        Setup Snowflake connection using environment variables.
        Supports multiple authentication methods including MFA.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        if not SNOWFLAKE_AVAILABLE:
            logger.error("Snowflake connector not available")
            return False
            
        try:
            # Reload environment variables to get fresh TOTP codes
            reload_env_vars()
            
            # Get connection parameters
            snowflake_config = {
                'user': os.getenv("SNOWFLAKE_USER"),
                'account': os.getenv("SNOWFLAKE_ACCOUNT"),
                'warehouse': os.getenv("SNOWFLAKE_WAREHOUSE"),
                'database': os.getenv("SNOWFLAKE_DATABASE"),
                'schema': os.getenv("SNOWFLAKE_SCHEMA"),
                'role': os.getenv("SNOWFLAKE_ROLE")  # Add role support
            }
            
            # Check required parameters
            required_params = ['user', 'account']
            missing_params = [k for k in required_params if not snowflake_config[k]]
            
            if missing_params:
                logger.error(f"Missing required Snowflake parameters: {missing_params}")
                logger.error("Please set the following environment variables:")
                for param in missing_params:
                    logger.error(f"  - SNOWFLAKE_{param.upper()}")
                return False
            
            # Determine authentication method
            auth_method = self._determine_auth_method()
            logger.info(f"Using Snowflake authentication method: {auth_method}")
            
            if auth_method == "key_pair":
                # RSA Key Pair Authentication (recommended for automated scripts)
                private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
                private_key_passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
                
                if not private_key_path:
                    logger.error("SNOWFLAKE_PRIVATE_KEY_PATH is required for key pair authentication")
                    return False
                
                from cryptography.hazmat.primitives import serialization
                from cryptography.hazmat.primitives.serialization import load_pem_private_key
                
                # Load private key
                with open(private_key_path, 'rb') as key_file:
                    private_key = load_pem_private_key(
                        key_file.read(),
                        password=private_key_passphrase.encode() if private_key_passphrase else None,
                    )
                
                # Convert to DER format for Snowflake
                private_key_der = private_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
                
                snowflake_config['private_key'] = private_key_der
                
            elif auth_method == "mfa":
                # MFA with TOTP
                password = os.getenv("SNOWFLAKE_PASSWORD")
                passcode = os.getenv("SNOWFLAKE_PASSCODE")
                
                if not password:
                    logger.error("SNOWFLAKE_PASSWORD is required for MFA authentication")
                    return False
                
                # Check if manual passcode input is requested
                if passcode == "MANUAL":
                    print("🔐 Manual TOTP passcode input requested")
                    print("📱 Please enter your current MFA/TOTP code from your authenticator app")
                    print("💡 TOTP codes expire every 30 seconds - use a fresh code")
                    print()  # Add blank line for better readability
                    
                    try:
                        passcode = getpass.getpass("Enter TOTP code (6 digits): ").strip()
                    except KeyboardInterrupt:
                        print("\n⚠️  Authentication cancelled by user")
                        return False
                    except Exception as e:
                        print(f"❌ Error reading passcode: {e}")
                        return False
                    
                    if not passcode:
                        print("❌ No passcode entered")
                        return False
                
                elif not passcode:
                    logger.error("SNOWFLAKE_PASSCODE is required for MFA authentication")
                    logger.error("Set SNOWFLAKE_PASSCODE to your current TOTP code, or set to 'MANUAL' for interactive input")
                    logger.error("💡 TOTP codes expire every 30 seconds - make sure to use a fresh code")
                    return False
                
                # Validate passcode format (should be 6 digits)
                if not passcode.isdigit() or len(passcode) != 6:
                    logger.error(f"Invalid TOTP passcode format: {passcode}")
                    logger.error("TOTP passcode should be 6 digits (e.g., 123456)")
                    return False
                
                logger.info(f"Using MFA authentication with passcode: {passcode[:2]}****")
                logger.info(f"📱 TOTP passcode loaded at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
                snowflake_config['password'] = password
                snowflake_config['passcode'] = passcode
                
            elif auth_method == "sso":
                # SSO Authentication
                authenticator = os.getenv("SNOWFLAKE_AUTHENTICATOR", "externalbrowser")
                snowflake_config['authenticator'] = authenticator
                
            else:
                # Standard password authentication
                password = os.getenv("SNOWFLAKE_PASSWORD")
                if not password:
                    logger.error("SNOWFLAKE_PASSWORD is required for password authentication")
                    return False
                snowflake_config['password'] = password
            
            # Remove None values
            snowflake_config = {k: v for k, v in snowflake_config.items() if v is not None}

            # Log connection info (excluding sensitive data)  
            connection_info = []
            connection_info.append(f"User: {snowflake_config.get('user', 'Not set')}")
            connection_info.append(f"Account: {snowflake_config.get('account', 'Not set')}")
            connection_info.append(f"Warehouse: {snowflake_config.get('warehouse', 'Not set')}")
            connection_info.append(f"Database: {snowflake_config.get('database', 'Not set')}")
            connection_info.append(f"Schema: {snowflake_config.get('schema', 'Not set')}")
            if snowflake_config.get('role'):
                connection_info.append(f"Role: {snowflake_config.get('role')}")
            else:
                connection_info.append("Role: Using default role")

            logger.info("Connecting to Snowflake with:")
            for info in connection_info:
                logger.info(f"  {info}")

            # Create connection
            if snowflake is None:
                logger.error("Snowflake connector not available")
                return False

            self.conn = snowflake.connector.connect(**snowflake_config)
            
            # Test connection
            cursor = self.conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            version = cursor.fetchone()[0]
            cursor.close()
            
            logger.info(f"✅ Connected to Snowflake version: {version}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            
            # Provide helpful error messages
            error_str = str(e)
            if "MFA with TOTP is required" in error_str:
                logger.error("💡 MFA (Multi-Factor Authentication) is required.")
                logger.error("   Set SNOWFLAKE_PASSCODE to your current TOTP code, or")
                logger.error("   Use key pair authentication (see README for setup)")
            elif "TOTP Invalid" in error_str:
                logger.error("💡 TOTP code is invalid or has expired.")
                logger.error("   TOTP codes expire every 30 seconds.")
                logger.error("   Please generate a fresh code and update SNOWFLAKE_PASSCODE")
            elif "Failed to authenticate" in error_str:
                logger.error("💡 Authentication failed. Check your credentials.")
            elif "cryptography" in error_str:
                logger.error("💡 Install cryptography: pip install cryptography")
            elif "private key" in error_str.lower():
                logger.error("💡 Check your private key path and passphrase")
            
            return False
    
    def _determine_auth_method(self) -> str:
        """
        Determine the authentication method based on environment variables.
        
        Returns:
            str: Authentication method ('key_pair', 'mfa', 'sso', 'password')
        """
        # Check for key pair authentication
        if os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"):
            return "key_pair"
        
        # Check for MFA (including manual input)
        passcode_env = os.getenv("SNOWFLAKE_PASSCODE")
        if passcode_env and (passcode_env.isdigit() or passcode_env == "MANUAL"):
            return "mfa"
        
        # Check for SSO
        if os.getenv("SNOWFLAKE_AUTHENTICATOR"):
            return "sso"
        
        # Default to password authentication
        return "password"
    
    def upload_data(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace', chunk_size: int = None) -> bool:
        """
        Upload DataFrame to Snowflake table using cursor method.
        
        Args:
            df: DataFrame to upload
            table_name: Target table name
            if_exists: What to do if table exists ('replace', 'append', 'fail')
            chunk_size: Optional batch size for uploads (None for X-Small optimization)
            
        Returns:
            bool: True if upload successful, False otherwise
        """
        if self.conn is None:
            logger.error("Snowflake connection not established")
            return False
        
        try:
            logger.info(f"Uploading {len(df)} rows to Snowflake table: {table_name}")
            return self._upload_via_cursor(df, table_name, if_exists, chunk_size)
            
        except Exception as e:
            logger.error(f"Failed to upload data to Snowflake: {e}")
            return False 
    
    def _upload_via_cursor(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace', chunk_size: int = None) -> bool:
        """
        Upload DataFrame using cursor method with automatic type coercion fallback.
        
        Args:
            df: DataFrame to upload
            table_name: Target table name
            if_exists: What to do if table exists
            chunk_size: Optional batch size for uploads (None for X-Small optimization)
            
        Returns:
            bool: True if upload successful, False otherwise
        """
        try:
            # First attempt: Normal upload
            return self._attempt_upload(df, table_name, if_exists, chunk_size)
            
        except Exception as e:
            error_msg = str(e)
            
            # Check if it's a type conversion error
            if any(keyword in error_msg.lower() for keyword in ['numeric value', 'invalid identifier', 'type mismatch', 'not recognized']):
                logger.warning(f"⚠️  Type conversion error detected: {error_msg}")
                
                # Extract problematic column name from error message
                problematic_column = self._extract_column_from_error(error_msg)
                
                if problematic_column:
                    logger.info(f"🔄 Attempting upload with string coercion for column: {problematic_column}")
                    
                    try:
                        # Fallback: Convert only the problematic column to string
                        df_fallback = self._coerce_specific_column(df, problematic_column)
                        return self._attempt_upload(df_fallback, table_name, if_exists, chunk_size)
                    except Exception as fallback_error:
                        logger.error(f"❌ Fallback upload also failed: {fallback_error}")
                        return False
                else:
                    logger.error(f"❌ Could not identify problematic column from error: {error_msg}")
                    return False
            else:
                # Re-raise if it's not a type error
                logger.error(f"❌ Cursor upload failed: {e}")
                return False

    def _attempt_upload(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace', chunk_size: int = None) -> bool:
        """
        Attempt the actual upload operation.
        
        Args:
            df: DataFrame to upload
            table_name: Target table name
            if_exists: What to do if table exists
            chunk_size: Optional batch size for uploads
            
        Returns:
            bool: True if upload successful, False otherwise
        """
        cursor = self.conn.cursor()
        
        # Escape table name for SQL operations
        escaped_table_name = f'"{table_name.upper()}"'
        
        # Normalize column names for Snowflake compatibility FIRST
        logger.info("🔧 Normalizing columns...")
        df_normalized = self._normalize_column_names(df)
    
        # Handle table existence
        if if_exists == 'replace':
            cursor.execute(f"DROP TABLE IF EXISTS {escaped_table_name}")
            logger.info(f"Dropped existing table: {table_name}")
        
        # Create table if it doesn't exist (using normalized columns)
        if if_exists in ['replace', 'fail']:
            create_sql = self._generate_create_table_sql(df_normalized, table_name)
            cursor.execute(create_sql)
            logger.info(f"Created table: {table_name}")
        
        # Prepare data for insertion
        columns = list(df_normalized.columns)
        
        # Handle column names for INSERT statement
        # Columns that are already quoted (reserved words) don't need additional escaping
        escaped_columns = []
        for col in columns:
            if col.startswith('"') and col.endswith('"'):
                # Already quoted (reserved word), use as is
                escaped_columns.append(col)
            else:
                # Regular column, add quotes
                escaped_columns.append(f'"{col}"')
        
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Insert data in batches
        if chunk_size is None:
            # X-Small warehouse optimized batch size determination
            batch_size = self._calculate_xsmall_optimized_batch_size(df_normalized)
            total_rows = len(df_normalized)
        else:
            batch_size = chunk_size
            total_rows = len(df_normalized)
        
        logger.info(f"📊 Batch size: {batch_size}")
        
        for i in range(0, total_rows, batch_size):
            batch = df_normalized.iloc[i:i + batch_size]
            values = [tuple(row) for row in batch.values]
            
            insert_sql = f"INSERT INTO {escaped_table_name} ({', '.join(escaped_columns)}) VALUES ({placeholders})"
            cursor.executemany(insert_sql, values)
            
            logger.info(f"Inserted batch {i//batch_size + 1}/{(total_rows + batch_size - 1)//batch_size}")
        
        cursor.close()
        logger.info(f"✅ Uploaded {total_rows} rows via cursor method")
        return True

    def _extract_column_from_error(self, error_msg: str) -> str:
        """
        Extract column name from Snowflake error message.
        
        Args:
            error_msg: Error message from Snowflake
            
        Returns:
            str: Column name that caused the error, or None if not found
        """
        import re
        
        # Pattern for "failed on column COLUMN_NAME with error"
        pattern = r"failed on column ([A-Za-z_][A-Za-z0-9_\s]*) with error"
        match = re.search(pattern, error_msg)
        
        if match:
            column_name = match.group(1).strip()
            logger.info(f"🔍 Identified problematic column: {column_name}")
            return column_name
        
        logger.warning(f"⚠️  Could not extract column name from error: {error_msg}")
        return None

    def _coerce_specific_column(self, df: pd.DataFrame, column_name: str) -> pd.DataFrame:
        """
        Convert a specific column to string type while preserving other columns.
        
        Args:
            df: DataFrame with potential type issues
            column_name: Name of the column to convert to string
            
        Returns:
            pd.DataFrame: DataFrame with only the specified column converted to string
        """
        df_coerced = df.copy()
        
        # Find the actual column name (case-insensitive and handle normalized names)
        actual_column = None
        for col in df_coerced.columns:
            if col.upper() == column_name.upper():
                actual_column = col
                break
        
        if actual_column:
            logger.info(f"🔧 Converting column '{actual_column}' to string type")
            df_coerced[actual_column] = df_coerced[actual_column].astype(str)
            logger.info(f"✅ Successfully converted column '{actual_column}' to string")
        else:
            logger.warning(f"⚠️  Column '{column_name}' not found in DataFrame. Available columns: {list(df_coerced.columns)}")
        
        return df_coerced

    def _normalize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize column names for Snowflake compatibility.
        
        Rules:
        - Spaces -> underscores
        - Special characters -> underscores  
        - # -> number
        - Convert to UPPERCASE (all columns)
        - Handle reserved words (store with quotes)
        - Remove leading/trailing underscores
        
        Args:
            df: DataFrame with original column names
            
        Returns:
            pd.DataFrame: DataFrame with normalized column names
        """
        import re
        
        # Snowflake reserved words that should be uppercase
        reserved_words = {
            'date', 'dates', 'time', 'timestamp', 'year', 'month', 'day',
            'hour', 'minute', 'second', 'order', 'group', 'select', 'from',
            'where', 'and', 'or', 'not', 'null', 'true', 'false', 'case',
            'when', 'then', 'else', 'end', 'as', 'in', 'like', 'between',
            'is', 'exists', 'all', 'any', 'some', 'distinct', 'count',
            'sum', 'avg', 'min', 'max', 'first', 'last', 'limit', 'offset'
        }
        
        # Create a mapping of old names to new names
        column_mapping = {}
        new_columns = []
        
        for col in df.columns:
            original_name = col
            
            # Apply normalization rules
            normalized = col
            
            # Replace spaces with underscores
            normalized = normalized.replace(' ', '_')
            
            # Replace # with 'number'
            normalized = normalized.replace('#', 'number')
            
            # Replace special characters with underscores (except alphanumeric and underscore)
            normalized = re.sub(r'[^a-zA-Z0-9_]', '_', normalized)
            
            # Convert to lowercase
            normalized = normalized.lower()
            
            # Remove multiple consecutive underscores
            normalized = re.sub(r'_+', '_', normalized)
            
            # Remove leading and trailing underscores
            normalized = normalized.strip('_')
            
            # Ensure column name is not empty
            if not normalized:
                normalized = 'unnamed_column'
            
            # Convert all columns to UPPERCASE for consistency
            normalized = normalized.upper()
            
            # Handle reserved words - store with quotes
            if normalized in reserved_words:
                # Store the name with quotes for Snowflake compatibility
                final_name = f'"{normalized}"'
                logger.info(f"🔄 Reserved word detected: '{original_name}' -> '{final_name}' (quoted UPPERCASE)")
            else:
                # Ensure uniqueness (add number suffix if duplicate)
                counter = 1
                final_name = normalized
                while final_name in new_columns:
                    final_name = f"{normalized}_{counter}"
                    counter += 1
            
            column_mapping[original_name] = final_name
            new_columns.append(final_name)
            
            # Log the transformation if it changed (but not for reserved words already logged)
            if original_name != final_name and normalized not in reserved_words:
                logger.info(f"🔄 Column normalized: '{original_name}' -> '{final_name}'")
        
        # Rename columns in the DataFrame
        df_normalized = df.rename(column_mapping)
        
        logger.info(f"✅ Columns normalized: {len(column_mapping)}")
        
        return df_normalized

    def _calculate_xsmall_optimized_batch_size(self, df: pd.DataFrame) -> int:
        """
        Calculate optimal batch size for X-Small warehouse based on dataset size.
        Simple, deterministic, and optimized for 128 MB memory limit.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            int: Optimal batch size in rows
        """
        total_rows = len(df)
        
        if total_rows < 50_000:
            batch_size = 25_000
            reason = "Estabilidad máxima para datasets pequeños"
        elif total_rows < 200_000:
            batch_size = 50_000
            reason = "Balance performance/riesgo para datasets medianos"
        elif total_rows < 1_000_000:
            batch_size = 75_000
            reason = "Performance optimizada para datasets grandes"
        else:
            batch_size = 100_000
            reason = "Límite máximo para X-Small warehouse"
        
        logger.info(f"📊 Batch size: {batch_size:,} rows")
        
        return batch_size

    def _generate_create_table_sql(self, df: pd.DataFrame, table_name: str) -> str:
        """
        Generate CREATE TABLE SQL based on DataFrame schema.
        
        Args:
            df: DataFrame to analyze
            table_name: Table name
            
        Returns:
            str: CREATE TABLE SQL statement
        """
        columns = []
        
        for col_name, dtype in df.dtypes.items():
            # Map pandas dtypes to Snowflake types
            dtype_str = str(dtype)
            if 'int' in dtype_str:
                sf_type = "INTEGER"
            elif 'float' in dtype_str:
                sf_type = "FLOAT"
            elif dtype_str == 'bool':
                sf_type = "BOOLEAN"
            elif 'datetime' in dtype_str:
                sf_type = "TIMESTAMP"
            else:
                sf_type = "VARCHAR(16777216)"  # Snowflake max varchar size
            
            # Handle column names for CREATE TABLE
            # Columns that are already quoted (reserved words) don't need additional escaping
            if col_name.startswith('"') and col_name.endswith('"'):
                # Already quoted (reserved word), use as is
                escaped_col_name = col_name
            else:
                # Regular column, add quotes
                escaped_col_name = f'"{col_name}"'
            
            columns.append(f"{escaped_col_name} {sf_type}")
        
        columns_sql = ', '.join(columns)
        return f"CREATE TABLE {table_name.upper()} ({columns_sql})"
    
    def verify_upload(self, table_name: str, expected_rows: int) -> bool:
        """
        Verify the upload by checking row count in Snowflake.
        
        Args:
            table_name: Table name to verify
            expected_rows: Expected number of rows
            
        Returns:
            bool: True if verification successful, False otherwise
        """
        if self.conn is None:
            logger.error("Snowflake connection not established")
            return False
        
        try:
            cursor = self.conn.cursor()
            # Escape table name to handle special characters
            escaped_table_name = f'"{table_name.upper()}"'
            cursor.execute(f"SELECT COUNT(*) FROM {escaped_table_name}")
            actual_rows = cursor.fetchone()[0]
            cursor.close()
            
            logger.info(f"Verification: Expected {expected_rows} rows, found {actual_rows} rows")
            
            if actual_rows == expected_rows:
                logger.info("✅ Upload verification successful")
                return True
            else:
                logger.warning(f"⚠️  Row count mismatch: expected {expected_rows}, got {actual_rows}")
                return False
                
        except Exception as e:
            logger.error(f"Error verifying upload: {e}")
            return False
    
    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        """
        Executes a SQL query and returns the result as a pandas DataFrame.

        Args:
            query (str): The SQL query to execute.

        Returns:
            Optional[pd.DataFrame]: A pandas DataFrame with the query results,
                                    or None if the query fails.
        """
        if not self.conn:
            logger.error("❌ No active Snowflake connection.")
            return None
        
        try:
            logger.info(f"Executing query: {query[:100]}...")
            cursor = self.conn.cursor()
            cursor.execute(query)
            
            # Fetch results directly into pandas DataFrame
            pandas_df = cursor.fetch_pandas_all()

            if pandas_df is not None and not pandas_df.empty:
                logger.info(f"✅ Query returned {len(pandas_df)} rows.")
                return pandas_df
            else:
                logger.info("ℹ️ Query executed successfully, but returned no rows.")
                return pd.DataFrame() # Return empty DataFrame for consistency

        except Exception as e:
            logger.error(f"❌ Failed to execute query: {e}")
            return None
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()

    def get_table_columns(self, database: str, schema: str, table_name: str, role: str = "DBT_ROLE", warehouse: str = None) -> list[dict]:
        """
        Get all column names and data types from a specific table in Snowflake.
        
        Args:
            database: Database name
            schema: Schema name  
            table_name: Table name
            role: Snowflake role to use (default: "DBT_ROLE")
            warehouse: Warehouse to use (if None, uses environment variable)
            
        Returns:
            list[dict]: List of column info dicts with 'name' and 'data_type' keys, empty list if error or table not found
        """
        if not self.conn:
            logger.error("❌ No active Snowflake connection.")
            return []
        
        try:
            cursor = self.conn.cursor()
            
            # Set the role first
            cursor.execute(f"USE ROLE {role}")
            logger.debug(f"Set role to: {role}")
            
            # Ensure warehouse is active before running queries
            warehouse_to_use = warehouse or os.getenv("SNOWFLAKE_WAREHOUSE")
            if warehouse_to_use:
                cursor.execute(f"USE WAREHOUSE {warehouse_to_use}")
                logger.debug(f"Activated warehouse: {warehouse_to_use}")
            
            # Query to get column information from INFORMATION_SCHEMA
            query = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM {database}.INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{schema.upper()}' 
            AND TABLE_NAME = '{table_name.upper()}'
            ORDER BY ORDINAL_POSITION
            """
            
            logger.info(f"Getting columns for table: {database}.{schema}.{table_name} using role: {role}")
            cursor.execute(query)
            
            # Fetch all column names and data types
            results = cursor.fetchall()
            columns = [{"name": row[0], "data_type": row[1]} for row in results]
            
            cursor.close()
            
            if columns:
                logger.info(f"✅ Found {len(columns)} columns in {table_name}")
                logger.debug(f"Columns with types: {columns}")
            else:
                logger.warning(f"⚠️  No columns found for table {database}.{schema}.{table_name}")
                logger.warning("   Table may not exist or you may not have permissions")
            
            return columns
            
        except Exception as e:
            logger.error(f"❌ Failed to get columns for table {table_name}: {e}")
            return []

    def cleanup(self):
        """Close Snowflake connection."""
        if self.conn:
            self.conn.close()
            logger.info("Snowflake connection closed") 