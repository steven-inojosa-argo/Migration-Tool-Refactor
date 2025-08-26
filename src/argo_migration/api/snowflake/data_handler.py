"""Snowflake data handling module."""

import logging
import time
from typing import Optional, Dict, Any
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class SnowflakeDataHandler:
    """Handles Snowflake data operations like upload and querying."""
    
    def __init__(self, connection):
        self.connection = connection
    
    def upload_data(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace', chunk_size: int = None) -> bool:
        """Upload DataFrame to Snowflake table."""
        if df.empty:
            logger.warning("⚠️ DataFrame is empty, skipping upload")
            return False
        
        try:
            logger.info(f"📤 Starting upload to {table_name}")
            logger.info(f"   Shape: {df.shape}")
            logger.info(f"   Mode: {if_exists}")
            
            # Clean the DataFrame
            cleaned_df = self._clean_dataframe_for_upload(df)
            
            # Determine chunk size
            effective_chunk_size = self._determine_chunk_size(cleaned_df, chunk_size)
            
            # Upload data
            if len(cleaned_df) <= effective_chunk_size:
                return self._upload_single_chunk(cleaned_df, table_name, if_exists)
            else:
                return self._upload_in_chunks(cleaned_df, table_name, if_exists, effective_chunk_size)
                
        except Exception as e:
            logger.error(f"❌ Upload failed: {e}")
            return False
    
    def _clean_dataframe_for_upload(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean DataFrame for Snowflake upload."""
        cleaned_df = df.copy()
        
        # Replace inf/-inf with NaN
        cleaned_df = cleaned_df.replace([np.inf, -np.inf], np.nan)
        
        # Clean string columns
        for col in cleaned_df.columns:
            if cleaned_df[col].dtype == 'object':
                # Handle None values and convert to string
                cleaned_df[col] = cleaned_df[col].astype(str)
                cleaned_df[col] = cleaned_df[col].replace('nan', '')
                cleaned_df[col] = cleaned_df[col].replace('None', '')
        
        logger.info(f"🧹 DataFrame cleaned for upload")
        return cleaned_df
    
    def _determine_chunk_size(self, df: pd.DataFrame, chunk_size: int = None) -> int:
        """Determine optimal chunk size for upload."""
        if chunk_size:
            return chunk_size
        
        # Auto-determine based on DataFrame size
        total_rows = len(df)
        num_cols = len(df.columns)
        
        if total_rows <= 10000:
            return total_rows  # Upload all at once
        elif num_cols <= 10:
            return 50000  # Smaller chunks for many columns
        else:
            return 25000  # Conservative for wide tables
    
    def _upload_single_chunk(self, df: pd.DataFrame, table_name: str, if_exists: str) -> bool:
        """Upload DataFrame in a single operation."""
        try:
            logger.info(f"📤 Uploading {len(df)} rows to {table_name}")
            
            # Use pandas to_sql for single chunk
            df.to_sql(
                name=table_name.lower(),
                con=self.connection,
                if_exists=if_exists,
                index=False,
                method='multi',
                chunksize=1000
            )
            
            logger.info(f"✅ Upload completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Single chunk upload failed: {e}")
            return False
    
    def _upload_in_chunks(self, df: pd.DataFrame, table_name: str, if_exists: str, chunk_size: int) -> bool:
        """Upload DataFrame in multiple chunks."""
        try:
            total_rows = len(df)
            num_chunks = (total_rows + chunk_size - 1) // chunk_size
            
            logger.info(f"📤 Uploading {total_rows} rows in {num_chunks} chunks of {chunk_size}")
            
            for i, chunk_start in enumerate(range(0, total_rows, chunk_size)):
                chunk_end = min(chunk_start + chunk_size, total_rows)
                chunk_df = df.iloc[chunk_start:chunk_end].copy()
                
                # First chunk replaces/creates, subsequent chunks append
                chunk_if_exists = if_exists if i == 0 else 'append'
                
                logger.info(f"📦 Uploading chunk {i+1}/{num_chunks} (rows {chunk_start+1}-{chunk_end})")
                
                chunk_df.to_sql(
                    name=table_name.lower(),
                    con=self.connection,
                    if_exists=chunk_if_exists,
                    index=False,
                    method='multi',
                    chunksize=1000
                )
                
                # Brief pause between chunks
                if i < num_chunks - 1:
                    time.sleep(0.5)
            
            logger.info(f"✅ Chunked upload completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Chunked upload failed: {e}")
            return False
    
    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        """Execute SQL query and return results as DataFrame."""
        try:
            logger.info(f"🔍 Executing query...")
            
            cursor = self.connection.cursor()
            cursor.execute(query)
            
            # Get results
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            cursor.close()
            
            # Convert to DataFrame
            df = pd.DataFrame(results, columns=columns)
            logger.info(f"✅ Query executed successfully, returned {len(df)} rows")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Query execution failed: {e}")
            return None
    
    def verify_upload(self, table_name: str, expected_rows: int) -> bool:
        """Verify that data was uploaded correctly."""
        try:
            logger.info(f"🔍 Verifying upload for table {table_name}")
            
            # Count rows in table
            count_query = f"SELECT COUNT(*) as row_count FROM {table_name}"
            result_df = self.execute_query(count_query)
            
            if result_df is None or result_df.empty:
                logger.error("❌ Could not get row count from table")
                return False
            
            actual_rows = result_df.iloc[0]['row_count']
            
            if actual_rows == expected_rows:
                logger.info(f"✅ Upload verified: {actual_rows} rows in table")
                return True
            else:
                logger.error(f"❌ Row count mismatch: expected {expected_rows}, got {actual_rows}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Upload verification failed: {e}")
            return False
    
    def get_table_columns(self, database: str, schema: str, table_name: str, role: str = "DBT_ROLE", warehouse: str = None) -> list[dict]:
        """Get column information for a table."""
        try:
            logger.info(f"📋 Getting columns for {database}.{schema}.{table_name}")
            
            # Set role and warehouse if specified
            if role:
                self.connection.cursor().execute(f"USE ROLE {role}")
            if warehouse:
                self.connection.cursor().execute(f"USE WAREHOUSE {warehouse}")
            
            # Query to get column information
            query = f"""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_CATALOG = '{database.upper()}'
              AND TABLE_SCHEMA = '{schema.upper()}'
              AND TABLE_NAME = '{table_name.upper()}'
            ORDER BY ORDINAL_POSITION
            """
            
            result_df = self.execute_query(query)
            
            if result_df is None or result_df.empty:
                logger.warning(f"⚠️ No columns found for table {table_name}")
                return []
            
            # Convert to list of dictionaries
            columns = []
            for _, row in result_df.iterrows():
                columns.append({
                    'name': row['COLUMN_NAME'],
                    'type': row['DATA_TYPE'],
                    'nullable': row['IS_NULLABLE'] == 'YES',
                    'default': row['COLUMN_DEFAULT'],
                    'position': row['ORDINAL_POSITION']
                })
            
            logger.info(f"✅ Found {len(columns)} columns")
            return columns
            
        except Exception as e:
            logger.error(f"❌ Error getting table columns: {e}")
            return []
