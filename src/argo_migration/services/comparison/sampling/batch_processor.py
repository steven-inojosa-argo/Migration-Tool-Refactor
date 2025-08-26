"""
Batch processing utilities for handling large datasets.

This module provides efficient batch processing with fail-fast capabilities
for processing large data comparisons.
"""

import logging
from typing import List, Tuple
import pandas as pd

from .query_builder import QueryBuilder
from ....api.domo import DomoHandler
from ....api.snowflake import SnowflakeHandler
from ....utils.file_logger import get_file_logger


class BatchProcessor:
    """Process data in batches with fail-fast and error handling."""
    
    def __init__(self, domo_handler: DomoHandler, snowflake_handler: SnowflakeHandler):
        """
        Initialize batch processor.
        
        Args:
            domo_handler: Initialized Domo handler
            snowflake_handler: Initialized Snowflake handler
        """
        self.domo_handler = domo_handler
        self.snowflake_handler = snowflake_handler
        self.logger = logging.getLogger("BatchProcessor")
        self.file_logger = get_file_logger()
        self.query_builder = QueryBuilder()
        self._current_batch_num = 0
    
    def set_domo_column_mapping(self, domo_column_mapping: dict):
        """Set the column mapping for query building."""
        self.query_builder.domo_column_mapping = domo_column_mapping
    
    def process_batches(self, domo_dataset_id: str, snowflake_table: str, 
                       key_columns: List[str], sampled_keys_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Process data in batches with fail-fast logic.
        
        Args:
            domo_dataset_id: Domo dataset ID
            snowflake_table: Snowflake table name
            key_columns: Key columns for comparison
            sampled_keys_df: DataFrame with sampled key combinations
            
        Returns:
            Tuple of (combined_domo_df, combined_snowflake_df)
        """
        max_batch_size = 50
        domo_dfs = []
        sf_dfs = []
        
        if len(sampled_keys_df) > max_batch_size:
            self.logger.info(f"🔄 Using batching: {len(sampled_keys_df)} keys split into batches of {max_batch_size}")
            self.logger.info("⚡ Fail-fast mode enabled: will stop immediately if first batch or any critical batch fails")
            
            # Dividir en batches
            for i in range(0, len(sampled_keys_df), max_batch_size):
                batch_end = min(i + max_batch_size, len(sampled_keys_df))
                batch_keys_df = sampled_keys_df.iloc[i:batch_end].reset_index(drop=True)
                batch_num = i//max_batch_size + 1
                is_first_batch = (i == 0)
                
                self.logger.info(f"📦 Processing batch {batch_num}: {len(batch_keys_df)} keys (rows {i+1}-{batch_end})")
                
                # Set current batch number for error logging
                self._current_batch_num = batch_num
                
                try:
                    # Procesar este batch
                    domo_batch_df, sf_batch_df = self._get_batch_data(
                        domo_dataset_id, snowflake_table, key_columns, batch_keys_df
                    )
                    
                    # Verificar si el batch retornó datos válidos
                    has_domo_data = domo_batch_df is not None and not domo_batch_df.empty
                    has_sf_data = sf_batch_df is not None and not sf_batch_df.empty
                    
                    # Fail-fast: Si es el primer batch y falla, detener inmediatamente
                    if is_first_batch and (not has_domo_data or not has_sf_data):
                        if not has_domo_data and not has_sf_data:
                            error_msg = f"FIRST BATCH FAILURE - No data returned from either Domo or Snowflake - stopping immediately"
                            self.logger.error(f"❌ {error_msg}")
                            raise Exception(error_msg)
                        elif not has_domo_data:
                            error_msg = f"FIRST BATCH FAILURE - No data returned from Domo - stopping immediately"
                            self.logger.error(f"❌ {error_msg}")
                            raise Exception(error_msg)
                        elif not has_sf_data:
                            error_msg = f"FIRST BATCH FAILURE - No data returned from Snowflake - stopping immediately"
                            self.logger.error(f"❌ {error_msg}")
                            raise Exception(error_msg)
                    
                    # Para batches posteriores, también fallar si ambas fuentes están vacías
                    if not is_first_batch and not has_domo_data and not has_sf_data:
                        error_msg = f"Batch {batch_num} failed: No data from either source - stopping process"
                        self.logger.error(f"❌ {error_msg}")
                        raise Exception(error_msg)
                    
                    # Si llegamos aquí, el batch tiene datos válidos o es tolerable
                    if has_domo_data:
                        domo_dfs.append(domo_batch_df)
                    if has_sf_data:
                        sf_dfs.append(sf_batch_df)
                        
                except Exception as e:
                    # En cualquier excepción, detener el proceso inmediatamente
                    self.logger.error(f"❌ Batch {batch_num} failed - stopping entire process: {str(e)}")
                    raise e
            
            # Combinar todos los batches
            if domo_dfs and sf_dfs:
                domo_df = pd.concat(domo_dfs, ignore_index=True)
                sf_df = pd.concat(sf_dfs, ignore_index=True)
                self.logger.info(f"✅ Combined {len(domo_dfs)} batches: Domo {len(domo_df)} rows, Snowflake {len(sf_df)} rows")
            else:
                raise Exception("No data returned from any batch")
                
        else:
            # Sample size pequeño, usar método original
            self.logger.info(f"📦 Single batch processing: {len(sampled_keys_df)} keys")
            self.logger.info("⚡ Fail-fast mode enabled: will stop immediately if single batch fails")
            
            try:
                domo_df, sf_df = self._get_batch_data(
                    domo_dataset_id, snowflake_table, key_columns, sampled_keys_df
                )
                
                # Verificar que el batch único tenga datos válidos
                has_domo_data = domo_df is not None and not domo_df.empty
                has_sf_data = sf_df is not None and not sf_df.empty
                
                if not has_domo_data and not has_sf_data:
                    error_msg = "SINGLE BATCH FAILURE - No data returned from either Domo or Snowflake - stopping immediately"
                    self.logger.error(f"❌ {error_msg}")
                    raise Exception(error_msg)
                elif not has_domo_data:
                    error_msg = "SINGLE BATCH FAILURE - No data returned from Domo - stopping immediately"
                    self.logger.error(f"❌ {error_msg}")
                    raise Exception(error_msg)
                elif not has_sf_data:
                    error_msg = "SINGLE BATCH FAILURE - No data returned from Snowflake - stopping immediately"
                    self.logger.error(f"❌ {error_msg}")
                    raise Exception(error_msg)
                    
            except Exception as e:
                self.logger.error(f"❌ Single batch processing failed - stopping process: {str(e)}")
                raise e
        
        # Verificar que ambos DataFrames tienen datos
        if domo_df is None or domo_df.empty:
            raise Exception("No data returned from Domo with sampled keys")
        if sf_df is None or sf_df.empty:
            raise Exception("No data returned from Snowflake with sampled keys")
        
        return domo_df, sf_df
    
    def _get_batch_data(self, domo_dataset_id: str, snowflake_table: str, 
                       key_columns: List[str], batch_keys_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Obtiene datos para un batch específico de keys.
        
        Args:
            domo_dataset_id: ID del dataset de Domo
            snowflake_table: Nombre de la tabla en Snowflake
            key_columns: Lista de columnas que actúan como keys
            batch_keys_df: DataFrame con las combinaciones de keys para este batch
            
        Returns:
            Tuple[domo_df, snowflake_df] para este batch
        """
        # Construir WHERE clause para Domo (con escape)
        domo_where_clause = self.query_builder.build_efficient_where_clause(batch_keys_df, key_columns)
        
        # Construir WHERE clause para Snowflake (con nombres normalizados)
        sf_where_clause = self.query_builder.build_snowflake_where_clause(batch_keys_df, key_columns)
        
        # Ejecutar query en Domo
        self.logger.info(f"📥 Getting {len(batch_keys_df)} rows from Domo...")
        domo_query = f"SELECT * FROM table WHERE {domo_where_clause}"
        
        try:
            domo_df = self.domo_handler.extract_data(
                dataset_id=domo_dataset_id,
                query=domo_query,
                chunk_size=999999999,  # Force single chunk to avoid pagination issues with WHERE clauses
                auto_convert_types=True
            )
            
            if domo_df is None or domo_df.empty:
                self.logger.warning(f"⚠️ No data returned from Domo for this batch")
                domo_df = pd.DataFrame()
                
        except Exception as e:
            batch_num = getattr(self, '_current_batch_num', 0)
            error_msg = f"Error executing Domo query in batch {batch_num}: {str(e)}"
            self.logger.error(f"❌ {error_msg}")
            raise Exception(error_msg)
        
        # Ejecutar query en Snowflake
        self.logger.info(f"📥 Getting {len(batch_keys_df)} rows from Snowflake...")
        sf_query = f"SELECT * FROM {snowflake_table} WHERE {sf_where_clause}"
        
        try:
            sf_df = self.snowflake_handler.execute_query(sf_query)
            
            if sf_df is None or sf_df.empty:
                self.logger.warning(f"⚠️ No data returned from Snowflake for this batch")
                sf_df = pd.DataFrame()
                # Log batch failure to file
                batch_num = getattr(self, '_current_batch_num', 0)
                self.file_logger.log_batch_failure(
                    batch_num, len(domo_df), 0, 
                    "No data returned from Snowflake - possible query execution error"
                )
                
        except Exception as e:
            batch_num = getattr(self, '_current_batch_num', 0)
            error_msg = f"Error executing Snowflake query in batch {batch_num}: {str(e)}"
            self.logger.error(f"❌ {error_msg}")
            self.file_logger.log_batch_failure(batch_num, len(domo_df), 0, error_msg)
            raise Exception(error_msg)
        
        self.logger.info(f"✅ Batch completed: Domo {len(domo_df)} rows, Snowflake {len(sf_df)} rows")
        return domo_df, sf_df
