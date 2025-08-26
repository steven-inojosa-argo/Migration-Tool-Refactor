"""
Smart sampling utilities for efficient data comparison.

This module provides intelligent sampling strategies including statistical sample size
calculation and smart random sampling with deterministic results.
"""

import logging
import math
import random
from typing import List, Tuple
import pandas as pd

from .query_builder import escape_domo_column_list, normalize_snowflake_column_list
from .batch_processor import BatchProcessor
from ....api.domo import DomoHandler
from ....api.snowflake import SnowflakeHandler


class SmartSampler:
    """Intelligent sampling for large datasets with statistical accuracy."""
    
    def __init__(self, domo_handler: DomoHandler, snowflake_handler: SnowflakeHandler):
        """
        Initialize smart sampler.
        
        Args:
            domo_handler: Initialized Domo handler
            snowflake_handler: Initialized Snowflake handler
        """
        self.domo_handler = domo_handler
        self.snowflake_handler = snowflake_handler
        self.logger = logging.getLogger("SmartSampler")
        self.batch_processor = BatchProcessor(domo_handler, snowflake_handler)
    
    def calculate_sample_size(self, total_rows: int, confidence_level: float = 0.95, 
                            margin_of_error: float = 0.05) -> int:
        """
        Calculate statistically significant sample size.
        
        Args:
            total_rows: Total number of rows in the dataset
            confidence_level: Statistical confidence level (default 95%)
            margin_of_error: Acceptable margin of error (default 5%)
        
        Returns:
            Recommended sample size
        """
        if total_rows <= 1000:
            return min(total_rows, 1000)
        
        # Statistical formula for finite population
        z_score = 1.96  # For 95% confidence
        p = 0.5  # Most conservative proportion
        
        numerator = (z_score ** 2) * p * (1 - p)
        denominator = (margin_of_error ** 2)
        
        sample_size = numerator / denominator
        sample_size = sample_size / (1 + (sample_size - 1) / total_rows)
        
        return min(int(math.ceil(sample_size)), total_rows)
    
    def get_smart_random_samples(self, domo_dataset_id: str, snowflake_table: str, 
                               key_columns: List[str], sample_size: int, 
                               domo_column_mapping: dict = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Obtiene muestras aleatorias sincronizadas usando random determinístico en Python.
        
        Esta es la "solución ganadora": rápida, garantiza mismas filas, y reproducible.
        
        Args:
            domo_dataset_id: ID del dataset de Domo
            snowflake_table: Nombre de la tabla en Snowflake
            key_columns: Lista de columnas que actúan como keys
            sample_size: Número de filas a muestrear
            domo_column_mapping: Mapping from normalized to original Domo column names
            
        Returns:
            Tuple[domo_df, snowflake_df] con exactamente las mismas filas
        """
        self.logger.info("🎲 Using smart random sampling (Python-controlled)")
        
        # Store the mapping for batch processor
        self.batch_processor.set_domo_column_mapping(domo_column_mapping or {})
        
        # Paso 1: Obtener TODAS las combinaciones únicas (UNA sola query rápida)
        all_keys_df = self._get_all_unique_keys(domo_dataset_id, key_columns, domo_column_mapping)
        
        if len(all_keys_df) <= sample_size:
            # Si hay pocas keys únicas, usar todas
            self.logger.info(f"📊 Only {len(all_keys_df)} unique keys available, using all")
            sampled_keys_df = all_keys_df
        else:
            # Paso 2: Python hace el random sampling (DETERMINÍSTICO)
            self.logger.info(f"🎲 Randomly sampling {sample_size} from {len(all_keys_df)} unique keys...")
            
            # Seed fijo para reproducibilidad (puedes cambiarlo por datetime si quieres variedad)
            random.seed(42)
            
            # Random sampling en Python (muy rápido)
            sampled_indices = random.sample(range(len(all_keys_df)), sample_size)
            sampled_keys_df = all_keys_df.iloc[sampled_indices].reset_index(drop=True)
        
        self.logger.info(f"✅ Selected {len(sampled_keys_df)} key combinations for sampling")
        
        # Paso 3: Usar batch processor para obtener los datos
        domo_df, sf_df = self.batch_processor.process_batches(
            domo_dataset_id, snowflake_table, key_columns, sampled_keys_df
        )
        
        # Validar resultados finales
        self.logger.info(f"✅ Smart random sampling completed:")
        self.logger.info(f"   📊 Domo: {len(domo_df)} rows, {len(domo_df.columns)} columns") 
        self.logger.info(f"   📊 Snowflake: {len(sf_df)} rows, {len(sf_df.columns)} columns")
        
        # Advertir si hay discrepancias significativas en el número de filas
        expected_rows = len(sampled_keys_df)
        if len(domo_df) < expected_rows * 0.8:
            self.logger.warning(f"⚠️ Domo returned fewer rows than expected: {len(domo_df)} vs {expected_rows}")
        if len(sf_df) < expected_rows * 0.8:
            self.logger.warning(f"⚠️ Snowflake returned fewer rows than expected: {len(sf_df)} vs {expected_rows}")
        
        return domo_df, sf_df
    
    def get_ordered_samples(self, domo_dataset_id: str, snowflake_table: str,
                          key_columns: List[str], sample_size: int,
                          domo_column_mapping: dict = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Get ordered samples from both Domo and Snowflake.
        
        Args:
            domo_dataset_id: Domo dataset ID
            snowflake_table: Snowflake table name
            key_columns: Key columns for ordering
            sample_size: Number of rows to sample
            domo_column_mapping: Mapping from normalized to original Domo column names
            
        Returns:
            Tuple of (domo_df, snowflake_df)
        """
        self.logger.info("🎯 Using direct ordered sampling")
        
        # Map key columns for Domo query
        domo_key_columns = key_columns
        if domo_column_mapping:
            domo_key_columns = []
            for normalized_key in key_columns:
                if normalized_key in domo_column_mapping:
                    domo_key_columns.append(domo_column_mapping[normalized_key])
                else:
                    domo_key_columns.append(normalized_key)
        
        # Get Domo sample using ordered method
        key_cols_str = escape_domo_column_list(domo_key_columns)
        sample_query = f"SELECT * FROM table ORDER BY {key_cols_str} LIMIT {sample_size}"
        
        domo_df = self.domo_handler.extract_data(
            dataset_id=domo_dataset_id, 
            query=sample_query,
            chunk_size=999999999,  # Force single chunk to avoid pagination issues
            auto_convert_types=True
        )
        
        if domo_df is None or domo_df.empty:
            raise Exception("No data returned from Domo")
        
        # Get Snowflake sample using ordered method  
        sf_key_cols_str = normalize_snowflake_column_list(key_columns)
        sf_query = f"SELECT * FROM {snowflake_table} ORDER BY {sf_key_cols_str} LIMIT {sample_size}"
        sf_df = self.snowflake_handler.execute_query(sf_query)
        
        if sf_df is None or sf_df.empty:
            raise Exception("No data returned from Snowflake")
        
        self.logger.info("✅ Ordered sampling completed")
        return domo_df, sf_df
    
    def _get_all_unique_keys(self, domo_dataset_id: str, key_columns: List[str], 
                           domo_column_mapping: dict = None) -> pd.DataFrame:
        """
        Obtiene todas las combinaciones únicas de keys del dataset de Domo.
        
        Args:
            domo_dataset_id: ID del dataset de Domo
            key_columns: Lista de columnas que actúan como keys (normalizadas)
            domo_column_mapping: Mapping from normalized to original Domo column names
            
        Returns:
            DataFrame con todas las combinaciones únicas de keys
        """
        # Map normalized key columns back to original Domo names for queries
        domo_key_columns = []
        if domo_column_mapping:
            for normalized_key in key_columns:
                if normalized_key in domo_column_mapping:
                    domo_key_columns.append(domo_column_mapping[normalized_key])
                else:
                    # Fallback: assume the key column name is already in Domo format
                    domo_key_columns.append(normalized_key)
        else:
            # No mapping available, use as-is
            domo_key_columns = key_columns
        
        key_cols_str = escape_domo_column_list(domo_key_columns)
        all_keys_query = f"SELECT DISTINCT {key_cols_str} FROM table"
        
        self.logger.info(f"📋 Getting all unique key combinations for columns: {key_columns} → {domo_key_columns}")
        
        all_keys_df = self.domo_handler.extract_data(
            dataset_id=domo_dataset_id,
            query=all_keys_query,
            auto_convert_types=False  # Keep original types for exact matching
        )
        
        if all_keys_df is None or all_keys_df.empty:
            raise Exception("Could not retrieve unique keys from Domo dataset")
        
        self.logger.info(f"📊 Found {len(all_keys_df)} unique key combinations")
        return all_keys_df
