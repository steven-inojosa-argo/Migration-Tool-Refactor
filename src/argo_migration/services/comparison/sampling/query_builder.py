"""
SQL query building utilities for Domo and Snowflake comparisons.

This module provides functions to build efficient WHERE clauses, escape column names,
and optimize SQL queries for both Domo and Snowflake platforms.
"""

import re
from typing import List
import pandas as pd

from ....utils.common import transform_column_name


def escape_domo_column_name(column_name: str) -> str:
    """
    Escapa nombres de columnas para consultas SQL de Domo.
    
    Args:
        column_name: Nombre original de la columna
        
    Returns:
        Nombre de columna escapado para SQL de Domo
        
    Examples:
        >>> escape_domo_column_name("Site Code")
        '"Site Code"'
        >>> escape_domo_column_name("site_code")
        'site_code'
        >>> escape_domo_column_name("Total Revenue")
        '"Total Revenue"'
    """
    # Si el nombre tiene espacios o caracteres especiales, envolver en comillas dobles
    if re.search(r'[^a-zA-Z0-9_]', column_name):
        return f'"{column_name}"'
    return column_name


def escape_domo_column_list(column_names: List[str]) -> str:
    """
    Escapa una lista de nombres de columnas para consultas SQL de Domo.
    
    Args:
        column_names: Lista de nombres de columnas
        
    Returns:
        String con nombres de columnas escapados, separados por comas
        
    Examples:
        >>> escape_domo_column_list(["Site Code", "Total Revenue"])
        '"Site Code", "Total Revenue"'
    """
    escaped_columns = [escape_domo_column_name(col) for col in column_names]
    return ', '.join(escaped_columns)


def normalize_snowflake_column_list(column_names: List[str]) -> str:
    """
    Normaliza una lista de nombres de columnas para consultas SQL de Snowflake.
    
    Args:
        column_names: Lista de nombres de columnas
        
    Returns:
        String con nombres de columnas normalizados, separados por comas
        
    Examples:
        >>> normalize_snowflake_column_list(["Site Code", "Total Revenue"])
        'SITE_CODE, TOTAL_REVENUE'
    """
    normalized_columns = [transform_column_name(col) for col in column_names]
    return ', '.join(normalized_columns)


class QueryBuilder:
    """SQL query builder for efficient data sampling and comparison."""
    
    def __init__(self, domo_column_mapping: dict = None):
        """
        Initialize query builder.
        
        Args:
            domo_column_mapping: Mapping from normalized names to original Domo names
        """
        self.domo_column_mapping = domo_column_mapping or {}
    
    def build_efficient_where_clause(self, sampled_keys_df: pd.DataFrame, key_columns: List[str]) -> str:
        """
        Construye una cláusula WHERE eficiente y compatible con MySQL.
        
        Args:
            sampled_keys_df: DataFrame con las combinaciones de keys seleccionadas
            key_columns: Lista de columnas que actúan como keys (normalizadas)
            
        Returns:
            String con la cláusula WHERE optimizada y compatible con MySQL
        """
        if sampled_keys_df.empty:
            raise Exception("No sampled keys provided")
        
        # Map normalized key columns back to original Domo names for queries
        domo_key_columns = []
        for normalized_key in key_columns:
            if normalized_key in self.domo_column_mapping:
                domo_key_columns.append(self.domo_column_mapping[normalized_key])
            else:
                # Fallback: assume the key column name is already in Domo format
                domo_key_columns.append(normalized_key)
        
        if len(domo_key_columns) == 1:
            # Un solo key column: usar simple IN (muy eficiente)
            col = domo_key_columns[0]
            values = sampled_keys_df[key_columns[0]].dropna().tolist()  # Use normalized name for DataFrame access
            
            # Manejar diferentes tipos de datos
            if all(isinstance(v, str) for v in values):
                # String values: escapar comillas (no podemos usar backslash en f-strings)
                escaped_values = []
                for v in values:
                    escaped_val = str(v).replace("'", "''")  # Escapar comillas simples
                    escaped_values.append(f"'{escaped_val}'")
            else:
                # Numeric or other values
                escaped_values = [str(v) for v in values]
            
            values_str = ', '.join(escaped_values)
            escaped_col = escape_domo_column_name(col)
            where_clause = f"{escaped_col} IN ({values_str})"
            
        elif len(key_columns) == 2:
            # Dos key columns: usar ORs para máxima compatibilidad con MySQL
            # (IN con tuples puede fallar en algunas versiones de MySQL)
            where_clause = self.build_or_where_clause(sampled_keys_df, key_columns)
                
        else:
            # 3+ columns: usar approach de ORs (pero son pocas filas, así que está bien)
            where_clause = self.build_or_where_clause(sampled_keys_df, key_columns)
        
        return where_clause
    
    def build_or_where_clause(self, sampled_keys_df: pd.DataFrame, key_columns: List[str]) -> str:
        """
        Construye cláusula WHERE usando ORs para casos complejos (fallback).
        
        Args:
            sampled_keys_df: DataFrame con las combinaciones de keys seleccionadas  
            key_columns: Lista de columnas que actúan como keys (normalizadas)
            
        Returns:
            String con la cláusula WHERE usando ORs
        """
        # Map normalized key columns back to original Domo names for queries
        domo_key_columns = []
        for normalized_key in key_columns:
            if normalized_key in self.domo_column_mapping:
                domo_key_columns.append(self.domo_column_mapping[normalized_key])
            else:
                # Fallback: assume the key column name is already in Domo format
                domo_key_columns.append(normalized_key)
        
        where_conditions = []
        
        for _, row in sampled_keys_df.iterrows():
            row_conditions = []
            
            for i, col in enumerate(domo_key_columns):
                value = row[key_columns[i]]  # Use normalized name for DataFrame access
                escaped_col = escape_domo_column_name(col)
                
                # Manejar diferentes tipos de datos
                if pd.isna(value) or value is None:
                    row_conditions.append(f"{escaped_col} IS NULL")
                elif isinstance(value, str):
                    if value.strip() == '':
                        row_conditions.append(f"({escaped_col} = '' OR {escaped_col} IS NULL)")
                    else:
                        escaped_value = str(value).replace("'", "''")
                        row_conditions.append(f"{escaped_col} = '{escaped_value}'")
                else:
                    row_conditions.append(f"{escaped_col} = {value}")
            
            # Unir condiciones de esta fila con AND
            row_condition = "(" + " AND ".join(row_conditions) + ")"
            where_conditions.append(row_condition)
        
        # Unir todas las filas con OR
        return " OR ".join(where_conditions)
    
    def build_snowflake_where_clause(self, sampled_keys_df: pd.DataFrame, key_columns: List[str]) -> str:
        """
        Construye cláusula WHERE para Snowflake usando nombres de columnas normalizados.
        
        Args:
            sampled_keys_df: DataFrame con las combinaciones de keys seleccionadas
            key_columns: Lista de columnas que actúan como keys
            
        Returns:
            String con la cláusula WHERE optimizada para Snowflake
        """
        if sampled_keys_df.empty:
            raise Exception("No sampled keys provided")
        
        if len(key_columns) == 1:
            # Un solo key column: usar simple IN (muy eficiente)
            col = key_columns[0]
            normalized_col = transform_column_name(col)
            values = sampled_keys_df[col].dropna().tolist()
            
            # Manejar diferentes tipos de datos
            # Siempre tratar como strings para evitar problemas de conversión de tipos
            escaped_values = []
            for v in values:
                escaped_val = str(v).replace("'", "''")  # Escapar comillas simples
                escaped_values.append(f"'{escaped_val}'")
            
            values_str = ', '.join(escaped_values)
            where_clause = f"{normalized_col} IN ({values_str})"
            
        elif len(key_columns) == 2:
            # Dos key columns: usar ORs para máxima compatibilidad
            where_clause = self.build_snowflake_or_where_clause(sampled_keys_df, key_columns)
                
        else:
            # 3+ columns: usar approach de ORs
            where_clause = self.build_snowflake_or_where_clause(sampled_keys_df, key_columns)
        
        return where_clause
    
    def build_snowflake_or_where_clause(self, sampled_keys_df: pd.DataFrame, key_columns: List[str]) -> str:
        """
        Construye cláusula WHERE para Snowflake usando ORs para casos complejos.
        
        Args:
            sampled_keys_df: DataFrame con las combinaciones de keys seleccionadas  
            key_columns: Lista de columnas que actúan como keys
            
        Returns:
            String con la cláusula WHERE usando ORs para Snowflake
        """
        where_conditions = []
        
        for _, row in sampled_keys_df.iterrows():
            row_conditions = []
            
            for col in key_columns:
                value = row[col]
                normalized_col = transform_column_name(col)
                
                # Manejar diferentes tipos de datos
                if pd.isna(value) or value is None:
                    row_conditions.append(f"{normalized_col} IS NULL")
                else:
                    # Siempre tratar como string para evitar problemas de conversión de tipos
                    escaped_value = str(value).replace("'", "''")
                    if escaped_value.strip() == '':
                        row_conditions.append(f"({normalized_col} = '' OR {normalized_col} IS NULL)")
                    else:
                        row_conditions.append(f"{normalized_col} = '{escaped_value}'")
            
            # Unir condiciones de esta fila con AND
            row_condition = "(" + " AND ".join(row_conditions) + ")"
            where_conditions.append(row_condition)
        
        # Unir todas las filas con OR
        return " OR ".join(where_conditions)
