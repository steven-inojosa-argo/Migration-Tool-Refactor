"""Domo utilities for data processing."""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


def clean_dataframe(df: pd.DataFrame, auto_convert_types: bool = False) -> pd.DataFrame:
    """Clean DataFrame by removing empty rows and cleaning column names."""
    if df.empty:
        return df
    
    # Remove empty rows and clean column names
    df = df.dropna(how='all')
    df.columns = df.columns.str.strip()
    
    if auto_convert_types:
        df = _convert_types(df)
    
    logger.info(f"✅ Cleaned DataFrame: {len(df)} rows, {len(df.columns)} columns")
    return df


def _convert_types(df: pd.DataFrame) -> pd.DataFrame:
    """Convert column types automatically."""
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            continue
            
        # Try numeric conversion
        try:
            numeric_series = pd.to_numeric(df[col], errors='coerce')
            if numeric_series.notna().sum() / len(df[col].dropna()) > 0.8:
                df[col] = numeric_series
                continue
        except:
            pass
            
        # Try datetime conversion
        try:
            if df[col].dtype == 'object':
                date_series = pd.to_datetime(df[col], errors='coerce')
                if date_series.notna().sum() / len(df[col].dropna()) > 0.8:
                    df[col] = date_series
        except:
            pass
    
    return df
