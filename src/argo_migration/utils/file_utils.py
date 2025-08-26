import os
import re
from pathlib import Path
import pandas as pd

def ensure_output_dir(file_path: str, create_dirs: bool = True) -> Path:
    """
    Ensure the output directory exists for a given file path.
    
    Args:
        file_path: Path to the file
        create_dirs: Whether to create directories if they don't exist
        
    Returns:
        Path object pointing to the file
    """
    path = Path(file_path)
    
    if create_dirs:
        path.parent.mkdir(parents=True, exist_ok=True)
    
    return path

def save_csv(df: pd.DataFrame, output_file: str) -> str:
    """
    Save a pandas DataFrame to a CSV file.
    
    Args:
        df: DataFrame to save
        output_file: Output file path
        
    Returns:
        Path to the saved file
    """
    # Ensure output directory exists
    output_path = ensure_output_dir(output_file, create_dirs=True)
    
    # Save the DataFrame
    df.to_csv(output_path, index=False)
    
    return str(output_path)


def ensure_directory_exists(directory: str) -> Path:
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        directory: Directory path to ensure exists
        
    Returns:
        Path object pointing to the directory
    """
    path = Path(directory)
    path.mkdir(parents=True, exist_ok=True)
    return path


def safe_filename(filename: str, max_length: int = 255) -> str:
    """
    Convert a string to a safe filename by removing/replacing invalid characters.
    
    Args:
        filename: Original filename string
        max_length: Maximum length for the filename
        
    Returns:
        Safe filename string
    """
    # Remove or replace invalid characters
    # Replace common problematic characters
    safe_name = re.sub(r'[<>:"/\\|?*]', '_', filename)
    
    # Replace multiple spaces/underscores with single underscore
    safe_name = re.sub(r'[_\s]+', '_', safe_name)
    
    # Remove leading/trailing underscores and dots
    safe_name = safe_name.strip('_.')
    
    # Ensure it's not empty
    if not safe_name:
        safe_name = "untitled"
    
    # Limit length
    if len(safe_name) > max_length:
        safe_name = safe_name[:max_length].rstrip('_.')
    
    return safe_name

