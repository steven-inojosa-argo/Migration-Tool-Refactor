"""Table utilities for Domo to Snowflake migration."""

import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def sanitize_table_name(dataset_id: str, dataset_name: str = None, use_prefix: bool = None) -> str:
    """
    Create a Snowflake-compatible table name from dataset ID and name.
    
    Args:
        dataset_id: The Domo dataset ID
        dataset_name: The dataset name (optional)
        use_prefix: Whether to use 'raw_' prefix (defaults to True)
        
    Returns:
        str: Snowflake-compatible table name
    """
    # Default to using prefix if not specified
    if use_prefix is None:
        use_prefix = True
    
    # If dataset_name is provided and valid, use it as base
    if dataset_name and isinstance(dataset_name, str) and dataset_name.strip():
        base_name = dataset_name.strip()
    else:
        # Use dataset_id as fallback
        base_name = dataset_id
    
    # Clean the base name
    # 1. Convert to lowercase
    clean_name = base_name.lower()
    
    # 2. Replace common separators with underscores
    clean_name = re.sub(r'[-\s\.]+', '_', clean_name)
    
    # 3. Remove non-alphanumeric characters (except underscores)
    clean_name = re.sub(r'[^a-z0-9_]', '', clean_name)
    
    # 4. Remove consecutive underscores
    clean_name = re.sub(r'_+', '_', clean_name)
    
    # 5. Remove leading/trailing underscores
    clean_name = clean_name.strip('_')
    
    # 6. Ensure it doesn't start with a number (Snowflake requirement)
    if clean_name and clean_name[0].isdigit():
        clean_name = 't_' + clean_name
    
    # 7. Handle empty names
    if not clean_name:
        clean_name = f"dataset_{dataset_id.lower()}"
    
    # 8. Truncate if too long (Snowflake limit is 255 chars, but keep reasonable)
    max_length = 100  # Leave room for prefix
    if len(clean_name) > max_length:
        clean_name = clean_name[:max_length].rstrip('_')
    
    # 9. Add prefix if requested
    if use_prefix:
        table_name = f"raw_{clean_name}"
    else:
        table_name = clean_name
    
    logger.debug(f"Sanitized table name: '{base_name}' -> '{table_name}'")
    return table_name


def validate_table_name(table_name: str) -> bool:
    """
    Validate that a table name is compatible with Snowflake.
    
    Args:
        table_name: The table name to validate
        
    Returns:
        bool: True if valid, False otherwise
    """
    if not table_name:
        return False
    
    # Check length
    if len(table_name) > 255:
        return False
    
    # Check starts with letter or underscore
    if not (table_name[0].isalpha() or table_name[0] == '_'):
        return False
    
    # Check contains only valid characters
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
        return False
    
    # Check for reserved words (basic check)
    reserved_words = {
        'table', 'select', 'insert', 'update', 'delete', 'create', 'drop',
        'alter', 'index', 'view', 'database', 'schema', 'user', 'role'
    }
    
    if table_name.lower() in reserved_words:
        return False
    
    return True


def generate_table_name_variants(dataset_id: str, dataset_name: str = None) -> list[str]:
    """
    Generate multiple table name variants for fallback options.
    
    Args:
        dataset_id: The Domo dataset ID
        dataset_name: The dataset name (optional)
        
    Returns:
        list[str]: List of possible table names in order of preference
    """
    variants = []
    
    # Primary option with prefix
    primary = sanitize_table_name(dataset_id, dataset_name, use_prefix=True)
    variants.append(primary)
    
    # Without prefix
    no_prefix = sanitize_table_name(dataset_id, dataset_name, use_prefix=False)
    if no_prefix != primary:
        variants.append(no_prefix)
    
    # Using just dataset_id
    id_only = sanitize_table_name(dataset_id, None, use_prefix=True)
    if id_only not in variants:
        variants.append(id_only)
    
    # Fallback with timestamp if needed
    import datetime
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    fallback = f"raw_dataset_{timestamp}"
    variants.append(fallback)
    
    # Validate all variants
    valid_variants = [v for v in variants if validate_table_name(v)]
    
    return valid_variants if valid_variants else [f"raw_dataset_{dataset_id.lower()}"]
