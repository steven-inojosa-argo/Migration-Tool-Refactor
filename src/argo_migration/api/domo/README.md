# Domo API Module - Refactored & Simplified

This module provides a clean, simplified interface for all Domo operations.

## Architecture Overview

```
src/argo_migration/api/domo/
├── __init__.py              # Main module interface
├── handler.py               # Simple main DomoHandler class
├── auth.py                  # Authentication management
├── data_extractor.py        # Data extraction operations
├── dataset_manager.py       # Dataset management operations
├── lineage_crawler.py       # Lineage crawling
├── utils.py                 # Data cleaning utilities
├── test_simple.py           # Test suite
└── README.md                # This documentation
```

## Simple Usage

```python
from argo_migration.api.domo import DomoHandler

# Initialize and authenticate
domo = DomoHandler()
domo.authenticate()

# Extract data
df = domo.extract_data("dataset_id")

# Get all datasets
datasets = domo.get_all_datasets()

# Get lineage information
dataflows = domo.get_all_dataflows(["dataset1", "dataset2"])

# Execute custom queries
result = domo.query_dataset("dataset_id", "SELECT COUNT(*) FROM table")
```

## Module Overview

### 🎯 `handler.py` - Main Interface
- **Class**: `DomoHandler`
- Simple, clean interface for all operations
- Handles authentication and component initialization

### 🔐 `auth.py` - Authentication
- **Class**: `DomoAuth`
- Developer Token and Client Credentials support
- Environment variable configuration

### 📥 `data_extractor.py` - Data Extraction
- **Class**: `DomoDataExtractor`
- Data extraction with automatic cleaning
- Custom SQL query support

### 🗂️ `dataset_manager.py` - Dataset Management
- **Class**: `DomoDatasetManager`
- Fetch datasets with pagination
- Dataset metadata retrieval

### 🔄 `lineage_crawler.py` - Lineage Tracking
- **Class**: `DomoLineageCrawler`
- Dataflow discovery and mapping
- Uses argo-domo CLI for lineage data

### 🧹 `utils.py` - Data Utilities
- DataFrame cleaning and preprocessing
- Optional automatic type conversion

## Environment Configuration

Set these environment variables for authentication:

```bash
# Method 1: Developer Token
export DOMO_DEVELOPER_TOKEN="your_token"
export DOMO_INSTANCE="your_instance"

# Method 2: Client Credentials  
export DOMO_CLIENT_ID="your_client_id"
export DOMO_CLIENT_SECRET="your_client_secret"
export DOMO_INSTANCE="your_instance"
```

## Testing

Run the test suite to verify everything works:

```bash
python3 src/argo_migration/api/domo/test_simple.py
```

This test works without requiring actual Domo credentials.

## Benefits

- **Simple**: Clean, straightforward API
- **Modular**: Each component has a single responsibility
- **Fast**: Minimal overhead, no backward compatibility cruft
- **Maintainable**: Easy to understand and modify
- **Testable**: Components can be tested independently

This refactored version provides a clean, modern interface for Domo operations while maintaining all essential functionality.
