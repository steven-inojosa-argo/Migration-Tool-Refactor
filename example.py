#!/usr/bin/env python3
"""Example usage of the refactored Domo module."""

import sys
import os
import logging

# Add src to path for importing
sys.path.insert(0, 'src')

from argo_migration.api.domo import DomoHandler

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

def test_module_structure():
    """Test that the module structure is correct."""
    print("🔍 Testing module structure...")
    
    try:
        # Test import
        handler = DomoHandler()
        print("✅ DomoHandler imported and instantiated successfully")
        
        # Test expected methods
        expected_methods = [
            'authenticate', 'extract_data', 'query_dataset', 
            'get_all_datasets', 'get_dataset_info', 'get_all_dataflows'
        ]
        
        for method in expected_methods:
            if hasattr(handler, method):
                print(f"✅ Method '{method}' available")
            else:
                print(f"❌ Method '{method}' missing")
                return False
        
        print(f"✅ All {len(expected_methods)} expected methods found")
        return True
        
    except Exception as e:
        print(f"❌ Module structure test failed: {e}")
        return False

def test_with_credentials():
    """Test with actual Domo credentials if available."""
    print("\n🔑 Testing with credentials...")
    
    # Check if credentials are available
    has_dev_token = os.getenv("DOMO_DEVELOPER_TOKEN") and os.getenv("DOMO_INSTANCE")
    has_client_creds = (os.getenv("DOMO_CLIENT_ID") and 
                       os.getenv("DOMO_CLIENT_SECRET") and 
                       os.getenv("DOMO_INSTANCE"))
    
    if not (has_dev_token or has_client_creds):
        print("⚠️  No Domo credentials found in environment")
        print("   Set one of these combinations:")
        print("   1. DOMO_DEVELOPER_TOKEN + DOMO_INSTANCE")
        print("   2. DOMO_CLIENT_ID + DOMO_CLIENT_SECRET + DOMO_INSTANCE")
        return False
    
    try:
        handler = DomoHandler()
        handler.authenticate()
        
        if handler.is_authenticated:
            print("✅ Authentication successful!")
            
            # Test getting datasets
            try:
                print("📊 Testing dataset retrieval...")
                datasets = handler.get_all_datasets(batch_size=5)
                print(f"✅ Retrieved {len(datasets)} datasets")
                
                if datasets:
                    sample_dataset = datasets[0]
                    print(f"   Sample dataset: {sample_dataset.get('name', 'N/A')}")
                    
            except Exception as e:
                print(f"⚠️  Dataset retrieval failed: {e}")
            
            return True
        else:
            print("❌ Authentication failed")
            return False
            
    except Exception as e:
        print(f"❌ Authentication error: {e}")
        return False

def show_usage_example():
    """Show usage example."""
    print("\n📝 Usage Example:")
    print("""
# Basic usage
from argo_migration.api.domo import DomoHandler

# Initialize and authenticate
domo = DomoHandler()
domo.authenticate()  # Requires environment variables

# Extract data from a dataset
df = domo.extract_data("your_dataset_id")

# Get all datasets
datasets = domo.get_all_datasets()

# Execute custom SQL query
result = domo.query_dataset("dataset_id", "SELECT COUNT(*) FROM table")

# Get dataflow lineage
dataflows = domo.get_all_dataflows(["dataset1", "dataset2"])
""")

def main():
    """Main execution function."""
    print("🚀 Domo Module Example & Test\n")
    
    # Test module structure (doesn't require credentials)
    structure_ok = test_module_structure()
    
    if not structure_ok:
        print("\n❌ Module structure has issues. Please check imports.")
        return 1
    
    # Test with credentials if available
    credentials_ok = test_with_credentials()
    
    # Show usage regardless
    show_usage_example()
    
    # Summary
    print("\n📊 Summary:")
    if structure_ok and credentials_ok:
        print("🎉 Module is fully functional with credentials!")
    elif structure_ok:
        print("✅ Module structure is correct. Configure credentials to test full functionality.")
    else:
        print("❌ Module has structural issues.")
    
    print("\n🔧 To set up credentials:")
    print("export DOMO_DEVELOPER_TOKEN='your_token'")
    print("export DOMO_INSTANCE='your_instance'")
    
    return 0

if __name__ == "__main__":
    exit(main())
