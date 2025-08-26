#!/usr/bin/env python3
"""Simple test for the refactored Domo module."""

import sys
import os
from pathlib import Path

# Add the src directory to Python path
current_dir = Path(__file__).parent
src_dir = current_dir.parent.parent.parent
sys.path.insert(0, str(src_dir))

print(f"🔍 Added to Python path: {src_dir}")

def test_imports():
    """Test that all modules import correctly."""
    try:
        from argo_migration.api.domo import DomoHandler
        from argo_migration.api.domo import DomoAuth, DomoDataExtractor, DomoDatasetManager, DomoLineageCrawler
        from argo_migration.api.domo import clean_dataframe
        print("✅ All imports successful")
        return True
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False

def test_instantiation():
    """Test that classes can be instantiated."""
    try:
        from argo_migration.api.domo import DomoHandler, DomoAuth, DomoLineageCrawler
        
        handler = DomoHandler()
        auth = DomoAuth()
        crawler = DomoLineageCrawler()
        
        print("✅ Class instantiation successful")
        print(f"   - DomoHandler authenticated: {handler.is_authenticated}")
        print(f"   - DomoAuth authenticated: {auth.is_authenticated}")
        
        return True
    except Exception as e:
        print(f"❌ Instantiation failed: {e}")
        return False

def test_utilities():
    """Test utility functions."""
    try:
        import pandas as pd
        from argo_migration.api.domo import clean_dataframe
        
        # Test DataFrame cleaning
        test_df = pd.DataFrame({
            'col1': [1, 2, None],
            'col2': ['a', 'b', 'c'],
            'col3': [None, None, None]
        })
        
        cleaned_df = clean_dataframe(test_df)
        print(f"✅ clean_dataframe works - shape: {cleaned_df.shape}")
        
        return True
    except Exception as e:
        print(f"❌ Utility test failed: {e}")
        return False

def test_api_structure():
    """Test that the API has the expected simple structure."""
    try:
        from argo_migration.api.domo import DomoHandler
        
        handler = DomoHandler()
        
        # Check that the simplified API methods exist
        expected_methods = [
            'authenticate', 'extract_data', 'query_dataset', 
            'get_all_datasets', 'get_dataset_info', 'get_all_dataflows'
        ]
        
        missing_methods = []
        for method in expected_methods:
            if not hasattr(handler, method):
                missing_methods.append(method)
        
        if missing_methods:
            print(f"❌ Missing methods: {missing_methods}")
            return False
        
        print(f"✅ API structure correct - {len(expected_methods)} methods found")
        return True
        
    except Exception as e:
        print(f"❌ API structure test failed: {e}")
        return False

def test_credentials_check():
    """Test credentials detection without requiring real credentials."""
    try:
        # Check if credentials are configured
        has_dev_token = os.getenv("DOMO_DEVELOPER_TOKEN") and os.getenv("DOMO_INSTANCE")
        has_client_creds = (os.getenv("DOMO_CLIENT_ID") and 
                           os.getenv("DOMO_CLIENT_SECRET") and 
                           os.getenv("DOMO_INSTANCE"))
        
        if has_dev_token:
            print("✅ Developer Token credentials detected")
        elif has_client_creds:
            print("✅ Client Credentials detected")
        else:
            print("ℹ️  No Domo credentials configured (this is normal for testing)")
        
        return True
        
    except Exception as e:
        print(f"❌ Credentials check failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🧪 Testing refactored Domo module\n")
    
    tests = [
        ("Imports", test_imports),
        ("Instantiation", test_instantiation),
        ("Utilities", test_utilities),
        ("API Structure", test_api_structure),
        ("Credentials Check", test_credentials_check)
    ]
    
    passed = 0
    for name, test_func in tests:
        print(f"🔄 Testing {name}...")
        if test_func():
            passed += 1
        print()
    
    print(f"📊 Results: {passed}/{len(tests)} tests passed")
    
    if passed == len(tests):
        print("🎉 All tests passed! Refactored module working correctly!")
        print("\n📝 Usage example:")
        print("from argo_migration.api.domo import DomoHandler")
        print("domo = DomoHandler()")
        print("domo.authenticate()  # Set environment variables first")
        print("df = domo.extract_data('dataset_id')")
        print("\n🔧 Environment variables needed:")
        print("export DOMO_DEVELOPER_TOKEN='your_token'")
        print("export DOMO_INSTANCE='your_instance'")
        print("# OR")
        print("export DOMO_CLIENT_ID='your_id'")
        print("export DOMO_CLIENT_SECRET='your_secret'")
        print("export DOMO_INSTANCE='your_instance'")
    else:
        print("❌ Some tests failed")
    
    return 0 if passed == len(tests) else 1

if __name__ == "__main__":
    exit(main())
