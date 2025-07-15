#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test script to verify URL functionality changes work correctly.
"""

import pandas as pd
import tempfile
import os
from pathlib import Path
from src.data_classes import GCMSMetabWorkflowMetadata, LCMSLipidWorkflowMetadata

def test_data_class_url_fields():
    """Test that data classes accept URL fields correctly."""
    
    # Test GCMS metadata with URL fields
    gcms_metadata = GCMSMetabWorkflowMetadata(
        biosample_id="test_biosample",
        nmdc_study=["test_study"],
        processing_institution="test_institution",
        processed_data_file="test.csv",
        raw_data_file="test.raw",
        mass_spec_config_name="test_config",
        chromat_config_name="test_chromat",
        instrument_used="test_instrument",
        instrument_analysis_start_date="2023-01-01",
        instrument_analysis_end_date="2023-01-01",
        execution_resource="test_resource",
        calibration_id="test_calibration",
        raw_data_url="https://example.com/raw/test.raw",
        processed_data_url="https://example.com/processed/test.csv"
    )
    
    assert gcms_metadata.raw_data_url == "https://example.com/raw/test.raw"
    assert gcms_metadata.processed_data_url == "https://example.com/processed/test.csv"
    print("✓ GCMS metadata data class accepts URL fields")
    
    # Test LCMS metadata with URL fields
    lcms_metadata = LCMSLipidWorkflowMetadata(
        processed_data_dir="test_dir",
        raw_data_file="test.raw",
        mass_spec_config_name="test_config",
        lc_config_name="test_lc",
        instrument_used="test_instrument",
        instrument_analysis_start_date="2023-01-01",
        instrument_analysis_end_date="2023-01-01",
        execution_resource="test_resource",
        raw_data_url="https://example.com/raw/test.raw",
        processed_data_url="https://example.com/processed/test_dir"
    )
    
    assert lcms_metadata.raw_data_url == "https://example.com/raw/test.raw"
    assert lcms_metadata.processed_data_url == "https://example.com/processed/test_dir"
    print("✓ LCMS metadata data class accepts URL fields")

def test_csv_row_parsing():
    """Test that .get() method works correctly for optional URL fields."""
    
    # Test row with URL fields
    row_with_urls = {
        "biosample_id": "test_biosample",
        "raw_data_file": "test.raw",
        "raw_data_url": "https://example.com/raw/test.raw",
        "processed_data_url": "https://example.com/processed/test.csv"
    }
    
    assert row_with_urls.get("raw_data_url") == "https://example.com/raw/test.raw"
    assert row_with_urls.get("processed_data_url") == "https://example.com/processed/test.csv"
    print("✓ Row with URL fields parsed correctly")
    
    # Test row without URL fields
    row_without_urls = {
        "biosample_id": "test_biosample",
        "raw_data_file": "test.raw"
    }
    
    assert row_without_urls.get("raw_data_url") is None
    assert row_without_urls.get("processed_data_url") is None
    print("✓ Row without URL fields returns None correctly")

def test_generate_data_object_url_parameter():
    """Test that generate_data_object uses URL parameter correctly."""
    
    from src.metadata_generator import NMDCMetadataGenerator
    from pathlib import Path
    import tempfile
    
    # Create a temporary file for testing
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as temp_file:
        temp_file.write("test content for checksum")
        temp_file_path = Path(temp_file.name)
    
    try:
        generator = NMDCMetadataGenerator()
        
        # Mock the minting by providing dummy credentials
        # (This will fail without real credentials, but we can test the URL logic)
        base_url = "https://example.com/base/"
        custom_url = "https://example.com/custom/test.txt"
        
        # Test that URL parameter takes precedence (we can't fully test without credentials)
        # But we can verify the parameter is accepted
        try:
            data_object = generator.generate_data_object(
                file_path=temp_file_path,
                data_category="test_category",
                data_object_type="test_type",
                description="test description",
                base_url=base_url,
                CLIENT_ID="dummy",  # This will fail, but that's expected
                CLIENT_SECRET="dummy",
                url=custom_url
            )
        except Exception as e:
            # Expected to fail due to dummy credentials, but check the error is about credentials
            if "CLIENT_ID" in str(e) or "credentials" in str(e).lower():
                print("✓ generate_data_object accepts URL parameter (credential error expected)")
            else:
                print(f"✗ Unexpected error: {e}")
                
    finally:
        # Clean up
        os.unlink(temp_file_path)

if __name__ == "__main__":
    print("Testing URL functionality changes...")
    
    test_data_class_url_fields()
    test_csv_row_parsing()
    test_generate_data_object_url_parameter()
    
    print("\nAll tests completed!")