#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test script to verify that workflow metadata creation works with URL columns.
"""

import pandas as pd
from src.gcms_metab_metadata_generator import GCMSMetabolomicsMetadataGenerator

def test_gcms_workflow_metadata_with_urls():
    """Test that GCMS workflow metadata creation handles URL fields correctly."""
    
    # Load test metadata with URL fields
    df = pd.read_csv('test_metadata_with_urls.csv')
    
    # Create a GCMS generator instance (without running it)
    generator = GCMSMetabolomicsMetadataGenerator(
        metadata_file='test_metadata_with_urls.csv',
        database_dump_json_path='test_output.json',
        raw_data_url='https://default.com/raw/',  # This should be overridden by URL columns
        process_data_url='https://default.com/processed/',  # This should be overridden by URL columns
        minting_config_creds=None
    )
    
    # Test workflow metadata creation for the first row
    first_row = df.iloc[0].to_dict()
    
    workflow_metadata = generator.create_workflow_metadata(first_row)
    
    # Verify URL fields are populated correctly
    assert workflow_metadata.raw_data_url == "https://example.com/raw/gcms_metab_sample_1.cdf"
    assert workflow_metadata.processed_data_url == "https://example.com/processed/gcms_metab_sample_1.csv"
    
    print("✓ GCMS workflow metadata creation handles URL fields correctly")
    print(f"  Raw data URL: {workflow_metadata.raw_data_url}")
    print(f"  Processed data URL: {workflow_metadata.processed_data_url}")
    
    # Test with row that has no URL fields
    row_without_urls = first_row.copy()
    del row_without_urls['raw_data_url']
    del row_without_urls['processed_data_url']
    
    workflow_metadata_no_urls = generator.create_workflow_metadata(row_without_urls)
    
    # Verify URL fields are None when not provided
    assert workflow_metadata_no_urls.raw_data_url is None
    assert workflow_metadata_no_urls.processed_data_url is None
    
    print("✓ GCMS workflow metadata handles missing URL fields correctly")
    print(f"  Raw data URL: {workflow_metadata_no_urls.raw_data_url}")
    print(f"  Processed data URL: {workflow_metadata_no_urls.processed_data_url}")

if __name__ == "__main__":
    print("Testing GCMS workflow metadata with URL fields...")
    test_gcms_workflow_metadata_with_urls()
    print("Test completed successfully!")