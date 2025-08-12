#!/usr/bin/env python3
"""
Unit tests for the generate_manifest function fix.

These tests verify that the generate_manifest function correctly handles the three scenarios:
1. No manifest_name or manifest_id; return without generating
2. No manifest_name column; return without generating  
3. Some rows in manifest_name column without corresponding manifest_id; generate manifest ids
"""

import pandas as pd
import nmdc_schema.nmdc as nmdc
from nmdc_ms_metadata_gen.gcms_metab_metadata_generator import GCMSMetabolomicsMetadataGenerator
from unittest.mock import Mock


def test_generate_manifest_early_return_conditions():
    """Test that generate_manifest returns early only when appropriate."""
    
    # Create a test generator instance
    generator = GCMSMetabolomicsMetadataGenerator(
        metadata_file="dummy.csv",
        database_dump_json_path="dummy.json", 
        raw_data_url="http://example.com/",
        process_data_url="http://example.com/",
        minting_config_creds=None,
        configuration_file_name="dummy_config.csv"
    )
    
    # Mock the ID pool to avoid network calls
    mock_id_pool = Mock()
    id_counter = 1000
    def mock_get_id(**kwargs):
        nonlocal id_counter
        id_counter += 1
        return f"nmdc:manifest-{id_counter:04d}"
    mock_id_pool.get_id.side_effect = mock_get_id
    generator.id_pool = mock_id_pool
    
    # Test Case 1: No manifest_name column - should return early
    df1 = pd.DataFrame({
        'biosample_id': ['nmdc:bsm-12345'],
        'some_other_column': ['value1']
    })
    nmdc_db1 = nmdc.Database()
    
    generator.generate_manifest(df1, nmdc_db1, "fake_client", "fake_secret")
    assert len(nmdc_db1.manifest_set or []) == 0, "Should not generate manifests when no manifest_name column"
    assert 'manifest_id' not in df1.columns, "Should not create manifest_id column"
    
    # Test Case 2: manifest_name column with all null values - should return early
    df2 = pd.DataFrame({
        'biosample_id': ['nmdc:bsm-12345'],
        'manifest_name': [None]
    })
    nmdc_db2 = nmdc.Database()
    
    generator.generate_manifest(df2, nmdc_db2, "fake_client", "fake_secret")
    assert len(nmdc_db2.manifest_set or []) == 0, "Should not generate manifests when all manifest_name values are null"
    assert 'manifest_id' not in df2.columns, "Should not create manifest_id column"
    
    # Test Case 3: manifest_name with values, no manifest_id column - should generate manifests
    df3 = pd.DataFrame({
        'biosample_id': ['nmdc:bsm-12345', 'nmdc:bsm-67890'],
        'manifest_name': ['test_manifest_1', 'test_manifest_2']
    })
    nmdc_db3 = nmdc.Database()
    
    generator.generate_manifest(df3, nmdc_db3, "fake_client", "fake_secret")
    assert len(nmdc_db3.manifest_set or []) == 2, "Should generate manifests when manifest_name has values"
    assert 'manifest_id' in df3.columns, "Should create manifest_id column"
    assert df3['manifest_id'].notna().all(), "All manifest_id values should be non-null"
    
    # Test Case 4: manifest_name with values, manifest_id column with null values - should generate manifests
    df4 = pd.DataFrame({
        'biosample_id': ['nmdc:bsm-12345', 'nmdc:bsm-67890'],
        'manifest_name': ['test_manifest_3', 'test_manifest_4'],
        'manifest_id': [None, None]
    })
    nmdc_db4 = nmdc.Database()
    
    generator.generate_manifest(df4, nmdc_db4, "fake_client", "fake_secret")
    assert len(nmdc_db4.manifest_set or []) == 2, "Should generate manifests when manifest_name has values"
    assert df4['manifest_id'].notna().all(), "All manifest_id values should be non-null"


if __name__ == "__main__":
    test_generate_manifest_early_return_conditions()
    print("✅ All tests passed!")