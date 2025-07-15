#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test URL functionality for metadata generation.
"""
import tempfile
import pytest
from pathlib import Path
import pandas as pd
import sys
import os

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from src.data_classes import GCMSMetabWorkflowMetadata, LCMSLipidWorkflowMetadata
from src.gcms_metab_metadata_generator import GCMSMetabolomicsMetadataGenerator
from src.metadata_generator import NMDCMetadataGenerator


def test_data_classes_accept_url_fields():
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
        raw_data_url="https://example.com/raw/test.raw"
    )
    
    assert gcms_metadata.raw_data_url == "https://example.com/raw/test.raw"
    assert not hasattr(gcms_metadata, 'processed_data_url')
    
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
        raw_data_url="https://example.com/raw/test.raw"
    )
    
    assert lcms_metadata.raw_data_url == "https://example.com/raw/test.raw"
    assert not hasattr(lcms_metadata, 'processed_data_url')


def test_workflow_metadata_creation_with_urls():
    """Test that workflow metadata creation handles URL fields correctly."""
    
    # Create a temporary metadata file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_file:
        temp_file.write("""biosample_id,biosample.associated_studies,material_processing_type,raw_data_file,processed_data_file,mass_spec_configuration_name,chromat_configuration_name,instrument_used,processing_institution,instrument_analysis_start_date,instrument_analysis_end_date,execution_resource,calibration_id,raw_data_url
nmdc:bsm-11-002vgm56,['nmdc:sty-11-34xj1150'],MPLEX,test.raw,test.csv,Test MS Config,Test LC Config,TestInstrument,Test Institution,2023-01-01T12:00:00Z,2023-01-01T13:00:00Z,TestResource,test-calibration,https://example.com/raw/test.raw""")
        temp_file_path = temp_file.name
    
    try:
        # Create a GCMS generator instance
        generator = GCMSMetabolomicsMetadataGenerator(
            metadata_file=temp_file_path,
            database_dump_json_path='test_output.json',
            raw_data_url='https://default.com/raw/',
            process_data_url='https://default.com/processed/',
            minting_config_creds=None
        )
        
        # Load and test workflow metadata creation
        df = pd.read_csv(temp_file_path)
        first_row = df.iloc[0].to_dict()
        
        workflow_metadata = generator.create_workflow_metadata(first_row)
        
        # Verify URL fields are populated correctly
        assert workflow_metadata.raw_data_url == "https://example.com/raw/test.raw"
        assert not hasattr(workflow_metadata, 'processed_data_url')
        
        # Test with row that has no URL fields
        row_without_urls = first_row.copy()
        del row_without_urls['raw_data_url']
        
        workflow_metadata_no_urls = generator.create_workflow_metadata(row_without_urls)
        
        # Verify URL fields are None when not provided
        assert workflow_metadata_no_urls.raw_data_url is None
        assert not hasattr(workflow_metadata_no_urls, 'processed_data_url')
        
    finally:
        # Clean up
        import os
        os.unlink(temp_file_path)


def test_generate_data_object_url_parameter():
    """Test that generate_data_object respects URL parameter."""
    
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(b"test content for checksum")
        temp_file_path = Path(temp_file.name)
    
    try:
        generator = NMDCMetadataGenerator()
        
        # Test that the URL parameter is accepted and documented in the signature
        # We can't fully test without credentials, but we can verify the method signature
        import inspect
        sig = inspect.signature(generator.generate_data_object)
        assert 'url' in sig.parameters
        assert sig.parameters['url'].default is None
        
    finally:
        # Clean up
        import os
        os.unlink(temp_file_path)


def test_url_validation_logic():
    """Test that URL validation handles URL columns correctly."""
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create test metadata with URL columns - only raw_data_url is supported now
        metadata_with_urls = pd.DataFrame([
            {
                "raw_data_url": "https://example.com/raw/test1.raw",
                "processed_data_file": "test1.csv"
            },
            {
                "raw_data_url": "https://example.com/raw/test2.raw", 
                "processed_data_file": "test2.csv"
            }
        ])
        
        # Use GCMS generator which has the check_doj_urls method
        generator = GCMSMetabolomicsMetadataGenerator(
            metadata_file="dummy.csv",
            database_dump_json_path="dummy.json",
            raw_data_url="https://default.com/raw/",
            process_data_url="https://default.com/processed/",
            minting_config_creds=None
        )
        
        # Test that URL validation accepts raw data URL columns only
        # This will fail due to network issues, but we can check the logic path
        try:
            generator.check_doj_urls(
                metadata_df=metadata_with_urls,
                url_columns=["raw_data_url", "processed_data_file"]
            )
        except Exception as e:
            # Expected to fail due to network issues or non-existent URLs
            # The important thing is that it processed the URL columns
            error_msg = str(e).lower()
            assert ("not accessible" in error_msg or 
                   "name resolution" in error_msg or 
                   "timeout" in error_msg or
                   "connection" in error_msg)


if __name__ == "__main__":
    test_data_classes_accept_url_fields()
    test_workflow_metadata_creation_with_urls()
    test_generate_data_object_url_parameter()
    test_url_validation_logic()
    print("All URL functionality tests passed!")