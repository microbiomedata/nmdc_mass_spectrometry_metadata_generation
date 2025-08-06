#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test passing in raw data urls in the metadata file functionality for metadata generation.
"""
import os
from datetime import datetime

python_path = os.getenv("PYTHONPATH")
if python_path:
    os.environ["PYTHONPATH"] = python_path


from nmdc_ms_metadata_gen.gcms_metab_metadata_generator import (
    GCMSMetabolomicsMetadataGenerator,
)


def test_workflow_metadata_creation_with_urls():
    """Test that workflow metadata creation handles URL fields correctly."""
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_gcms_raw_urls_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Create a GCMS generator instance
    generator = GCMSMetabolomicsMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_gcms_raw_urls.csv",
        database_dump_json_path=output_file,
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/metabolomics/test_data/test_processed_gcms_metab/",
        minting_config_creds=None,
        configuration_file_name="emsl_gcms_corems_params.toml",
    )

    # Run the metadata generation
    generator.run()
    # Check if the output file was created
    assert os.path.exists(output_file)
