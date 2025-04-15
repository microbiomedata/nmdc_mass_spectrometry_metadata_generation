# -*- coding: utf-8 -*-
# This script will serve as a test for the lipdomics metadata generation script.
from datetime import datetime
from src.gcms_metab_metadata_generator import GCMSMetabolomicsMetadataGenerator
from dotenv import load_dotenv

load_dotenv()
import os
import json

python_path = os.getenv("PYTHONPATH")
if python_path:
    os.environ["PYTHONPATH"] = python_path


def test_gcms_metadata_gen():
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_gcms_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = GCMSMetabolomicsMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_gcms.csv",
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/metabolomics/test_data/test_raw_gcms_metab/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/metabolomics/test_data/test_processed_gcms_metab/",
    )

    # Run the metadata generation process
    generator.run()
    assert os.path.exists(output_file)


def test_gcms_biosample_gen():
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_gcms_no_biosample"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = GCMSMetabolomicsMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_gcms_no_biosample_id.csv",
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/metabolomics/test_data/test_raw_gcms_metab/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/metabolomics/test_data/test_processed_gcms_metab/",
        minting_config_creds="src/config.toml",
    )

    # Run the metadata generation process
    generator.run()
    assert os.path.exists(output_file)

    file = open(output_file, "r")
    working_data = json.load(file)
    file.close()
    assert len(working_data["biosample_set"]) == 1


def test_gcms_calibration_exists():
    """
    Test to handle the case where calibration IDs already exist in the metadata file.
    """
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_gcms_calibration_id"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = GCMSMetabolomicsMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_gcms_calibration_id.csv",
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/metabolomics/test_data/test_raw_gcms_metab/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/metabolomics/test_data/test_processed_gcms_metab/",
    )

    # Run the metadata generation process
    generator.run()
    assert os.path.exists(output_file)
