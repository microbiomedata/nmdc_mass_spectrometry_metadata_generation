# -*- coding: utf-8 -*-
# This script will serve as a test for the lipdomics metadata generation script.
from datetime import datetime
from src.lcms_lipid_metadata_generator import LCMSLipidomicsMetadataGenerator
from dotenv import load_dotenv

load_dotenv()
import os
import json

python_path = os.getenv("PYTHONPATH")
if python_path:
    os.environ["PYTHONPATH"] = python_path


def test_lcms_lipid_metadata_gen():
    current_directory = os.path.dirname(__file__)
    csv_file_path = os.path.join(
        current_directory, "test_data", "test_metadata_file_lcms_lipid.csv"
    )
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_lcms_lipid_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )
    # Start the metadata generation setup
    generator = LCMSLipidomicsMetadataGenerator(
        metadata_file=csv_file_path,
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_raw_lcms_lipid/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_processed_lcms_lipid/",
        existing_data_objects=[
            "nmdc:dobj-11-00095294"
        ],  # random, existing data object for testing
    )
    # Run the metadata generation process
    generator.run()
    assert os.path.exists(output_file)


def test_lcms_lipid_metadata_gen_rerun():
    current_directory = os.path.dirname(__file__)
    csv_file_path = os.path.join(
        current_directory, "test_data", "test_metadata_file_lcms_lipid_rerun.csv"
    )
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_lcms_lipid_rerun_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )
    # Start the metadata generation setup
    generator = LCMSLipidomicsMetadataGenerator(
        metadata_file=csv_file_path,
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/lipidomics/blanchard_11_8ws97026/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_processed_lcms_lipid/",
    )
    # Run the metadata generation process
    generator.rerun()
    assert os.path.exists(output_file)


def test_lcms_lipid_biosample_gen():
    current_directory = os.path.dirname(__file__)
    csv_file_path = os.path.join(
        current_directory,
        "test_data",
        "test_metadata_file_lcms_lipid_no_biosample_id.csv",
    )
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_lcms_lipid_no_biosample_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )
    # Start the metadata generation setup
    generator = LCMSLipidomicsMetadataGenerator(
        metadata_file=csv_file_path,
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_raw_lcms_lipid/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_processed_lcms_lipid/",
    )
    # Run the metadata generation process
    generator.run()
    assert os.path.exists(output_file)
    file = open(output_file, "r")
    working_data = json.load(file)
    file.close()
    # expecting 1 since we only have 1 unique biosample name in the csv
    assert len(working_data["biosample_set"]) == 1
