# -*- coding: utf-8 -*-
# This script will serve as a test for the lipdomics metadata generation script.
from datetime import datetime
from src.metadata_generator import LCMSLipidomicsMetadataGenerator
from dotenv import load_dotenv

load_dotenv()
import os
import json

python_path = os.getenv("PYTHONPATH")
if python_path:
    os.environ["PYTHONPATH"] = python_path


def test_lipid_metadata_gen():
    current_directory = os.path.dirname(__file__)
    csv_file_path = os.path.join(
        current_directory, "test_data", "test_metadata_file_lipid.csv"
    )
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_lipid_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )
    # Start the metadata generation setup
    generator = LCMSLipidomicsMetadataGenerator(
        metadata_file=csv_file_path,
        database_dump_json_path=output_file,
        raw_data_url="https://example_raw_data_url/",
        process_data_url="https://example_processed_data_url/",
    )
    # Run the metadata generation process
    generator.run()
    assert os.path.exists(output_file)


def test_lipid_biosample_gen():
    current_directory = os.path.dirname(__file__)
    csv_file_path = os.path.join(
        current_directory, "test_data", "test_metadata_file_lipid_no_biosample_id.csv"
    )
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_lipid_no_biosample_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )
    # Start the metadata generation setup
    generator = LCMSLipidomicsMetadataGenerator(
        metadata_file=csv_file_path,
        database_dump_json_path=output_file,
        raw_data_url="https://example_raw_data_url/",
        process_data_url="https://example_processed_data_url/",
    )
    # Run the metadata generation process
    generator.run()
    assert os.path.exists(output_file)
    file = open(output_file, "r")
    working_data = json.load(file)
    file.close()
    assert len(working_data["biosample_set"]) == 4
