# -*- coding: utf-8 -*-
# This script will serve as a test for the lipdomics metadata generation script.
from datetime import datetime
from src.nom_metadata_generation import NOMMetadataGenerator
from dotenv import load_dotenv
import json
import pytest

load_dotenv()
import os

python_path = os.getenv("PYTHONPATH")
if python_path:
    os.environ["PYTHONPATH"] = python_path


def test_nom_metadata_gen():
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_nom_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = NOMMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_nom.csv",
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/",
        process_data_url="https://example_processed_data_url/",
    )

    # Run the metadata generation process
    generator.run()
    assert os.path.exists(output_file)


def test_nom_biosample_gen():
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_nom_no_biosample_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = NOMMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_nom_no_biosample_id.csv",
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/",
        process_data_url="https://example_processed_data_url/",
    )

    # Run the metadata generation process
    generator.run()
    assert os.path.exists(output_file)

    file = open(output_file, "r")
    working_data = json.load(file)
    file.close()
    assert len(working_data["biosample_set"]) == 1


@pytest.mark.skip(reason="Test relies on a specific file that may not be present")
def test_has_input():
    working = "tests/test_data/test_database_nom_20250219123935.json"
    testing = "tests/test_data/test_database_nom_20250220105753.json"

    file = open(working, "r")
    working_data = json.load(file)
    file.close()
    file = open(testing, "r")
    testing_data = json.load(file)
    file.close()
    for i in range(len(working_data["data_generation_set"])):
        assert (
            working_data["data_generation_set"][i]["has_input"]
            == testing_data["data_generation_set"][i]["has_input"]
        )


test_nom_biosample_gen()
