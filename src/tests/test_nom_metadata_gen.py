# -*- coding: utf-8 -*-
# This script will serve as a test for the lipdomics metadata generation script.
from datetime import datetime
from src.nom_metadata_generator import NOMMetadataGenerator
from dotenv import load_dotenv
import json
import pytest

load_dotenv()
import os

python_path = os.getenv("PYTHONPATH")
if python_path:
    os.environ["PYTHONPATH"] = python_path


def test_nom_metadata_gen():
    """
    Test the NOM metadata generation script.
    Test case does not include generating a biosample
    """
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
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_raw_nom/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_processed_nom/",
    )

    # Run the metadata generation process
    generator.run()
    assert os.path.exists(output_file)


def test_nom_biosample_gen_more_fields():
    """
    Test the NOM metadata generation script.
    Test case includes generating a biosample with more than the required fields
    """
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_nom_no_biosample_weird_data_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = NOMMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_nom_no_biosample_id_weird_data.csv",
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_raw_nom/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_processed_nom/",
    )

    # Run the metadata generation process
    generator.run()
    assert os.path.exists(output_file)

    file = open(output_file, "r")
    working_data = json.load(file)
    file.close()
    assert len(working_data["biosample_set"]) == 2


def test_nom_biosample_gen_no_biosample():
    """
    Test the NOM metadata generation script.
    Test case includes generating a biosample with no biosample id
    """
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
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_raw_nom/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_processed_nom/",
    )

    # Run the metadata generation process
    generator.run()
    assert os.path.exists(output_file)

    file = open(output_file, "r")
    working_data = json.load(file)
    file.close()
    assert len(working_data["biosample_set"]) == 2


def test_config_file():
    """
    Test the NOM metadata generation script.
    Test purpose is to test the config file
    """
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
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_raw_nom/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_processed_nom/",
        minting_config_creds="/Users/hess887/Projects/NMDC/nmdc_mass_spectrometry_metadata_generation/src/config.toml",
    )

    # Run the metadata generation process
    generator.run()
    assert os.path.exists(output_file)
