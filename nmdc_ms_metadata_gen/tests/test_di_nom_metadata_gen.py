# -*- coding: utf-8 -*-
# This script will serve as a test for the lipdomics metadata generation script.
import os
from datetime import datetime
from nmdc_ms_metadata_gen.di_nom_metadata_generator import DINOMMetaDataGenerator
from dotenv import load_dotenv
import json

load_dotenv()

python_path = os.getenv("PYTHONPATH")
if python_path:
    os.environ["PYTHONPATH"] = python_path


def test_di_nom_metadata_gen():
    """
    Test the DI NOM metadata generation script.
    Test case does not include generating a biosample
    """
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_nom_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = DINOMMetaDataGenerator(
        metadata_file="tests/test_data/test_metadata_file_nom.csv",
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
    exists = any(
        any("QC" in str(value) for value in d.values())
        for d in working_data["data_object_set"]
    )
    assert exists
    count = sum(
        1
        for d in working_data["data_object_set"]
        if any("QC" in str(value) for value in d.values())
    )
    assert count >= 5

    assert (
        "specifier"
        in working_data["data_generation_set"][0]["instrument_instance_specifier"]
    )


test_di_nom_metadata_gen()


def test_di_nom_metadata_gen_rerun():
    """
    Test the DI NOM metadata generation script.
    Test case does not include generating a biosample
    """
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_nom_rerun_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = DINOMMetaDataGenerator(
        metadata_file="tests/test_data/test_metadata_file_nom_rerun.csv",
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/blanchard/raw/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_processed_nom/",
    )

    # Run the metadata generation process
    generator.rerun()
    assert os.path.exists(output_file)

    file = open(output_file, "r")
    working_data = json.load(file)
    file.close()
    exists = any(
        any("QC" in str(value) for value in d.values())
        for d in working_data["data_object_set"]
    )
    assert exists
    count = sum(
        1
        for d in working_data["data_object_set"]
        if any("QC" in str(value) for value in d.values())
    )
    assert count >= 1


def test_di_nom_biosample_gen_more_fields():
    """
    Test the DI NOM metadata generation script.
    Test case includes generating a biosample with more than the required fields
    """
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_nom_no_biosample_weird_data_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = DINOMMetaDataGenerator(
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

    exists = any(
        any("QC" in str(value) for value in d.values())
        for d in working_data["data_object_set"]
    )
    assert exists
    count = sum(
        1
        for d in working_data["data_object_set"]
        if any("QC" in str(value) for value in d.values())
    )
    assert count >= 3


def test_di_nom_biosample_gen_no_biosample():
    """
    Test the DI NOM metadata generation script.
    Test case includes generating a biosample with no biosample id
    """
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_nom_no_biosample_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = DINOMMetaDataGenerator(
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

    exists = any(
        any("QC" in str(value) for value in d.values())
        for d in working_data["data_object_set"]
    )
    assert exists
    count = sum(
        1
        for d in working_data["data_object_set"]
        if any("QC" in str(value) for value in d.values())
    )
    assert count >= 2


def test_di_nom_config_file():
    """
    Test the DI NOM metadata generation script.
    Test purpose is to test the config file
    """
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_nom_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = DINOMMetaDataGenerator(
        metadata_file="tests/test_data/test_metadata_file_nom.csv",
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_raw_nom/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_processed_nom/",
        minting_config_creds="/Users/hess887/Projects/NMDC/nmdc_mass_spectrometry_metadata_generation/src/config.toml",
    )

    # Run the metadata generation process
    generator.run()
    assert os.path.exists(output_file)
    file = open(output_file, "r")
    working_data = json.load(file)
    file.close()

    exists = any(
        any("QC" in str(value) for value in d.values())
        for d in working_data["data_object_set"]
    )
    assert exists
    count = sum(
        1
        for d in working_data["data_object_set"]
        if any("QC" in str(value) for value in d.values())
    )
    assert count >= 5
