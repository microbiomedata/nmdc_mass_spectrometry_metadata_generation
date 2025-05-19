# -*- coding: utf-8 -*-
# This script will serve as a test for the lipdomics metadata generation script.
from datetime import datetime
from src.lcms_nom_metadata_generator import LCMSNOMMetadataGenerator
from dotenv import load_dotenv
import json

load_dotenv()
import os

python_path = os.getenv("PYTHONPATH")
if python_path:
    os.environ["PYTHONPATH"] = python_path


def test_lcms_nom_metadata_gen():
    """
    Test the LCMS NOM metadata generation script.
    Test case does not include generating a biosample
    """
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_nom_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = LCMSNOMMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_lcms_nom.csv",
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_raw_nom/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_processed_nom/",
    )

    # Run the metadata generation process
    generator.run()
    assert os.path.exists(output_file)


def test_lcms_nom_metadata_gen_rerun():
    """
    Test the LCMS NOM metadata generation script.
    Test case does not include generating a biosample
    """
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_nom_rerun_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = LCMSNOMMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_lcms_nom_rerun.csv",
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/blanchard/raw/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_processed_nom/",
    )

    # Run the metadata generation process
    generator.rerun()
    assert os.path.exists(output_file)


def test_lcms_nom_biosample_gen_no_biosample():
    """
    Test the LCMS NOM metadata generation script.
    Test case includes generating a biosample with no biosample id
    """
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_nom_no_biosample_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = LCMSNOMMetadataGenerator(
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
