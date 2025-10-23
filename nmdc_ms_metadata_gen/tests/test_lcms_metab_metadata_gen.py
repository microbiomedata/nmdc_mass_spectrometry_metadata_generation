# This script will serve as a test for the lipdomics metadata generation script.
import json
import os
from datetime import datetime

from dotenv import load_dotenv
from linkml_runtime.dumpers import json_dumper

from nmdc_ms_metadata_gen.lcms_metab_metadata_generator import (
    LCMSMetabolomicsMetadataGenerator,
)

load_dotenv()

python_path = os.getenv("PYTHONPATH")
if python_path:
    os.environ["PYTHONPATH"] = python_path


def test_lcms_metab_metadata_gen():
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
    generator = LCMSMetabolomicsMetadataGenerator(
        metadata_file=csv_file_path,
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_raw_lcms_lipid/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_processed_lcms_lipid/",
    )
    # Run the metadata generation process
    metadata = generator.run()
    validate = generator.validate_nmdc_database(
        json=json.loads(json_dumper.dumps(metadata)), use_api=False
    )
    assert validate["result"] == "All Okay!"

    assert os.path.exists(output_file)

    file = open(output_file)
    working_data = json.load(file)
    file.close()
    # expect metabolite identifications for each workflow_execution records
    for record in working_data["workflow_execution_set"]:
        assert "has_metabolite_identifications" in record


def test_lcms_metab_metadata_gen_rerun():
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
    generator = LCMSMetabolomicsMetadataGenerator(
        metadata_file=csv_file_path,
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/lipidomics/blanchard_11_8ws97026/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_processed_lcms_lipid/",
    )
    # Run the metadata generation process
    metadata = generator.rerun()
    validate = generator.validate_nmdc_database(
        json=json.loads(json_dumper.dumps(metadata)), use_api=False
    )
    assert validate["result"] == "All Okay!"

    assert os.path.exists(output_file)


def test_lcms_metab_biosample_gen():
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
    generator = LCMSMetabolomicsMetadataGenerator(
        metadata_file=csv_file_path,
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_raw_lcms_lipid/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_processed_lcms_lipid/",
    )
    # Run the metadata generation process
    metadata = generator.run()
    validate = generator.validate_nmdc_database(
        json=json.loads(json_dumper.dumps(metadata)), use_api=False
    )
    assert validate["result"] == "All Okay!"

    assert os.path.exists(output_file)
    file = open(output_file)
    working_data = json.load(file)
    file.close()
    # expecting 1 since we only have 1 unique biosample name in the csv
    assert len(working_data["biosample_set"]) == 1
