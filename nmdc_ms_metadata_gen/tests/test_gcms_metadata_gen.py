# This script will serve as a test for the lipdomics metadata generation script.
import json
import os
from datetime import datetime

from dotenv import load_dotenv
from linkml_runtime.dumpers import json_dumper

from nmdc_ms_metadata_gen.gcms_metab_metadata_generator import (
    GCMSMetabolomicsMetadataGenerator,
)

load_dotenv()

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
        configuration_file_name="emsl_gcms_corems_params.toml",
        test=True,
    )

    # Run the metadata generation process
    metadata = generator.run()
    validate = generator.validate_nmdc_database(json=metadata, use_api=False)
    assert validate["result"] == "All Okay!"

    assert os.path.exists(output_file)


def test_gcms_metadata_gen_processed_sample():
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_gcms_processed_sample_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = GCMSMetabolomicsMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_gcms_processed_sample.csv",
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/metabolomics/test_data/test_raw_gcms_metab/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/metabolomics/test_data/test_processed_gcms_metab/",
        configuration_file_name="emsl_gcms_corems_params.toml",
        test=True,
    )

    # Run the metadata generation process
    metadata = generator.run()
    validate = generator.validate_nmdc_database(json=metadata, use_api=False)
    assert validate["result"] == "All Okay!"

    assert os.path.exists(output_file)


def test_gcms_metadata_rerun_gen():
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_gcms_rerun_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = GCMSMetabolomicsMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_gcms_rerun.csv",
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/metabolomics/blanchard_11_8ws97026/raw/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/metabolomics/test_data/test_processed_gcms_metab/",
        configuration_file_name="emsl_gcms_corems_params.toml",
        test=True,
    )

    # Run the metadata generation process
    metadata = generator.rerun()
    validate = generator.validate_nmdc_database(json=metadata, use_api=False)
    assert validate["result"] == "All Okay!"
    assert os.path.exists(output_file)

    file = open(output_file)
    working_data = json.load(file)
    for workflow_execution in working_data["workflow_execution_set"]:
        assert "has_metabolite_identifications" in workflow_execution
    file.close()


def test_gcms_calibration_manifest_exists():
    """
    Test to handle the case where calibration and manifest IDs already exist in the metadata file.
    """
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_gcms_calibration_manifest_id"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = GCMSMetabolomicsMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_gcms_calibration_manifest_id.csv",
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/metabolomics/test_data/test_raw_gcms_metab/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/metabolomics/test_data/test_processed_gcms_metab/",
        configuration_file_name="emsl_gcms_corems_params.toml",
        test=True,
    )

    # Run the metadata generation process
    metadata = generator.run()
    validate = generator.validate_nmdc_database(json=metadata, use_api=False)
    assert validate["result"] == "All Okay!"
    assert os.path.exists(output_file)


def test_gcms_metadata_gen_no_execution_resource():
    """
    Test to verify that the metadata generation works when execution_resource is not provided in the input file.
    This tests the optional nature of execution_resource for schema-compliant metadata.
    """
    # Set up output file with datetime stamp
    output_file = (
        "tests/test_data/test_database_gcms_no_execution_resource_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup without execution_resource in the input file
    generator = GCMSMetabolomicsMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_gcms_no_execution_resource.csv",
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/metabolomics/test_data/test_raw_gcms_metab/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/metabolomics/test_data/test_processed_gcms_metab/",
        configuration_file_name="emsl_gcms_corems_params.toml",
        test=True,
    )

    # Run the metadata generation process
    metadata = generator.run()
    validate = generator.validate_nmdc_database(json=metadata, use_api=False)
    assert validate["result"] == "All Okay!"

    assert os.path.exists(output_file)

    # Verify that the generated metadata doesn't contain execution_resource or that it's None/omitted
    file = open(output_file)
    working_data = json.load(file)
    for workflow_execution in working_data["workflow_execution_set"]:
        # execution_resource should either not be present or be None (which wouldn't be in the JSON due to clean_dict)
        assert "execution_resource" not in workflow_execution or workflow_execution.get("execution_resource") is None
    file.close()

