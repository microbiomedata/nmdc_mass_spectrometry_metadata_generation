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
    with open(output_file) as file:
        working_data = json.load(file)
        for workflow_execution in working_data["workflow_execution_set"]:
            # execution_resource should not be present (removed by clean_dict when None)
            assert workflow_execution.get("execution_resource") is None


def test_gcms_metadata_gen_with_qc_fields():
    """
    Test the GCMS metadata generation script with qc_status and qc_comment fields.
    """
    # Set up output file with datetime stamp
    output_file = (
        "tests/test_data/test_database_gcms_qc_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = GCMSMetabolomicsMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_gcms_qc.csv",
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

    file = open(output_file)
    working_data = json.load(file)
    file.close()

    # Check that workflow_execution_set has qc_status and qc_comment where provided
    workflow_executions = working_data["workflow_execution_set"]

    # We have 2 samples: 1 pass, 1 fail
    assert len(workflow_executions) == 2

    # Find the workflow execution with qc_status = "pass"
    pass_wf = [wf for wf in workflow_executions if wf.get("qc_status") == "pass"]
    assert len(pass_wf) == 1
    assert pass_wf[0].get("qc_comment") == "Sample passed quality control"
    # Verify pass workflow has has_output
    assert "has_output" in pass_wf[0], "Pass QC workflow should have has_output"
    assert (
        len(pass_wf[0]["has_output"]) > 0
    ), "Pass QC workflow should have non-empty has_output"
    # Verify pass workflow has metabolite_identifications
    assert (
        "has_metabolite_identifications" in pass_wf[0]
    ), "Pass QC workflow should have metabolite identifications"

    # Find the workflow execution with qc_status = "fail"
    fail_wf = [wf for wf in workflow_executions if wf.get("qc_status") == "fail"]
    assert len(fail_wf) == 1
    assert fail_wf[0].get("qc_comment") == "Poor peak resolution"
    # Verify fail workflow does NOT have has_output
    assert "has_output" not in fail_wf[0] or not fail_wf[0].get(
        "has_output"
    ), "Failed QC workflow should not have has_output"
    # Verify fail workflow does NOT have metabolite_identifications
    assert "has_metabolite_identifications" not in fail_wf[0] or not fail_wf[0].get(
        "has_metabolite_identifications"
    ), "Failed QC workflow should not have metabolite identifications"

    # Count data objects: we should have:
    # - 3 raw data objects (2 samples + 1 calibration file)
    # - 1 processed data object (only for pass sample)
    data_objects = working_data["data_object_set"]
    raw_data_objects = [
        do for do in data_objects if do.get("data_category") == "instrument_data"
    ]
    processed_data_objects = [
        do for do in data_objects if do.get("data_category") == "processed_data"
    ]

    assert (
        len(raw_data_objects) == 3
    ), f"Expected 3 raw data objects (2 samples + 1 calibration), got {len(raw_data_objects)}"
    assert (
        len(processed_data_objects) == 1
    ), f"Expected 1 processed data object (only pass sample), got {len(processed_data_objects)}"
