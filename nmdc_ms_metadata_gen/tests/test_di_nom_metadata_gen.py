# This script will serve as a test for the di nom metadata generation script.
import json
import os
from datetime import datetime

from dotenv import load_dotenv
from linkml_runtime.dumpers import json_dumper

from nmdc_ms_metadata_gen.di_nom_metadata_generator import DINOMMetaDataGenerator

load_dotenv()

python_path = os.getenv("PYTHONPATH")
if python_path:
    os.environ["PYTHONPATH"] = python_path


def test_di_nom_metadata_gen():
    """
    Test the DI NOM metadata generation script.
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
        test=True,
    )

    metadata = generator.rerun()
    validate = generator.validate_nmdc_database(json=metadata, use_api=False)
    assert validate["result"] == "All Okay!"
    assert os.path.exists(output_file)

    file = open(output_file)
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


def test_di_nom_metadata_gen_processed_sample():
    """
    Test the DI NOM metadata generation script.
    Test case includes using processed sample ids
    """
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_nom_processed_sample_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = DINOMMetaDataGenerator(
        metadata_file="tests/test_data/test_metadata_file_nom_processed_sample.csv",
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_raw_nom/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_processed_nom/",
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

    assert (
        "specifier"
        in working_data["data_generation_set"][0]["instrument_instance_specifier"]
    )


def test_di_nom_metadata_gen_with_qc_fields():
    """
    Test the DI NOM metadata generation script with qc_status and qc_comment fields.
    """
    # Set up output file with datetime stamp
    output_file = (
        "tests/test_data/test_database_nom_qc_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = DINOMMetaDataGenerator(
        metadata_file="tests/test_data/test_metadata_file_nom_qc.csv",
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_raw_nom/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_processed_nom/",
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

    # We have 5 samples: 2 explicit pass, 2 fail, 1 no qc_status
    # Verify total count
    assert len(workflow_executions) == 5

    # Verify all workflows have uses_calibration populated
    for wf in workflow_executions:
        assert (
            "uses_calibration" in wf
        ), f"Workflow {wf['id']} should have uses_calibration"
        assert (
            wf["uses_calibration"] is not None
        ), f"Workflow {wf['id']} uses_calibration should not be None"
        assert wf["uses_calibration"][0].startswith(
            "nmdc:calib-"
        ), f"Workflow {wf['id']} uses_calibration should be a valid NMDC calibration ID"

    # Find the workflow execution with qc_status = "pass" explicitly set
    pass_wf = [wf for wf in workflow_executions if wf.get("qc_status") == "pass"]
    assert len(pass_wf) == 2
    # Check first one has the expected comment
    assert pass_wf[0].get("qc_comment") == "Sample passed all quality control checks"
    # Verify pass workflows have has_output
    for wf in pass_wf:
        assert "has_output" in wf, f"Pass QC workflow {wf['id']} should have has_output"
        assert (
            len(wf["has_output"]) > 0
        ), f"Pass QC workflow {wf['id']} should have non-empty has_output"

    # Find the workflow execution with qc_status = "fail"
    fail_wf = [wf for wf in workflow_executions if wf.get("qc_status") == "fail"]
    assert len(fail_wf) == 2
    # Check that both fail status workflows have expected comments
    fail_comments = [wf.get("qc_comment") for wf in fail_wf]
    assert "Low signal intensity detected" in fail_comments
    assert "Contamination suspected in blank" in fail_comments
    # Verify fail workflows do NOT have has_output
    for wf in fail_wf:
        assert "has_output" not in wf or not wf.get(
            "has_output"
        ), f"Failed QC workflow {wf['id']} should not have has_output"

    # Count data objects: we should have:
    # - 5 raw data objects (one per sample)
    # - 5 workflow parameter objects (one per sample - always created)
    # - 6 processed data objects (only for pass/no-qc samples: 3 samples × 2 files each = 6)
    data_objects = working_data["data_object_set"]
    raw_data_objects = [
        do
        for do in data_objects
        if do.get("data_object_type") == "Direct Infusion FT ICR-MS Raw Data"
    ]
    workflow_param_objects = [
        do
        for do in data_objects
        if do.get("data_object_type") == "Analysis Tool Parameter File"
    ]
    processed_data_objects = [
        do for do in data_objects if do.get("data_category") == "processed_data"
    ]

    assert (
        len(raw_data_objects) == 5
    ), f"Expected 5 raw data objects, got {len(raw_data_objects)}"
    assert (
        len(workflow_param_objects) == 5
    ), f"Expected 5 workflow parameter objects (all samples), got {len(workflow_param_objects)}"
    # 3 samples without fail status × 2 processed files each (csv + png) = 6 processed data objects
    assert (
        len(processed_data_objects) == 6
    ), f"Expected 6 processed data objects (3 pass/no-qc samples × 2 files), got {len(processed_data_objects)}"


def test_di_nom_metadata_gen_rerun_with_qc_fields():
    """
    Test the DI NOM metadata generation rerun with qc_status and qc_comment fields.
    """
    # Set up output file with datetime stamp
    output_file = (
        "tests/test_data/test_database_nom_rerun_qc_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = DINOMMetaDataGenerator(
        metadata_file="tests/test_data/test_metadata_file_nom_rerun_qc.csv",
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/blanchard/raw/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_processed_nom/",
        test=True,
    )

    metadata = generator.rerun()
    validate = generator.validate_nmdc_database(json=metadata, use_api=False)
    assert validate["result"] == "All Okay!"
    assert os.path.exists(output_file)

    file = open(output_file)
    working_data = json.load(file)
    file.close()

    # Check that workflow_execution_set has qc_status and qc_comment
    workflow_executions = working_data["workflow_execution_set"]
    assert len(workflow_executions) >= 1

    # Check the rerun workflow has qc fields
    wf = workflow_executions[0]
    assert wf.get("qc_status") == "pass"
    assert wf.get("qc_comment") == "Reprocessed data meets quality standards"
