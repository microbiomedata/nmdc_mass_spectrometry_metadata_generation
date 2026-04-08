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
    """Run metadata generation using a biosample id as input and validate the output."""
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

    working_data = json.load(open(output_file))
    for wf in working_data["workflow_execution_set"]:
        has_qc_plot = any(
            "QC" in str(d.get("data_object_type", ""))
            for d in working_data["data_object_set"]
            if d.get("was_generated_by") == wf["id"]
        )
        assert has_qc_plot == (wf.get("qc_status") == "pass")

    assert (
        "specifier"
        in working_data["data_generation_set"][0]["instrument_instance_specifier"]
    )

    assert (
        len(working_data["workflow_execution_set"][0]["uses_calibration"]) == 2
    ), f"Workflow {working_data['workflow_execution_set'][0]['id']} uses_calibration should have two values"


def test_di_nom_metadata_gen_rerun():
    """Run the rerun code path using raw data file paths as input and validate the output."""
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

    working_data = json.load(open(output_file))
    for wf in working_data["workflow_execution_set"]:
        has_qc_plot = any(
            "QC" in str(d.get("data_object_type", ""))
            for d in working_data["data_object_set"]
            if d.get("was_generated_by") == wf["id"]
        )
        assert has_qc_plot == (wf.get("qc_status") == "pass")


def test_di_nom_config_file():
    """Run metadata generation with a minting config file path supplied and validate the output."""
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
    working_data = json.load(open(output_file))
    for wf in working_data["workflow_execution_set"]:
        has_qc_plot = any(
            "QC" in str(d.get("data_object_type", ""))
            for d in working_data["data_object_set"]
            if d.get("was_generated_by") == wf["id"]
        )
        assert has_qc_plot == (wf.get("qc_status") == "pass")


def test_di_nom_metadata_gen_processed_sample():
    """Run metadata generation using a processed sample id as input and validate the output."""
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

    working_data = json.load(open(output_file))
    for wf in working_data["workflow_execution_set"]:
        has_qc_plot = any(
            "QC" in str(d.get("data_object_type", ""))
            for d in working_data["data_object_set"]
            if d.get("was_generated_by") == wf["id"]
        )
        assert has_qc_plot == (wf.get("qc_status") == "pass")

    assert (
        "specifier"
        in working_data["data_generation_set"][0]["instrument_instance_specifier"]
    )


def test_di_nom_metadata_gen_with_csv_qc_fields():
    """Assert QC status/comment from CSV are correctly applied and only passing workflows produce processed data objects."""
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

    # We have 5 samples: 2 with explicit CSV pass AND QC threshold pass, 1 with no explicit qc_status BUT QC threshold pass, 2 with explicit CSV fail which OVERWRITES QC threshold pass
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

    # Find the workflow execution with qc_status = "pass"
    pass_wf = [wf for wf in workflow_executions if wf.get("qc_status") == "pass"]
    assert len(pass_wf) == 3
    # Check first one has the expected comment from the CSV (should not be overwritten by stats)
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
    # - 5 raw data objects (one per sample) + 1 new calibration dobj = 6
    # - 5 workflow parameter objects (one per sample - always created)
    # - 6 processed data objects (only for pass samples: 3 samples × 2 files each = 6)
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
        len(raw_data_objects) == 6
    ), f"Expected 6 raw data objects, got {len(raw_data_objects)}"
    assert (
        len(workflow_param_objects) == 5
    ), f"Expected 5 workflow parameter objects (all samples), got {len(workflow_param_objects)}"
    # 3 samples without fail status × 2 processed files each (csv + png) = 6 processed data objects
    assert (
        len(processed_data_objects) == 6
    ), f"Expected 6 processed data objects (3 pass samples × 2 files), got {len(processed_data_objects)}"


def test_di_nom_metadata_gen_rerun_with_csv_qc_fields():
    """Run the rerun code path with CSV qc_status/qc_comment fields and validate they are applied correctly."""
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


def test_di_nom_metadata_gen_csv_pass_overridden_by_failing_stats():
    """Assert that a CSV-provided 'pass' is overridden by failing stats, and comments from both sources are concatenated."""
    # Use the QC CSV which has a sample with qc_status="pass"
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

    # Run the metadata generation process with an impossibly high threshold so stats fail
    generator.peak_count_threshold = 999999
    metadata = generator.run()
    validate = generator.validate_nmdc_database(json=metadata, use_api=False)
    assert validate["result"] == "All Okay!"
    assert os.path.exists(output_file)

    with open(output_file) as f:
        working_data = json.load(f)

    # All records should be "fail" regardless of CSV-provided "pass"
    for record in working_data["workflow_execution_set"]:
        assert record.get("qc_status") == "fail"

    # Second record in the CSV should have concatenated qc_comment with CSV comment and the stat failure message
    concat_comment = working_data["workflow_execution_set"][1].get("qc_comment", "")
    assert "peak_count" in concat_comment
    assert (
        "< 999999" in concat_comment
        and "Low signal intensity detected" in concat_comment
    )

    # Fifth record in the CSV should have concatenated qc_comment with CSV comment and the stat failure message
    concat_comment = working_data["workflow_execution_set"][4].get("qc_comment", "")
    assert "peak_count" in concat_comment
    assert (
        "< 999999" in concat_comment
        and "Contamination suspected in blank" in concat_comment
    )
