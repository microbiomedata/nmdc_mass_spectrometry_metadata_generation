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


def test_gcms_metadata_gen_with_qc_fields_from_csv():
    """
    Test GCMS metadata generation with qc_status and qc_comment fields from the CSV.
    CSV-provided values are accepted as-is when stats also pass (no prefix is added).
    A CSV "fail" is accepted unconditionally when stats pass.
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

    # Check QC status fields
    qc_pass_count = 0
    qc_fail_count = 0
    for record in working_data["workflow_execution_set"]:
        if "qc_status" in record and record["qc_status"] == "pass":
            qc_pass_count += 1
            # For passing QC, should have has_output and has_metabolite_identifications
            assert "has_output" in record
            assert record["has_output"] is not None
            assert "has_metabolite_identifications" in record
            assert record["has_metabolite_identifications"] is not None
            assert "qc_comment" in record
            # CSV comment is returned as-is; no prefix is added
            assert record["qc_comment"] == "Sample passed quality control"
        elif "qc_status" in record and record["qc_status"] == "fail":
            qc_fail_count += 1
            # For failing QC, should NOT have has_output or has_metabolite_identifications
            assert "has_output" not in record or record["has_output"] is None
            assert (
                "has_metabolite_identifications" not in record
                or record["has_metabolite_identifications"] is None
            )
            assert "qc_comment" in record
            # CSV comment is returned as-is; no prefix is added
            assert record["qc_comment"] == "Poor peak resolution"

    assert qc_pass_count == 1
    assert qc_fail_count == 1

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


def test_gcms_metab_csv_pass_overridden_by_failing_stats():
    """Test that a CSV-provided 'pass' is overridden when stats fail the thresholds.
    Stats always prevail for failures.
    """
    current_directory = os.path.dirname(__file__)
    # Use the QC CSV which has a sample with qc_status="pass"
    csv_file_path = os.path.join(
        current_directory, "test_data", "test_metadata_file_gcms_qc.csv"
    )
    output_file = (
        "tests/test_data/test_database_gcms_metab_csv_pass_stat_fail_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )
    generator = GCMSMetabolomicsMetadataGenerator(
        metadata_file=csv_file_path,
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/metabolomics/test_data/test_raw_gcms_metab/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/metabolomics/test_data/test_processed_gcms_metab/",
        configuration_file_name="emsl_gcms_corems_params.toml",
        test=True,
    )
    # Set an impossibly high threshold so stats always fail
    generator.peak_count_threshold = 999999

    metadata = generator.run()
    validate = generator.validate_nmdc_database(json=metadata, use_api=False)
    assert validate["result"] == "All Okay!"

    with open(output_file) as f:
        working_data = json.load(f)

    # All records should be "fail" regardless of CSV-provided "pass"
    for record in working_data["workflow_execution_set"]:
        assert record.get("qc_status") == "fail"
        assert "peak_count" in record.get("qc_comment", "")
        assert "< 999999" in record.get("qc_comment", "")


def test_gcms_metab_csv_fail_and_stats_fail_concatenated_comment():
    """Test that when both CSV provides 'fail' and stats fail, the qc_comment contains
    both the CSV comment and the stat failure message concatenated together.
    """
    current_directory = os.path.dirname(__file__)
    # Use the QC CSV which has a sample with qc_status="fail" and a qc_comment
    csv_file_path = os.path.join(
        current_directory, "test_data", "test_metadata_file_gcms_qc.csv"
    )
    output_file = (
        "tests/test_data/test_database_gcms_metab_csv_fail_stat_fail_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )
    generator = GCMSMetabolomicsMetadataGenerator(
        metadata_file=csv_file_path,
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/metabolomics/test_data/test_raw_gcms_metab/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/metabolomics/test_data/test_processed_gcms_metab/",
        configuration_file_name="emsl_gcms_corems_params.toml",
        test=True,
    )
    # Set an impossibly high threshold so stats always fail
    generator.peak_count_threshold = 999999

    metadata = generator.run()
    validate = generator.validate_nmdc_database(json=metadata, use_api=False)
    assert validate["result"] == "All Okay!"

    with open(output_file) as f:
        working_data = json.load(f)

    # Find the record that had CSV qc_status="fail"
    fail_records = [
        r
        for r in working_data["workflow_execution_set"]
        if r.get("qc_status") == "fail"
    ]
    assert (
        len(fail_records) == 2
    )  # Both samples fail (one from CSV+stats, one from stats only)

    # The record that originally had CSV qc_status="fail" should have a combined comment
    csv_fail_record = next(
        (r for r in fail_records if "Poor peak resolution" in r.get("qc_comment", "")),
        None,
    )
    assert csv_fail_record is not None, "Expected a record with the CSV fail comment"
    # Should also contain the stat failure message
    assert "peak_count" in csv_fail_record["qc_comment"]
    assert "< 999999" in csv_fail_record["qc_comment"]
