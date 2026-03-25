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
    # expect metabolite identifications for each workflow_execution records
    for record in working_data["workflow_execution_set"]:
        assert "has_metabolite_identifications" in record

    # ensure empty instrument_instance_specifier does not appear as "nan"
    for record in working_data["data_generation_set"]:
        assert record.get("instrument_instance_specifier") != "nan"
        assert "provenance_metadata" in record
        assert "add_date" in record["provenance_metadata"]


def test_lcms_metab_metadata_gen_processed_sample():
    current_directory = os.path.dirname(__file__)
    csv_file_path = os.path.join(
        current_directory,
        "test_data",
        "test_metadata_file_lcms_lipid_processed_sample.csv",
    )
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_lcms_lipid_processed_sample_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )
    # Start the metadata generation setup
    generator = LCMSMetabolomicsMetadataGenerator(
        metadata_file=csv_file_path,
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_raw_lcms_lipid/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_processed_lcms_lipid/",
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
    # expect metabolite identifications for each workflow_execution records
    for record in working_data["workflow_execution_set"]:
        assert "has_metabolite_identifications" in record


def test_lcms_metab_metadata_gen_with_qc_fields_from_csv():
    """Test LCMS metadata generation with qc_status and qc_comment fields from the CSV.
    CSV-provided values are accepted as-is when stats also pass (no prefix is added).
    A CSV "fail" is accepted unconditionally when stats pass.
    """
    current_directory = os.path.dirname(__file__)
    csv_file_path = os.path.join(
        current_directory, "test_data", "test_metadata_file_lcms_lipid_qc.csv"
    )
    # Set up output file with datetime stamp
    output_file = (
        "tests/test_data/test_database_lcms_lipid_qc_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )
    # Start the metadata generation setup
    generator = LCMSMetabolomicsMetadataGenerator(
        metadata_file=csv_file_path,
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_raw_lcms_lipid/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_processed_lcms_lipid/",
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

    # Should have 2 workflow executions and 2 raw data objects (one per sample)
    assert len(working_data["workflow_execution_set"]) == 2
    assert (
        len(working_data["data_object_set"]) == 5
    )  # 2 raw + 3 processed for passing sample

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
            assert record["qc_comment"] == "Sample passed QC"
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
            assert record["qc_comment"] == "Sample failed QC due to low signal"

    assert qc_pass_count == 1
    assert qc_fail_count == 1


def test_lcms_metab_csv_pass_overridden_by_failing_stats():
    """Test that a CSV-provided 'pass' is overridden when stats fail the thresholds.
    Stats always prevail for failures.
    """
    current_directory = os.path.dirname(__file__)
    # Use the QC CSV which has a sample with qc_status="pass"
    csv_file_path = os.path.join(
        current_directory, "test_data", "test_metadata_file_lcms_lipid_qc.csv"
    )
    output_file = (
        "tests/test_data/test_database_lcms_lipid_csv_pass_stat_fail_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )
    generator = LCMSMetabolomicsMetadataGenerator(
        metadata_file=csv_file_path,
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_raw_lcms_lipid/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_processed_lcms_lipid/",
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


def test_lcms_metab_csv_fail_and_stats_fail_concatenated_comment():
    """Test that when both CSV provides 'fail' and stats fail, the qc_comment contains
    both the CSV comment and the stat failure message concatenated together.
    """
    current_directory = os.path.dirname(__file__)
    # Use the QC CSV which has a sample with qc_status="fail" and a qc_comment
    csv_file_path = os.path.join(
        current_directory, "test_data", "test_metadata_file_lcms_lipid_qc.csv"
    )
    output_file = (
        "tests/test_data/test_database_lcms_lipid_csv_fail_stat_fail_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )
    generator = LCMSMetabolomicsMetadataGenerator(
        metadata_file=csv_file_path,
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_raw_lcms_lipid/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_processed_lcms_lipid/",
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
        (
            r
            for r in fail_records
            if "Sample failed QC due to low signal" in r.get("qc_comment", "")
        ),
        None,
    )
    assert csv_fail_record is not None, "Expected a record with the CSV fail comment"
    # Should also contain the stat failure message
    assert "peak_count" in csv_fail_record["qc_comment"]
    assert "< 999999" in csv_fail_record["qc_comment"]


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
        test=True,
    )
    # Run the metadata generation process
    metadata = generator.rerun()
    validate = generator.validate_nmdc_database(json=metadata, use_api=False)
    assert validate["result"] == "All Okay!"

    assert os.path.exists(output_file)


def test_lcms_metab_metadata_gen_with_qc_fields_from_processed_data():
    """Test that QC status and comment are derived from computed stats when no qc columns in CSV.
    Uses the plain lipid CSV (no qc_status/qc_comment columns).
    Runs twice: once with default thresholds (all pass) and once with impossibly high
    thresholds (all fail), verifying the comment content differs accordingly.
    """
    current_directory = os.path.dirname(__file__)
    # Use the plain CSV — no qc_status/qc_comment columns, so thresholds drive QC
    csv_file_path = os.path.join(
        current_directory, "test_data", "test_metadata_file_lcms_lipid.csv"
    )

    # --- Run 1: default thresholds (=1), all samples should pass ---
    output_file_pass = (
        "tests/test_data/test_database_lcms_lipid_threshold_pass_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )
    generator = LCMSMetabolomicsMetadataGenerator(
        metadata_file=csv_file_path,
        database_dump_json_path=output_file_pass,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_raw_lcms_lipid/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_processed_lcms_lipid/",
        test=True,
    )
    metadata = generator.run()
    validate = generator.validate_nmdc_database(json=metadata, use_api=False)
    assert validate["result"] == "All Okay!"

    with open(output_file_pass) as f:
        working_data = json.load(f)

    for record in working_data["workflow_execution_set"]:
        assert record.get("qc_status") == "pass"
        assert (
            record.get("qc_comment") == "QC passed all computed peak count thresholds."
        )

    # --- Run 2: impossibly high peak_count_threshold, all samples should fail ---
    output_file_fail = (
        "tests/test_data/test_database_lcms_lipid_threshold_fail_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )
    generator_high = LCMSMetabolomicsMetadataGenerator(
        metadata_file=csv_file_path,
        database_dump_json_path=output_file_fail,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_raw_lcms_lipid/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_processed_lcms_lipid/",
        test=True,
    )
    generator_high.peak_count_threshold = 999999

    metadata_fail = generator_high.run()
    validate_fail = generator_high.validate_nmdc_database(
        json=metadata_fail, use_api=False
    )
    assert validate_fail["result"] == "All Okay!"

    with open(output_file_fail) as f:
        working_data_fail = json.load(f)

    for record in working_data_fail["workflow_execution_set"]:
        assert record.get("qc_status") == "fail"
        assert f"peak_count" in record.get("qc_comment", "")
        assert f"< {generator_high.peak_count_threshold}" in record.get(
            "qc_comment", ""
        )
