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


def test_lcms_metab_metadata_gen_with_qc_fields():
    """Test LCMS metadata generation with qc_status and qc_comment fields"""
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
            assert record["qc_comment"] == "Sample failed QC due to low signal"

    assert qc_pass_count == 1
    assert qc_fail_count == 1


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
