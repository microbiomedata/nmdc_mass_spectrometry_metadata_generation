# This script will serve as a test for the LCMS NOM metadata generation script.
import json
import os
from datetime import datetime

from dotenv import load_dotenv
from linkml_runtime.dumpers import json_dumper

from nmdc_ms_metadata_gen.lcms_nom_metadata_generator import LCMSNOMMetadataGenerator

load_dotenv()

python_path = os.getenv("PYTHONPATH")
if python_path:
    os.environ["PYTHONPATH"] = python_path


def test_lcms_nom_metadata_gen_processed_sample():
    """Run metadata generation using a processed sample id as input and validate the output."""
    output_file = (
        "tests/test_data/test_database_lcms_nom_processed_sample_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    generator = LCMSNOMMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_lcms_nom_processed_sample.csv",
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_raw_lcms_nom/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_processed_lcms_nom/",
        test=True,
    )
    metadata = generator.run()
    validate = generator.validate_nmdc_database(json=metadata, use_api=False)
    assert validate["result"] == "All Okay!"
    assert os.path.exists(output_file)


def test_lcms_nom_metadata_gen():
    """Run metadata generation using a biosample id as input and validate the output."""
    # Set up output file with datetime stamp
    output_file = (
        "tests/test_data/test_database_lcms_nom_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = LCMSNOMMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_lcms_nom.csv",
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_raw_lcms_nom/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_processed_lcms_nom/",
        test=True,
    )

    # Run the metadata generation process
    metadata = generator.run()
    validate = generator.validate_nmdc_database(json=metadata, use_api=False)
    assert validate["result"] == "All Okay!"
    assert os.path.exists(output_file)


def test_lcms_nom_metadata_gen_rerun():
    """Run the rerun code path using raw data file paths as input and validate the output."""
    # Set up output file with datetime stamp
    output_file = (
        "tests/test_data/test_database_lcms_nom_rerun_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    # TODO - switch to a LCMS NOM raw example
    generator = LCMSNOMMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_lcms_nom_rerun.csv",
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/blanchard/raw/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_processed_lcms_nom/",
        test=True,
    )

    # Run the metadata generation process
    metadata = generator.rerun()
    validate = generator.validate_nmdc_database(json=metadata, use_api=False)
    assert validate["result"] == "All Okay!"
    assert os.path.exists(output_file)


def test_lcms_nom_metadata_gen_qc_thresholds():
    """Assert QC plot data objects are absent when the sample fails QC thresholds and present when it passes."""
    ts = datetime.now().strftime("%Y%m%d%H%M%S")

    # --- Assert QC fails with default thresholds ---
    generator = LCMSNOMMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_lcms_nom.csv",
        database_dump_json_path=f"tests/test_data/test_database_lcms_nom_fail_{ts}.json",
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_raw_lcms_nom/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_processed_lcms_nom/",
        test=True,
    )
    generator.peak_count_threshold = 0
    generator.peak_assignment_count_threshold = 250
    generator.peak_assignment_rate_threshold = 0.3
    metadata = generator.run()
    assert (
        generator.validate_nmdc_database(json=metadata, use_api=False)["result"]
        == "All Okay!"
    )
    working_data = json.load(open(generator.database_dump_json_path))
    for wf in working_data["workflow_execution_set"]:
        assert wf.get("qc_status") == "fail"
        assert not any(
            "QC" in str(d.get("data_object_type", ""))
            for d in working_data["data_object_set"]
            if d.get("was_generated_by") == wf["id"]
        )

    # --- Assert QC passes when all thresholds are 0 ---
    generator_pass = LCMSNOMMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_lcms_nom.csv",
        database_dump_json_path=f"tests/test_data/test_database_lcms_nom_pass_{ts}.json",
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_raw_lcms_nom/",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_processed_lcms_nom/",
        test=True,
    )
    generator_pass.peak_count_threshold = 0
    generator_pass.peak_assignment_count_threshold = 0
    generator_pass.peak_assignment_rate_threshold = 0.0
    metadata_pass = generator_pass.run()
    assert (
        generator_pass.validate_nmdc_database(json=metadata_pass, use_api=False)[
            "result"
        ]
        == "All Okay!"
    )
    working_data_pass = json.load(open(generator_pass.database_dump_json_path))
    for wf in working_data_pass["workflow_execution_set"]:
        assert wf.get("qc_status") == "pass"
        assert any(
            "QC" in str(d.get("data_object_type", ""))
            for d in working_data_pass["data_object_set"]
            if d.get("was_generated_by") == wf["id"]
        )
