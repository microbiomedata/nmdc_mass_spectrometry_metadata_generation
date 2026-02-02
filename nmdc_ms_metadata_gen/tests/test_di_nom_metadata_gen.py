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

    # Find the workflow execution with qc_status = "pass" and qc_comment
    pass_wf = [wf for wf in workflow_executions if wf.get("qc_status") == "pass"]
    assert len(pass_wf) >= 1
    # Check first one has the expected comment
    assert pass_wf[0].get("qc_comment") == "Sample passed all quality control checks"

    # Find the workflow execution with qc_status = "fail"
    fail_wf = [wf for wf in workflow_executions if wf.get("qc_status") == "fail"]
    assert len(fail_wf) >= 2
    # Check that both fail status workflows have expected comments
    fail_comments = [wf.get("qc_comment") for wf in fail_wf]
    assert "Low signal intensity detected" in fail_comments
    assert "Contamination suspected in blank" in fail_comments


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
