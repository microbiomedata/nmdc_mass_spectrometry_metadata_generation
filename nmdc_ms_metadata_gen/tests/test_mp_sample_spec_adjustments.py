import os
import sys
from pathlib import Path

import pandas as pd
import pytest

# clarifying path variable for relative imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from nmdc_ms_metadata_gen.material_processing_generator import (
    MaterialProcessingMetadataGenerator,
)
from nmdc_ms_metadata_gen.metadata_parser import YamlSpecifier

# Constants
TEST_DATA_DIR = Path("tests/test_data/test_material_processing")
MINTING_CONFIG = "../config.toml"


@pytest.fixture
def yaml_specifier_add_info():
    """Fixture for YamlSpecifier with add_info test configuration."""
    return YamlSpecifier(
        yaml_outline_path=str(TEST_DATA_DIR / "test_yaml_for_add_info_test.yaml")
    )


@pytest.fixture
def yaml_specifier_output_adjust():
    """Fixture for YamlSpecifier with output adjustment test configuration."""
    return YamlSpecifier(
        yaml_outline_path=str(TEST_DATA_DIR / "test_yaml_for_output_adjust_test.yaml")
    )


@pytest.fixture
def add_info_data():
    """Load all data needed for add_info test."""
    return {
        "mapping": pd.read_csv(TEST_DATA_DIR / "mapping_input_for_add_info_test.csv"),
        "sample_info": pd.read_csv(
            TEST_DATA_DIR / "add_info_input_for_add_info_test.csv"
        ),
    }


@pytest.fixture
def output_adjust_data():
    """Load mapping data for output adjustment test."""
    return pd.read_csv(TEST_DATA_DIR / "mapping_input_for_output_adjust_test.csv")


def get_biosample_protocol_subset(
    mapping_df: pd.DataFrame, biosample_id: str, protocol_id: str
) -> pd.DataFrame:
    """Filter mapping data for a specific biosample and protocol."""
    return mapping_df[
        (mapping_df["biosample_id"] == biosample_id)
        & (mapping_df["material_processing_protocol_id"] == protocol_id)
    ].reset_index(drop=True)


def extract_final_outputs(yaml_outline: dict) -> set:
    """Extract final outputs (outputs not used as inputs) from yaml outline."""
    all_outputs = set()
    all_inputs = set()

    for step in yaml_outline.get("steps", []):
        step_data = list(list(step.values())[0].values())[0]
        all_outputs.update(step_data["has_output"])
        all_inputs.update(step_data["has_input"])

    return all_outputs - all_inputs


def test_yaml_outline_populated_with_sample_specific_dates(
    yaml_specifier_add_info, add_info_data
):
    """
    Verify that YamlSpecifier populates date fields from sample-specific info.

    Tests that start_date and end_date are correctly added to processing steps
    when sample-specific information is provided for a protocol, and are not
    populated when sample-specific information is missing.
    """
    reference_mapping = add_info_data["mapping"]
    sample_specific_info = add_info_data["sample_info"]

    # Iterate through biosamples
    for biosample in reference_mapping["biosample_id"].unique():

        # Get mapping info for this biosample
        sample_mapping = reference_mapping[
            reference_mapping["biosample_id"] == biosample
        ].reset_index(drop=True)

        # Get any additional info for this biosample
        sample_specific_info_subset = sample_specific_info[
            sample_specific_info["biosample_id"] == biosample
        ].reset_index(drop=True)

        # Get protocols for this biosample
        protocols = sample_mapping["material_processing_protocol_id"].unique().tolist()

        # Iterate through each protocol
        for protocol in protocols:

            yaml_parameters = {}
            yaml_parameters["protocol_id"] = protocol

            # Get mapping info for this protocol
            protocol_mapping = sample_mapping[
                sample_mapping["material_processing_protocol_id"] == protocol
            ].reset_index(drop=True)

            # Get any additional info for this protocol
            if not sample_specific_info.empty:
                yaml_parameters["sample_specific_info_protocol_subset"] = (
                    sample_specific_info_subset[
                        sample_specific_info_subset["material_processing_protocol_id"]
                        == protocol
                    ].reset_index(drop=True)
                )

            # Get placeholders for this protocol
            yaml_parameters["target_outputs"] = (
                protocol_mapping["processedsample_placeholder"].unique().tolist()
            )

            # Use the additional info and target output information to adjust the yaml outline
            full_outline = yaml_specifier_add_info.yaml_generation(**yaml_parameters)

            # Check if this protocol has sample-specific info
            has_sample_info = (
                "sample_specific_info_protocol_subset" in yaml_parameters
                and not yaml_parameters["sample_specific_info_protocol_subset"].empty
            )

            # Verify date fields based on whether sample info was provided
            for step in full_outline["steps"]:
                step_number = list(step.keys())[0]
                step_name = list(step[step_number].keys())[0]
                step_data = list(step[step_number].values())[0]

                # Check if this protocol's step has sample-specific info
                if (
                    has_sample_info
                    and step_name
                    in yaml_parameters["sample_specific_info_protocol_subset"][
                        "stepname"
                    ]
                    .unique()
                    .tolist()
                ):

                    if has_sample_info:
                        assert step_data.get("start_date"), (
                            f"{biosample} - {protocol} - {step_name} start_date should be populated "
                            f"when sample-specific info is provided"
                        )
                        assert step_data.get("end_date"), (
                            f"{biosample} - {protocol} - {step_name} end_date should be populated "
                            f"when sample-specific info is provided"
                        )
                else:
                    assert not step_data.get("start_date"), (
                        f"{biosample} - {protocol} - {step_name} start_date should NOT be populated "
                        f"when sample-specific info is not provided for protocol or step"
                    )
                    assert not step_data.get("end_date"), (
                        f"{biosample} - {protocol} - {step_name} end_date should NOT be populated "
                        f"when sample-specific info is not provided for protocol or step"
                    )


def test_yaml_outline_adjusts_outputs_for_target_processed_samples(
    yaml_specifier_output_adjust, output_adjust_data
):
    """
    Verify that YamlSpecifier adjusts the number of outputs based on target samples.

    Tests that the yaml outline is correctly modified to produce only the number
    of final outputs needed for the biosample's data generation records.
    """
    # Track expected results for each biosample-protocol combination
    expected_outputs = {
        "nmdc:bsm-11-64vz3p24": 1,  # only cold water output
        "nmdc:bsm-11-5r7c6h25": 1,  # only hot water output
        "nmdc:bsm-11-q0awnq72": 2,  # 2 cold + 1 hot → 2 final outputs
        "nmdc:bsm-11-cama3617": 2,  # 1 cold + 2 hot → 2 final outputs
    }

    reference_mapping = output_adjust_data

    # Test protocol
    protocol = "NOM"

    # Iterate through biosamples
    for biosample in reference_mapping["biosample_id"].unique():

        # Get mapping info for this biosample
        sample_mapping = reference_mapping[
            reference_mapping["biosample_id"] == biosample
        ].reset_index(drop=True)

        yaml_parameters = {}
        yaml_parameters["protocol_id"] = protocol

        # Get mapping info for this protocol
        protocol_mapping = sample_mapping[
            sample_mapping["material_processing_protocol_id"] == protocol
        ].reset_index(drop=True)

        # Get placeholders for this protocol
        yaml_parameters["target_outputs"] = (
            protocol_mapping["processedsample_placeholder"].unique().tolist()
        )

        # Use the target output information to adjust the yaml outline
        full_outline = yaml_specifier_output_adjust.yaml_generation(**yaml_parameters)

        final_outputs = extract_final_outputs(full_outline)

        assert len(final_outputs) == expected_outputs[biosample], (
            f"Expected {expected_outputs[biosample]} final outputs for {biosample} "
            f"with protocol {protocol}, got {len(final_outputs)}"
        )


def test_all_processed_samples_mapped_to_data_generation_records():
    """
    Run MaterialProcessingMetadataGenerator, verifying that all processed samples being created
    from test data are mapped to data generation records.
    """
    generator = MaterialProcessingMetadataGenerator(
        database_dump_json_path="tests/test_data/test_mp_map_samples_output.json",
        study_id="nmdc:sty-11-8xdqsn54",
        yaml_outline_path=str(TEST_DATA_DIR / "test_yaml_for_output_adjust_test.yaml"),
        sample_to_dg_mapping_path=str(
            TEST_DATA_DIR / "mapping_input_for_output_adjust_test.csv"
        ),
        test=True,
        minting_config_creds=MINTING_CONFIG,
    )

    metadata = generator.run()
    validation_result = generator.validate_nmdc_database(json=metadata, use_api=False)

    assert (
        validation_result["result"] == "All Okay!"
    ), f"Validation failed: {validation_result.get('detail', 'Unknown error')}"


def test_generates_changesheets_for_existing_and_workflowsheets_for_new_records():
    """
    Run MaterialProcessingMetadataGenerator, verifying that it correctly produces Changesheets for
    processed samples linked to existing NMDC data generation records and Workflowsheets for
    processed samples linked to non-NMDC identifiers.
    """
    generator = MaterialProcessingMetadataGenerator(
        database_dump_json_path="tests/test_data/test_mp_changesheet_workflowsheet_output.json",
        study_id="nmdc:sty-11-8xdqsn54",
        yaml_outline_path=str(TEST_DATA_DIR / "test_yaml_for_output_adjust_test.yaml"),
        sample_to_dg_mapping_path=str(
            TEST_DATA_DIR / "mapping_input_for_changesheet_workflowsheet_test.csv"
        ),
        minting_config_creds=MINTING_CONFIG,
        test=True,
    )

    metadata = generator.run()
    validation_result = generator.validate_nmdc_database(json=metadata, use_api=False)
    assert validation_result["result"] == "All Okay!"

    # Verify output files were created and contain data
    filepath = generator.database_dump_json_path.split(".json")[0]
    changesheet = pd.read_csv(f"{filepath}_changesheet.csv")
    workflowsheet = pd.read_csv(f"{filepath}_workflowreference.csv")

    assert not changesheet.empty, "Changesheet should contain data for existing records"
    assert not workflowsheet.empty, "Workflowsheet should contain data for new records"


def test_yaml_outline_contains_only_specified_protocol_steps(
    yaml_specifier_add_info, add_info_data
):
    """
    Verify that YamlSpecifier generates steps only for the specified protocol.

    Tests that when a specific protocol is requested for a biosample, the resulting
    yaml outline contains only steps for that protocol (NOM_water in this case),
    and not steps from other protocols that might exist for the biosample.
    """

    reference_mapping = add_info_data["mapping"]

    # Use first biosample (NOM_sediment) as test case
    test_biosample = "nmdc:bsm-11-p2xdkc25"
    test_protocol = "NOM_sediment"

    # Get mapping info for this biosample
    sample_mapping = reference_mapping[
        reference_mapping["biosample_id"] == test_biosample
    ].reset_index(drop=True)

    yaml_parameters = {}
    yaml_parameters["protocol_id"] = test_protocol

    # Get mapping info for this protocol
    protocol_mapping = sample_mapping[
        sample_mapping["material_processing_protocol_id"] == test_protocol
    ].reset_index(drop=True)

    # Get placeholders for this protocol
    yaml_parameters["target_outputs"] = (
        protocol_mapping["processedsample_placeholder"].unique().tolist()
    )

    # Use the target output information to adjust the yaml outline
    full_outline = yaml_specifier_add_info.yaml_generation(**yaml_parameters)

    # Verify only NOM_sediment protocol steps exist
    assert (
        len(full_outline["steps"]) == 5
    ), "Should have exactly 5 steps for NOM_sediment protocol"
    assert all(
        list(step.keys())[0].endswith("NOM_sediment") for step in full_outline["steps"]
    ), f"All steps should be for protocol {test_protocol} (NOM_sediment) only"
