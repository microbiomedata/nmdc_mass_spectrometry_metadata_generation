import json
import os
import sys

import pandas as pd
from linkml_runtime.dumpers import json_dumper

# clarifying path variable for relative imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from nmdc_ms_metadata_gen.material_processing_generator import (
    MaterialProcessingMetadataGenerator,
)
from nmdc_ms_metadata_gen.metadata_parser import YamlSpecifier


def test_yamlspecifier_add_info():
    """
    Test YamlSpecifier: Does it add biosample specific info to the yaml outline when provided as additional tsv?
    """

    # Spruce test study
    yaml_outline_path = (
        "tests/test_data/test_material_processing/spruce_proteins_test.yaml"
    )
    sample_to_dg_mapping_path = (
        "tests/test_data/test_material_processing/add_info_test_mapping_input.csv"
    )
    sample_specific_info_path = (
        "tests/test_data/test_material_processing/add_info_test_input.csv"
    )

    yaml_parameters = {}

    sample_specific_info = pd.read_csv(sample_specific_info_path)
    sample_to_dg_mapping_path = pd.read_csv(sample_to_dg_mapping_path)

    # Subset to one biosample test
    test_biosample = sample_to_dg_mapping_path["biosample_id"].iloc[0]
    sample_mapping = sample_to_dg_mapping_path[
        sample_to_dg_mapping_path["biosample_id"] == test_biosample
    ].reset_index(drop=True)
    yaml_parameters["target_outputs"] = (
        sample_mapping["processedsample_placeholder"].unique().tolist()
    )
    yaml_parameters["sample_specific_info_subset"] = sample_specific_info[
        sample_specific_info["biosample_id"] == test_biosample
    ].reset_index(drop=True)

    # See if updated yaml outline has QuantityValue filled out
    data_parser = YamlSpecifier(yaml_outline_path=yaml_outline_path)
    full_outline = data_parser.yaml_generation(**yaml_parameters)

    assert full_outline["steps"][0]["Step 1"]["SubSamplingProcess"]["mass"][
        "has_raw_value"
    ]
    assert full_outline["steps"][0]["Step 1"]["SubSamplingProcess"]["mass"][
        "has_numeric_value"
    ]
    assert full_outline["steps"][0]["Step 1"]["SubSamplingProcess"]["mass"]["has_unit"]
    assert full_outline["steps"][1]["Step 2"]["Extraction"]["input_mass"][
        "has_raw_value"
    ]


def test_yamlspecifier_adjust_outputs():
    """
    Test YamlSpecifier: Does it adjust the number of outputs according to number of raw identifiers provided?
    i.e. reduce to only one output if only one data generation record
    """

    #'nmdc:bsm-11-64vz3p24', # only cold water output
    #'nmdc:bsm-11-5r7c6h25', # only hot water output
    #'nmdc:bsm-11-q0awnq72', # 3 total dg ids, 2 cold water (replicate) and 1 hot
    #'nmdc:bsm-11-cama3617', # 3 total dg ids, 1 cold water (replicate) and 2 hot

    tracking = {
        "nmdc:bsm-11-64vz3p24": 1,
        "nmdc:bsm-11-5r7c6h25": 1,
        "nmdc:bsm-11-q0awnq72": 2,
        "nmdc:bsm-11-cama3617": 2,
    }

    yaml_parameters = {}
    yaml_outline_path = (
        "tests/test_data/test_material_processing/SanClements-NOM_test.yaml"
    )
    sample_to_dg_mapping_path = (
        "tests/test_data/test_material_processing/outputs_test_mapping_input.csv"
    )
    sample_to_dg_mapping = pd.read_csv(sample_to_dg_mapping_path)

    # Subset to biosamples with varying number of outputs
    for test_biosample in tracking.keys():
        sample_mapping = sample_to_dg_mapping[
            sample_to_dg_mapping["biosample_id"] == test_biosample
        ].reset_index(drop=True)
        yaml_parameters["target_outputs"] = (
            sample_mapping["processedsample_placeholder"].unique().tolist()
        )
        data_parser = YamlSpecifier(yaml_outline_path=yaml_outline_path)
        full_outline = data_parser.yaml_generation(**yaml_parameters)

        # Does yamlspecifier provide the expected number of outputs for this biosample in the adjusted outline?
        steps = full_outline.get("steps", [])
        all_outputs = []
        all_inputs = []
        for step in steps:
            all_outputs.extend(list(list(step.values())[0].values())[0]["has_output"])
            all_inputs.extend(list(list(step.values())[0].values())[0]["has_input"])
        final_outputs = list({item for item in all_outputs if item not in all_inputs})
        assert len(final_outputs) == tracking[test_biosample]


def test_map_final_samples():
    """
    Test that there are no unmatched processed samples being created and that a valid json is being created
    """

    #'nmdc:bsm-11-64vz3p24', # only cold water output
    #'nmdc:bsm-11-5r7c6h25', # only hot water output
    #'nmdc:bsm-11-q0awnq72', # 3 total dg ids, 2 cold water (replicate) and 1 hot
    #'nmdc:bsm-11-cama3617', # 3 total dg ids, 1 cold water (replicate) and 2 hot

    # SanClements test study
    generator = MaterialProcessingMetadataGenerator(
        database_dump_json_path="tests/test_data/test_mp_map_samples_output.json",
        study_id="nmdc:sty-11-8xdqsn54",
        yaml_outline_path="tests/test_data/test_material_processing/SanClements-NOM_test.yaml",
        sample_to_dg_mapping_path="tests/test_data/test_material_processing/outputs_test_mapping_input.csv",
        test=True,
        # minting_config_creds='../config.toml',
    )

    metadata = generator.run()
    validate = generator.validate_nmdc_database(json=metadata, use_api=False)
    assert validate["result"] == "All Okay!"


def test_changesheet_workflowsheet():
    """
    Test that changesheet and workflowsheet are generated appropriately for raw identifiers that are nmdc ids or not
    """

    # SanClements test study
    generator = MaterialProcessingMetadataGenerator(
        database_dump_json_path="tests/test_data/test_mp_changesheet_workflowsheet_output.json",
        study_id="nmdc:sty-11-8xdqsn54",
        yaml_outline_path="tests/test_data/test_material_processing/SanClements-NOM_test.yaml",
        sample_to_dg_mapping_path="tests/test_data/test_material_processing/changesheet_workflowsheet_test_mapping_input.csv",
        # minting_config_creds='../config.toml',
        test=True,
    )

    metadata = generator.run()
    validate = generator.validate_nmdc_database(json=metadata, use_api=False)
    assert validate["result"] == "All Okay!"
    filepath = generator.database_dump_json_path.split(".json")[0]
    changesheet = pd.read_csv(f"{filepath}_changesheet.csv")
    workflowsheet = pd.read_csv(f"{filepath}_workflowreference.csv")

    assert (changesheet.shape[0] > 0) & (workflowsheet.shape[0] > 0)
