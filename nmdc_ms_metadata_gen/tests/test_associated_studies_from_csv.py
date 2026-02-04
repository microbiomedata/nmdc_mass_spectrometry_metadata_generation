"""
Test that associated_studies can be passed in via the input CSV when skip_sample_id_check=False.
"""

import pandas as pd
import pytest

from nmdc_ms_metadata_gen.gcms_metab_metadata_generator import (
    GCMSMetabolomicsMetadataGenerator,
)


def test_gcms_associated_studies_from_csv_with_skip_check_false():
    """
    Test that when skip_sample_id_check=False and associated_studies is in the CSV,
    the values from the CSV are used instead of being derived.
    """
    generator = GCMSMetabolomicsMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_gcms_with_associated_studies.csv",
        database_dump_json_path="test_output.json",
        raw_data_url="https://example.com/raw/",
        process_data_url="https://example.com/processed/",
        configuration_file_name="test_config.toml",
        test=True,
        skip_sample_id_check=False,
    )

    # Load metadata
    metadata_df = generator.load_metadata()

    # Verify that associated_studies column exists
    assert "associated_studies" in metadata_df.columns

    # Verify that the associated_studies values match what was in the CSV
    expected_studies = {
        "nmdc:bsm-11-002vgm56": "['nmdc:sty-11-abc12345']",
        "nmdc:bsm-11-006pnx90": "['nmdc:sty-11-xyz67890']",
    }

    for sample_id, expected_study in expected_studies.items():
        actual_study = metadata_df.loc[
            metadata_df["sample_id"] == sample_id, "associated_studies"
        ].values[0]
        assert actual_study == expected_study, (
            f"Expected associated_studies for {sample_id} to be {expected_study}, "
            f"but got {actual_study}"
        )


def test_gcms_associated_studies_from_csv_in_test_mode():
    """
    Test that when test=True and associated_studies is in the CSV,
    the values from the CSV are preserved.
    """
    generator = GCMSMetabolomicsMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_gcms_with_associated_studies.csv",
        database_dump_json_path="test_output.json",
        raw_data_url="https://example.com/raw/",
        process_data_url="https://example.com/processed/",
        configuration_file_name="test_config.toml",
        test=True,
        skip_sample_id_check=True,
    )

    # Load metadata
    metadata_df = generator.load_metadata()

    # Verify that associated_studies column exists
    assert "associated_studies" in metadata_df.columns

    # Verify that the associated_studies values match what was in the CSV
    # (should NOT be overwritten with the default placeholder)
    expected_studies = {
        "nmdc:bsm-11-002vgm56": "['nmdc:sty-11-abc12345']",
        "nmdc:bsm-11-006pnx90": "['nmdc:sty-11-xyz67890']",
    }

    for sample_id, expected_study in expected_studies.items():
        actual_study = metadata_df.loc[
            metadata_df["sample_id"] == sample_id, "associated_studies"
        ].values[0]
        assert actual_study == expected_study


def test_gcms_without_associated_studies_uses_placeholder_in_test_mode():
    """
    Test that when test=True and associated_studies is NOT in the CSV,
    the default placeholder is used.
    """
    generator = GCMSMetabolomicsMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_gcms.csv",
        database_dump_json_path="test_output.json",
        raw_data_url="https://example.com/raw/",
        process_data_url="https://example.com/processed/",
        configuration_file_name="test_config.toml",
        test=True,
        skip_sample_id_check=True,
    )

    # Load metadata
    metadata_df = generator.load_metadata()

    # Verify that associated_studies column exists
    assert "associated_studies" in metadata_df.columns

    # Verify that all rows have the default placeholder
    assert (metadata_df["associated_studies"] == "['nmdc:sty-00-000001']").all()


def test_associated_studies_invalid_format_raises_error():
    """
    Test that providing associated_studies in an invalid format raises a ValueError.
    Validation should happen in both test and production modes.
    """
    # Create a temporary CSV with invalid format
    import os
    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(
            "sample_id,material_processing_type,raw_data_file,processed_data_file,"
            "calibration_file,mass_spec_configuration_name,chromat_configuration_name,"
            "instrument_used,processing_institution_workflow,processing_institution_generation,"
            "instrument_analysis_start_date,instrument_analysis_end_date,execution_resource,"
            "manifest_name,instrument_instance_specifier,associated_studies\n"
        )
        f.write(
            "nmdc:bsm-11-002vgm56,MPLEX,tests/test_data/test_raw_gcms_metab/gcms_metab_sample_1.cdf,"
            "tests/test_data/test_processed_gcms_metab/gcms_metab_sample_1.csv,"
            "tests/test_data/test_raw_gcms_metab/gcms_metab_fames_1.cdf,"
            '"EMSL lipidomics DDA mass spectrometry method, positive",'
            "EMSL LC method for non-polar metabolites,Thermo LTQ Orbitrap Velos,Battelle,ANL,"
            "2018-01-23T19:56:13Z,2018-01-23T19:56:13Z,EMSL-RZR,Manifest Name,specifier,"
            "nmdc:sty-11-abc12345\n"  # Invalid: not a string representation of a list
        )
        temp_file = f.name

    try:
        generator = GCMSMetabolomicsMetadataGenerator(
            metadata_file=temp_file,
            database_dump_json_path="test_output.json",
            raw_data_url="https://example.com/raw/",
            process_data_url="https://example.com/processed/",
            configuration_file_name="test_config.toml",
            test=True,
            skip_sample_id_check=False,
        )

        # This should now raise a ValueError in test mode too
        with pytest.raises(
            ValueError,
            match="associated_studies at row.*must be a string representation of a list",
        ):
            metadata_df = generator.load_metadata()

    finally:
        os.unlink(temp_file)
