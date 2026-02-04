"""
Test the skip_sample_id_check parameter across all NMDCWorkflowMetadataGenerator subclasses.
"""

import pytest
from nmdc_ms_metadata_gen.gcms_metab_metadata_generator import (
    GCMSMetabolomicsMetadataGenerator,
)
from nmdc_ms_metadata_gen.lcms_metadata_generator import LCMSMetadataGenerator
from nmdc_ms_metadata_gen.nom_metadata_generator import NOMMetadataGenerator
from nmdc_ms_metadata_gen.lcms_metab_metadata_generator import (
    LCMSMetabolomicsMetadataGenerator,
)
from nmdc_ms_metadata_gen.lcms_lipid_metadata_generator import (
    LCMSLipidomicsMetadataGenerator,
)
from nmdc_ms_metadata_gen.di_nom_metadata_generator import DINOMMetaDataGenerator
from nmdc_ms_metadata_gen.lcms_nom_metadata_generator import LCMSNOMMetadataGenerator


def test_gcms_skip_sample_id_check_parameter():
    """Test that GCMSMetabolomicsMetadataGenerator accepts skip_sample_id_check parameter."""
    generator = GCMSMetabolomicsMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_gcms.csv",
        database_dump_json_path="test_output.json",
        raw_data_url="https://example.com/raw/",
        process_data_url="https://example.com/processed/",
        configuration_file_name="test_config.toml",
        test=True,
        skip_sample_id_check=True,
    )
    assert generator.skip_sample_id_check is True
    assert generator.test is True


def test_gcms_skip_sample_id_check_default():
    """Test that skip_sample_id_check defaults to False."""
    generator = GCMSMetabolomicsMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_gcms.csv",
        database_dump_json_path="test_output.json",
        raw_data_url="https://example.com/raw/",
        process_data_url="https://example.com/processed/",
        configuration_file_name="test_config.toml",
        test=True,
    )
    assert generator.skip_sample_id_check is False


def test_lcms_skip_sample_id_check_parameter():
    """Test that LCMSMetadataGenerator accepts skip_sample_id_check parameter."""
    generator = LCMSMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_lcms.csv",
        database_dump_json_path="test_output.json",
        raw_data_url="https://example.com/raw/",
        process_data_url="https://example.com/processed/",
        test=True,
        skip_sample_id_check=True,
    )
    assert generator.skip_sample_id_check is True


def test_nom_skip_sample_id_check_parameter():
    """Test that NOMMetadataGenerator accepts skip_sample_id_check parameter."""
    generator = NOMMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_nom.csv",
        database_dump_json_path="test_output.json",
        raw_data_url="https://example.com/raw/",
        process_data_url="https://example.com/processed/",
        test=True,
        skip_sample_id_check=True,
    )
    assert generator.skip_sample_id_check is True


def test_lcms_metab_skip_sample_id_check_parameter():
    """Test that LCMSMetabolomicsMetadataGenerator accepts skip_sample_id_check parameter."""
    generator = LCMSMetabolomicsMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_lcms.csv",
        database_dump_json_path="test_output.json",
        raw_data_url="https://example.com/raw/",
        process_data_url="https://example.com/processed/",
        test=True,
        skip_sample_id_check=True,
    )
    assert generator.skip_sample_id_check is True


def test_lcms_lipid_skip_sample_id_check_parameter():
    """Test that LCMSLipidomicsMetadataGenerator accepts skip_sample_id_check parameter."""
    generator = LCMSLipidomicsMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_lcms.csv",
        database_dump_json_path="test_output.json",
        raw_data_url="https://example.com/raw/",
        process_data_url="https://example.com/processed/",
        test=True,
        skip_sample_id_check=True,
    )
    assert generator.skip_sample_id_check is True


def test_di_nom_skip_sample_id_check_parameter():
    """Test that DINOMMetaDataGenerator accepts skip_sample_id_check parameter."""
    generator = DINOMMetaDataGenerator(
        metadata_file="tests/test_data/test_metadata_file_nom.csv",
        database_dump_json_path="test_output.json",
        raw_data_url="https://example.com/raw/",
        process_data_url="https://example.com/processed/",
        test=True,
        skip_sample_id_check=True,
    )
    assert generator.skip_sample_id_check is True


def test_lcms_nom_skip_sample_id_check_parameter():
    """Test that LCMSNOMMetadataGenerator accepts skip_sample_id_check parameter."""
    generator = LCMSNOMMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_nom.csv",
        database_dump_json_path="test_output.json",
        raw_data_url="https://example.com/raw/",
        process_data_url="https://example.com/processed/",
        test=True,
        skip_sample_id_check=True,
    )
    assert generator.skip_sample_id_check is True


def test_skip_sample_id_check_production_mode():
    """Test that skip_sample_id_check works even when test=False (production mode)."""
    generator = GCMSMetabolomicsMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_gcms.csv",
        database_dump_json_path="test_output.json",
        raw_data_url="https://example.com/raw/",
        process_data_url="https://example.com/processed/",
        configuration_file_name="test_config.toml",
        test=False,
        skip_sample_id_check=True,
    )
    assert generator.skip_sample_id_check is True
    assert generator.test is False
