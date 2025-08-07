# -*- coding: utf-8 -*-
from click.testing import CliRunner
from datetime import datetime
import os
import sys
from pathlib import Path

# Add the parent directory to the path to import your CLI
sys.path.insert(0, str(Path(__file__).parent.parent))
from main import cli  # Import your Click CLI group


def test_cli_lcms_lipid():
    """Test the lcms-lipid command."""
    runner = CliRunner()
    current_directory = os.path.dirname(__file__)
    csv_file_path = os.path.join(
        current_directory, "test_data", "test_metadata_file_lcms_lipid.csv"
    )
    output_file = os.path.join(
        current_directory,
        "test_data",
        f"test_database_lipid_cli_{datetime.now().strftime('%Y%m%d%H%M%S')}.json",
    )

    result = runner.invoke(
        cli,
        [
            "lcms-lipid",
            "--metadata-file",
            csv_file_path,
            "--database-dump-path",
            output_file,
            "--raw-data-url",
            "https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_raw_lcms_lipid/",
            "--process-data-url",
            "https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_processed_lcms_lipid/",
        ],
    )
    assert result.exit_code == 0
    assert os.path.exists(output_file)


def test_cli_lcms_lipid_rerun():
    """Test the lcms-lipid command with rerun option."""
    runner = CliRunner()
    current_directory = os.path.dirname(__file__)
    csv_file_path = os.path.join(
        current_directory, "test_data", "test_metadata_file_lcms_lipid_rerun.csv"
    )
    output_file = os.path.join(
        current_directory,
        "test_data",
        f"test_database_lcms_lipid_rerun_cli_{datetime.now().strftime('%Y%m%d%H%M%S')}.json",
    )

    result = runner.invoke(
        cli,
        [
            "lcms-lipid",
            "--metadata-file",
            str(csv_file_path),
            "--database-dump-path",
            str(output_file),
            "--raw-data-url",
            "https://nmdcdemo.emsl.pnnl.gov/lipidomics/blanchard_11_8ws97026/",
            "--process-data-url",
            "https://nmdcdemo.emsl.pnnl.gov/lipidomics/test_data/test_processed_lcms_lipid/",
            "--rerun",  # Click boolean flags don't need a value
        ],
    )

    assert result.exit_code == 0
    assert os.path.exists(output_file)


def test_cli_gcms_with_url_column():
    """Test CLI functionality when using raw_data_url column in metadata file."""
    runner = CliRunner()
    current_directory = os.path.dirname(__file__)
    csv_file_path = os.path.join(
        current_directory, "test_data", "test_metadata_file_gcms_raw_urls.csv"
    )
    output_file = os.path.join(
        current_directory,
        "test_data",
        f"test_database_gcms_with_raw_urls_cli_{datetime.now().strftime('%Y%m%d%H%M%S')}.json",
    )

    result = runner.invoke(
        cli,
        [
            "gcms-metab",
            "--metadata-file",
            str(csv_file_path),
            "--database-dump-path",
            str(output_file),
            "--process-data-url",
            "https://nmdcdemo.emsl.pnnl.gov/metabolomics/test_data/test_processed_gcms_metab/",
            "--configuration-file",
            "emsl_gcms_corems_params.toml",
        ],
    )

    assert result.exit_code == 0
    assert os.path.exists(output_file)


def test_info_command_invalid():
    """Test the info command with invalid input."""
    runner = CliRunner()
    result = runner.invoke(cli, ["info", "invalid-command"])

    assert result.exit_code != 0  # Should fail for invalid command
