import ast
from functools import wraps

import click
import pandas as pd

from nmdc_ms_metadata_gen.di_nom_metadata_generator import DINOMMetaDataGenerator
from nmdc_ms_metadata_gen.gcms_metab_metadata_generator import (
    GCMSMetabolomicsMetadataGenerator,
)
from nmdc_ms_metadata_gen.lcms_lipid_metadata_generator import (
    LCMSLipidomicsMetadataGenerator,
)
from nmdc_ms_metadata_gen.lcms_metab_metadata_generator import (
    LCMSMetabolomicsMetadataGenerator,
)
from nmdc_ms_metadata_gen.lcms_nom_metadata_generator import LCMSNOMMetadataGenerator
from nmdc_ms_metadata_gen.material_processing_generator import (
    MaterialProcessingMetadataGenerator,
)


@click.group()
def cli():
    """Thank you for using the NMDC Mass Spectrometry Metadata Generator CLI. Here you can run different metadata generation process compatible with the NMDC API."""
    pass


def global_options(f):
    """Decorator to add global options to commands"""

    @click.option(
        "--rerun",
        is_flag=True,
        default=False,
        help="Indicates the script is expecting data that has been re-processed.",
    )
    @click.option(
        "--raw-data-url", default=None, help="URL base for the raw data files."
    )
    @click.option(
        "--minting-config-creds",
        default=None,
        help="Path to the config file with credentials.",
    )
    @click.option(
        "--workflow-version", default=None, help="Version of the workflow to use."
    )
    @click.option(
        "--metadata-file", required=True, help="Path to the input CSV metadata file"
    )
    @click.option(
        "--database-dump-path",
        required=True,
        help="Path where the output database dump JSON file will be saved",
    )
    @click.option(
        "--process-data-url",
        required=True,
        help="URL base for the processed data files.",
    )
    @wraps(f)
    def wrapper(*args, **kwargs):
        return f(*args, **kwargs)

    return wrapper


def material_processing_options(f):
    """Decorator to add material processing options to commands"""

    @click.option(
        "--minting-config-creds",
        default=None,
        help="Path to the config file with credentials.",
    )
    @click.option(
        "--yaml-outline-path",
        required=True,
        help="Path to YAML file that contains the sample processing steps.",
    )
    @click.option(
        "--sample-to-dg-mapping-path",
        required=True,
        help="Path to CSV file mapping biosample ids to their data generation record id.",
    )
    @click.option(
        "--sample-specific-info-path",
        required=False,
        default=None,
        help="Path to a CSV file containing sample specific information.",
    )
    @click.option(
        "--database-dump-path",
        required=True,
        help="Path where the output database dump JSON file will be saved",
    )
    @click.option(
        "--study-id",
        required=True,
        help="The id of the study the samples are related to.",
    )
    @click.option(
        "--test",
        is_flag=True,
        default=False,
        help="Deliminates the run as a test to skip extra mongo db checks.",
    )
    @wraps(f)
    def wrapper(*args, **kwargs):
        return f(*args, **kwargs)

    return wrapper


def existing_option(f):
    """Decorator for existing data objects option"""

    @click.option(
        "--existing-data-objects",
        default=[],
        help="List of existing data object IDs to use for the workflow set has input. Used ONLY in lcms_lipid and lcms_metab generators.",
    )
    @wraps(f)
    def wrapper(*args, **kwargs):
        return f(*args, **kwargs)

    return wrapper


def configuration_options(f):
    """Decorator for GCMS calibration and configuration options"""

    @click.option(
        "--calibration-standard",
        default="fames",
        help="Calibration standard to use for the GCMS metadata generation. Must be a value from the NMDC Schema. Default is 'fames'.",
    )
    @click.option(
        "--configuration-file",
        required=True,
        help="Path to the configuration file for the GCMS metadata generator",
    )
    @wraps(f)
    def wrapper(*args, **kwargs):
        return f(*args, **kwargs)

    return wrapper


@cli.command()
def example():
    """Print an example of a csv where biosample record creation is required because the biosample does not already exist in NMDC"""
    df = pd.read_csv(
        "nmdc_ms_metadata_gen/example_data/biosample_creation_required_info.csv"
    )
    click.echo(df)


@cli.command()
@global_options
def di_nom(
    rerun: bool,
    raw_data_url: str,
    minting_config_creds: str,
    workflow_version: str,
    metadata_file: str,
    database_dump_path: str,
    process_data_url: str,
):
    """Generate Direct Infusion NOM metadata"""
    generator = DINOMMetaDataGenerator(
        metadata_file=metadata_file,
        database_dump_json_path=database_dump_path,
        raw_data_url=raw_data_url,
        process_data_url=process_data_url,
        minting_config_creds=minting_config_creds,
        workflow_version=workflow_version,
    )
    if rerun:
        generator.rerun()
    else:
        generator.run()


@cli.command()
@global_options
def lcms_nom(
    rerun: bool,
    raw_data_url: str,
    minting_config_creds: str,
    workflow_version: str,
    metadata_file: str,
    database_dump_path: str,
    process_data_url: str,
):
    """Generate LCMS NOM metadata"""
    generator = LCMSNOMMetadataGenerator(
        metadata_file=metadata_file,
        database_dump_json_path=database_dump_path,
        raw_data_url=raw_data_url,
        process_data_url=process_data_url,
        minting_config_creds=minting_config_creds,
        workflow_version=workflow_version,
    )
    if rerun:
        generator.rerun()
    else:
        generator.run()


@cli.command()
@global_options
@existing_option
def lcms_lipid(
    rerun: bool,
    raw_data_url: str,
    minting_config_creds: str,
    workflow_version: str,
    metadata_file: str,
    database_dump_path: str,
    process_data_url: str,
    existing_data_objects: list,
):
    """Generate LCMS Lipid metadata"""
    generator = LCMSLipidomicsMetadataGenerator(
        metadata_file=metadata_file,
        database_dump_json_path=database_dump_path,
        raw_data_url=raw_data_url,
        process_data_url=process_data_url,
        minting_config_creds=minting_config_creds,
        workflow_version=workflow_version,
        existing_data_objects=ast.literal_eval(existing_data_objects),
    )
    if rerun:
        generator.rerun()
    else:
        generator.run()


@cli.command()
@global_options
@existing_option
def lcms_metab(
    rerun: bool,
    raw_data_url: str,
    minting_config_creds: str,
    workflow_version: str,
    metadata_file: str,
    database_dump_path: str,
    process_data_url: str,
    existing_data_objects: list,
):
    """Generate LCMS Metabolomics metadata"""
    generator = LCMSMetabolomicsMetadataGenerator(
        metadata_file=metadata_file,
        database_dump_json_path=database_dump_path,
        raw_data_url=raw_data_url,
        process_data_url=process_data_url,
        minting_config_creds=minting_config_creds,
        workflow_version=workflow_version,
        existing_data_objects=ast.literal_eval(existing_data_objects),
    )
    if rerun:
        generator.rerun()
    else:
        generator.run()


@cli.command()
@global_options
@configuration_options
def gcms_metab(
    rerun: bool,
    raw_data_url: str,
    minting_config_creds: str,
    workflow_version: str,
    metadata_file: str,
    database_dump_path: str,
    process_data_url: str,
    calibration_standard: str,
    configuration_file: str,
):
    """Generate GCMS Metabolomics metadata"""
    generator = GCMSMetabolomicsMetadataGenerator(
        metadata_file=metadata_file,
        database_dump_json_path=database_dump_path,
        raw_data_url=raw_data_url,
        process_data_url=process_data_url,
        minting_config_creds=minting_config_creds,
        workflow_version=workflow_version,
        calibration_standard=calibration_standard,
        configuration_file_name=configuration_file,
    )
    if rerun:
        generator.rerun()
    else:
        generator.run()


@cli.command()
@material_processing_options
def material_processing(
    minting_config_creds: str,
    yaml_outline_path: str,
    database_dump_path: str,
    study_id: str,
    sample_to_dg_mapping_path: str,
    sample_specific_info_path: str,
    test: bool,
):
    """Generate Material Processing metadata"""
    generator = MaterialProcessingMetadataGenerator(
        database_dump_json_path=database_dump_path,
        study_id=study_id,
        yaml_outline_path=yaml_outline_path,
        sample_to_dg_mapping_path=sample_to_dg_mapping_path,
        minting_config_creds=minting_config_creds,
        sample_specific_info_path=sample_specific_info_path,
        test=test,
    )
    generator.run()


if __name__ == "__main__":
    cli()
