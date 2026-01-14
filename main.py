import ast
from functools import wraps

import click
import pandas as pd

from nmdc_ms_metadata_gen.biosample_generator import BiosampleGenerator
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
from nmdc_ms_metadata_gen.validate_yaml_outline import validate_yaml_outline


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
    @click.option(
        "--test",
        is_flag=True,
        default=False,
        help="Flags the run as a test to skip extra mongo db checks.",
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
        help="Flags the run as a test to skip extra mongo db checks.",
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


def biosample_generation_options(f):
    """Decorator for biosample generation options"""

    @click.option(
        "--metadata-file", required=True, help="Path to the input CSV metadata file"
    )
    @click.option(
        "--minting-config-creds",
        default=None,
        help="Path to the config file with credentials.",
    )
    @click.option(
        "--database-dump-path",
        required=True,
        help="Path where the output database dump JSON file will be saved",
    )
    @click.option(
        "--id-pool-size",
        default=50,
        help="The size of the ID pool to maintain for minting biosample IDs. Default is 50.",
    )
    @click.option(
        "--id-refill-threshold",
        default=10,
        help="The threshold at which to refill the ID pool. Default is 10.",
    )
    @click.option(
        "--test",
        is_flag=True,
        default=False,
        help="Flags the run as a test to skip extra mongo db id generation.",
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
    test: bool,
):
    """Generate Direct Infusion NOM metadata"""
    generator = DINOMMetaDataGenerator(
        metadata_file=metadata_file,
        database_dump_json_path=database_dump_path,
        raw_data_url=raw_data_url,
        process_data_url=process_data_url,
        minting_config_creds=minting_config_creds,
        workflow_version=workflow_version,
        test=test,
    )
    if rerun:
        metadata = generator.rerun()
    else:
        metadata = generator.run()
    return metadata


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
    test: bool,
):
    """Generate LCMS NOM metadata"""
    generator = LCMSNOMMetadataGenerator(
        metadata_file=metadata_file,
        database_dump_json_path=database_dump_path,
        raw_data_url=raw_data_url,
        process_data_url=process_data_url,
        minting_config_creds=minting_config_creds,
        workflow_version=workflow_version,
        test=test,
    )
    if rerun:
        metadata = generator.rerun()
    else:
        metadata = generator.run()
    return metadata


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
    test: bool,
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
        test=test,
    )
    if rerun:
        metadata = generator.rerun()
    else:
        metadata = generator.run()
    return metadata


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
    test: bool,
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
        test=test,
    )
    if rerun:
        metadata = generator.rerun()
    else:
        metadata = generator.run()
    return metadata


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
    test: bool,
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
        test=test,
    )
    if rerun:
        metadata = generator.rerun()
    else:
        metadata = generator.run()
    return metadata


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
    metadata = generator.run()
    return metadata


@cli.command()
@biosample_generation_options
def biosample_generation(
    metadata_file: str,
    minting_config_creds: str,
    database_dump_path: str,
    id_pool_size: int,
    id_refill_threshold: int,
    test: bool,
):
    generator = BiosampleGenerator(
        metadata_file=metadata_file,
        database_dump_json_path=database_dump_path,
        minting_config_creds=minting_config_creds,
        id_pool_size=id_pool_size,
        id_refill_threshold=id_refill_threshold,
        test=test,
    )

    metadata = generator.run()
    return metadata


@cli.command()
@click.option(
    "--yaml-outline-path",
    required=True,
    help="Path to YAML file that contains the sample processing steps.",
)
@click.option(
    "--protocol-id-list",
    required=False,
    default=None,
    help="Comma separated list of protocol ids to validate. If not provided, all protocols in the outline will be validated.",
)
@click.option(
    "--test",
    is_flag=True,
    default=False,
    help="Whether to run in test mode. Will not use the NMDC API for validation if so.",
)
@click.option(
    "--dump-database",
    is_flag=True,
    default=False,
    help="Whether to dump the NMDC database to a JSON file during validation.",
)
def validate_yaml(
    yaml_outline_path: str, protocol_id_list: list, test: bool, dump_database: bool
):
    """
    Test to make sure yaml will generate valid json if given a random biosample (no adjustments for dg/filename)

    Parameters
    ----------
    yaml_outline_path: str
        Path to yaml outline to validate
    protocol_id_list: list, optional
        List of protocol ids (or names) to validate
    test: bool
        Whether to run in test mode.
    dump_database: bool
        Whether to dump the NMDC database to a JSON file during validation.
    Returns
    -------
    list[dict]
        List of validation results

    Examples
    --------
    Command line example
    `python main.py validate-yaml --yaml-outline-path 'path_to_yaml/example.yaml' --protocol-id-list 'example_protocol1,example_protocol2' --test`
    """
    return validate_yaml_outline(
        yaml_outline_path=yaml_outline_path,
        protocol_id_list=protocol_id_list,
        test=test,
        dump_database=dump_database,
    )


if __name__ == "__main__":
    cli()
