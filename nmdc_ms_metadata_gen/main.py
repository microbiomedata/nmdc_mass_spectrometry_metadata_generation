# -*- coding: utf-8 -*-
import argparse
from .gcms_metab_metadata_generator import GCMSMetabolomicsMetadataGenerator
from .lcms_lipid_metadata_generator import LCMSLipidomicsMetadataGenerator
from .lcms_metab_metadata_generator import LCMSMetabolomicsMetadataGenerator
from .lcms_nom_metadata_generator import LCMSNOMMetadataGenerator
from .di_nom_metadata_generator import DINOMMetaDataGenerator
import click
from functools import wraps


def main():
    """
    Parse command-line arguments and run the a metadata generator.

    This function sets up argument parsing for the script, initializes a
    the chosen metadata generator instance with the provided arguments, and
    runs the metadata generation process.

    Parameters
    ----------
    None

    Returns
    -------
    None

    Side Effects
    ------------
    Generates a JSON file with the database dump at the specified path.

    Command-line Arguments
    ----------------------
    --generator : str
        The name of the metadata generator to use.
        Options:
        lcms_lipid for 'LipidomicsMetadataGenerator'
        gcms_metab for 'GCMSMetabolomicsMetadataGenerator'
        lcms_nom for 'LCMSNOMMetadataGenerator'
        di_nom for 'DINOMMetadataGenerator'
    --rerun : bool
        If True, this will indicate the run is a rerun. Default is False.
    --metadata_file : str
        Path to the input CSV metadata file.
        Example: See example_metadata_file.csv in this directory.
    --database_dump_json_path : str
        Path where the output database dump JSON file will be saved.
    --raw_data_url : str, optional
        URL base for the raw data files. Optional if raw_data_url column is provided in metadata.
        Example: 'https://nmdcdemo.emsl.pnnl.gov/nom/1000soils/raw/'
    --process_data_url : str
        URL base for the processed data files.
        Example: 'https://nmdcdemo.emsl.pnnl.gov/nom/1000soils/results/'
    --minting_config_creds : str
        Path to the config file with credentials for minting IDs.
        Default: uses .env variables for credentials.
    --calibration_standard : str
        Calibration standard to use for the GCMS metadata generation.
        Must be a value from the NMDC Schema.
        Default: 'fames'.
    --configuration_file : str
        Path to the configuration file for the GCMS metadata generator.
        Default: 'emsl_gcms_corems_params.toml'.
    --minting_config_creds : str
        Path to the config file with credentials for minting IDs.
        OPTIONAL: uses .env variables for credentials.
    --workflow_version : str, optional
        Version of the workflow to use. If not provided, it will be fetched from the Git URL.
    --existing_data_objects : list[str], optional
        List of existing data object IDs to use for the workflow set has input. Used ONLY in lcms_lipid and lcms_metab generators.
    Notes
    -----
    See example_data directory in this package for an example of
    the expected metadata file format.
    """
    parser = argparse.ArgumentParser(
        description="Generate NMDC metadata from input files"
    )
    parser.add_argument(
        "--generator",
        required=True,
        help="Metadata generator to use (lcms_lipid, gcms_metab, nom)",
    )
    parser.add_argument(
        "--rerun",
        help="Is this a rerun? (True/False)",
        type=bool,
        default=False,
    )
    parser.add_argument(
        "--metadata_file", required=True, help="Path to the input CSV metadata file"
    )
    parser.add_argument(
        "--database_dump_json_path",
        required=True,
        help="Path where the output database dump JSON file will be saved",
    )
    parser.add_argument(
        "--raw_data_url",
        required=False,
        help="URL base for the raw data files. Optional if raw_data_url column is provided in metadata.",
    )
    parser.add_argument(
        "--process_data_url",
        required=True,
        help="URL base for the processed data files.",
    )
    parser.add_argument(
        "--minting_config_creds",
        required=False,
        help="Path to the config file with credentials for minting IDs. Default uses .env variables for credentials",
    )
    parser.add_argument(
        "--calibration_standard",
        required=False,
        default="fames",
        help="Calibration standard to use for the GCMS metadata generation. Must be a value from the NMDC Schema. Default is 'fames'.",
    )
    parser.add_argument(
        "--configuration_file",
        required=False,
        default="emsl_gcms_corems_params.toml",
        help="Path to the configuration file for the GCMS metadata generator. Default is 'emsl_gcms_corems_params.toml'.",
    )
    parser.add_argument(
        "--workflow_version",
        required=False,
        help="Version of the workflow to use. If not provided, it will be fetched from the Git URL.",
    )
    parser.add_argument(
        "--existing_data_objects",
        required=False,
        default=[],
        help="List of existing data object IDs to use for the workflow set has input. Used ONLY in lcms_lipid and lcms_metab generators.",
    )

    args = parser.parse_args()

    # Validate that URL parameters are provided either as CLI arguments or in metadata columns
    import pandas as pd

    try:
        metadata_df = pd.read_csv(args.metadata_file)
        has_raw_url_column = "raw_data_url" in metadata_df.columns

        # Check if we have either CLI args or metadata columns for raw data URLs
        if not args.raw_data_url and not has_raw_url_column:
            raise ValueError(
                "Either --raw_data_url must be provided as CLI argument or raw_data_url column must exist in metadata file."
            )

        # Process data URL is always required as CLI argument
        if not args.process_data_url:
            raise ValueError("--process_data_url must be provided as CLI argument.")

        # Set raw_data_url to None if not specified but column exists
        if not args.raw_data_url and has_raw_url_column:
            args.raw_data_url = None

    except FileNotFoundError:
        raise ValueError(f"Metadata file not found: {args.metadata_file}")
    except Exception as e:
        raise ValueError(f"Error reading metadata file: {e}")

    if args.generator not in ["lcms_lipid", "gcms_metab", "di_nom", "lcms_nom"]:
        raise ValueError(
            "Invalid generator specified. Choose from 'lcms_lipid', 'gcms_metab', 'di_nom', or 'lcms_nom'."
        )
    if args.generator == "lcms_lipid":
        generator = LCMSLipidomicsMetadataGenerator(
            metadata_file=args.metadata_file,
            database_dump_json_path=args.database_dump_json_path,
            raw_data_url=args.raw_data_url,
            process_data_url=args.process_data_url,
            minting_config_creds=args.minting_config_creds,
            workflow_version=args.workflow_version,
            existing_data_objects=args.existing_data_objects,
        )
    elif args.generator == "gcms_metab":
        generator = GCMSMetabolomicsMetadataGenerator(
            metadata_file=args.metadata_file,
            database_dump_json_path=args.database_dump_json_path,
            raw_data_url=args.raw_data_url,
            process_data_url=args.process_data_url,
            minting_config_creds=args.minting_config_creds,
            calibration_standard=args.calibration_standard,
            configuration_file_name=args.configuration_file,
            workflow_version=args.workflow_version,
        )
    elif args.generator == "lcms_nom":
        generator = LCMSNOMMetadataGenerator(
            metadata_file=args.metadata_file,
            database_dump_json_path=args.database_dump_json_path,
            raw_data_url=args.raw_data_url,
            process_data_url=args.process_data_url,
            minting_config_creds=args.minting_config_creds,
            workflow_version=args.workflow_version,
        )
    elif args.generator == "di_nom":
        generator = DINOMMetaDataGenerator(
            metadata_file=args.metadata_file,
            database_dump_json_path=args.database_dump_json_path,
            raw_data_url=args.raw_data_url,
            process_data_url=args.process_data_url,
            minting_config_creds=args.minting_config_creds,
            workflow_version=args.workflow_version,
        )
    if args.rerun:
        generator.rerun()
    else:
        generator.run()


def global_options(f):
    """Decorator to add global options to commands"""

    @click.option(
        "--rerun",
        is_flag=True,
        default=False,
        help="Indicates the script is expecting data that has been re-processed.",
    )
    @click.option(
        "--raw_data_url", default=None, help="URL base for the raw data files."
    )
    @click.option(
        "--minting_config_creds",
        default=None,
        help="Path to the config file with credentials.",
    )
    @click.option(
        "--workflow_version", default=None, help="Version of the workflow to use."
    )
    @click.option(
        "--metadata_file", required=True, help="Path to the input CSV metadata file"
    )
    @click.option(
        "--database_dump_path",
        required=True,
        help="Path where the output database dump JSON file will be saved",
    )
    @click.option(
        "--process_data_url",
        required=True,
        help="URL base for the processed data files.",
    )
    @wraps(f)
    def wrapper(*args, **kwargs):
        return f(*args, **kwargs)

    return wrapper


def existing_option(f):
    "Decorator for existing data objects option"

    @click.option(
        "--existing_data_objects",
        default=[],
        help="List of existing data object IDs to use for the workflow set has input. Used ONLY in lcms_lipid and lcms_metab generators.",
    )
    @wraps(f)
    def wrapper(*args, **kwargs):
        return f(*args, **kwargs)

    return wrapper


@click.group()
def cli():
    """Your CLI tool description"""
    pass


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
        existing_data_objects=existing_data_objects,
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
        existing_data_objects=existing_data_objects,
    )
    if rerun:
        generator.rerun()
    else:
        generator.run()


if __name__ == "__main__":
    cli()
