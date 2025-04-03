# -*- coding: utf-8 -*-
import argparse
from src.gcms_metadata_generator import GCMSMetabolomicsMetadataGenerator
from src.lcms_metadata_generator import LCMSLipidomicsMetadataGenerator
from src.nom_metadata_generator import NOMMetadataGenerator


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
        gcms_metab 'GCMSMetabolomicsMetadataGenerator'
        nom for 'NOMMetadataGenerator'.
    --metadata_file : str
        Path to the input CSV metadata file.
        Example: See example_metadata_file.csv in this directory.
    --database_dump_json_path : str
        Path where the output database dump JSON file will be saved.
    --raw_data_url : str
        URL base for the raw data files.
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
        "--metadata_file", required=True, help="Path to the input CSV metadata file"
    )
    parser.add_argument(
        "--database_dump_json_path",
        required=True,
        help="Path where the output database dump JSON file will be saved",
    )
    parser.add_argument(
        "--raw_data_url", required=True, help="URL base for the raw data files"
    )
    parser.add_argument(
        "--process_data_url",
        required=True,
        help="URL base for the processed data files",
    )
    parser.add_argument(
        "--minting_config_creds",
        required=False,
        help="Path to the config file with credentials for minting IDs. Default uses .env variables for credentials",
    )
    parser.add_argument(
        "--calibration_standard",
        required=False,
        help="Calibration standard to use for the GCMS metadata generation. Must be a value from the NMDC Schema. Default is 'fames'.",
    )
    parser.add_argument(
        "--configuration_file",
        required=False,
        help="Path to the configuration file for the GCMS metadata generator. Default is 'emsl_gcms_corems_params.toml'.",
    )

    args = parser.parse_args()
    if args.generator not in ["lcms_lipid", "gcms_metab", "nom"]:
        raise ValueError(
            "Invalid generator specified. Choose from 'lcms_lipid', 'gcms_metab', or 'nom'."
        )
    if args.generator == "lcms_lipid":
        generator = LCMSLipidomicsMetadataGenerator(
            metadata_file=args.metadata_file,
            database_dump_json_path=args.database_dump_json_path,
            raw_data_url=args.raw_data_url,
            process_data_url=args.process_data_url,
            minting_config_creds=args.minting_config_creds,
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
        )
    elif args.generator == "nom":
        generator = NOMMetadataGenerator(
            metadata_file=args.metadata_file,
            database_dump_json_path=args.database_dump_json_path,
            raw_data_url=args.raw_data_url,
            process_data_url=args.process_data_url,
            minting_config_creds=args.minting_config_creds,
        )

    generator.run()


if __name__ == "__main__":
    main()
