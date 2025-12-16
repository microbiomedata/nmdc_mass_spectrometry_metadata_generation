import os
from pathlib import Path

import nmdc_schema.nmdc as nmdc
import pandas as pd
import toml

import nmdc_ms_metadata_gen
from nmdc_ms_metadata_gen.data_classes import NmdcTypes
from nmdc_ms_metadata_gen.id_pool import IDPool
from nmdc_ms_metadata_gen.metadata_generator import NMDCMetadataGenerator
from nmdc_ms_metadata_gen.metadata_parser import BiosampleMetadataParser


class BiosampleGenerator(NMDCMetadataGenerator):
    """
    Class to handle biosample generation for the NMDC database.

    Parameters
    ----------
    metadata_file : str
        Path to the metadata CSV file containing biosample information.
    database_dump_json_path : str
        Path to output the generated NMDC database JSON file.
    minting_config_creds : str, optional
        Path to the configuration file containing credentials for minting biosample IDs.
    id_pool_size : int, optional
        The size of the ID pool to maintain for minting biosample IDs. Default is 50.
    id_refill_threshold : int, optional
        The threshold at which to refill the ID pool. Default is 10.
    """

    def __init__(
        self,
        metadata_file: str,
        database_dump_json_path: str,
        minting_config_creds: str = None,
        id_pool_size: int = 50,
        id_refill_threshold: int = 10,
    ):
        # Initialize superclass with ID pool parameters
        super().__init__(
            id_pool_size=id_pool_size, id_refill_threshold=id_refill_threshold
        )

        # Add class-specific attributes
        self.metadata_file = metadata_file
        self.database_dump_json_path = database_dump_json_path
        self.minting_config_creds = minting_config_creds

    def run(self) -> dict:
        """
        Main method to run the biosample generation process.

        Returns
        -------
            The generated NMDC database instance containing all generated biosample records as a dictionary.
        """
        # load file
        try:
            metadata_df = pd.read_csv(self.metadata_file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Metadata file not found: {self.metadata_file}")
        # Start NMDC database and make metadata dataframe
        nmdc_database_inst = self.start_nmdc_database()
        # load credentials
        client_id, client_secret = self.load_credentials(
            config_file=self.minting_config_creds
        )
        bio_api_key = self.load_bio_credentials(config_file=self.minting_config_creds)

        self.check_biosample_rows(
            metadata_df=metadata_df,
            nmdc_database_inst=nmdc_database_inst,
            BIO_API_KEY=bio_api_key,
            CLIENT_ID=client_id,
            CLIENT_SECRET=client_secret,
        )
        self.dump_nmdc_database(
            nmdc_database=nmdc_database_inst, json_path=self.database_dump_json_path
        )
        # change db object to dict
        return self.nmdc_db_to_dict(nmdc_database_inst)

    def check_biosample_rows(
        self,
        metadata_df: pd.DataFrame,
        nmdc_database_inst: nmdc.Database,
        CLIENT_ID: str,
        CLIENT_SECRET: str,
        BIO_API_KEY: str,
    ) -> None:
        """
        This method verifies the presence of the 'biosample_id' in the provided metadata DataFrame. It will loop over each row to verify the presence of the 'biosample_id', giving the option for some rows to need generation and some to already exist.
        It checks for the presence of required columns to generate a new biosample_id using the NMDC API. If they are all there, the function calls the dynam_parse_biosample_metadata method from the MetadataParser class to create the JSON for the biosample.
        If the required columns are missing it raises a ValueError.
        After the biosample_id is generated, it updates the DataFrame row with the newly minted biosample_id and the NMDC database instance with the new biosample JSON.

        Parameters
        ----------
        metadata_df : pd.DataFrame
            the dataframe containing the metadata information.
        nmdc_database_inst : nmdc.Database
            The NMDC Database instance to add the biosample to.
        CLIENT_ID : str
            The client ID for the NMDC API. Used to mint a biosmaple id.
        CLIENT_SECRET : str
            The client secret for the NMDC API. Used to mint a biosmaple id.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the 'biosample.name' column is missing and 'biosample_id' is empty.
            If any required columns for biosample generation are missing.

        """
        parser = BiosampleMetadataParser()

        if "biosample.name" not in metadata_df.columns:
            raise ValueError(
                "The 'biosample.name' column is required to create biosamples."
            )
        rows = metadata_df.groupby("biosample.name")

        for _, group in rows:
            row = group.iloc[0]
            if pd.isnull(row.get("biosample_id")):
                required_columns = [
                    "biosample.name",
                    "biosample.associated_studies",
                    "biosample.env_broad_scale",
                    "biosample.env_local_scale",
                    "biosample.env_medium",
                ]
                # Check for the existence of all required columns
                missing_columns = [
                    col for col in required_columns if col not in metadata_df.columns
                ]
                if missing_columns:
                    raise ValueError(
                        f"The following required columns are missing from the DataFrame: {', '.join(missing_columns)}"
                    )

                # Generate biosamples
                biosample_metadata = parser.dynam_parse_biosample_metadata(
                    row=row, bio_api_key=BIO_API_KEY
                )
                biosample = self.generate_biosample(
                    biosamp_metadata=biosample_metadata,
                    CLIENT_ID=CLIENT_ID,
                    CLIENT_SECRET=CLIENT_SECRET,
                )
                biosample_id = biosample.id
                metadata_df.loc[
                    metadata_df["biosample.name"] == row["biosample.name"],
                    "biosample_id",
                ] = biosample_id
                nmdc_database_inst.biosample_set.append(biosample)

    def generate_biosample(
        self, biosamp_metadata: dict, CLIENT_ID: str, CLIENT_SECRET: str
    ) -> nmdc.Biosample:
        """
        Mint a biosample id from the given metadata and create a biosample record.

        Parameters
        ----------
        biosamp_metadata : dict
            The metadata object containing biosample information.
        CLIENT_ID : str
            The client ID for the NMDC API.
        CLIENT_SECRET : str
            The client secret for the NMDC API.

        Returns
        -------
        nmdc.Biosample
            The generated biosample instance.

        """

        # If no biosample id in spreadsheet, mint biosample ids
        if biosamp_metadata["id"] is None:
            biosamp_metadata["id"] = self.id_pool.get_id(
                nmdc_type=NmdcTypes.Biosample,
                client_id=CLIENT_ID,
                client_secret=CLIENT_SECRET,
            )

        # Filter dictionary to remove any key/value pairs with None as the value
        biosamp_dict = self.clean_dict(biosamp_metadata)

        # Add provenance metadata
        biosamp_dict["provenance_metadata"] = self.provenance_metadata

        biosample_record = nmdc.Biosample(**biosamp_dict)

        return biosample_record

    def load_bio_credentials(self, config_file: str = None) -> str:
        """
        Load bio ontology API key from the environment or a configuration file.

        Parameters
        ----------
        config_file: str
            The path to the configuration file.

        Returns
        -------
        str
            The bio ontology API key.

        Raises
        ------
        FileNotFoundError
            If the configuration file is not found, and the API key is not set in the environment.
        ValueError
            If the configuration file is not valid or does not contain the API key.

        """
        BIO_API_KEY = os.getenv("BIO_API_KEY")

        if not BIO_API_KEY:
            if config_file:
                config_file = Path(config_file)
                try:
                    config = toml.load(config_file)
                    BIO_API_KEY = config.get("BIO_API_KEY")
                except FileNotFoundError:
                    raise FileNotFoundError(f"Config file {config_file} not found.")
                except toml.TomlDecodeError:
                    raise ValueError("Error decoding TOML from the config file.")
                except KeyError:
                    raise ValueError(
                        "Config file must contain BIO_API_KEY to generate biosample ids."
                    )

        if not BIO_API_KEY:
            raise ValueError(
                "BIO_API_KEY must be set either in environment variable or passed in the config file. It must be named BIO_API_KEY."
            )

        return BIO_API_KEY
