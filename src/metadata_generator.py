# -*- coding: utf-8 -*-
import logging
from pathlib import Path
from datetime import datetime
import re
from typing import List, Dict
from abc import ABC
import pandas as pd
import hashlib
import nmdc_schema.nmdc as nmdc
from linkml_runtime.dumpers import json_dumper
from src.metadata_parser import MetadataParser
from src.data_classes import NmdcTypes
from nmdc_api_utilities.instrument_search import InstrumentSearch
from nmdc_api_utilities.configuration_search import ConfigurationSearch
from nmdc_api_utilities.biosample_search import BiosampleSearch
from nmdc_api_utilities.study_search import StudySearch
from nmdc_api_utilities.data_object_search import DataObjectSearch
from nmdc_api_utilities.minter import Minter
import ast
import numpy as np
import toml
import requests
from dotenv import load_dotenv
import os

load_dotenv()
ENV = os.getenv("NMDC_ENV", "prod")

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# TODO: Update script to for Sample Processing - has_input for MassSpectrometry will have to be changed to be a processed sample id - not biosample id


class NMDCMetadataGenerator(ABC):
    """
    Abstract class for generating NMDC metadata objects using provided metadata files and configuration.

    Parameters
    ----------
    metadata_file : str
        Path to the input CSV metadata file.
    database_dump_json_path : str
        Path where the output database dump JSON file will be saved.
    raw_data_url : str
        Base URL for the raw data files.
    process_data_url : str
        Base URL for the processed data files.
    """

    def __init__(
        self,
        metadata_file: str,
        database_dump_json_path: str,
        raw_data_url: str,
        process_data_url: str,
    ):
        self.metadata_file = metadata_file
        self.database_dump_json_path = database_dump_json_path
        self.raw_data_url = raw_data_url
        self.process_data_url = process_data_url
        self.raw_data_category = "instrument_data"

    def load_credentials(self, config_file: str = None) -> tuple:
        """
        Load the client ID and secret from the environment or a configuration file.

        Parameters
        ----------
        config_file: str
            The path to the configuration file.

        Returns
        -------
        tuple
            A tuple containing the client ID and client secret.

        """
        client_id = os.getenv("CLIENT_ID")
        client_secret = os.getenv("CLIENT_SECRET")

        if not client_id or not client_secret:
            if config_file:
                config_file = Path(config_file)
                try:
                    config = toml.load(config_file)
                    client_id = config.get("CLIENT_ID")
                    client_secret = config.get("CLIENT_SECRET")
                except FileNotFoundError:
                    raise ValueError(f"Config file {config_file} not found.")
                except toml.TomlDecodeError:
                    raise ValueError("Error decoding TOML from the config file.")
                except KeyError:
                    raise ValueError(
                        "Config file must contain CLIENT_ID and CLIENT_SECRET. If generating biosample ids BIO_API_KEY is required."
                    )

        if not client_id or not client_secret:
            raise ValueError(
                "CLIENT_ID and CLIENT_SECRET must be set either in environment variables or passed in the config file.\nThey must be named CLIENT_ID and CLIENT_SECRET respectively."
            )

        return client_id, client_secret

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

    def start_nmdc_database(self) -> nmdc.Database:
        """
        Initialize and return a new NMDC Database instance.

        Returns
        -------
        nmdc.Database
            A new instance of an NMDC Database.

        Notes
        -----
        This method simply creates and returns a new instance of the NMDC
        Database. It does not perform any additional initialization or
        configuration.

        """
        return nmdc.Database()

    def load_metadata(self) -> pd.core.frame.DataFrame:
        """
        Load and group workflow metadata from a CSV file.

        This method reads the metadata CSV file, checks for uniqueness in
        specified columns, checks that biosamples exist, and groups the data by biosample ID.

        Returns
        -------
        pd.core.frame.DataFrame
            A DataFrame containing the loaded and grouped metadata.

        Raises
        ------
        FileNotFoundError
            If the `metadata_file` does not exist.
        ValueError
            If values in columns 'Raw Data File',
            and 'Processed Data Directory' are not unique.

        Notes
        -----
        See example_metadata_file.csv in this directory for an example of
        the expected input file format.
        """
        try:
            metadata_df = pd.read_csv(self.metadata_file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Metadata file not found: {self.metadata_file}")

        # Check for uniqueness in specified columns
        columns_to_check = self.unique_columns
        for column in columns_to_check:
            if not metadata_df[column].is_unique:
                raise ValueError(f"Duplicate values found in column '{column}'.")

        # Check that all biosamples exist
        biosample_ids = metadata_df["biosample_id"].unique()
        bs_client = BiosampleSearch(env=ENV)
        if pd.isna(biosample_ids)[0] == np.False_:
            if not bs_client.check_ids_exist(list(biosample_ids)):
                raise ValueError("Biosample IDs do not exist in the collection.")

        # Check that all studies exist
        if "biosample.associated_studies" in metadata_df.columns:
            # Convert string to list, make sure the values are unique, conmvert
            try:
                study_ids = ast.literal_eval(
                    metadata_df["biosample.associated_studies"].iloc[0]
                )
            except SyntaxError:
                study_ids = [metadata_df["biosample.associated_studies"].iloc[0]]
            ss_client = StudySearch(env=ENV)
            if not ss_client.check_ids_exist(study_ids):
                raise ValueError("Study IDs do not exist in the collection.")

        return metadata_df

    def clean_dict(self, dict: Dict) -> Dict:
        """
        Clean the dictionary by removing keys with empty or None values.

        Parameters
        ----------
        dict : Dict
            The dictionary to be cleaned.
        Returns
        -------
        Dict
            A new dictionary with keys removed where the values are None, an empty string, or a string with only whitespace.

        """
        return {k: v for k, v in dict.items() if v not in [None, "", ""]}

    def generate_mass_spectrometry(
        self,
        file_path: Path,
        instrument_name: str,
        sample_id: str,
        raw_data_id: str,
        study_id: str,
        processing_institution: str,
        mass_spec_config_name: str,
        start_date: str,
        end_date: str,
        CLIENT_ID: str,
        CLIENT_SECRET: str,
        lc_config_name: str = None,
        calibration_id: str = None,
    ) -> nmdc.DataGeneration:
        """
        Create an NMDC DataGeneration object for mass spectrometry and mint an NMDC ID.

        Parameters
        ----------
        file_path : Path
            File path of the mass spectrometry data.
        instrument_name : str
            Name of the instrument used for data generation.
        sample_id : str
            ID of the input sample associated with the data generation.
        raw_data_id : str
            ID of the raw data object associated with the data generation.
        study_id : str
            ID of the study associated with the data generation.
        processing_institution : str
            Name of the processing institution.
        mass_spec_config_name : str
            Name of the mass spectrometry configuration.
        start_date : str
            Start date of the data generation.
        end_date : str
            End date of the data generation.
        CLIENT_ID : str
            The client ID for the NMDC API.
        CLIENT_SECRET : str
            The client secret for the NMDC API.
        lc_config_name : str
            Name of the liquid chromatography configuration.
        calibration_id : str, optional
            ID of the calibration information generated with the data.
            Default is None, indicating no calibration information.

        Returns
        -------
        nmdc.DataGeneration
            An NMDC DataGeneration object with the provided metadata.

        Notes
        -----
        This method uses the nmdc_api_utilities package to fetch IDs for the instrument
        and configurations. It also mints a new NMDC ID for the DataGeneration object.

        """

        is_client = InstrumentSearch(env=ENV)
        cs_client = ConfigurationSearch(env=ENV)
        minter = Minter(env=ENV)
        nmdc_id = minter.mint(
            nmdc_type=NmdcTypes.MassSpectrometry,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )
        instrument_id = is_client.get_record_by_attribute(
            attribute_name="name",
            attribute_value=instrument_name,
            fields="id",
            exact_match=True,
        )[0]["id"]
        if lc_config_name:
            lc_config_id = cs_client.get_record_by_attribute(
                attribute_name="name",
                attribute_value=lc_config_name,
                fields="id",
                exact_match=True,
            )[0]["id"]
        else:
            lc_config_id = ""
        mass_spec_id = cs_client.get_record_by_attribute(
            attribute_name="name",
            attribute_value=mass_spec_config_name,
            fields="id",
            exact_match=True,
        )[0]["id"]

        data_dict = {
            "id": nmdc_id,
            "name": file_path.stem,
            "description": self.mass_spec_desc,
            "add_date": datetime.now().strftime("%Y-%m-%d"),
            "eluent_introduction_category": self.mass_spec_eluent_intro,
            "has_mass_spectrometry_configuration": mass_spec_id,
            "has_chromatography_configuration": lc_config_id,
            "analyte_category": self.analyte_category,
            "instrument_used": instrument_id,
            "has_input": [sample_id],
            "has_output": [raw_data_id],
            "associated_studies": study_id,
            "processing_institution": processing_institution,
            "start_date": start_date,
            "end_date": end_date,
            "type": NmdcTypes.MassSpectrometry,
        }

        if calibration_id is not None:
            data_dict["generates_calibration"] = calibration_id
        data_dict = self.clean_dict(data_dict)
        mass_spectrometry = nmdc.DataGeneration(**data_dict)

        return mass_spectrometry

    def generate_data_object(
        self,
        file_path: Path,
        data_category: str,
        data_object_type: str,
        description: str,
        base_url: str,
        CLIENT_ID: str,
        CLIENT_SECRET: str,
        was_generated_by: str = None,
        alternative_id: str = None,
    ) -> nmdc.DataObject:
        """
        Create an NMDC DataObject with metadata from the specified file and details.

        This method generates an NMDC DataObject and assigns it a unique NMDC ID.
        The DataObject is populated with metadata derived from the provided file
        and input parameters.

        Parameters
        ----------
        file_path : Path
            Path to the file representing the data object. The file's name is
            used as the `name` attribute.
        data_category : str
            Category of the data object (e.g., 'instrument_data').
        data_object_type : str
            Type of the data object (e.g., 'LC-DDA-MS/MS Raw Data').
        description : str
            Description of the data object.
        base_url : str
            Base URL for accessing the data object, to which the file name is
            appended to form the complete URL.
        CLIENT_ID : str
            The client ID for the NMDC API.
        CLIENT_SECRET : str
            The client secret for the NMDC API.
        was_generated_by : str, optional
            ID of the process or entity that generated the data object
            (e.g., the DataGeneration id or the MetabolomicsAnalysis id).
        alternative_id : str, optional
            An optional alternative identifier for the data object.

        Returns
        -------
        nmdc.DataObject
            An NMDC DataObject instance with the specified metadata.

        Notes
        -----
        This method calculates the MD5 checksum of the file, which may be
        time-consuming for large files.

        """
        mint = Minter(env=ENV)
        nmdc_id = mint.mint(
            nmdc_type=NmdcTypes.DataObject,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )
        data_dict = {
            "id": nmdc_id,
            "data_category": data_category,
            "data_object_type": data_object_type,
            "name": file_path.name,
            "description": description,
            "file_size_bytes": file_path.stat().st_size,
            "md5_checksum": hashlib.md5(file_path.open("rb").read()).hexdigest(),
            "url": base_url + str(file_path.name),
            "type": NmdcTypes.DataObject,
            "was_generated_by": was_generated_by,
            "alternative_identifiers": alternative_id,
        }

        # If any of the data_dict values are None or empty strings, remove them
        data_dict = self.clean_dict(data_dict)
        data_object = nmdc.DataObject(**data_dict)

        return data_object

    def generate_metabolomics_analysis(
        self,
        cluster_name: str,
        raw_data_name: str,
        raw_data_id: str,
        data_gen_id: str,
        processed_data_id: str,
        parameter_data_id: str,
        processing_institution: str,
        CLIENT_ID: str,
        CLIENT_SECRET: str,
        calibration_id: str = None,
        incremeneted_id: str = None,
        metabolite_identifications: List[nmdc.MetaboliteIdentification] = None,
        type: str = NmdcTypes.MetabolomicsAnalysis,
    ) -> nmdc.MetabolomicsAnalysis:
        """
        Create an NMDC MetabolomicsAnalysis object with metadata for a workflow analysis.

        This method generates an NMDC MetabolomicsAnalysis object, including details
        about the analysis, the processing institution, and relevant workflow information.

        Parameters
        ----------
        cluster_name : str
            Name of the cluster or computing resource used for the analysis.
        raw_data_name : str
            Name of the raw data file that was analyzed.
        raw_data_id : str
            ID of the raw data object that was analyzed.
        data_gen_id : str
            ID of the DataGeneration object that generated the raw data.
        processed_data_id : str
            ID of the processed data resulting from the analysis.
        parameter_data_id : str
            ID of the parameter data object used for the analysis.
        processing_institution : str
            Name of the institution where the analysis was performed.
        CLIENT_ID : str
            The client ID for the NMDC API.
        CLIENT_SECRET : str
            The client secret for the NMDC API.
        calibration_id : str, optional
            ID of the calibration information used for the analysis.
            Default is None, indicating no calibration information.
        incremeneted_id : str, optional
            An optional incremented ID for the MetabolomicsAnalysis object.
            If not provided, a new NMDC ID will be minted.
        metabolite_identifications : List[nmdc.MetaboliteIdentification], optional
            List of MetaboliteIdentification objects associated with the analysis.
            Default is None, which indicates no metabolite identifications.
        type : str, optional
            The type of the analysis. Default is NmdcTypes.MetabolomicsAnalysis.

        Returns
        -------
        nmdc.MetabolomicsAnalysis
            An NMDC MetabolomicsAnalysis instance with the provided metadata.

        Notes
        -----
        The 'started_at_time' and 'ended_at_time' fields are initialized with
        placeholder values and should be updated with actual timestamps later
        when the processed files are iterated over in the run method.

        """
        if incremeneted_id is None:
            # If no incremented id is provided, mint a new one
            mint = Minter(env=ENV)
            nmdc_id = (
                mint.mint(
                    nmdc_type=NmdcTypes.MetabolomicsAnalysis,
                    client_id=CLIENT_ID,
                    client_secret=CLIENT_SECRET,
                )
                + ".1"
            )

        data_dict = {
            "id": incremeneted_id if incremeneted_id is not None else nmdc_id,
            "name": f"{self.workflow_analysis_name} for {raw_data_name}",
            "description": self.workflow_description,
            "processing_institution": processing_institution,
            "execution_resource": cluster_name,
            "git_url": self.workflow_git_url,
            "version": self.workflow_version,
            "was_informed_by": data_gen_id,
            "has_input": [raw_data_id, parameter_data_id],
            "has_output": [processed_data_id],
            "started_at_time": "placeholder",
            "ended_at_time": "placeholder",
            "type": type,
            "metabolomics_analysis_category": self.workflow_category,
        }

        if calibration_id is not None:
            data_dict["uses_calibration"] = calibration_id

        if metabolite_identifications is not None:
            data_dict["has_metabolite_identifications"] = metabolite_identifications
        data_dict = self.clean_dict(data_dict)
        metab_analysis = nmdc.MetabolomicsAnalysis(**data_dict)

        return metab_analysis

    def update_outputs(
        self,
        analysis_obj: object,
        raw_data_obj_id: str,
        parameter_data_id: str,
        processed_data_id_list: list,
        mass_spec_obj: object = None,
        rerun: bool = False,
    ) -> None:
        """
        Update output references for Mass Spectrometry and Workflow Analysis objects.

        This method assigns the output references for a Mass Spectrometry object
        and a Workflow Execution Analysis object. It sets `mass_spec_obj.has_output`
        to the ID of `raw_data_obj` and `analysis_obj.has_output` to a list of
        processed data IDs.

        Parameters
        ----------
        analysis_obj : object
            The Workflow Execution Analysis object to update
            (e.g., MetabolomicsAnalysis).
        raw_data_obj_id : str
            The Raw Data Object associated with the Mass Spectrometry.
        parameter_data_id : str
            ID of the data object representing the parameter data used for the analysis.
        processed_data_id_list : list
            List of IDs representing processed data objects associated with
            the Workflow Execution.
        mass_spec_obj : object , optional
            The Mass Spectrometry object to update. Optional for rerun cases.
        rerun : bool, optional
            If True, this indicates the run is a rerun, and the method will not set `mass_spec_obj.has_output` because there is not one.
            Default is False.

        Returns
        -------
        None

        Notes
        ------
        - Sets `mass_spec_obj.has_output` to [raw_data_obj.id].
        - Sets `analysis_obj.has_output` to `processed_data_id_list`.

        """
        if not rerun:
            # if it is not a rerun, set the mass spec object, otherwise there will not be a mass spec object
            mass_spec_obj.has_output = [raw_data_obj_id]
        analysis_obj.has_input = parameter_data_id
        analysis_obj.has_output = processed_data_id_list

    def dump_nmdc_database(self, nmdc_database: nmdc.Database) -> None:
        """
        Dump the NMDC database to a JSON file.

        This method serializes the NMDC Database instance to a JSON file
        at the specified path.

        Parameters
        ----------
        nmdc_database : nmdc.Database
            The NMDC Database instance to dump.

        Returns
        -------
        None

        Side Effects
        ------------
        Writes the database content to the file specified by
        `self.database_dump_json_path`.
        """
        json_dumper.dump(nmdc_database, self.database_dump_json_path)
        logging.info("Database successfully dumped in %s", self.database_dump_json_path)

    def handle_biosample(self, row: pd.Series) -> tuple:
        """
        Process biosample information from metadata row.

        Checks if a biosample ID exists in the row. If it does, returns the existing
        biosample information. If not, generates a new biosample.

        Parameters
        ----------
        row : pd.Series
            A row from the metadata DataFrame containing biosample information

        Returns
        -------
        tuple
            A tuple containing:
            - emsl_metadata : Dict
                Parsed metadata from input csv row
            - biosample_id : str
                The ID of the biosample (existing or newly generated)

        """
        parser = MetadataParser()
        emsl_metadata = parser.parse_biosample_metadata(row)
        biosample_id = emsl_metadata["biosample_id"]
        return emsl_metadata, biosample_id

    def check_for_biosamples(
        self,
        metadata_df: pd.DataFrame,
        nmdc_database_inst: nmdc.Database,
        CLIENT_ID: str,
        CLIENT_SECRET: str,
    ) -> None:
        """
        This method verifies the presence of the 'biosample_id' in the provided metadata DataFrame. It will loop over each row to verify the presence of the 'biosample_id', giving the option for some rows to need generation and some to already exist.
        If the 'biosample_id' is missing, it checks for the presence of required columns to generate a new biosample_id using the NMDC API. If they are all there, the function calls the dynam_parse_biosample_metadata method from the MetadataParser class to create the JSON for the biosample.
        If the required columns are missing and there is no biosample_id - it raises a ValueError.
        After the biosample_id is generated,it updates the DataFrame row with the newly minted biosample_id and the NMDC database instance with the new biosample JSON.

        Parameters
        ----------
        metadata_df : pd.DataFrame
            the dataframe containing the metadata information.
        nmdc_database_inst : nmdc.Database
            The NMDC Database instance to add the biosample to if one needs to be generated.
        CLIENT_ID : str
            The client ID for the NMDC API. Used to mint a biosmaple id if one does not exist.
        CLIENT_SECRET : str
            The client secret for the NMDC API. Used to mint a biosmaple id if one does not exist.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the 'biosample.name' column is missing and 'biosample_id' is empty.
            If any required columns for biosample generation are missing.

        """
        parser = MetadataParser()
        metadata_df["biosample_id"] = metadata_df["biosample_id"].astype("object")

        if "biosample.name" not in metadata_df.columns:
            # if biosample.name does not exists check if biosample_id is empty. biosample_id should not be empty if biosample.name does not exist
            if len(metadata_df["biosample_id"]) == len(
                metadata_df.dropna(subset=["biosample_id"])
            ):
                return
            else:
                raise ValueError(
                    "biosample.name column is missing from the metadata file. Please provide biosample.name or biosample_id for each row. biosample.name is required to generate new biosample_id."
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
                bio_api_key = self.load_bio_credentials(
                    config_file=self.minting_config_creds
                )
                # Generate biosamples if no biosample_id in spreadsheet
                biosample_metadata = parser.dynam_parse_biosample_metadata(
                    row=row, bio_api_key=bio_api_key
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

    def check_doj_urls(self, metadata_df: pd.DataFrame, url_columns: List) -> None:
        """
        Check if the URLs in the input list already exist in the database.

        Parameters
        ----------
        metadata_df : pd.DataFrame
            The DataFrame containing the metadata information.
        url_columns : List
            The list of columns in the DataFrame that contain URLs to check.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If any URL in the metadata DataFrame is invalid or inaccessible.
        FileNotFoundError
            If no files are found in the specified directory columns.

        """
        urls = []
        for col in url_columns:
            if "directory" in col:
                # if its a directory, we need to gather all the files in the directory
                file_data_paths = [
                    list(Path(x).glob("**/*")) for x in metadata_df[col].to_list()
                ]
                # Add a check that the processed data directory is not empty
                if not any(file_data_paths):
                    raise FileNotFoundError(
                        f"No files found in {col}: " f"{metadata_df[col]}"
                    )
                file_data_paths = [
                    file for sublist in file_data_paths for file in sublist
                ]
                if "process" in col:
                    urls += [
                        self.process_data_url + str(x.relative_to(Path(x).parents[1]))
                        for x in file_data_paths
                    ]
                elif "raw" in col:
                    urls += [self.raw_data_url + str(x.name) for x in file_data_paths]
                # check if the urls are valid
                for url in urls[
                    :5
                ]:  # Check up to 5 URLs, or fewer if the list is shorter
                    try:
                        response = requests.head(url)
                        if response.status_code != 200:
                            raise ValueError(f"URL {url} is not accessible.")
                    except requests.RequestException as e:
                        raise ValueError(f"URL {url} is not accessible. Error: {e}")
            else:
                # if its a file, we need to gather the file paths
                file_data_paths = [Path(x) for x in metadata_df[col].to_list()]
                if "process" in col:
                    urls += [
                        self.process_data_url + str(x.name) for x in file_data_paths
                    ]
                elif "raw" in col:
                    urls += [self.raw_data_url + str(x.name) for x in file_data_paths]
                # check if the urls are valid
                for url in urls[
                    :5
                ]:  # Check up to 5 URLs, or fewer if the list is shorter
                    try:
                        response = requests.head(url)
                        if response.status_code != 200:
                            raise ValueError(f"URL {url} is not accessible.")
                    except requests.RequestException as e:
                        raise ValueError(f"URL {url} is not accessible. Error: {e}")

        doj_client = DataObjectSearch(env=ENV)
        resp = doj_client.get_batch_records(
            id_list=urls, search_field="url", fields="id"
        )
        if resp:
            raise ValueError(
                f"The following URLs already exist in the database: {', '.join(resp)}"
            )

    def generate_biosample(
        self, biosamp_metadata: dict, CLIENT_ID: str, CLIENT_SECRET: str
    ) -> nmdc.Biosample:
        """
        Mint a biosample id from the given metadata and create a biosample instance.

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
        mint = Minter(env=ENV)

        # If no biosample id in spreadsheet, mint biosample ids
        if biosamp_metadata["id"] is None:
            biosamp_metadata["id"] = mint.mint(
                nmdc_type=NmdcTypes.Biosample,
                client_id=CLIENT_ID,
                client_secret=CLIENT_SECRET,
            )

        # Filter dictionary to remove any key/value pairs with None as the value
        biosamp_dict = self.clean_dict(biosamp_metadata)

        biosample_object = nmdc.Biosample(**biosamp_dict)

        return biosample_object

    def get_workflow_version(self, workflow_version_git_url: str) -> str:
        """
        Get the version of the workflow from the git repository.

        Parameters
        ----------
        repo_link : str
            The URL of the git repository containing the workflow version.

        Returns
        -------
        str
            The version of the workflow.
        """
        resp = requests.get(workflow_version_git_url)
        if resp.status_code == 200:
            # Regular expression to find the current_version
            match = re.search(r"current_version\s*=\s*([\d.]+)", resp.text)
            if match:
                current_version = match.group(1)
            return current_version
        else:
            logging.warning(
                f"Failed to fetch the workflow version from the Git repository {workflow_version_git_url}"
            )
        return None
