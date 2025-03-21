# -*- coding: utf-8 -*-
import logging
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from typing import List, Dict
from abc import ABC
from dataclasses import asdict
import pandas as pd
import hashlib
from tqdm import tqdm
import nmdc_schema.nmdc as nmdc
from linkml_runtime.dumpers import json_dumper
from src.metadata_parser import MetadataParser
from nmdc_api_utilities.instrument_search import InstrumentSearch
from nmdc_api_utilities.configuration_search import ConfigurationSearch
from nmdc_api_utilities.biosample_search import BiosampleSearch
from nmdc_api_utilities.study_search import StudySearch
from nmdc_api_utilities.data_object_search import DataObjectSearch
from nmdc_api_utilities.minter import Minter
from nmdc_api_utilities.metadata import Metadata
import ast
import numpy as np
from dotenv import load_dotenv

load_dotenv()
import os

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# TODO: Update script to for Sample Processing - has_input for MassSpectrometry will have to be changed to be a processed sample id - not biosample id


@dataclass
class GroupedMetadata:
    """
    Data class for holding grouped metadata information.

    Attributes
    ----------
    biosample_id : str
        Identifier for the biosample.
    processing_type : str
        Type of processing applied to the data.
    processing_institution : str
        Institution responsible for processing the data.
    nmdc_study : float
        Identifier for the NMDC study associated with the data.
    """

    biosample_id: str
    processing_type: str
    processing_institution: str
    nmdc_study: float


@dataclass
class LCMSLipidWorkflowMetadata:
    """
    Data class for holding LC-MS lipidomics workflow metadata information.

    Attributes
    ----------
    processed_data_dir : str
        Directory containing processed data files.
    raw_data_file : str
        Path or name of the raw data file.
    mass_spec_config_name : str
        Name of the mass spectrometry configuration used.
    lc_config_name : str
        Name of the liquid chromatography configuration used.
    instrument_used : str
        Name of the instrument used for analysis.
    instrument_analysis_start_date : str
        Start date of the instrument analysis.
    instrument_analysis_end_date : str
        End date of the instrument analysis.
    execution_resource : float
        Identifier for the execution resource.
    """

    processed_data_dir: str
    raw_data_file: str
    mass_spec_config_name: str
    lc_config_name: str
    instrument_used: str
    instrument_analysis_start_date: str
    instrument_analysis_end_date: str
    execution_resource: float


@dataclass
class GCMSMetabWorkflowMetadata:
    """
    Data class for holding LC-MS lipidomics workflow metadata information.

    Attributes
    ----------
    processed_data_file : str
        Path or name of the processed data file.
    raw_data_file : str
        Path or name of the raw data file.
    mass_spec_config_name : str
        Name of the mass spectrometry configuration used.
    chromat_config_name : str
        Name of the chromatography configuration used.
    instrument_used : str
        Name of the instrument used for analysis.
    instrument_analysis_start_date : str
        Start date of the instrument analysis.
    instrument_analysis_end_date : str
        End date of the instrument analysis.
    execution_resource : float
        Identifier for the execution resource.
    calibration_id : str
        Identifier for the calibration information used.
    """

    biosample_id: str
    nmdc_study: str
    processing_institution: str
    processed_data_file: str
    raw_data_file: str
    mass_spec_config_name: str
    chromat_config_name: str
    instrument_used: str
    instrument_analysis_start_date: str
    instrument_analysis_end_date: str
    execution_resource: float
    calibration_id: str


@dataclass
class NmdcTypes:
    """
    Data class holding NMDC type constants.

    Attributes
    ----------
    Biosample : str
        NMDC type for Biosample.
    MassSpectrometry : str
        NMDC type for Mass Spectrometry.
    MetabolomicsAnalysis : str
        NMDC type for Metabolomics Analysis.
    DataObject : str
        NMDC type for Data Object.
    """

    Biosample: str = "nmdc:Biosample"
    MassSpectrometry: str = "nmdc:MassSpectrometry"
    MetabolomicsAnalysis: str = "nmdc:MetabolomicsAnalysis"
    DataObject: str = "nmdc:DataObject"
    CalibrationInformation: str = "nmdc:CalibrationInformation"
    MetaboliteIdentification: str = "nmdc:MetaboliteIdentification"
    NomAnalysis: str = "nmdc:NomAnalysis"
    OntologyClass: str = "nmdc:OntologyClass"
    ControlledIdentifiedTermValue: str = "nmdc:ControlledIdentifiedTermValue"
    TextValue: str = "nmdc:TextValue"
    GeolocationValue: str = "nmdc:GeolocationValue"
    TimeStampValue: str = "nmdc:TimestampValue"
    QuantityValue: str = "nmdc:QuantityValue"


class NMDCMetadataGenerator(ABC):
    """
    Abstract class for generating NMDC metadata objects using provided metadata files and configuration.

    Attributes
    ----------
    metadata_file : str
        Path to the CSV file containing the metadata about a sample data lifecycle.
    database_dump_json_path : str
        Path to the output JSON file for dumping the NMDC database results.
    raw_data_url : str
        Base URL for raw data files.
    process_data_url : str
        Base URL for processed data files.

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
        """
        Initialize the MetadataGenerator with required file paths and configuration.

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

        Returns
        -------
        None

        Notes
        -----
        This method sets up various attributes used throughout the class,
        including file paths, URLs, and predefined values for different
        data categories and descriptions.
        """
        self.metadata_file = metadata_file
        self.database_dump_json_path = database_dump_json_path
        self.raw_data_url = raw_data_url
        self.process_data_url = process_data_url
        self.raw_data_category = "instrument_data"

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
        pd.core.groupby.DataFrameGroupBy
            DataFrame grouped by biosample ID.

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
        bs_client = BiosampleSearch()
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
            ss_client = StudySearch()
            if not ss_client.check_ids_exist(study_ids):
                raise ValueError("Study IDs do not exist in the collection.")

        return metadata_df

    def clean_dict(self, dict: Dict) -> Dict:
        """
        Clean the dictionary of empty values.
        dict : Dict
            The dictionary to clean.
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
        lc_config_name : str
            Name of the liquid chromatography configuration.
        start_date : str
            Start date of the data generation.
        end_date : str
            End date of the data generation.
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

        is_client = InstrumentSearch()
        cs_client = ConfigurationSearch()
        minter = Minter()
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
        mint = Minter()
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
        calibration_id: str = None,
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
        calibration_id : str, optional
            ID of the calibration information used for the analysis.
            Default is None, indicating no calibration information.
        metabolite_identifications : List[nmdc.MetaboliteIdentification], optional
            List of MetaboliteIdentification objects associated with the analysis.
            Default is None, which indicates no metabolite identifications.

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
        mint = Minter()
        nmdc_id = (
            mint.mint(
                nmdc_type=NmdcTypes.MetabolomicsAnalysis,
                client_id=CLIENT_ID,
                client_secret=CLIENT_SECRET,
            )
            + ".1"
        )

        # TODO: Update the minting to handle versioning in the future

        data_dict = {
            "id": nmdc_id,
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
        mass_spec_obj: object,
        analysis_obj: object,
        raw_data_obj: object,
        parameter_data_id: list,
        processed_data_id_list: list,
    ) -> None:
        """
        Update output references for Mass Spectrometry and Workflow Analysis objects.

        This method assigns the output references for a Mass Spectrometry object
        and a Workflow Execution Analysis object. It sets `mass_spec_obj.has_output`
        to the ID of `raw_data_obj` and `analysis_obj.has_output` to a list of
        processed data IDs.

        Parameters
        ----------
        mass_spec_obj : object
            The Mass Spectrometry object to update.
        analysis_obj : object
            The Workflow Execution Analysis object to update
            (e.g., MetabolomicsAnalysis).
        parameter_data_id : str
            ID of the data object representing the parameter data used for the analysis.
        raw_data_obj : object
            The Raw Data Object associated with the Mass Spectrometry.
        processed_data_id_list : list
            List of IDs representing processed data objects associated with
            the Workflow Execution.

        Returns
        -------
        None

        Side Effects
        ------------
        - Sets `mass_spec_obj.has_output` to [raw_data_obj.id].
        - Sets `analysis_obj.has_output` to `processed_data_id_list`.
        """
        mass_spec_obj.has_output = [raw_data_obj.id]
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
            - biosample : Biosample or None
                The generated biosample object if new, None if existing
        """
        parser = MetadataParser()
        emsl_metadata = parser.parse_biosample_metadata(row)
        biosample_id = emsl_metadata["biosample_id"]
        return emsl_metadata, biosample_id

    def check_for_biosamples(
        self, metadata_df: pd.DataFrame, nmdc_database_inst: nmdc.Database
    ) -> bool:
        """
        Check if the biosample_id is not None, NaN, or empty.

        Parameters
        ----------
        row : pd.Series
            A row from the DataFrame containing metadata.

        Returns
        -------
        Tuple

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
                # Generate biosamples if no biosample_id in spreadsheet
                biosample_metadata = parser.dynam_parse_biosample_metadata(row)
                biosample = self.generate_biosample(biosamp_metadata=biosample_metadata)
                biosample_id = biosample.id
                metadata_df.loc[
                    metadata_df["biosample.name"] == row["biosample.name"],
                    "biosample_id",
                ] = biosample_id
                nmdc_database_inst.biosample_set.append(biosample)

    def generate_biosample(self, biosamp_metadata: Dict) -> nmdc.Biosample:
        """
        Generate a biosample from the given metadata.

        Parameters
        ----------
        biosamp_metadata : object
            The metadata object containing biosample information.

        Returns
        -------
        object
            The generated biosample instance.
        """
        mint = Minter()

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


class LCMSLipidomicsMetadataGenerator(NMDCMetadataGenerator):
    """
    A class for generating NMDC metadata objects using provided metadata files and configuration
    for LC-MS lipidomics data.

    This class processes input metadata files, generates various NMDC objects, and produces
    a database dump in JSON format.

    Attributes
    ----------
    grouped_columns : List[str]
        List of columns used for grouping metadata.
    mass_spec_desc : str
        Description of the mass spectrometry analysis.
    mass_spec_eluent_intro : str
        Eluent introduction category for mass spectrometry.
    analyte_category : str
        Category of the analyte.
    raw_data_category : str
        Category of the raw data.
    raw_data_obj_type : str
        Type of the raw data object.
    raw_data_obj_desc : str
        Description of the raw data object.
    workflow_analysis_name : str
        Name of the workflow analysis.
    workflow_description : str
        Description of the workflow.
    workflow_git_url : str
        URL of the workflow's Git repository.
    workflow_version : str
        Version of the workflow.
    wf_config_process_data_category : str
        Category of the workflow configuration process data.
    wf_config_process_data_obj_type : str
        Type of the workflow configuration process data object.
    wf_config_process_data_description : str
        Description of the workflow configuration process data.
    no_config_process_data_category : str
        Category for processed data without configuration.
    no_config_process_data_obj_type : str
        Type of processed data object without configuration.
    csv_process_data_description : str
        Description of CSV processed data.
    hdf5_process_data_obj_type : str
        Type of HDF5 processed data object.
    hdf5_process_data_description : str
        Description of HDF5 processed data.
    """

    def __init__(
        self,
        metadata_file: str,
        database_dump_json_path: str,
        raw_data_url: str,
        process_data_url: str,
    ):
        super().__init__(
            metadata_file=metadata_file,
            database_dump_json_path=database_dump_json_path,
            raw_data_url=raw_data_url,
            process_data_url=process_data_url,
        )

        self.unique_columns = ["raw_data_file", "processed_data_directory"]

        # Data Generation attributes
        self.mass_spec_desc = (
            "Generation of mass spectrometry data for the analysis of lipids."
        )
        self.mass_spec_eluent_intro = "liquid_chromatography"
        self.analyte_category = "lipidome"
        self.raw_data_obj_type = "LC-DDA-MS/MS Raw Data"
        self.raw_data_obj_desc = (
            "LC-DDA-MS/MS raw data for lipidomics data acquisition."
        )

        # Workflow attributes
        self.workflow_analysis_name = "Lipidomics analysis"
        self.workflow_description = (
            "Analysis of raw mass spectrometry data for the annotation of lipids."
        )
        self.workflow_git_url = (
            "https://github.com/microbiomedata/metaMS/wdl/metaMS_lipidomics.wdl"
        )
        self.workflow_version = "1.0.0"
        self.workflow_category = "lc_ms_lipidomics"

        # Processed data attributes
        self.wf_config_process_data_category = "workflow_parameter_data"
        self.wf_config_process_data_obj_type = "Configuration toml"
        self.wf_config_process_data_description = (
            "CoreMS parameters used for Lipidomics workflow."
        )
        self.no_config_process_data_category = "processed_data"
        self.no_config_process_data_obj_type = "LC-MS Lipidomics Results"
        self.csv_process_data_description = (
            "Lipid annotations as a result of a lipidomics workflow activity."
        )

        self.hdf5_process_data_obj_type = "LC-MS Lipidomics Processed Data"
        self.hdf5_process_data_description = "CoreMS hdf5 file representing a lipidomics data file including annotations."

    def run(self):
        """
        Execute the metadata generation process for lipidomics data.

        This method performs the following steps:
        1. Initialize an NMDC Database instance.
        2. Load and process metadata to create NMDC objects.
        3. Generate Mass Spectrometry, Raw Data, Metabolomics Analysis, and
        Processed Data objects.
        4. Update outputs for Mass Spectrometry and Metabolomics Analysis objects.
        5. Append generated objects to the NMDC Database.
        6. Dump the NMDC Database to a JSON file.
        7. Validate the JSON file using the NMDC API.

        Returns
        -------
        None

        Notes
        -----
        This method uses tqdm to display progress bars for the processing of
        biosamples and mass spectrometry metadata.
        """

        nmdc_database_inst = self.start_nmdc_database()
        metadata_df = self.load_metadata()
        self.check_for_biosamples(
            metadata_df=metadata_df, nmdc_database_inst=nmdc_database_inst
        )
        for _, data in tqdm(
            metadata_df.iterrows(),
            total=metadata_df.shape[0],
            desc="Processing LCMS biosamples",
        ):
            group_metadata_obj = self.create_grouped_metadata(data)

            workflow_metadata = self.create_workflow_metadata(data)

            mass_spec = self.generate_mass_spectrometry(
                file_path=Path(workflow_metadata.raw_data_file),
                instrument_name=workflow_metadata.instrument_used,
                sample_id=group_metadata_obj.biosample_id,
                raw_data_id="nmdc:placeholder",
                study_id=group_metadata_obj.nmdc_study,
                processing_institution=group_metadata_obj.processing_institution,
                mass_spec_config_name=workflow_metadata.mass_spec_config_name,
                lc_config_name=workflow_metadata.lc_config_name,
                start_date=workflow_metadata.instrument_analysis_start_date,
                end_date=workflow_metadata.instrument_analysis_end_date,
            )

            raw_data_object = self.generate_data_object(
                file_path=Path(workflow_metadata.raw_data_file),
                data_category=self.raw_data_category,
                data_object_type=self.raw_data_obj_type,
                description=self.raw_data_obj_desc,
                base_url=self.raw_data_url,
                was_generated_by=mass_spec.id,
            )

            metab_analysis = self.generate_metabolomics_analysis(
                cluster_name=workflow_metadata.execution_resource,
                raw_data_name=Path(workflow_metadata.raw_data_file).name,
                raw_data_id=raw_data_object.id,
                data_gen_id=mass_spec.id,
                processed_data_id="nmdc:placeholder",
                parameter_data_id="nmdc:placeholder",
                processing_institution=group_metadata_obj.processing_institution,
            )

            # list all paths in the processed data directory
            processed_data_paths = list(
                Path(workflow_metadata.processed_data_dir).glob("**/*")
            )

            # Add a check that the processed data directory is not empty
            if not any(processed_data_paths):
                raise FileNotFoundError(
                    f"No files found in processed data directory: "
                    f"{workflow_metadata.processed_data_dir}"
                )

            # Check that there is a .csv, .hdf5, and .toml file in the processed data directory and no other files
            processed_data_paths = [x for x in processed_data_paths if x.is_file()]
            if len(processed_data_paths) != 3:
                raise ValueError(
                    f"Expected 3 files in the processed data directory {processed_data_paths}, found {len(processed_data_paths)}."
                )

            processed_data = []

            for file in processed_data_paths:
                file_type = file.suffixes
                if file_type:
                    file_type = file_type[0].lstrip(".")

                    if file_type == "toml":
                        # Generate a data object for the parameter data
                        processed_data_object_config = self.generate_data_object(
                            file_path=file,
                            data_category=self.wf_config_process_data_category,
                            data_object_type=self.wf_config_process_data_obj_type,
                            description=self.wf_config_process_data_description,
                            base_url=self.process_data_url,
                            was_generated_by=metab_analysis.id,
                        )
                        nmdc_database_inst.data_object_set.append(
                            processed_data_object_config
                        )
                        parameter_data_id = processed_data_object_config.id

                    elif file_type == "csv":
                        # Generate a data object for the annotated data
                        processed_data_object_annot = self.generate_data_object(
                            file_path=file,
                            data_category=self.no_config_process_data_category,
                            data_object_type=self.no_config_process_data_obj_type,
                            description=self.csv_process_data_description,
                            base_url=self.process_data_url,
                            was_generated_by=metab_analysis.id,
                        )
                        nmdc_database_inst.data_object_set.append(
                            processed_data_object_annot
                        )
                        processed_data.append(processed_data_object_annot.id)

                    elif file_type == "hdf5":
                        # Generate a data object for the HDF5 processed data
                        processed_data_object = self.generate_data_object(
                            file_path=file,
                            data_category=self.no_config_process_data_category,
                            data_object_type=self.hdf5_process_data_obj_type,
                            description=self.hdf5_process_data_description,
                            base_url=self.process_data_url,
                            was_generated_by=metab_analysis.id,
                        )
                        nmdc_database_inst.data_object_set.append(processed_data_object)
                        processed_data.append(processed_data_object.id)

                        # Update MetabolomicsAnalysis times based on HDF5 file
                        metab_analysis.started_at_time = datetime.fromtimestamp(
                            file.stat().st_ctime
                        ).strftime("%Y-%m-%d %H:%M:%S")
                        metab_analysis.ended_at_time = datetime.fromtimestamp(
                            file.stat().st_mtime
                        ).strftime("%Y-%m-%d %H:%M:%S")

                    else:
                        raise ValueError(f"Unexpected file type found for file {file}.")

            # Check that all processed data objects were created
            if (
                processed_data_object_config is None
                or processed_data_object_annot is None
                or processed_data_object is None
            ):
                raise ValueError(
                    f"Not all processed data objects were created for {workflow_metadata.processed_data_dir}."
                )
            has_input = [parameter_data_id, raw_data_object.id]
            self.update_outputs(
                mass_spec_obj=mass_spec,
                analysis_obj=metab_analysis,
                raw_data_obj=raw_data_object,
                parameter_data_id=has_input,
                processed_data_id_list=processed_data,
            )

            nmdc_database_inst.data_generation_set.append(mass_spec)
            nmdc_database_inst.data_object_set.append(raw_data_object)
            nmdc_database_inst.workflow_execution_set.append(metab_analysis)

            # Set processed data objects to none for next iteration
            (
                processed_data_object_config,
                processed_data_object_annot,
                processed_data_object,
            ) = (
                None,
                None,
                None,
            )

        self.dump_nmdc_database(nmdc_database=nmdc_database_inst)
        api_metadata = Metadata()
        api_metadata.validate_json(self.database_dump_json_path)
        logging.info("Metadata processing completed.")

    def create_grouped_metadata(self, row: pd.Series) -> GroupedMetadata:
        """
        Construct a GroupedMetadata object from a DataFrame row.

        Parameters
        ----------
        row : pd.Series
            A row from the grouped metadata DataFrame.

        Returns
        -------
        GroupedMetadata
            A GroupedMetadata object populated with data from the input row.

        Notes
        -----
        """

        return GroupedMetadata(
            biosample_id=row["biosample_id"],
            nmdc_study=ast.literal_eval(row["biosample.associated_studies"]),
            processing_type=row["material_processing_type"],
            processing_institution=row["processing_institution"],
        )

    def create_workflow_metadata(
        self, row: dict[str, str]
    ) -> LCMSLipidWorkflowMetadata:
        """
        Create a LCMSLipidWorkflowMetadata object from a dictionary of workflow metadata.

        Parameters
        ----------
        row : dict[str, str]
            Dictionary containing metadata for a workflow. This is typically
            a row from the input metadata CSV file.

        Returns
        -------
        LCMSLipidWorkflowMetadata
            A LCMSLipidWorkflowMetadata object populated with data from the input dictionary.

        Notes
        -----
        The input dictionary is expected to contain the following keys:
        'Processed Data Directory', 'Raw Data File', 'Raw Data Object Alt Id',
        'mass spec configuration name', 'lc config name', 'instrument used',
        'instrument analysis start date', 'instrument analysis end date',
        'execution resource'.
        """
        return LCMSLipidWorkflowMetadata(
            processed_data_dir=row["processed_data_directory"],
            raw_data_file=row["raw_data_file"],
            mass_spec_config_name=row["mass_spec_configuration_name"],
            lc_config_name=row["chromat_configuration_name"],
            instrument_used=row["instrument_used"],
            instrument_analysis_start_date=row["instrument_analysis_start_date"],
            instrument_analysis_end_date=row["instrument_analysis_end_date"],
            execution_resource=row["execution_resource"],
        )


class GCMSMetabolomicsMetadataGenerator(NMDCMetadataGenerator):
    """
    A class for generating NMDC metadata objects related to GC/MS metabolomics data.

    This class processes input metadata files, generates various NMDC objects, and produces
    a database dump in JSON format.

    Parameters
    ----------
    metadata_file : str
        Path to the metadata CSV file.
    database_dump_json_path : str
        Path to the output JSON file for the NMDC database dump.
    raw_data_url : str
        Base URL for the raw data files.
    process_data_url : str
        Base URL for the processed data files.
    minting_config_creds : str
        Path to the minting configuration credentials file.
    calibration_standard : str, optional
        Calibration standard used for the data. Default is "fames".
    configuration_file_name : str, optional
        Name of the configuration file. Default is "emsl_gcms_corems_params.toml".

    Attributes
    ----------
    calibration_standard : str
        Name of the calibration standard used for the data.
    grouped_columns : List[str]
        List of columns used for grouping metadata. No affect on the metadata generation for this subclass.
    unique_columns : List[str]
        List of columns used to check for uniqueness in the metadata before processing.
    mass_spec_desc : str
        Description of the mass spectrometry analysis.
    mass_spec_eluent_intro : str
        Eluent introduction category for mass spectrometry.
    analyte_category : str
        Category of the analyte.
    raw_data_obj_type : str
        Type of the raw data object.
    raw_data_obj_desc : str
        Description of the raw data object.
    workflow_analysis_name : str
        Name of the workflow analysis.
    workflow_description : str
        Description of the workflow.
    workflow_git_url : str
        URL of the workflow's Git repository.
    workflow_version : str
        Version of the workflow.
    workflow_category : str
        Category of the workflow.
    processed_data_category : str
        Category of the processed data.
    processed_data_object_type : str
        Type of the processed data object.
    processed_data_object_description : str
    """

    def __init__(
        self,
        metadata_file: str,
        database_dump_json_path: str,
        raw_data_url: str,
        process_data_url: str,
        calibration_standard: str = "fames",
        configuration_file_name: str = "emsl_gcms_corems_params.toml",
    ):
        super().__init__(
            metadata_file=metadata_file,
            database_dump_json_path=database_dump_json_path,
            raw_data_url=raw_data_url,
            process_data_url=process_data_url,
        )

        # Calibration attributes
        self.calibration_standard = calibration_standard

        # Workflow Configuration attributes
        self.configuration_file_name = configuration_file_name

        # Metadata attributes
        self.unique_columns = ["raw_data_file", "processed_data_file"]

        # Data Generation attributes
        self.mass_spec_desc = "Generation of mass spectrometry data by GC/MS for the analysis of metabolites."
        self.mass_spec_eluent_intro = "gas_chromatography"
        self.analyte_category = "metabolome"
        self.raw_data_obj_type = "GC-MS Raw Data"
        self.raw_data_obj_desc = (
            "GC/MS low resolution raw data for metabolomics data acquisition."
        )

        # Workflow metadata
        self.workflow_analysis_name = "GC/MS Metabolomics analysis"
        self.workflow_description = (
            "Analysis of raw mass spectrometry data for the annotation of metabolites."
        )
        self.workflow_git_url = (
            "https://github.com/microbiomedata/metaMS/wdl/metaMS_gcms.wdl"
        )
        self.workflow_version = "3.0.0"
        self.workflow_category = "gc_ms_metabolomics"

        # Processed data attributes
        self.processed_data_category = "processed_data"
        self.processed_data_object_type = "GC-MS Metabolomics Results"
        self.processed_data_object_description = "Metabolomics annotations as a result of a GC/MS metabolomics workflow activity."

    def run(self):
        """
        Execute the metadata generation process for GC/MS metabolomics data.

        This method performs the following steps:
        1. Initialize an NMDC Database instance.
        2. Generate calibration information and data objects for each calibration file.
        3. Load and process metadata to create NMDC objects.
        4. Generate Mass Spectrometry (including metabolite identifications), Raw Data, Metabolomics Analysis, and
        Processed Data objects.
        5. Update outputs for Mass Spectrometry and Metabolomics Analysis objects.
        6. Append generated objects to the NMDC Database.
        7. Dump the NMDC Database to a JSON file.
        8. Validate the JSON file using the NMDC API.

        Returns
        -------

        Notes
        -----
        This method uses tqdm to display progress bars for the processing of calibration information and
        mass spectrometry metadata.
        """
        if self.calibration_standard != "fames":
            raise ValueError("Only FAMES calibration is supported at this time.")

        # Start NMDC database and make metadata dataframe
        nmdc_database_inst = self.start_nmdc_database()
        df = self.load_metadata()
        metadata_df = df.apply(lambda x: x.reset_index(drop=True))
        self.check_for_biosamples(
            metadata_df=metadata_df, nmdc_database_inst=nmdc_database_inst
        )

        # Get the configuration file data object id and add it to the metadata_df
        do_client = DataObjectSearch()
        config_do_id = do_client.get_record_by_attribute(
            attribute_name="name",
            attribute_value=self.configuration_file_name,
            fields="id",
            exact_match=True,
        )[0]["id"]
        # This is the ID for the parameters toml file
        parameter_data_id = "nmdc:dobj-11-5tax4f20"
        metadata_df["corems_config_file"] = config_do_id

        # check if there is an existing calibration_id in the metadata. If not, we need to generate them
        if (
            "calibration_id" not in metadata_df.columns
            or metadata_df["calibration_id"].isnull().all()
        ):
            self.generate_calibration_id(
                metadata_df=metadata_df, nmdc_database_inst=nmdc_database_inst
            )

        # process workflow metadata
        for _, data in tqdm(
            metadata_df.iterrows(),
            total=metadata_df.shape[0],
            desc="Processing Remaining Metadata",
        ):
            workflow_metadata_obj = self.create_workflow_metadata(data)

            # Generate data generation / mass spectrometry object
            mass_spec = self.generate_mass_spectrometry(
                file_path=Path(workflow_metadata_obj.raw_data_file),
                instrument_name=workflow_metadata_obj.instrument_used,
                sample_id=workflow_metadata_obj.biosample_id,
                raw_data_id="nmdc:placeholder",
                study_id=workflow_metadata_obj.nmdc_study,
                processing_institution=workflow_metadata_obj.processing_institution,
                mass_spec_config_name=workflow_metadata_obj.mass_spec_config_name,
                lc_config_name=workflow_metadata_obj.chromat_config_name,
                start_date=workflow_metadata_obj.instrument_analysis_start_date,
                end_date=workflow_metadata_obj.instrument_analysis_end_date,
                calibration_id=workflow_metadata_obj.calibration_id,
            )

            # Generate raw data object
            raw_data_object = self.generate_data_object(
                file_path=Path(workflow_metadata_obj.raw_data_file),
                data_category=self.raw_data_category,
                data_object_type=self.raw_data_obj_type,
                description=self.raw_data_obj_desc,
                base_url=self.raw_data_url,
                was_generated_by=mass_spec.id,
            )

            # Generate metabolite identifications
            metabolite_identifications = self.generate_metab_identifications(
                processed_data_file=workflow_metadata_obj.processed_data_file
            )

            # Generate metabolomics analysis object with metabolite identifications
            metab_analysis = self.generate_metabolomics_analysis(
                cluster_name=workflow_metadata_obj.execution_resource,
                raw_data_name=Path(workflow_metadata_obj.raw_data_file).name,
                raw_data_id=raw_data_object.id,
                data_gen_id=mass_spec.id,
                processed_data_id="nmdc:placeholder",
                parameter_data_id=parameter_data_id,
                processing_institution=workflow_metadata_obj.processing_institution,
                calibration_id=workflow_metadata_obj.calibration_id,
                metabolite_identifications=metabolite_identifications,
            )

            # Generate processed data object
            processed_data_object = self.generate_data_object(
                file_path=Path(workflow_metadata_obj.processed_data_file),
                data_category=self.processed_data_category,
                data_object_type=self.processed_data_object_type,
                description=self.processed_data_object_description,
                base_url=self.process_data_url,
                was_generated_by=metab_analysis.id,
            )

            # Update MetabolomicsAnalysis times based on processed data file
            processed_file = Path(workflow_metadata_obj.processed_data_file)
            metab_analysis.started_at_time = datetime.fromtimestamp(
                processed_file.stat().st_ctime
            ).strftime("%Y-%m-%d %H:%M:%S")
            metab_analysis.ended_at_time = datetime.fromtimestamp(
                processed_file.stat().st_mtime
            ).strftime("%Y-%m-%d %H:%M:%S")
            has_inputs = [parameter_data_id, raw_data_object.id]
            self.update_outputs(
                mass_spec_obj=mass_spec,
                analysis_obj=metab_analysis,
                raw_data_obj=raw_data_object,
                parameter_data_id=has_inputs,
                processed_data_id_list=[processed_data_object.id],
            )

            nmdc_database_inst.data_generation_set.append(mass_spec)
            nmdc_database_inst.data_object_set.append(raw_data_object)
            nmdc_database_inst.data_object_set.append(processed_data_object)
            nmdc_database_inst.workflow_execution_set.append(metab_analysis)

        self.dump_nmdc_database(nmdc_database=nmdc_database_inst)
        api_metadata = Metadata()
        api_metadata.validate_json(self.database_dump_json_path)
        logging.info("Metadata processing completed.")

    def generate_calibration_id(
        self, metadata_df: pd.DataFrame, nmdc_database_inst: nmdc.Database
    ) -> None:
        """
        Generate calibration information and data objects for each calibration file.
        Parameters
        ----------
        metadata_df : pd.DataFrame
            The metadata DataFrame.
        nmdc_database_inst : nmdc.Database
            The NMDC Database instance.
        Returns
        -------
        None
        """
        # Get unique calibration file, create data object and Calibration information for each and attach associated ids to metadata_df
        calibration_files = metadata_df["calibration_file"].unique()
        for calibration_file in tqdm(
            calibration_files,
            total=len(calibration_files),
            desc="Generating calibration information and data objects",
        ):
            calibration_data_object = self.generate_data_object(
                file_path=Path(calibration_file),
                data_category=self.raw_data_category,
                data_object_type=self.raw_data_obj_type,
                description=self.raw_data_obj_desc,
                base_url=self.raw_data_url,
            )
            nmdc_database_inst.data_object_set.append(calibration_data_object)

            calibration = self.generate_calibration(
                calibration_object=calibration_data_object,
                fames=self.calibration_standard,
                internal=False,
            )
            nmdc_database_inst.calibration_set.append(calibration)

            # Add calibration information id to metadata_df
            metadata_df.loc[
                metadata_df["calibration_file"] == calibration_file, "calibration_id"
            ] = calibration.id

    def generate_calibration(
        self, calibration_object: dict, fames: bool = True, internal: bool = False
    ) -> nmdc.CalibrationInformation:
        """
        Generate a CalibrationInformation object for the NMDC Database.

        Parameters
        ----------
        calibration_object : dict
            The calibration data object.
        fames : bool, optional
            Whether the calibration is for FAMES. Default is True.
        internal : bool, optional
            Whether the calibration is internal. Default is False.

        Returns
        -------
        nmdc.CalibrationInformation
            A CalibrationInformation object for the NMDC Database.

        Notes
        -----
        This method generates a CalibrationInformation object based on the calibration data object
        and the calibration type.

        Raises
        ------
        ValueError
            If the calibration type is not supported.
        """
        mint = Minter()
        if fames and not internal:
            nmdc_id = mint.mint(
                nmdc_type=NmdcTypes.CalibrationInformation,
                client_id=CLIENT_ID,
                client_secret=CLIENT_SECRET,
            )
            data_dict = {
                "id": nmdc_id,
                "type": NmdcTypes.CalibrationInformation,
                "name": f"GC/MS FAMES calibration ({calibration_object.name})",
                "description": f"Full scan GC/MS FAMES calibration run ({calibration_object.name}).",
                "internal_calibration": False,
                "calibration_target": "retention_index",
                "calibration_standard": "fames",
                "calibration_object": calibration_object.id,
            }

            calibration_information = nmdc.CalibrationInformation(**data_dict)

            return calibration_information
        else:
            raise ValueError(
                "Calibration type not implemented, only external FAMES calibration is currently supported."
            )

    def create_workflow_metadata(
        self, row: dict[str, str]
    ) -> GCMSMetabWorkflowMetadata:
        """
        Create a LCMSLipidWorkflowMetadata object from a dictionary of workflow metadata.

        Parameters
        ----------
        row : dict[str, str]
            Dictionary containing metadata for a workflow. This is typically
            a row from the input metadata CSV file.

        Returns
        -------
        LCMSLipidWorkflowMetadata
            A LCMSLipidWorkflowMetadata object populated with data from the input dictionary.

        Notes
        -----
        The input dictionary is expected to contain the following keys:
        'Processed Data Directory', 'Raw Data File', 'Raw Data Object Alt Id',
        'mass spec configuration name', 'lc config name', 'instrument used',
        'instrument analysis start date', 'instrument analysis end date',
        'execution resource'.
        """
        return GCMSMetabWorkflowMetadata(
            biosample_id=row["biosample_id"],
            nmdc_study=ast.literal_eval(row["biosample.associated_studies"]),
            processing_institution=row["processing_institution"],
            processed_data_file=row["processed_data_file"],
            raw_data_file=row["raw_data_file"],
            mass_spec_config_name=row["mass_spec_configuration_name"],
            chromat_config_name=row["chromat_configuration_name"],
            instrument_used=row["instrument_used"],
            instrument_analysis_start_date=row["instrument_analysis_start_date"],
            instrument_analysis_end_date=row["instrument_analysis_end_date"],
            execution_resource=row["execution_resource"],
            calibration_id=row["calibration_id"],
        )

    def generate_metab_identifications(
        self, processed_data_file
    ) -> List[nmdc.MetaboliteIdentification]:
        """
        Generate MetaboliteIdentification objects from processed data file.

        Parameters
        ----------
        processed_data_file : str
            Path to the processed data file.

        Returns
        -------
        List[nmdc.MetaboliteIdentification]
            List of MetaboliteIdentification objects generated from the processed data file.

        Notes
        -----
        This method reads in the processed data file and generates MetaboliteIdentification objects,
        pulling out the best hit for each peak based on the highest "Similarity Score".
        """
        # Open the file and read in the data as a pandas dataframe
        processed_data = pd.read_csv(processed_data_file)

        # Drop any rows with missing similarity scores
        processed_data = processed_data.dropna(subset=["Similarity Score"])
        # Group by "Peak Index" and find the best hit for each peak based on the highest "Similarity Score"
        best_hits = processed_data.groupby("Peak Index").apply(
            lambda x: x.loc[x["Similarity Score"].idxmax()]
        )

        metabolite_identifications = []
        for index, best_hit in best_hits.iterrows():
            # Check if the best hit has a Chebi ID, if not, do not create a MetaboliteIdentification object
            if pd.isna(best_hit["Chebi ID"]):
                continue
            chebi_id = "chebi:" + str(int(best_hit["Chebi ID"]))

            # Prepare KEGG Compound ID as an alternative identifier
            alt_ids = []
            if not pd.isna(best_hit["Kegg Compound ID"]):
                # Check for | in Kegg Compound ID and split if necessary
                if "|" in best_hit["Kegg Compound ID"]:
                    alt_ids.extend(
                        [
                            "kegg:" + x.strip()
                            for x in best_hit["Kegg Compound ID"].split("|")
                        ]
                    )
                else:
                    alt_ids.append("kegg:" + best_hit["Kegg Compound ID"])
            alt_ids = list(set(alt_ids))

            data_dict = {
                "metabolite_identified": chebi_id,
                "alternative_identifiers": alt_ids,
                "type": NmdcTypes.MetaboliteIdentification,
                "highest_similarity_score": best_hit["Similarity Score"],
            }

            metabolite_identification = nmdc.MetaboliteIdentification(**data_dict)
            metabolite_identifications.append(metabolite_identification)

        return metabolite_identifications
