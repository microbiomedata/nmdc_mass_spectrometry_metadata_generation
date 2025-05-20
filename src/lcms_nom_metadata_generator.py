# -*- coding: utf-8 -*-
from src.nom_metadata_generator import NOMMetadataGenerator
import pandas as pd
from pathlib import Path
from datetime import datetime
import nmdc_schema.nmdc as nmdc
import os
import zipfile


class LCMSNOMMetadataGenerator(NOMMetadataGenerator):
    """
    A class for generating NMDC metadata objects using provided metadata files and configuration
    for LC FT-ICR MS NOM data.

    This class processes input metadata files, generates various NMDC objects, and produces
    a database dump in JSON format.

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
    minting_config_creds : str, optional
        Path to the configuration file containing the client ID and client secret for minting NMDC IDs. It can also include the bio ontology API key if generating biosample ids is needed.
        If not provided, the CLIENT_ID, CLIENT_SECRET, and BIO_API_KEY environment variables will be used.

    Attributes
    ----------

    """

    processed_data_object_desc: str = (
        "NOM annotations as a result of a NOM workflow activity."
    )
    qc_process_data_obj_type: str = "LC FT-ICR MS QC Plots"
    qc_process_data_description: str = "EnviroMS QC file representing a NOM."

    raw_data_object_type: str = "LC FT-ICR MS Raw Data"
    raw_data_obj_desc: str = "LC FT-ICR MS Raw Data raw data for NOM data acquisition."
    processed_data_object_type: str = "LC FT-ICR MS Analysis Results"
    processed_data_category: str = "processed_data"
    execution_resource: str = "EMSL"
    analyte_category: str = "nom"
    workflow_analysis_name: str = "LC FT-ICR MS NOM Analysis"
    workflow_param_data_object_desc: str = "Natural Organic Matter analysis of raw mass spectrometry data when aquired by liquid chromatography."
    workflow_param_data_category: str = "workflow_parameter_data"
    workflow_param_data_object_type: str = "Analysis Tool Parameter File"
    unique_columns: list[str] = ["raw_data_file", "processed_data_directory"]
    mass_spec_eluent_intro: str = "liquid_chromatography"
    mass_spec_desc: str = "Generation of mass spectrometry data for the analysis of nom using liquid chromatography."
    processing_institution: str = "EMSL"
    workflow_git_url: str = "https://github.com/microbiomedata/enviroMS"
    workflow_version: str
    # on the mass spec records, you will need to add has_chromatograohy_configuration - created in parent metadata gen class

    def __init__(
        self,
        metadata_file: str,
        database_dump_json_path: str,
        raw_data_url: str,
        process_data_url: str,
        minting_config_creds: str = None,
        workflow_version: str = None,
    ):
        super().__init__(
            metadata_file=metadata_file,
            database_dump_json_path=database_dump_json_path,
            raw_data_url=raw_data_url,
            process_data_url=process_data_url,
        )
        # Set the workflow version, prioritizing user input, then fetching from the Git URL, and finally using a default.
        self.workflow_version = workflow_version or self.get_workflow_version(
            workflow_version_git_url="https://github.com/microbiomedata/enviroMS/blob/master/.bumpversion.cfg"
        )
        self.minting_config_creds = minting_config_creds

    def rerun(self):
        super().rerun()

    def run(self):
        super().run()

    def create_proccesed_data_objects(
        self,
        row: pd.Series,
        client_id: str,
        client_secret: str,
        nom_analysis: nmdc.NomAnalysis,
    ) -> tuple:
        """
        Create processed data objects for LCMS NOM metadata generation. This process expects two zip files.
        The first zip file should contain 2 csv files with the processed data and the second zip file should contain png images.

        Parameters
        ----------
        row : pd.Series
            A row from the metadata DataFrame containing information about the processed data.
        client_id : str
            The client ID for minting NMDC IDs.
        client_secret : str
            The client secret for minting NMDC IDs.
        nom_analysis : nmdc.NomAnalysis
            The NomAnalysis object to which the processed data objects will be associated.

        Returns
        -------
        tuple
            A tuple containing the processed data object and the workflow parameter data object.

        """
        processed_data_paths = list(Path(row["processed_data_directory"]).glob("**/*"))
        # Add a check that the processed data directory is not empty
        if not any(processed_data_paths):
            raise FileNotFoundError(
                f"No files found in processed data directory: "
                f"{row['processed_data_directory']}"
            )
        processed_data_paths = [x for x in processed_data_paths if x.is_file()]
        # loop through both zip files
        for file in processed_data_paths:
            # check what file extensions are in the zip file
            if file.suffix == ".zip":
                with zipfile.ZipFile(processed_data_paths, "r") as z:
                    # Get a list of all file names in the zip
                    file_names = z.namelist()
                    # Extract the file extensions
                    extensions = {
                        os.path.splitext(file_name)[1]
                        for file_name in file_names
                        if os.path.splitext(file_name)[1]
                    }
                    if "csv" in extensions:
                        # this is the .csv file of the processed data
                        processed_data_object = self.generate_data_object(
                            file_path=file,
                            data_category=self.processed_data_category,
                            data_object_type=self.processed_data_object_type,
                            description=self.processed_data_object_desc,
                            base_url=self.process_data_url
                            + Path(row["processed_data_directory"]).name
                            + "/",
                            CLIENT_ID=client_id,
                            CLIENT_SECRET=client_secret,
                            was_generated_by=nom_analysis.id,
                            alternative_id=None,
                        )
                        # Update NomAnalysis times based on csv file
                        nom_analysis.started_at_time = datetime.fromtimestamp(
                            file.stat().st_ctime
                        ).strftime("%Y-%m-%d %H:%M:%S")
                        nom_analysis.ended_at_time = datetime.fromtimestamp(
                            file.stat().st_mtime
                        ).strftime("%Y-%m-%d %H:%M:%S")
                    # if the zip
                    elif "png" in extensions:
                        # Generate workflow parameter data object
                        workflow_data_object = self.generate_data_object(
                            file_path=file,
                            data_category=self.processed_data_category,
                            data_object_type=self.qc_process_data_obj_type,
                            description=self.qc_process_data_description,
                            base_url=self.process_data_url
                            + Path(row["processed_data_directory"]).name
                            + "/",
                            was_generated_by=nom_analysis.id,
                            CLIENT_ID=client_id,
                            CLIENT_SECRET=client_secret,
                            alternative_id=None,
                        )
            elif file.suffix == ".toml":
                # Generate workflow parameter data object
                workflow_data_object = self.generate_data_object(
                    file_path=file,
                    data_category=self.workflow_param_data_category,
                    data_object_type=self.workflow_param_data_object_type,
                    description=self.workflow_param_data_object_desc,
                    base_url=self.process_data_url
                    + Path(row["processed_data_directory"]).name
                    + "/",
                    was_generated_by=nom_analysis.id,
                    CLIENT_ID=client_id,
                    CLIENT_SECRET=client_secret,
                    alternative_id=None,
                )

        return processed_data_object, workflow_data_object
