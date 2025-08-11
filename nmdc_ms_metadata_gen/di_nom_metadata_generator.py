# -*- coding: utf-8 -*-
from nmdc_ms_metadata_gen.nom_metadata_generator import NOMMetadataGenerator
import pandas as pd
from pathlib import Path
from datetime import datetime
import nmdc_schema.nmdc as nmdc


class DINOMMetaDataGenerator(NOMMetadataGenerator):
    """
    This class is responsible for generating metadata for the DI NOM model.
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
    raw_data_object_type : str
        The type of the raw data object.
    processed_data_object_type : str
        The type of the processed data object.
    processed_data_category : str
        The category of the processed data.
    analyte_category : str
        The category of the analyte.
    workflow_analysis_name : str
        The name of the workflow analysis.
    workflow_description : str
        The description of the workflow.
    workflow_param_data_category : str
        The category of the workflow parameter data.
    workflow_param_data_object_type : str
        The type of the workflow parameter data object.
    unique_columns : list[str]
        List of unique columns in the metadata file.
    mass_spec_eluent_intro : str
        The introduction to the mass spectrometry eluent.
    workflow_git_url : str
        The URL of the workflow Git repository.
    workflow_version : str
        The version of the workflow.
    """

    raw_data_object_type: str = "Direct Infusion FT ICR-MS Raw Data"
    processed_data_object_type: str = "Direct Infusion FT-ICR MS Analysis Results"
    processed_data_object_desc = "EnviroMS natural organic matter workflow molecular formula assignment output details"
    processed_data_category: str = "processed_data"
    analyte_category: str = "nom"
    workflow_analysis_name: str = "NOM Analysis"
    workflow_description: str = (
        "Processing of raw DI FT-ICR MS data for natural organic matter identification"
    )
    workflow_param_data_category: str = "workflow_parameter_data"
    workflow_param_data_object_type: str = "Analysis Tool Parameter File"
    workflow_param_data_object_desc = (
        "EnviroMS processing parameters for natural organic matter analysis."
    )
    unique_columns: list[str] = ["processed_data_directory"]
    mass_spec_eluent_intro: str = "direct_infusion_autosampler"
    mass_spec_desc: str = "ultra high resolution mass spectrum"
    workflow_git_url: str = (
        "https://github.com/microbiomedata/enviroMS/blob/master/wdl/di_fticr_ms.wdl"
    )
    workflow_version: str

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
        # Set the workflow version, prioritizing user input, then fetching from the Git URL.
        self.workflow_version = workflow_version or self.get_workflow_version(
            workflow_version_git_url="https://github.com/microbiomedata/enviroMS/blob/master/.bumpversion.cfg"
        )
        self.minting_config_creds = minting_config_creds

    def rerun(self):
        super().rerun()

    def run(self):
        super().run()

    def create_processed_data_objects(
        self,
        row: pd.Series,
        client_id: str,
        client_secret: str,
        nom_analysis: nmdc.NomAnalysis,
        nmdc_database_inst: nmdc.Database,
    ) -> tuple:
        """
        Create processed data objects for DI NOM metadata generation. This process expects a csv and json processed output.

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
        nmdc_database_inst : nmdc.Database
            The NMDC database instance to which the processed data objects will be added.

        Returns
        -------
        tuple
            A tuple containing the processed data object and the workflow parameter data object.

        """
        processed_ids = []
        processed_data_paths = list(Path(row["processed_data_directory"]).glob("**/*"))
        # Add a check that the processed data directory is not empty
        if not any(processed_data_paths):
            raise FileNotFoundError(
                f"No files found in processed data directory: "
                f"{row['processed_data_directory']}"
            )
        processed_data_paths = [x for x in processed_data_paths if x.is_file()]

        for file in processed_data_paths:
            if file.suffix == ".csv":
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
                # Add the processed data object to the NMDC database
                nmdc_database_inst.data_object_set.append(processed_data_object)
                # add the processed data object id to the list
                processed_ids.append(processed_data_object.id)
            if file.suffix == ".json":
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
        return processed_ids, workflow_data_object
