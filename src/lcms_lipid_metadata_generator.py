# -*- coding: utf-8 -*-
from src.lcms_metadata_generator import LCMSMetadataGenerator
from typing import List


class LCMSLipidomicsMetadataGenerator(LCMSMetadataGenerator):
    """
    A class for generating NMDC metadata objects using provided metadata files and configuration
    for LC-MS lipidomics data.

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
    unique_columns : List[str]
        List of unique columns in the metadata file.
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

    unique_columns: List[str] = ["raw_data_file", "processed_data_directory"]
    # Data Generation attributes
    mass_spec_desc: str = (
        "Generation of mass spectrometry data for the analysis of lipids."
    )
    mass_spec_eluent_intro: str = "liquid_chromatography"
    analyte_category: str = "lipidome"
    raw_data_obj_type: str = "LC-DDA-MS/MS Raw Data"
    raw_data_obj_desc: str = "LC-DDA-MS/MS raw data for lipidomics data acquisition."

    # Workflow attributes
    workflow_analysis_name: str = "Lipidomics analysis"
    workflow_description: str = (
        "Analysis of raw mass spectrometry data for the annotation of lipids."
    )
    workflow_git_url: str = (
        "https://github.com/microbiomedata/metaMS/wdl/metaMS_lipidomics.wdl"
    )
    workflow_version: str
    workflow_category: str = "lc_ms_lipidomics"

    # Processed data attributes
    wf_config_process_data_category: str = "workflow_parameter_data"
    wf_config_process_data_obj_type: str = "Configuration toml"
    wf_config_process_data_description: str = (
        "CoreMS parameters used for Lipidomics workflow."
    )
    no_config_process_data_category: str = "processed_data"
    no_config_process_data_obj_type: str = "LC-MS Lipidomics Results"
    csv_process_data_description: str = (
        "Lipid annotations as a result of a lipidomics workflow activity."
    )

    hdf5_process_data_obj_type: str = "LC-MS Lipidomics Processed Data"
    hdf5_process_data_description: str = (
        "CoreMS hdf5 file representing a lipidomics data file including annotations."
    )

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
            workflow_version_git_url="https://github.com/microbiomedata/metaMS/blob/master/.bumpversion_lipid.cfg"
        )
        self.minting_config_creds = minting_config_creds

    def rerun(self):
        super().rerun()

    def run(self):
        super().run()
