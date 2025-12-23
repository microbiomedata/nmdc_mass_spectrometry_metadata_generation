from nmdc_ms_metadata_gen.nom_metadata_generator import NOMMetadataGenerator


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
    test : bool, optional
        Flag indicating whether to run in test mode. If True, will skip biosample ID checks in the database, data object URL check, and will use local IDs (skip API minting). Default is False.

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

    qc_process_data_obj_type: str = "Direct Infusion FT-ICR MS QC Plots"
    qc_process_data_description: str = (
        "EnviroMS QC plots representing a Direct Infusion NOM analysis."
    )

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
        test: bool = False,
    ):
        super().__init__(
            metadata_file=metadata_file,
            database_dump_json_path=database_dump_json_path,
            raw_data_url=raw_data_url,
            process_data_url=process_data_url,
            test=test,
        )
        # Set the workflow version, prioritizing user input, then fetching from the Git URL.
        self.workflow_version = workflow_version or self.get_workflow_version(
            workflow_version_git_url="https://github.com/microbiomedata/enviroMS/blob/master/.bumpversion.cfg"
        )
        self.minting_config_creds = minting_config_creds

    def rerun(self):
        return super().rerun()

    def run(self):
        return super().run()
