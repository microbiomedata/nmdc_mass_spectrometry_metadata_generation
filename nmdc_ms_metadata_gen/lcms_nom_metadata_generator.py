from nmdc_ms_metadata_gen.nom_metadata_generator import NOMMetadataGenerator


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
    processed_data_object_desc : str
    qc_process_data_obj_type : str
    qc_process_data_description : str
    raw_data_object_type : str
    raw_data_obj_desc : str
    processed_data_object_type : str
    processed_data_category: str
    analyte_category: str
    workflow_analysis_name: str
    workflow_param_data_object_desc: str
    workflow_param_data_category: str
    workflow_param_data_object_type: str
    unique_columns: list[str]
    mass_spec_eluent_intro: str
    mass_spec_desc: str
    workflow_git_url: str
    workflow_version: str
    workflow_description: str
    """

    processed_data_object_desc: str = (
        "NOM annotations as a result of a NOM workflow activity."
    )
    qc_process_data_obj_type: str = "LC FT-ICR MS QC Plots"
    qc_process_data_description: str = "EnviroMS QC plots representing a NOM analysis."

    raw_data_object_type: str = "LC FT-ICR MS Raw Data"
    raw_data_obj_desc: str = "LC FT-ICR MS Raw Data raw data for NOM data acquisition."
    processed_data_object_type: str = "LC FT-ICR MS Analysis Results"
    processed_data_category: str = "processed_data"
    analyte_category: str = "nom"
    workflow_analysis_name: str = "LC FT-ICR MS NOM Analysis"
    workflow_param_data_object_desc: str = (
        "EnviroMS processing parameters for natural organic matter analysis when acquired using liquid chromatography."
    )
    workflow_param_data_category: str = "workflow_parameter_data"
    workflow_param_data_object_type: str = "Analysis Tool Parameter File"
    unique_columns: list[str] = ["processed_data_directory"]
    mass_spec_eluent_intro: str = "liquid_chromatography"
    mass_spec_desc: str = (
        "Generation of mass spectrometry data for the analysis of NOM when acquired using liquid chromatography."
    )
    workflow_git_url: str = (
        "https://github.com/microbiomedata/enviroMS/blob/master/wdl/lc_ftirc_ms.wdl"
    )
    workflow_version: str
    workflow_description: str = (
        "Processing of raw liquid chromatography FT-ICR MS data for natural organic matter identification."
    )
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
        # Set the workflow version, prioritizing user input, then fetching from the Git URL.
        self.workflow_version = workflow_version or self.get_workflow_version(
            workflow_version_git_url="https://github.com/microbiomedata/enviroMS/blob/master/.bumpversion.cfg"
        )
        self.minting_config_creds = minting_config_creds

    def rerun(self):
        return super().rerun()

    def run(self):
        return super().run()
