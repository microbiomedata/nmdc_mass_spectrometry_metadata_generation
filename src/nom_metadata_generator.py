# -*- coding: utf-8 -*-
from src.metadata_generator import NMDCMetadataGenerator
from src.metadata_parser import NmdcTypes
from tqdm import tqdm
from pathlib import Path
from nmdc_api_utilities.data_object_search import DataObjectSearch
from nmdc_api_utilities.calibration_search import CalibrationSearch
from nmdc_api_utilities.workflow_execution_search import WorkflowExecutionSearch
from nmdc_api_utilities.minter import Minter
from nmdc_api_utilities.metadata import Metadata
import nmdc_schema.nmdc as nmdc
import hashlib
import pandas as pd
import re
from datetime import datetime


class NOMMetadataGenerator(NMDCMetadataGenerator):
    """
    A class for generating NMDC metadata objects using provided metadata files and configuration
    for Natural Organic Matter (NOM) data.
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
    execution_resource : str
        The execution resource for the workflow.
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
    mass_spec_desc : str
        The description of the mass spectrometry data.
    mass_spec_eluent_intro : str
        The introduction to the mass spectrometry eluent.
    processing_institution : str
        The institution responsible for processing the data.
    workflow_git_url : str
        The URL of the workflow Git repository.
    workflow_version : str
        The version of the workflow.
    """

    raw_data_object_type: str = "Direct Infusion FT ICR-MS Raw Data"
    processed_data_object_type: str = "FT ICR-MS Analysis Results"
    processed_data_category: str = "processed_data"
    execution_resource: str = "EMSL-RZR"
    analyte_category: str = "nom"
    workflow_analysis_name: str = "NOM Analysis"
    workflow_description: str = (
        "Natural Organic Matter analysis of raw mass spectrometry data."
    )
    workflow_param_data_category: str = "workflow_parameter_data"
    workflow_param_data_object_type: str = "Analysis Tool Parameter File"
    unique_columns: list[str] = ["raw_data_file", "processed_data_directory"]
    mass_spec_desc: str = "ultra high resolution mass spectrum"
    mass_spec_eluent_intro: str = "direct_infusion_autosampler"
    processing_institution: str = "EMSL"
    workflow_git_url: str = "https://github.com/microbiomedata/enviroMS"
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
        self.minting_config_creds = minting_config_creds
        # Set the workflow version, prioritizing user input, then fetching from the Git URL, and finally using a default.
        self.workflow_version = (
            workflow_version
            or self.get_workflow_version(
                workflow_version_git_url="https://github.com/microbiomedata/enviroMS/blob/master/.bumpversion.cfg"
            )
            or "4.3.1"
        )

    def rerun(self):
        """
        Execute a rerun of the metadata generation process.

        This method processes the metadata file, generates biosamples (if needed)
        and metadata, and manages the workflow for generating NOM analysis data.

        Assumes raw data for NOM are on minio and that the raw data object URL field is populated.
        """
        do_client = DataObjectSearch()
        wf_client = WorkflowExecutionSearch()
        client_id, client_secret = self.load_credentials(
            config_file=self.minting_config_creds
        )
        nmdc_database_inst = self.start_nmdc_database()
        try:
            df = pd.read_csv(self.metadata_file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Metadata file not found: {self.metadata_file}")
        metadata_df = df.apply(lambda x: x.reset_index(drop=True))
        tqdm.write("\033[92mStarting metadata processing...\033[0m")

        # check for duplicate doj urls in the database
        self.check_doj_urls(
            metadata_df=metadata_df, url_columns=["processed_data_directory"]
        )
        # Iterate through each row in df to generate metadata
        for _, row in tqdm(
            metadata_df.iterrows(),
            total=metadata_df.shape[0],
            desc="Processing NOM rows",
        ):
            try:
                raw_data_object_id = do_client.get_record_by_attribute(
                    attribute_name="url",
                    attribute_value=self.raw_data_url + Path(row["raw_data_file"]).name,
                    fields="id",
                    exact_match=True,
                )[0]["id"]
            except Exception as e:
                raise ValueError(
                    f"Raw data object not found for URL: {self.raw_data_url + Path(row['raw_data_file']).name}"
                ) from e
            try:
                # find the NomAnalysis object - this is the old one
                prev_nom_analysis = wf_client.get_record_by_filter(
                    filter=f'{{"has_input":"{raw_data_object_id}","type":"{NmdcTypes.NomAnalysis}"}}',
                    fields="id,uses_calibration,execution_resource,processing_institution,was_informed_by",
                    all_pages=True,
                )
                # find the most recent metabolomics analysis object by the max id
                prev_nom_analysis = max(prev_nom_analysis, key=lambda x: x["id"])

                # increment the metab_id, find the last .digit group with a regex
                regex = r"(\d+)$"
                metab_analysis_id = re.sub(
                    regex,
                    lambda x: str(int(x.group(1)) + 1),
                    prev_nom_analysis["id"],
                )
            except Exception as e:
                raise IndexError(
                    f"NomAnalysis object not found for raw data object ID: {raw_data_object_id}"
                )
            processed_data = []
            # grab the calibration_id from the previous metabolomics analysis
            # Generate nom analysis instance, workflow_execution_set (metabolomics analysis), uses the raw data zip file
            nom_analysis = self.generate_nom_analysis(
                file_path=Path(row["raw_data_file"]),
                raw_data_id=raw_data_object_id,
                data_gen_id=prev_nom_analysis["was_informed_by"],
                processed_data_id="nmdc:placeholder",
                calibration_id=prev_nom_analysis["uses_calibration"],
                incremented_id=metab_analysis_id,
                CLIENT_ID=client_id,
                CLIENT_SECRET=client_secret,
            )
            processed_data_paths = list(
                Path(row["processed_data_directory"]).glob("**/*")
            )
            # Add a check that the processed data directory is not empty
            if not any(processed_data_paths):
                raise FileNotFoundError(
                    f"No files found in processed data directory: "
                    f"{row['processed_data_directory']}"
                )
            processed_data_paths = [x for x in processed_data_paths if x.is_file()]
            ### we will have processed data object AFTER the workflow is ran. Since this is how the lipidomics and gcms work, that is how this will function as well.
            for file in processed_data_paths:
                if file.suffix == ".csv":
                    # this is the .csv file of the processed data
                    processed_data_object_desc = "EnviroMS natural organic matter workflow molecular formula assignment output details"
                    processed_data_object = self.generate_data_object(
                        file_path=file,
                        data_category=self.workflow_param_data_category,
                        data_object_type=self.workflow_param_data_object_type,
                        description=processed_data_object_desc,
                        base_url=self.process_data_url
                        + Path(row["processed_data_directory"]).name
                        + "/",
                        CLIENT_ID=client_id,
                        CLIENT_SECRET=client_secret,
                        was_generated_by=nom_analysis.id,
                        alternative_id=None,
                    )
                    processed_data.append(processed_data_object.id)
                    # Update NomAnalysis times based on csv file
                    nom_analysis.started_at_time = datetime.fromtimestamp(
                        file.stat().st_ctime
                    ).strftime("%Y-%m-%d %H:%M:%S")
                    nom_analysis.ended_at_time = datetime.fromtimestamp(
                        file.stat().st_mtime
                    ).strftime("%Y-%m-%d %H:%M:%S")
                if file.suffix == ".json":
                    # Generate workflow parameter data object
                    # this is the .json file of processed data
                    workflow_param_data_object_desc = f"CoreMS processing parameters for natural organic matter analysis used to generate {nom_analysis.id}"

                    workflow_data_object = self.generate_data_object(
                        file_path=file,
                        data_category=self.workflow_param_data_category,
                        data_object_type=self.workflow_param_data_object_type,
                        description=workflow_param_data_object_desc,
                        base_url=self.process_data_url
                        + Path(row["processed_data_directory"]).name
                        + "/",
                        was_generated_by=nom_analysis.id,
                        CLIENT_ID=client_id,
                        CLIENT_SECRET=client_secret,
                        alternative_id=None,
                    )
            has_input = [workflow_data_object.id, raw_data_object_id]
            # Update the outputs for mass_spectrometry and nom_analysis
            self.update_outputs(
                analysis_obj=nom_analysis,
                raw_data_obj_id=raw_data_object_id,
                parameter_data_id=has_input,
                processed_data_id_list=processed_data,
                rerun=True,
            )
            nmdc_database_inst.data_object_set.append(processed_data_object)
            nmdc_database_inst.data_object_set.append(workflow_data_object)
            nmdc_database_inst.workflow_execution_set.append(nom_analysis)
            processed_data = []

        self.dump_nmdc_database(nmdc_database=nmdc_database_inst)
        api_metadata = Metadata()
        api_metadata.validate_json(self.database_dump_json_path)

    def run(self):
        """
        Execute the metadata generation process.

        This method processes the metadata file, generates biosamples (if needed)
        and metadata, and manages the workflow for generating NOM analysis data.
        """
        client_id, client_secret = self.load_credentials(
            config_file=self.minting_config_creds
        )
        nmdc_database_inst = self.start_nmdc_database()
        metadata_df = self.load_metadata()
        tqdm.write("\033[92mStarting metadata processing...\033[0m")
        processed_data = []
        self.check_for_biosamples(
            metadata_df=metadata_df,
            nmdc_database_inst=nmdc_database_inst,
            CLIENT_ID=client_id,
            CLIENT_SECRET=client_secret,
        )
        # check for duplicate doj urls in the database
        self.check_doj_urls(metadata_df=metadata_df, url_columns=self.unique_columns)
        # Iterate through each row in df to generate metadata
        for _, row in tqdm(
            metadata_df.iterrows(),
            total=metadata_df.shape[0],
            desc="Processing NOM rows",
        ):
            emsl_metadata, biosample_id = self.handle_biosample(row)
            # Generate MassSpectrometry record
            mass_spec = self.generate_mass_spectrometry(
                file_path=Path(emsl_metadata["data_path"]),
                instrument_name=emsl_metadata["instrument_used"],
                sample_id=biosample_id,
                raw_data_id="nmdc:placeholder",
                study_id=emsl_metadata["associated_studies"],
                processing_institution=self.processing_institution,
                mass_spec_config_name=emsl_metadata["mass_spec_config"],
                start_date=row["instrument_analysis_start_date"],
                end_date=row["instrument_analysis_end_date"],
                CLIENT_ID=client_id,
                CLIENT_SECRET=client_secret,
            )
            eluent_intro_pretty = self.mass_spec_eluent_intro.replace("_", " ")
            # raw is the zipped .d directory
            raw_data_object_desc = (
                f"Raw {emsl_metadata['instrument_used']} {eluent_intro_pretty} data."
            )
            raw_data_object = self.generate_data_object(
                file_path=Path(row["raw_data_file"]),
                data_category=self.raw_data_category,
                data_object_type=self.raw_data_object_type,
                description=raw_data_object_desc,
                base_url=self.raw_data_url,
                CLIENT_ID=client_id,
                CLIENT_SECRET=client_secret,
                was_generated_by=mass_spec.id,
            )
            # Generate nom analysis instance, workflow_execution_set (metabolomics analysis), uses the raw data zip file
            calibration_id = self.get_calibration_id(
                calibration_path=Path(row["ref_calibration_path"])
            )
            nom_analysis = self.generate_nom_analysis(
                file_path=Path(row["raw_data_file"]),
                calibration_id=calibration_id,
                raw_data_id=raw_data_object.id,
                data_gen_id=mass_spec.id,
                processed_data_id="nmdc:placeholder",
                CLIENT_ID=client_id,
                CLIENT_SECRET=client_secret,
            )
            processed_data_paths = list(
                Path(row["processed_data_directory"]).glob("**/*")
            )
            # Add a check that the processed data directory is not empty
            if not any(processed_data_paths):
                raise FileNotFoundError(
                    f"No files found in processed data directory: "
                    f"{row['processed_data_directory']}"
                )
            processed_data_paths = [x for x in processed_data_paths if x.is_file()]
            ### we will have processed data object AFTER the workflow is ran. Since this is how the lipidomics and gcms work, that is how this will function as well.
            for file in processed_data_paths:
                if file.suffix == ".csv":
                    # this is the .csv file of the processed data
                    processed_data_object_desc = (
                        f"EnviroMS {emsl_metadata['instrument_used']} "
                        "natural organic matter workflow molecular formula assignment output details"
                    )
                    processed_data_object = self.generate_data_object(
                        file_path=file,
                        data_category=self.workflow_param_data_category,
                        data_object_type=self.workflow_param_data_object_type,
                        description=processed_data_object_desc,
                        base_url=self.process_data_url
                        + Path(row["processed_data_directory"]).name
                        + "/",
                        CLIENT_ID=client_id,
                        CLIENT_SECRET=client_secret,
                        was_generated_by=nom_analysis.id,
                        alternative_id=None,
                    )
                    processed_data.append(processed_data_object.id)
                    # Update NomAnalysis times based on csv file
                    nom_analysis.started_at_time = datetime.fromtimestamp(
                        file.stat().st_ctime
                    ).strftime("%Y-%m-%d %H:%M:%S")
                    nom_analysis.ended_at_time = datetime.fromtimestamp(
                        file.stat().st_mtime
                    ).strftime("%Y-%m-%d %H:%M:%S")
                if file.suffix == ".json":
                    # Generate workflow parameter data object
                    # this is the .json file of processed data
                    workflow_param_data_object_desc = f"CoreMS processing parameters for natural organic matter analysis used to generate {nom_analysis.id}"

                    workflow_data_object = self.generate_data_object(
                        file_path=file,
                        data_category=self.workflow_param_data_category,
                        data_object_type=self.workflow_param_data_object_type,
                        description=workflow_param_data_object_desc,
                        base_url=self.process_data_url
                        + Path(row["processed_data_directory"]).name
                        + "/",
                        was_generated_by=nom_analysis.id,
                        CLIENT_ID=client_id,
                        CLIENT_SECRET=client_secret,
                        alternative_id=None,
                    )
            has_input = [workflow_data_object.id, raw_data_object.id]
            # Update the outputs for mass_spectrometry and nom_analysis
            self.update_outputs(
                mass_spec_obj=mass_spec,
                analysis_obj=nom_analysis,
                raw_data_obj_id=raw_data_object.id,
                parameter_data_id=has_input,
                processed_data_id_list=processed_data,
                rerun=False,
            )
            nmdc_database_inst.data_generation_set.append(mass_spec)
            nmdc_database_inst.data_object_set.append(raw_data_object)
            nmdc_database_inst.data_object_set.append(processed_data_object)
            nmdc_database_inst.data_object_set.append(workflow_data_object)
            nmdc_database_inst.workflow_execution_set.append(nom_analysis)
            processed_data = []

        self.dump_nmdc_database(nmdc_database=nmdc_database_inst)
        api_metadata = Metadata()
        api_metadata.validate_json(self.database_dump_json_path)

    def get_calibration_id(
        self,
        calibration_path: str,
    ) -> str:
        """
        Get the calibration ID from the NMDC API using the md5 checksum of the calibration file.

        Parameters
        ----------
        calibration_path : str
            The file path of the calibration file.

        Returns
        -------
        str
            The calibration ID if found, otherwise None.
        """
        # Lookup calibration id by md5 checksum of calibration_path file
        calib_md5 = hashlib.md5(calibration_path.open("rb").read()).hexdigest()
        do_client = DataObjectSearch()
        cs_client = CalibrationSearch()
        try:
            calib_do_id = do_client.get_record_by_attribute(
                attribute_name="md5_checksum",
                attribute_value=calib_md5,
                fields="id",
                exact_match=True,
            )[0]["id"]
            calibration_id = cs_client.get_record_by_attribute(
                attribute_name="calibration_object",
                attribute_value=calib_do_id,
                fields="id",
                exact_match=True,
            )[0]["id"]
        except ValueError as e:
            print(f"Calibration object does not exist: {e}")
            calibration_id = None
        except IndexError as e:
            print(f"Calibration object not found: {e}")
            calibration_id = None
        except Exception as e:
            print(f"An error occurred: {e}")
        return calibration_id

    def generate_nom_analysis(
        self,
        file_path: Path,
        raw_data_id: str,
        data_gen_id: str,
        processed_data_id: str,
        CLIENT_ID: str,
        CLIENT_SECRET: str,
        calibration_id: str = None,
        incremented_id: str = None,
    ) -> nmdc.NomAnalysis:
        """
        Generate a metabolomics analysis object from the provided file information.

        Parameters
        ----------
        file_path : Path
            The file path of the metabolomics analysis data file.
        raw_data_id : str
            The ID of the raw data associated with the analysis.
        data_gen_id : str
            The ID of the data generation process that informed this analysis.
        processed_data_id : str
            The ID of the processed data resulting from this analysis.
        CLIENT_ID : str
            The client ID for the NMDC API.
        CLIENT_SECRET : str
            The client secret for the NMDC API.
        calibration_id : str, optional
            The ID of the calibration object used in the analysis. If None, no calibration is used.
        incremented_id : str, optional
            The incremented ID for the metabolomics analysis. If None, a new ID will be minted.

        Returns
        -------
        nmdc.NomAnalysis
            The generated metabolomics analysis object.
        """
        if incremented_id is None:
            mint = Minter()
            nmdc_id = mint.mint(
                nmdc_type=NmdcTypes.NomAnalysis,
                client_id=CLIENT_ID,
                client_secret=CLIENT_SECRET,
            )
            incremented_id = nmdc_id + ".1"

        data_dict = {
            "id": incremented_id,
            "name": f"{self.workflow_analysis_name} for {file_path.name}",
            "description": self.workflow_description,
            "uses_calibration": calibration_id,
            "processing_institution": self.processing_institution,
            "execution_resource": self.execution_resource,
            "git_url": self.workflow_git_url,
            "version": self.workflow_version,
            "was_informed_by": data_gen_id,
            "has_input": [raw_data_id],
            "has_output": [processed_data_id],
            "started_at_time": "placeholder",
            "ended_at_time": "placeholder",
            "type": NmdcTypes.NomAnalysis,
        }
        self.clean_dict(data_dict)
        nomAnalysis = nmdc.NomAnalysis(**data_dict)

        return nomAnalysis
