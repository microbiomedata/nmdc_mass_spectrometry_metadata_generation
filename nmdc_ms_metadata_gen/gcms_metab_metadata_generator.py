import ast
import logging
import os
import re
from pathlib import Path
from typing import List

import nmdc_schema.nmdc as nmdc
import pandas as pd
from dotenv import load_dotenv
from nmdc_api_utilities.data_object_search import DataObjectSearch
from nmdc_api_utilities.workflow_execution_search import WorkflowExecutionSearch
from tqdm import tqdm

from nmdc_ms_metadata_gen.data_classes import GCMSMetabWorkflowMetadata, NmdcTypes
from nmdc_ms_metadata_gen.metadata_generator import NMDCWorkflowMetadataGenerator

load_dotenv()
ENV = os.getenv("NMDC_ENV", "prod")


class GCMSMetabolomicsMetadataGenerator(NMDCWorkflowMetadataGenerator):
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
    raw_data_url : str, optional
        Base URL for the raw data files. If the raw data url is not directly passed in, it will use the raw data urls from the metadata file.
    process_data_url : str
        Base URL for the processed data files.
    minting_config_creds : str
        Path to the minting configuration credentials file.
    calibration_standard : str, optional
        Calibration standard used for the data. Default is "fames".
    configuration_file_name : str
        Name of the configuration file.

    Attributes
    ----------
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

    # Metadata attributes
    unique_columns: List[str] = ["processed_data_file"]

    # Data Generation attributes
    mass_spec_desc: str = (
        "Generation of mass spectrometry data by GC/MS for the analysis of metabolites."
    )
    mass_spec_eluent_intro: str = "gas_chromatography"
    analyte_category: str = "metabolome"
    raw_data_obj_type: str = "GC-MS Raw Data"
    raw_data_obj_desc: str = (
        "GC/MS low resolution raw data for metabolomics data acquisition."
    )

    # Workflow metadata
    workflow_analysis_name: str = "GC/MS Metabolomics analysis"
    workflow_description: str = (
        "Analysis of raw mass spectrometry data for the annotation of metabolites."
    )
    workflow_git_url: str = (
        "https://github.com/microbiomedata/metaMS/wdl/metaMS_gcms.wdl"
    )
    workflow_version: str
    workflow_category: str = "gc_ms_metabolomics"

    # Processed data attributes
    processed_data_category: str = "processed_data"
    processed_data_object_type: str = "GC-MS Metabolomics Results"
    processed_data_object_description: str = (
        "Metabolomics annotations as a result of a GC/MS metabolomics workflow activity."
    )

    def __init__(
        self,
        metadata_file: str,
        database_dump_json_path: str,
        process_data_url: str,
        configuration_file_name: str,
        raw_data_url: str = None,
        minting_config_creds: str = None,
        workflow_version: str = None,
        calibration_standard: str = "fames",
    ):
        super().__init__(
            metadata_file=metadata_file,
            database_dump_json_path=database_dump_json_path,
            raw_data_url=raw_data_url,
            process_data_url=process_data_url,
        )
        # Set the workflow version, prioritizing user input, then fetching from the Git URL, and finally using a default.
        self.workflow_version = workflow_version or self.get_workflow_version(
            workflow_version_git_url="https://github.com/microbiomedata/metaMS/blob/master/.bumpversion.cfg"
        )

        self.minting_config_creds = minting_config_creds
        # Calibration attributes
        self.calibration_standard = calibration_standard

        # Workflow Configuration attributes
        self.configuration_file_name = configuration_file_name

    def rerun(self) -> nmdc.Database:
        """
        Execute a re run of the metadata generation process for GC/MS metabolomics data.

        This method performs the following steps:
        1. Initialize an NMDC Database instance.
        3. Load and process metadata to create NMDC objects.
        4. Generate Metabolomics Analysis and Processed Data objects.
        5. Update outputs for the Metabolomics Analysis object.
        6. Append generated objects to the NMDC Database.
        7. Dump the NMDC Database to a JSON file.

        Returns
        -------
        nmdc.Database
            The generated NMDC database instance containing all generated metadata objects.

        Raises
        ------
        FileNotFoundError
            If the metadata file is not found.

        Notes
        -----
        This method uses tqdm to display progress bars for the processing of calibration information and
        mass spectrometry metadata.

        """
        wf_client = WorkflowExecutionSearch(env=ENV)
        client_id, client_secret = self.load_credentials(
            config_file=self.minting_config_creds
        )

        # Start NMDC database and make metadata dataframe
        nmdc_database_inst = self.start_nmdc_database()
        try:
            df = pd.read_csv(self.metadata_file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Metadata file not found: {self.metadata_file}")
        metadata_df = df.apply(lambda x: x.reset_index(drop=True))

        # just check the process data urls
        self.check_doj_urls(
            metadata_df=metadata_df, url_columns=["processed_data_file"]
        )

        # Get the configuration file data object id and add it to the metadata_df
        do_client = DataObjectSearch(env=ENV)
        config_do_id = do_client.get_record_by_attribute(
            attribute_name="name",
            attribute_value=self.configuration_file_name,
            fields="id",
            exact_match=True,
        )[0]["id"]

        # process workflow metadata
        for _, data in tqdm(
            metadata_df.iterrows(),
            total=metadata_df.shape[0],
            desc="Processing Remaining Metadata",
        ):
            raw_data_object_id = do_client.get_record_by_attribute(
                attribute_name="url",
                attribute_value=self.raw_data_url + Path(data["raw_data_file"]).name,
                fields="id",
                exact_match=True,
            )[0]["id"]
            # find the MetabolomicsAnalysis object - this is the old one
            prev_metab_analysis = wf_client.get_record_by_filter(
                filter=f'{{"has_input":"{raw_data_object_id}","type":"{NmdcTypes.MetabolomicsAnalysis}"}}',
                fields="id,uses_calibration,execution_resource,processing_institution,was_informed_by",
                all_pages=True,
            )
            # find the most recent metabolomics analysis object by the max id
            prev_metab_analysis = max(prev_metab_analysis, key=lambda x: x["id"])
            # increment the metab_id, find the last .digit group with a regex
            regex = r"(\d+)$"
            metab_analysis_id = re.sub(
                regex,
                lambda x: str(int(x.group(1)) + 1),
                prev_metab_analysis["id"],
            )

            # Generate processed data object
            processed_data_object = self.generate_data_object(
                file_path=Path(data["processed_data_file"]),
                data_category=self.processed_data_category,
                data_object_type=self.processed_data_object_type,
                description=self.processed_data_object_description,
                base_url=self.process_data_url,
                CLIENT_ID=client_id,
                CLIENT_SECRET=client_secret,
                was_generated_by=metab_analysis_id,
            )

            # Generate metabolite identifications
            metabolite_identifications = self.generate_metab_identifications(
                processed_data_file=Path(data["processed_data_file"])
            )

            # need to generate a new metabolomics analysis object with the newly incremented id
            metab_analysis = self.generate_metabolomics_analysis(
                cluster_name=prev_metab_analysis["execution_resource"],
                raw_data_name=Path(data["raw_data_file"]).name,
                raw_data_id=raw_data_object_id,
                data_gen_id_list=prev_metab_analysis["was_informed_by"],
                processed_data_id=processed_data_object.id,
                parameter_data_id=config_do_id,
                processing_institution=prev_metab_analysis["processing_institution"],
                incremeneted_id=metab_analysis_id,
                CLIENT_ID=client_id,
                CLIENT_SECRET=client_secret,
                calibration_id=prev_metab_analysis["uses_calibration"],
                metabolite_identifications=metabolite_identifications,
            )

            # Update MetabolomicsAnalysis times based on processed data file
            processed_file = Path(data["processed_data_file"])
            start_time, end_time = self.get_start_end_times(processed_file)
            metab_analysis.started_at_time = start_time
            metab_analysis.ended_at_time = end_time

            has_inputs = [config_do_id, raw_data_object_id]
            self.update_outputs(
                analysis_obj=metab_analysis,
                raw_data_obj_id=raw_data_object_id,
                parameter_data_id=has_inputs,
                processed_data_id_list=[processed_data_object.id],
                rerun=True,
            )
            nmdc_database_inst.data_object_set.append(processed_data_object)
            nmdc_database_inst.workflow_execution_set.append(metab_analysis)

        self.dump_nmdc_database(
            nmdc_database=nmdc_database_inst, json_path=self.database_dump_json_path
        )
        return self.nmdc_db_to_dict(nmdc_database_inst)

    def run(self) -> nmdc.Database:
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

        Returns
        -------
        nmdc.Database
            The generated NMDC database instance containing all generated metadata objects.

        Raises
        ------
        ValueError
            If the calibration standard is not supported.

        Notes
        -----
        This method uses tqdm to display progress bars for the processing of calibration information and
        mass spectrometry metadata.

        """

        client_id, client_secret = self.load_credentials(
            config_file=self.minting_config_creds
        )

        if self.calibration_standard != "fames":
            raise ValueError("Only FAMES calibration is supported at this time.")

        # Start NMDC database and make metadata dataframe
        nmdc_database_inst = self.start_nmdc_database()
        df = self.load_metadata()
        metadata_df = df.apply(lambda x: x.reset_index(drop=True))
        self.check_for_biosamples(
            metadata_df=metadata_df,
            nmdc_database_inst=nmdc_database_inst,
            CLIENT_ID=client_id,
            CLIENT_SECRET=client_secret,
        )
        # check if the raw data url is directly passed in or needs to be built with raw data file
        raw_col = (
            "raw_data_url" if "raw_data_url" in metadata_df.columns else "raw_data_file"
        )
        urls_columns = self.unique_columns + [raw_col]
        self.check_doj_urls(metadata_df=metadata_df, url_columns=urls_columns)

        # Get the configuration file data object id and add it to the metadata_df
        do_client = DataObjectSearch(env=ENV)
        config_do_id = do_client.get_record_by_attribute(
            attribute_name="name",
            attribute_value=self.configuration_file_name,
            fields="id",
            exact_match=True,
        )[0]["id"]

        # check if there is an existing calibration_id in the metadata. If not, we need to generate them
        if (
            "calibration_id" not in metadata_df.columns
            or metadata_df["calibration_id"].isnull().all()
        ):
            self.generate_calibration_id(
                metadata_df=metadata_df,
                nmdc_database_inst=nmdc_database_inst,
                CLIENT_ID=client_id,
                CLIENT_SECRET=client_secret,
            )
        # check if manifest ids are provided or if we need to generate them
        self.check_manifest(
            metadata_df=metadata_df,
            nmdc_database_inst=nmdc_database_inst,
            CLIENT_ID=client_id,
            CLIENT_SECRET=client_secret,
        )

        self.generate_mass_spec_fields(
            metadata_df=metadata_df,
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
                instrument_id=workflow_metadata_obj.instrument_id,
                sample_id=workflow_metadata_obj.sample_id,
                raw_data_id="nmdc:placeholder",
                study_id=workflow_metadata_obj.nmdc_study,
                processing_institution=(
                    workflow_metadata_obj.processing_institution_generation
                    if workflow_metadata_obj.processing_institution_generation
                    else workflow_metadata_obj.processing_institution
                ),
                mass_spec_configuration_id=workflow_metadata_obj.mass_spec_configuration_id,
                lc_config_id=workflow_metadata_obj.lc_config_id,
                start_date=workflow_metadata_obj.instrument_analysis_start_date,
                end_date=workflow_metadata_obj.instrument_analysis_end_date,
                CLIENT_ID=client_id,
                CLIENT_SECRET=client_secret,
                calibration_id=workflow_metadata_obj.calibration_id,
                instrument_instance_specifier=workflow_metadata_obj.instrument_instance_specifier,
            )
            # Generate raw data object
            raw_data_object = self.generate_data_object(
                file_path=Path(workflow_metadata_obj.raw_data_file),
                data_category=self.raw_data_category,
                data_object_type=self.raw_data_obj_type,
                description=self.raw_data_obj_desc,
                base_url=self.raw_data_url,
                CLIENT_ID=client_id,
                CLIENT_SECRET=client_secret,
                was_generated_by=mass_spec.id,
                url=workflow_metadata_obj.raw_data_url,
                in_manifest=workflow_metadata_obj.manifest_id,
            )
            raw_data_object_id = raw_data_object.id

            # Generate metabolite identifications
            metabolite_identifications = self.generate_metab_identifications(
                processed_data_file=workflow_metadata_obj.processed_data_file
            )

            # Generate metabolomics analysis object with metabolite identifications
            metab_analysis = self.generate_metabolomics_analysis(
                cluster_name=workflow_metadata_obj.execution_resource,
                raw_data_name=Path(workflow_metadata_obj.raw_data_file).name,
                raw_data_id=raw_data_object_id,
                data_gen_id_list=[mass_spec.id],
                processed_data_id="nmdc:placeholder",
                parameter_data_id=config_do_id,
                processing_institution=(
                    workflow_metadata_obj.processing_institution_workflow
                    if workflow_metadata_obj.processing_institution_workflow
                    else workflow_metadata_obj.processing_institution
                ),
                CLIENT_ID=client_id,
                CLIENT_SECRET=client_secret,
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
                CLIENT_ID=client_id,
                CLIENT_SECRET=client_secret,
                was_generated_by=metab_analysis.id,
            )

            # Update MetabolomicsAnalysis times based on processed data file
            processed_file = Path(workflow_metadata_obj.processed_data_file)
            start_time, end_time = self.get_start_end_times(processed_file)
            metab_analysis.started_at_time = start_time
            metab_analysis.ended_at_time = end_time

            has_inputs = [config_do_id, raw_data_object_id]
            self.update_outputs(
                mass_spec_obj=mass_spec,
                analysis_obj=metab_analysis,
                raw_data_obj_id=raw_data_object_id,
                parameter_data_id=has_inputs,
                processed_data_id_list=[processed_data_object.id],
            )

            nmdc_database_inst.data_generation_set.append(mass_spec)
            nmdc_database_inst.data_object_set.append(raw_data_object)
            nmdc_database_inst.data_object_set.append(processed_data_object)
            nmdc_database_inst.workflow_execution_set.append(metab_analysis)

        self.dump_nmdc_database(
            nmdc_database=nmdc_database_inst, json_path=self.database_dump_json_path
        )
        logging.info("Metadata processing completed.")
        # change db object to dict
        return self.nmdc_db_to_dict(nmdc_database_inst)

    def generate_calibration_id(
        self,
        metadata_df: pd.DataFrame,
        nmdc_database_inst: nmdc.Database,
        CLIENT_ID: str,
        CLIENT_SECRET: str,
    ) -> None:
        """
        Generate calibration information and data objects for each calibration file.

        Parameters
        ----------
        metadata_df : pd.DataFrame
            The metadata DataFrame.
        nmdc_database_inst : nmdc.Database
            The NMDC Database instance.
        CLIENT_ID : str
            The client ID for the NMDC API.
        CLIENT_SECRET : str
            The client secret for the NMDC API.

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
            # Check if the df has calibration_file_url, if not, set url to None to use the raw_data_url
            if "calibration_file_url" in metadata_df.columns:
                url = metadata_df.loc[
                    metadata_df["calibration_file"].eq(calibration_file),
                    "calibration_file_url",
                ].iloc[0]
            else:
                url = None

            calibration_data_object = self.generate_data_object(
                file_path=Path(calibration_file),
                data_category=self.raw_data_category,
                data_object_type=self.raw_data_obj_type,
                description=self.raw_data_obj_desc,
                base_url=self.raw_data_url,
                CLIENT_ID=CLIENT_ID,
                CLIENT_SECRET=CLIENT_SECRET,
                url=url,
            )
            nmdc_database_inst.data_object_set.append(calibration_data_object)

            calibration = self.generate_calibration(
                calibration_object=calibration_data_object,
                CLIENT_ID=CLIENT_ID,
                CLIENT_SECRET=CLIENT_SECRET,
                fames=self.calibration_standard,
                internal=False,
            )
            nmdc_database_inst.calibration_set.append(calibration)

            # Add calibration information id to metadata_df
            metadata_df.loc[
                metadata_df["calibration_file"] == calibration_file, "calibration_id"
            ] = calibration.id

    def generate_calibration(
        self,
        calibration_object: dict,
        CLIENT_ID: str,
        CLIENT_SECRET: str,
        fames: bool = True,
        internal: bool = False,
    ) -> nmdc.CalibrationInformation:
        """
        Generate a CalibrationInformation object for the NMDC Database.

        Parameters
        ----------
        calibration_object : dict
            The calibration data object.
        CLIENT_ID : str
            The client ID for the NMDC API.
        CLIENT_SECRET : str
            The client secret for the NMDC API.
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
        if fames and not internal:
            nmdc_id = self.id_pool.get_id(
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
        Create a GCMSMetabWorkflowMetadata object from a dictionary of workflow metadata.

        Parameters
        ----------
        row : dict[str, str]
            Dictionary containing metadata for a workflow. This is typically
            a row from the input metadata CSV file.

        Returns
        -------
        GCMSMetabWorkflowMetadata
            A GCMSMetabWorkflowMetadata object populated with data from the input dictionary.

        """
        return GCMSMetabWorkflowMetadata(
            sample_id=row["sample_id"],
            nmdc_study=ast.literal_eval(row["biosample.associated_studies"]),
            processing_institution=row.get("processing_institution"),
            processing_institution_generation=row.get(
                "processing_institution_generation"
            ),
            processing_institution_workflow=row.get("processing_institution_workflow"),
            processed_data_file=row["processed_data_file"],
            raw_data_file=row["raw_data_file"],
            mass_spec_configuration_id=row["mass_spec_configuration_id"],
            lc_config_id=row["lc_config_id"],
            instrument_id=row["instrument_id"],
            instrument_analysis_start_date=row.get("instrument_analysis_start_date"),
            instrument_analysis_end_date=row.get("instrument_analysis_end_date"),
            execution_resource=row["execution_resource"],
            calibration_id=row["calibration_id"],
            raw_data_url=row.get("raw_data_url"),
            manifest_id=row.get("manifest_id"),
            instrument_instance_specifier=row.get("instrument_instance_specifier"),
        )

    def generate_metab_identifications(
        self, processed_data_file: str
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
            lambda x: x.loc[x["Similarity Score"].idxmax()], include_groups=False
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
