# -*- coding: utf-8 -*-
from src.metadata_generator import NMDCMetadataGenerator
from tqdm import tqdm
from pathlib import Path
from datetime import datetime
import logging
from nmdc_api_utilities.metadata import Metadata
import ast
import pandas as pd
from nmdc_api_utilities.data_object_search import DataObjectSearch
import nmdc_schema.nmdc as nmdc
from nmdc_api_utilities.minter import Minter
from typing import List
from src.data_classes import NmdcTypes, GCMSMetabWorkflowMetadata


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
        minting_config_creds: str = None,
        calibration_standard: str = "fames",
        configuration_file_name: str = "emsl_gcms_corems_params.toml",
    ):
        super().__init__(
            metadata_file=metadata_file,
            database_dump_json_path=database_dump_json_path,
            raw_data_url=raw_data_url,
            process_data_url=process_data_url,
        )
        self.minting_config_creds = minting_config_creds
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
        self.check_doj_urls(metadata_df=metadata_df, url_columns=self.unique_columns)
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
                metadata_df=metadata_df,
                nmdc_database_inst=nmdc_database_inst,
                CLIENT_ID=client_id,
                CLIENT_SECRET=client_secret,
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
                CLIENT_ID=client_id,
                CLIENT_SECRET=client_secret,
                calibration_id=workflow_metadata_obj.calibration_id,
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
            calibration_data_object = self.generate_data_object(
                file_path=Path(calibration_file),
                data_category=self.raw_data_category,
                data_object_type=self.raw_data_obj_type,
                description=self.raw_data_obj_desc,
                base_url=self.raw_data_url,
                CLIENT_ID=CLIENT_ID,
                CLIENT_SECRET=CLIENT_SECRET,
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
