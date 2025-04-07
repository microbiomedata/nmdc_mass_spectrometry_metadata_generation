# -*- coding: utf-8 -*-
from src.metadata_generator import NMDCMetadataGenerator
from tqdm import tqdm
from pathlib import Path
from datetime import datetime
import logging
from nmdc_api_utilities.metadata import Metadata
import ast
import pandas as pd
from src.data_classes import LCMSLipidWorkflowMetadata


class LCMSLipidomicsMetadataGenerator(NMDCMetadataGenerator):
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
    minting_config_creds : str, OPTIONAL
        Path to the configuration file containing the client ID and client secret for minting NMDC IDs. It can also include the bio ontology API key if generating biosample ids is needed.
        If not provided, the CLIENT_ID, CLIENT_SECRET, and BIO_API_KEY environment variables will be used.

    Attributes
    ----------
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
        minting_config_creds: str = None,
    ):
        super().__init__(
            metadata_file=metadata_file,
            database_dump_json_path=database_dump_json_path,
            raw_data_url=raw_data_url,
            process_data_url=process_data_url,
        )

        self.unique_columns = ["raw_data_file", "processed_data_directory"]
        self.minting_config_creds = minting_config_creds
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
        client_id, client_secret = self.load_credentials(
            config_file=self.minting_config_creds
        )
        nmdc_database_inst = self.start_nmdc_database()
        metadata_df = self.load_metadata()
        self.check_for_biosamples(
            metadata_df=metadata_df,
            nmdc_database_inst=nmdc_database_inst,
            CLIENT_ID=client_id,
            CLIENT_SECRET=client_secret,
        )
        # check for duplicate doj urls in the database
        processed_data_paths = [
            list(Path(x).glob("**/*"))
            for x in metadata_df["processed_data_directory"].to_list()
        ]
        # Add a check that the processed data directory is not empty
        if not any(processed_data_paths):
            raise FileNotFoundError(
                f"No files found in processed data directory: "
                f"{metadata_df['processed_data']}"
            )
        processed_data_paths = [
            file for sublist in processed_data_paths for file in sublist
        ]
        raw_data_paths = [Path(x) for x in metadata_df["raw_data_file"].to_list()]
        urls = [self.process_data_url + str(x.name) for x in processed_data_paths] + [
            self.raw_data_url + str(x.name) for x in raw_data_paths
        ]
        self.check_doj_urls(urls=urls)

        for _, data in tqdm(
            metadata_df.iterrows(),
            total=metadata_df.shape[0],
            desc="Processing LCMS biosamples",
        ):
            workflow_metadata = self.create_workflow_metadata(data)

            mass_spec = self.generate_mass_spectrometry(
                file_path=Path(workflow_metadata.raw_data_file),
                instrument_name=workflow_metadata.instrument_used,
                sample_id=data["biosample_id"],
                raw_data_id="nmdc:placeholder",
                study_id=ast.literal_eval(data["biosample.associated_studies"]),
                processing_institution=data["processing_institution"],
                mass_spec_config_name=workflow_metadata.mass_spec_config_name,
                lc_config_name=workflow_metadata.lc_config_name,
                start_date=workflow_metadata.instrument_analysis_start_date,
                end_date=workflow_metadata.instrument_analysis_end_date,
                CLIENT_ID=client_id,
                CLIENT_SECRET=client_secret,
            )

            raw_data_object = self.generate_data_object(
                file_path=Path(workflow_metadata.raw_data_file),
                data_category=self.raw_data_category,
                data_object_type=self.raw_data_obj_type,
                description=self.raw_data_obj_desc,
                base_url=self.raw_data_url,
                CLIENT_ID=client_id,
                CLIENT_SECRET=client_secret,
                was_generated_by=mass_spec.id,
            )

            metab_analysis = self.generate_metabolomics_analysis(
                cluster_name=workflow_metadata.execution_resource,
                raw_data_name=Path(workflow_metadata.raw_data_file).name,
                raw_data_id=raw_data_object.id,
                data_gen_id=mass_spec.id,
                processed_data_id="nmdc:placeholder",
                parameter_data_id="nmdc:placeholder",
                processing_institution=data["processing_institution"],
                CLIENT_ID=client_id,
                CLIENT_SECRET=client_secret,
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
                            CLIENT_ID=client_id,
                            CLIENT_SECRET=client_secret,
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
                            CLIENT_ID=client_id,
                            CLIENT_SECRET=client_secret,
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
                            CLIENT_ID=client_id,
                            CLIENT_SECRET=client_secret,
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
