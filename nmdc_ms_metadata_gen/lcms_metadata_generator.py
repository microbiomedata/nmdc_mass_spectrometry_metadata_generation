import ast
import logging
import os
import re
from pathlib import Path

import nmdc_schema.nmdc as nmdc
import pandas as pd
from dotenv import load_dotenv
from nmdc_api_utilities.data_object_search import DataObjectSearch
from nmdc_api_utilities.workflow_execution_search import WorkflowExecutionSearch
from tqdm import tqdm

from nmdc_ms_metadata_gen.data_classes import LCMSLipidWorkflowMetadata, NmdcTypes
from nmdc_ms_metadata_gen.metadata_generator import NMDCWorkflowMetadataGenerator

load_dotenv()
ENV = os.getenv("NMDC_ENV", "prod")


class LCMSMetadataGenerator(NMDCWorkflowMetadataGenerator):
    """
    A class for generating NMDC metadata objects using provided metadata files and configuration
    for LC-MS data.

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
        Path to the configuration file containing the client ID and client secret for minting NMDC IDs.
    test : bool, optional
        Flag indicating whether to run in test mode. If True, will skip biosample ID checks in the database, data object URL check, and will use local IDs (skip API minting). Default is False.
    skip_sample_id_check : bool, optional
        Flag to skip sample ID checking in MongoDB. If True, will skip biosample and
        processed sample ID checks even in production mode. Default is False.
    """

    def __init__(
        self,
        metadata_file: str,
        database_dump_json_path: str,
        raw_data_url: str,
        process_data_url: str,
        test: bool = False,
        skip_sample_id_check: bool = False,
    ):
        super().__init__(
            metadata_file=metadata_file,
            database_dump_json_path=database_dump_json_path,
            raw_data_url=raw_data_url,
            process_data_url=process_data_url,
            test=test,
            skip_sample_id_check=skip_sample_id_check,
        )
        self.test = test

    def run(self) -> nmdc.Database:
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


        Returns
        -------
        nmdc.Database
            The generated NMDC database instance containing all generated metadata objects.

        Raises
        ------
        FileNotFoundError
            If the processed data directory is empty or not found.
        ValueError
            If the number of files in the processed data directory is not as expected

        Notes
        -----
        This method uses tqdm to display progress bars for the processing of
        biosamples and mass spectrometry metadata.

        """
        client_id, client_secret = self.load_credentials(
            config_file=self.minting_config_creds
        )
        client_id, client_secret = self.load_credentials(
            config_file=self.minting_config_creds
        )
        nmdc_database_inst = self.start_nmdc_database()
        metadata_df = self.load_metadata()

        # check if manifest ids are provided or if we need to generate them
        self.check_manifest(
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

        if not self.test:
            self.check_doj_urls(metadata_df=metadata_df, url_columns=urls_columns)

        # Generate mass spec fields
        self.generate_mass_spec_fields(
            metadata_df=metadata_df,
        )

        for _, data in tqdm(
            metadata_df.iterrows(),
            total=metadata_df.shape[0],
            desc="Processing LCMS samples",
        ):
            workflow_metadata = self.create_workflow_metadata(data)

            mass_spec = self.generate_mass_spectrometry(
                file_path=Path(workflow_metadata.raw_data_file),
                instrument_id=workflow_metadata.instrument_id,
                sample_id=data["sample_id"],
                raw_data_id="nmdc:placeholder",
                study_id=ast.literal_eval(data["associated_studies"]),
                processing_institution=(
                    workflow_metadata.processing_institution_generation
                    if workflow_metadata.processing_institution_generation
                    else workflow_metadata.processing_institution
                ),
                mass_spec_configuration_id=workflow_metadata.mass_spec_configuration_id,
                lc_config_id=workflow_metadata.lc_config_id,
                start_date=workflow_metadata.instrument_analysis_start_date,
                end_date=workflow_metadata.instrument_analysis_end_date,
                instrument_instance_specifier=workflow_metadata.instrument_instance_specifier,
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
                url=workflow_metadata.raw_data_url,
                in_manifest=workflow_metadata.manifest_id,
            )

            if self.add_metabolite_ids:
                # Generate metabolite identifications
                metabolite_identifications = self.generate_metab_identifications(
                    processed_data_dir=workflow_metadata.processed_data_dir
                )
            else:
                metabolite_identifications = None

            metab_analysis = self.generate_metabolomics_analysis(
                cluster_name=workflow_metadata.execution_resource,
                raw_data_name=Path(workflow_metadata.raw_data_file).name,
                raw_data_id=raw_data_object.id,
                data_gen_id_list=[mass_spec.id],
                processed_data_id="nmdc:placeholder",
                parameter_data_id="nmdc:placeholder",
                processing_institution=(
                    workflow_metadata.processing_institution_workflow
                    if workflow_metadata.processing_institution_workflow
                    else workflow_metadata.processing_institution
                ),
                CLIENT_ID=client_id,
                CLIENT_SECRET=client_secret,
                metabolite_identifications=metabolite_identifications,
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
                            base_url=self.process_data_url
                            + Path(workflow_metadata.processed_data_dir).name
                            + "/",
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
                            base_url=self.process_data_url
                            + Path(workflow_metadata.processed_data_dir).name
                            + "/",
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
                            base_url=self.process_data_url
                            + Path(workflow_metadata.processed_data_dir).name
                            + "/",
                            CLIENT_ID=client_id,
                            CLIENT_SECRET=client_secret,
                            was_generated_by=metab_analysis.id,
                        )
                        nmdc_database_inst.data_object_set.append(processed_data_object)
                        processed_data.append(processed_data_object.id)

                        # Update MetabolomicsAnalysis times based on HDF5 file
                        start_time, end_time = self.get_start_end_times(file)
                        metab_analysis.started_at_time = start_time
                        metab_analysis.ended_at_time = end_time

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
            has_input = [
                parameter_data_id,
                raw_data_object.id,
            ] + self.existing_data_objects

            self.update_outputs(
                mass_spec_obj=mass_spec,
                analysis_obj=metab_analysis,
                raw_data_obj_id=raw_data_object.id,
                parameter_data_id=has_input,
                processed_data_id_list=processed_data,
                rerun=False,
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

        self.dump_nmdc_database(
            nmdc_database=nmdc_database_inst, json_path=self.database_dump_json_path
        )
        logging.info("Metadata processing completed.")
        # change db object to dict
        return self.nmdc_db_to_dict(nmdc_database_inst)

    def rerun(self) -> nmdc.Database:
        """
        Execute a rerun of the metadata generation process for metabolomics data.

        This method performs the following steps:
        1. Initialize an NMDC Database instance.
        2. Load and process metadata to create NMDC objects.
        3. Generate Metabolomics Analysis and Processed Data objects.
        4. Update outputs for the Metabolomics Analysis object.
        5. Append generated objects to the NMDC Database.
        6. Dump the NMDC Database to a JSON file.

        Returns
        -------
        nmdc.Database
            The generated NMDC database instance containing all generated metadata objects.

        Raises
        ------
        FileNotFoundError
            If the processed data directory is empty or not found.
        ValueError
            If the number of files in the processed data directory is not as expected.

        Notes
        -----
        This method uses tqdm to display progress bars for the processing of
        biosamples and mass spectrometry metadata.

        """
        client_id, client_secret = self.load_credentials(
            config_file=self.minting_config_creds
        )
        wf_client = WorkflowExecutionSearch(env=ENV)
        do_client = DataObjectSearch(env=ENV)
        client_id, client_secret = self.load_credentials(
            config_file=self.minting_config_creds
        )
        nmdc_database_inst = self.start_nmdc_database()
        try:
            df = pd.read_csv(self.metadata_file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Metadata file not found: {self.metadata_file}")
        metadata_df = df.apply(lambda x: x.reset_index(drop=True))
        if not self.test:
            # check for duplicate doj urls in the database
            self.check_doj_urls(
                metadata_df=metadata_df, url_columns=["processed_data_directory"]
            )

        for _, data in tqdm(
            metadata_df.iterrows(),
            total=metadata_df.shape[0],
            desc="Processing LCMS samples",
        ):
            # workflow_metadata = self.create_workflow_metadata(data)
            raw_data_object_id = do_client.get_record_by_attribute(
                attribute_name="url",
                attribute_value=self.raw_data_url + Path(data["raw_data_file"]).name,
                fields="id",
                exact_match=True,
            )[0]["id"]
            # find the MetabolomicsAnalysis object - this is the old one
            prev_metab_analysis = wf_client.get_record_by_filter(
                filter=f'{{"has_input":"{raw_data_object_id}","type":"{NmdcTypes.get("MetabolomicsAnalysis")}"}}',
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
            metab_analysis = self.generate_metabolomics_analysis(
                cluster_name=prev_metab_analysis["execution_resource"],
                raw_data_name=Path(data["raw_data_file"]).name,
                raw_data_id=raw_data_object_id,
                data_gen_id_list=prev_metab_analysis["was_informed_by"],
                processed_data_id="nmdc:placeholder",
                parameter_data_id="nmdc:placeholder",
                processing_institution=prev_metab_analysis["processing_institution"],
                CLIENT_ID=client_id,
                CLIENT_SECRET=client_secret,
                incremeneted_id=metab_analysis_id,
            )

            # list all paths in the processed data directory
            processed_data_paths = list(
                Path(data["processed_data_directory"]).glob("**/*")
            )

            # Add a check that the processed data directory is not empty
            if not any(processed_data_paths):
                raise FileNotFoundError(
                    f"No files found in processed data directory: "
                    f"{data['processed_data_directory']}"
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
                            base_url=self.process_data_url
                            + Path(data["processed_data_directory"]).name
                            + "/",
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
                            base_url=self.process_data_url
                            + Path(data["processed_data_directory"]).name
                            + "/",
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
                            base_url=self.process_data_url
                            + Path(data["processed_data_directory"]).name
                            + "/",
                            CLIENT_ID=client_id,
                            CLIENT_SECRET=client_secret,
                            was_generated_by=metab_analysis.id,
                        )
                        nmdc_database_inst.data_object_set.append(processed_data_object)
                        processed_data.append(processed_data_object.id)

                        # Update MetabolomicsAnalysis times based on HDF5 file
                        start_time, end_time = self.get_start_end_times(file)
                        metab_analysis.started_at_time = start_time
                        metab_analysis.ended_at_time = end_time

                    else:
                        raise ValueError(f"Unexpected file type found for file {file}.")

            # Check that all processed data objects were created
            if (
                processed_data_object_config is None
                or processed_data_object_annot is None
                or processed_data_object is None
            ):
                raise ValueError(
                    f"Not all processed data objects were created for {data['processed_data_directory']}."
                )
            has_input = [
                parameter_data_id,
                raw_data_object_id,
            ] + self.existing_data_objects
            self.update_outputs(
                analysis_obj=metab_analysis,
                raw_data_obj_id=raw_data_object_id,
                parameter_data_id=has_input,
                processed_data_id_list=processed_data,
                rerun=True,
            )

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

        self.dump_nmdc_database(
            nmdc_database=nmdc_database_inst, json_path=self.database_dump_json_path
        )
        logging.info("Metadata processing completed.")
        # change db object to dict
        return self.nmdc_db_to_dict(nmdc_database_inst)

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
        'mass spec configuration id', 'lc config id', 'instrument id',
        'instrument analysis start date', 'instrument analysis end date',
        'execution resource'.

        """
        return LCMSLipidWorkflowMetadata(
            processed_data_dir=row["processed_data_directory"],
            raw_data_file=row["raw_data_file"],
            mass_spec_configuration_id=row["mass_spec_configuration_id"],
            lc_config_id=row["lc_config_id"],
            instrument_id=row["instrument_id"],
            instrument_analysis_start_date=row.get("instrument_analysis_start_date"),
            instrument_analysis_end_date=row.get("instrument_analysis_end_date"),
            processing_institution=row.get("processing_institution"),
            processing_institution_generation=row.get(
                "processing_institution_generation", None
            ),
            processing_institution_workflow=row.get(
                "processing_institution_workflow", None
            ),
            execution_resource=row.get("execution_resource", None),
            raw_data_url=row.get("raw_data_url"),
            manifest_id=row.get("manifest_id", None),
            instrument_instance_specifier=row.get(
                "instrument_instance_specifier", None
            ),
        )
