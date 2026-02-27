import ast
import hashlib
import os
import re
from abc import abstractmethod
from pathlib import Path

import nmdc_schema.nmdc as nmdc
import pandas as pd
from dotenv import load_dotenv
from nmdc_api_utilities.calibration_search import CalibrationSearch
from nmdc_api_utilities.data_object_search import DataObjectSearch
from nmdc_api_utilities.workflow_execution_search import WorkflowExecutionSearch
from tqdm import tqdm

from nmdc_ms_metadata_gen.data_classes import NmdcTypes, NOMMetadata
from nmdc_ms_metadata_gen.metadata_generator import NMDCWorkflowMetadataGenerator

load_dotenv()
ENV = os.getenv("NMDC_ENV", "prod")


class NOMMetadataGenerator(NMDCWorkflowMetadataGenerator):
    """
    A class for generating NMDC metadata objects using provided metadata files and configuration
    for Natural Organic Matter (NOM) data.
    """

    def __init__(
        self,
        metadata_file: str,
        database_dump_json_path: str,
        raw_data_url: str,
        process_data_url: str,
        minting_config_creds: str = None,
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
        self.minting_config_creds = minting_config_creds
        self.test = test

    def rerun(self) -> nmdc.Database:
        """
        Execute a rerun of the metadata generation process.

        This method processes the metadata file, generates biosamples (if needed)
        and metadata, and manages the workflow for generating NOM analysis data.

        Assumes raw data for NOM are on minio and that the raw data object URL field is populated.

        Returns
        -------
        nmdc.Database
            The generated NMDC database instance containing all generated metadata objects.
        """
        do_client = DataObjectSearch(env=ENV)
        wf_client = WorkflowExecutionSearch(env=ENV)
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

        if not self.test:
            self.check_doj_urls(
                metadata_df=metadata_df, url_columns=["processed_data_directory"]
            )

        # Iterate through each row in df to generate metadata
        for _, row in tqdm(
            metadata_df.iterrows(),
            total=metadata_df.shape[0],
            desc="Processing NOM rows",
        ):
            workflow_metadata_obj = self.create_nom_metadata(row=row)
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
                    filter=f'{{"has_input":"{raw_data_object_id}","type":"{NmdcTypes.get("NomAnalysis")}"}}',
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
            except Exception:
                raise IndexError(
                    f"NomAnalysis object not found for raw data object ID: {raw_data_object_id}"
                )
            workflow_metadata_obj.processing_institution = prev_nom_analysis[
                "processing_institution"
            ]
            workflow_metadata_obj.execution_resource = (
                prev_nom_analysis["execution_resource"]
                if prev_nom_analysis["execution_resource"]
                else None
            )

            # grab the calibration_ids from the previous metabolomics analysis
            # Get qc fields, converting NaN to None
            qc_status, qc_comment = self._get_qc_fields(row)

            # Generate nom analysis instance, workflow_execution_set (metabolomics analysis), uses the raw data zip file
            nom_analysis = self.generate_nom_analysis(
                file_path=Path(row["raw_data_file"]),
                raw_data_id=raw_data_object_id,
                data_gen_id=prev_nom_analysis["was_informed_by"],
                processed_data_id="nmdc:placeholder",
                calibration_ids=prev_nom_analysis["uses_calibration"],
                incremented_id=metab_analysis_id,
                processing_institution=(
                    workflow_metadata_obj.processing_institution_generation
                    if workflow_metadata_obj.processing_institution_generation
                    else workflow_metadata_obj.processing_institution
                ),
                execution_resource=workflow_metadata_obj.execution_resource,
                CLIENT_ID=client_id,
                CLIENT_SECRET=client_secret,
                qc_status=qc_status,
                qc_comment=qc_comment,
            )

            # Always generate processed data objects (even for failed QC)
            (
                processed_ids,
                workflow_data_object,
            ) = self.create_processed_data_objects(
                row=row,
                client_id=client_id,
                client_secret=client_secret,
                nom_analysis=nom_analysis,
                nmdc_database_inst=nmdc_database_inst,
            )
            has_input = [workflow_data_object.id, raw_data_object_id]

            # Update the outputs for mass_spectrometry and nom_analysis
            self.update_outputs(
                analysis_obj=nom_analysis,
                raw_data_obj_id=raw_data_object_id,
                parameter_data_id=has_input,
                processed_data_id_list=processed_ids,
                rerun=True,
            )

            # If QC failed, remove processed data objects and has_output
            if qc_status == "fail":
                # Remove has_output from workflow
                if hasattr(nom_analysis, "has_output"):
                    delattr(nom_analysis, "has_output")
                # Remove processed data objects (csv, png) from database, keep parameter file (json)
                nmdc_database_inst.data_object_set = [
                    obj
                    for obj in nmdc_database_inst.data_object_set
                    if obj.id not in processed_ids
                ]

            nmdc_database_inst.data_object_set.append(workflow_data_object)
            nmdc_database_inst.workflow_execution_set.append(nom_analysis)

        self.dump_nmdc_database(
            nmdc_database=nmdc_database_inst, json_path=self.database_dump_json_path
        )
        # change db object to dict
        return self.nmdc_db_to_dict(nmdc_database_inst)

    def run(self) -> nmdc.Database:
        """
        Execute the metadata generation process.

        This method processes the metadata file, generates biosamples (if needed)
        and metadata, and manages the workflow for generating NOM analysis data.

        Returns
        -------
        nmdc.Database
            The generated NMDC database instance containing all generated metadata objects.
        """
        client_id, client_secret = self.load_credentials(
            config_file=self.minting_config_creds
        )
        nmdc_database_inst = self.start_nmdc_database()
        metadata_df = self.load_metadata()
        tqdm.write("\033[92mStarting metadata processing...\033[0m")

        # check if manifest ids are provided or if we need to generate them
        self.check_manifest(
            metadata_df=metadata_df,
            nmdc_database_inst=nmdc_database_inst,
            CLIENT_ID=client_id,
            CLIENT_SECRET=client_secret,
        )
        if not self.test:
            # check for duplicate doj urls in the database
            self.check_doj_urls(
                metadata_df=metadata_df, url_columns=self.unique_columns
            )

        # Check if batch calibration is being used
        if "srfa_calib_id" not in metadata_df.columns and "srfa_calib_path" not in metadata_df.columns:
            print(
                "Generating metadata without SRFA batch calibration records. "
                "Include srfa_calib_id or srfa_calib_path columns in the metadata input CSV if you ran the enviroMS workflow with batch calibration."
            )

        self.generate_mass_spec_fields(
            metadata_df=metadata_df,
        )

        # Iterate through each row in df to generate metadata
        for _, row in tqdm(
            metadata_df.iterrows(),
            total=metadata_df.shape[0],
            desc="Processing NOM rows",
        ):
            workflow_metadata_obj = self.create_nom_metadata(row=row)
            # Generate MassSpectrometry record

            mass_spec = self.generate_mass_spectrometry(
                file_path=Path(workflow_metadata_obj.raw_data_file),
                instrument_id=workflow_metadata_obj.instrument_id,
                sample_id=workflow_metadata_obj.sample_id,
                raw_data_id="nmdc:placeholder",
                study_id=workflow_metadata_obj.associated_studies,
                # check which processing institution to use
                processing_institution=(
                    workflow_metadata_obj.processing_institution_generation
                    if workflow_metadata_obj.processing_institution_generation
                    else workflow_metadata_obj.processing_institution
                ),
                mass_spec_configuration_id=workflow_metadata_obj.mass_spec_configuration_id,
                start_date=row["instrument_analysis_start_date"],
                end_date=row["instrument_analysis_end_date"],
                lc_config_id=workflow_metadata_obj.lc_config_id,
                instrument_instance_specifier=workflow_metadata_obj.instrument_instance_specifier,
                CLIENT_ID=client_id,
                CLIENT_SECRET=client_secret,
            )
            eluent_intro_pretty = self.mass_spec_eluent_intro.replace("_", " ")
            # raw is the zipped .d directory
            raw_data_object_desc = (
                f"Raw {row['instrument_used']} {eluent_intro_pretty} data."
            )
            raw_data_object = self.generate_data_object(
                file_path=Path(workflow_metadata_obj.raw_data_file),
                data_category=self.raw_data_category,
                data_object_type=self.raw_data_object_type,
                description=raw_data_object_desc,
                base_url=self.raw_data_url,
                CLIENT_ID=client_id,
                CLIENT_SECRET=client_secret,
                was_generated_by=mass_spec.id,
                url=row.get("raw_data_url"),
                in_manifest=workflow_metadata_obj.manifest_id,
            )

            # Generate nom analysis instance, workflow_execution_set uses the raw data zip file
            # Use calibration_ids from CSV if provided, otherwise look it up by MD5
            if "calibration_id" in row and row.get("calibration_id"):
                workflow_metadata_obj.reference_calibration_id = row["calibration_id"]
            elif "ref_calibration_path" in row and row.get("ref_calibration_path"):
                workflow_metadata_obj.reference_calibration_id = self.get_calibration_ids(
                    calibration_path=Path(row["ref_calibration_path"])
                )

            # If SRFA calibration ID is included, add it, otherwise look it up by name
            if "srfa_calib_id" in row and row.get("srfa_calib_id"):
                workflow_metadata_obj.srfa_calibration_id = row["srfa_calib_id"]
            
            # If you cannot look it up by name (ie it doesn't exist) then we have to create a data object and a calibration record
            elif "srfa_calib_path" in row and row.get("srfa_calib_path"):

                # Trim to base name. Does it exist?
                srfa_name_trim = Path(row.get("srfa_calib_path")).stem
                srfa_mongo_id = self.get_srfa_ids(srfa_name_trim)

                # If yes, look up and use that calib id
                if srfa_mongo_id is not None:
                    workflow_metadata_obj.srfa_calibration_id = srfa_mongo_id

                # If no, create objects
                else:
                    workflow_metadata_obj.srfa_calibration_id = self.generate_calibration_ids(
                        metadata_row = row,
                        nmdc_database_inst=nmdc_database_inst,
                        CLIENT_ID=client_id,
                        CLIENT_SECRET=client_secret
                    )

            # List calibration IDs for generate_nom_analysis and remove blanks
            calibration_ids = [
                workflow_metadata_obj.reference_calibration_id,
                workflow_metadata_obj.srfa_calibration_id
            ]
            calibration_ids = [cid for cid in calibration_ids if cid is not None]

            # Get qc fields, converting NaN to None
            qc_status, qc_comment = self._get_qc_fields(row)

            nom_analysis = self.generate_nom_analysis(
                file_path=Path(workflow_metadata_obj.raw_data_file),
                calibration_ids=calibration_ids,
                raw_data_id=raw_data_object.id,
                data_gen_id=mass_spec.id,
                processed_data_id="nmdc:placeholder",
                processing_institution=(
                    workflow_metadata_obj.processing_institution_workflow
                    if workflow_metadata_obj.processing_institution_workflow
                    else workflow_metadata_obj.processing_institution
                ),
                execution_resource=workflow_metadata_obj.execution_resource,
                CLIENT_ID=client_id,
                CLIENT_SECRET=client_secret,
                qc_status=qc_status,
                qc_comment=qc_comment,
            )

            # Always generate processed data objects (even for failed QC)
            (
                processed_data_id_list,
                workflow_data_object,
            ) = self.create_processed_data_objects(
                row=row,
                client_id=client_id,
                client_secret=client_secret,
                nom_analysis=nom_analysis,
                nmdc_database_inst=nmdc_database_inst,
            )
            has_input = [workflow_data_object.id, raw_data_object.id]

            # Update the outputs for mass_spectrometry and nom_analysis
            self.update_outputs(
                mass_spec_obj=mass_spec,
                analysis_obj=nom_analysis,
                raw_data_obj_id=raw_data_object.id,
                parameter_data_id=has_input,
                processed_data_id_list=processed_data_id_list,
                rerun=False,
            )

            # If QC failed, remove processed data objects and has_output
            if qc_status == "fail":
                # Remove has_output from workflow
                if hasattr(nom_analysis, "has_output"):
                    delattr(nom_analysis, "has_output")
                # Remove processed data objects (csv, png) from database, keep parameter file (json)
                nmdc_database_inst.data_object_set = [
                    obj
                    for obj in nmdc_database_inst.data_object_set
                    if obj.id not in processed_data_id_list
                ]

            nmdc_database_inst.data_generation_set.append(mass_spec)
            nmdc_database_inst.data_object_set.append(raw_data_object)
            nmdc_database_inst.data_object_set.append(workflow_data_object)
            nmdc_database_inst.workflow_execution_set.append(nom_analysis)

        self.dump_nmdc_database(
            nmdc_database=nmdc_database_inst, json_path=self.database_dump_json_path
        )
        # change db object to dict
        return self.nmdc_db_to_dict(nmdc_database_inst)

    def get_calibration_ids(
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
        do_client = DataObjectSearch(env=ENV)
        cs_client = CalibrationSearch(env=ENV)
        try:
            calib_do_id = do_client.get_record_by_attribute(
                attribute_name="md5_checksum",
                attribute_value=calib_md5,
                fields="id",
                exact_match=True,
            )[0]["id"]
            calibration_ids = cs_client.get_record_by_attribute(
                attribute_name="calibration_object",
                attribute_value=calib_do_id,
                fields="id",
                exact_match=True,
            )[0]["id"]
        except ValueError as e:
            raise ValueError(
                f"Calibration object does not exist for file {calibration_path}: {e}"
            )
        except IndexError as e:
            raise ValueError(
                f"Calibration object not found for file {calibration_path} with MD5 {calib_md5}: {e}"
            )
        except Exception as e:
            raise RuntimeError(
                f"An error occurred while looking up calibration for file {calibration_path}: {e}"
            )
        return calibration_ids

    def get_srfa_ids(
        self,
        srfa_calib_name: str,
    ) -> str:
        """
        Get the calibration ID from the NMDC API using the name of the SRFA file.
        If there is no existing SRFA calibration record with this name,
        don't error out, but return None.

        Parameters
        ----------
        srfa_calib_name : str
            The name of the SRFA dataset used for calibration.

        Returns
        -------
        str
            The calibration ID if found, otherwise None.
        """
        do_client = DataObjectSearch(env=ENV)
        cs_client = CalibrationSearch(env=ENV)
        try:
            calib_do_id = do_client.get_record_by_attribute(
                attribute_name="name",
                attribute_value=srfa_calib_name,
                fields="id",
                exact_match=False,
            )[0]["id"]
            calibration_ids = cs_client.get_record_by_attribute(
                attribute_name="calibration_object",
                attribute_value=calib_do_id,
                fields="id",
                exact_match=True,
            )[0]["id"]
        except ValueError as e:
            calibration_ids = None
        except IndexError as e:
            calibration_ids = None
        except Exception as e:
            raise RuntimeError(
                f"An error occurred while looking up calibration for file {srfa_calib_name}: {e}"
            )
        return calibration_ids


    def generate_calibration_ids(
        self,
        metadata_row: pd.Series,
        nmdc_database_inst: nmdc.Database,
        CLIENT_ID: str,
        CLIENT_SECRET: str,
    ) -> str:
        """
        Generate calibration information and data objects for each calibration file.

        Parameters
        ----------
        metadata_row : pd.Series
            One row from the metadata input CSV.
        nmdc_database_inst : nmdc.Database
            The NMDC Database instance.
        CLIENT_ID : str
            The client ID for the NMDC API.
        CLIENT_SECRET : str
            The client secret for the NMDC API.

        Returns
        -------
        ID of the calibration object
        """
        # Check if the df has calibration_file_url, if not, set url to None to use the raw_data_url
        if "calibration_file_url" in metadata_row:
            url = metadata_row["calibration_file"]
        else:
            url = None

        # Create data object and Calibration information for each and attach associated ids to metadata_df
        calibration_data_object = self.generate_data_object(
            file_path=Path(metadata_row["srfa_calib_path"]),
            data_category=self.raw_data_category,
            data_object_type=self.raw_data_object_type,
            description=self.raw_data_object_desc,
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
            srfa=(self.calibration_standard=="srfa"),
            internal=False,
        )
        nmdc_database_inst.calibration_set.append(calibration)

        return(calibration["id"])


    def generate_calibration(
        self,
        calibration_object: dict,
        CLIENT_ID: str,
        CLIENT_SECRET: str,
        srfa: bool = True,
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
        SRFA : bool, optional
            Whether the calibration is for SRFA. Default is True.
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
        if srfa and not internal:
            nmdc_id = self.id_pool.get_id(
                nmdc_type=NmdcTypes.get("CalibrationInformation"),
                client_id=CLIENT_ID,
                client_secret=CLIENT_SECRET,
            )
            data_dict = {
                "id": nmdc_id,
                "type": NmdcTypes.get("CalibrationInformation"),
                "name": f"SRFA calibration ({calibration_object.name})",
                "description": f"FT-ICR SRFA calibration run ({calibration_object.name}).",
                "internal_calibration": False,
                "calibration_target": "mass_charge_ratio",
                "calibration_standard": "srfa",
                "calibration_object": calibration_object.id,
            }

            calibration_information = nmdc.CalibrationInformation(**data_dict)

            return calibration_information
        else:
            raise ValueError(
                "Calibration type not implemented, only external SRFA calibration is currently supported."
            )


    def generate_nom_analysis(
        self,
        file_path: Path,
        raw_data_id: str,
        data_gen_id: str,
        processed_data_id: str,
        processing_institution: str,
        CLIENT_ID: str,
        CLIENT_SECRET: str,
        execution_resource: str = None,
        calibration_ids: list[str] = None,
        incremented_id: str = None,
        qc_status: str = None,
        qc_comment: str = None,
    ) -> nmdc.NomAnalysis:
        """
        Generate a NOM analysis object from the provided file information.

        Parameters
        ----------
        file_path : Path
            The file path of the NOM analysis data file.
        raw_data_id : str
            The ID of the raw data associated with the analysis.
        data_gen_id : str
            The ID of the data generation process that informed this analysis.
        processed_data_id : str
            The ID of the processed data resulting from this analysis.
        processing_institution: str
            The name of the processing institution. Must be a value from ProcessingInstitutionEnum.
        CLIENT_ID : str
            The client ID for the NMDC API.
        CLIENT_SECRET : str
            The client secret for the NMDC API.
        execution_resource: str, optional
            The name of the execution resource. Must be a value from ExecutionResourceEnum.
        calibration_ids : list[str], optional
            The IDs of the calibration objects used in the analysis. If None, no calibration is used.
        incremented_id : str, optional
            The incremented ID for the NOM analysis. If None, a new ID will be minted.
        qc_status : str, optional
            The quality control status for the analysis.
        qc_comment : str, optional
            The quality control comment for the analysis.

        Returns
        -------
        nmdc.NomAnalysis
            The generated NOM analysis object.
        """
        if incremented_id is None:
            nmdc_id = self.id_pool.get_id(
                nmdc_type=NmdcTypes.get("NomAnalysis"),
                client_id=CLIENT_ID,
                client_secret=CLIENT_SECRET,
            )
            incremented_id = nmdc_id + ".1"

        data_dict = {
            "id": incremented_id,
            "name": f"{self.workflow_analysis_name} for {file_path.name}",
            "description": self.workflow_description,
            "uses_calibration": calibration_ids,
            "processing_institution": processing_institution,
            "execution_resource": execution_resource,
            "git_url": self.workflow_git_url,
            "version": self.workflow_version,
            "was_informed_by": data_gen_id,
            "has_input": [raw_data_id],
            "has_output": [processed_data_id],
            "started_at_time": "placeholder",
            "ended_at_time": "placeholder",
            "type": NmdcTypes.get("NomAnalysis"),
            "qc_status": qc_status,
            "qc_comment": qc_comment,
        }
        self.clean_dict(data_dict)
        nomAnalysis = nmdc.NomAnalysis(**data_dict)

        return nomAnalysis

    def create_nom_metadata(self, row: pd.Series) -> NOMMetadata:
        """
        Parse the metadata row to get non-biosample class information.

        Parameters
        ----------
        row : pd.Series
            A row from the DataFrame containing metadata.

        Returns
        -------
        NOMMetadata

        """
        data = NOMMetadata(
            raw_data_file=row.get("raw_data_file"),
            processed_data_directory=row.get("processed_data_directory"),
            sample_id=row.get("sample_id"),
            instrument_id=row.get("instrument_id"),
            mass_spec_configuration_id=row.get("mass_spec_configuration_id"),
            lc_config_id=row.get("lc_config_id"),
            manifest_id=row.get("manifest_id"),
            execution_resource=row.get("execution_resource"),
            processing_institution_generation=row.get(
                "processing_institution_generation"
            ),
            associated_studies=(
                ast.literal_eval(row.get("associated_studies"))
                if row.get("associated_studies") is not None
                else None
            ),
            processing_institution_workflow=row.get("processing_institution_workflow"),
            processing_institution=row.get("processing_institution"),
            instrument_instance_specifier=row.get("instrument_instance_specifier"),
        )
        return data

    def create_processed_data_objects(
        self,
        row: pd.Series,
        client_id: str,
        client_secret: str,
        nom_analysis: nmdc.NomAnalysis,
        nmdc_database_inst: nmdc.Database,
    ):
        """
        Abstract method to create processed data objects.

        Parameters
        ----------
        row : pd.Series
            A row from the DataFrame containing metadata.
        client_id : str
            The client ID for authentication.
        client_secret : str
            The client secret for authentication.
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
            # Both DI and LCMS NOM have csv files
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
                start_time, end_time = self.get_start_end_times(file)
                nom_analysis.started_at_time = start_time
                nom_analysis.ended_at_time = end_time

                # Add the processed data object to the NMDC database
                nmdc_database_inst.data_object_set.append(processed_data_object)
                # add the processed data object id to the list
                processed_ids.append(processed_data_object.id)
            # DI NOM has json files
            elif file.suffix == ".json":
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
            # Both DI and LCMS NOM have QC plots
            elif ".png" in file.suffix:
                # Generate QC plots processed data object
                qc_data_object = self.generate_data_object(
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
                # add to the nmdc database
                nmdc_database_inst.data_object_set.append(qc_data_object)
                # add id to the processed id list
                processed_ids.append(qc_data_object.id)
            # LCMS NOM has toml files
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
        return processed_ids, workflow_data_object
