# -*- coding: utf-8 -*-
from src.metadata_generator import NMDCMetadataGenerator
from src.metadata_parser import MetadataParser, NmdcTypes
from src.api_info_retriever import ApiInfoRetriever, NMDCAPIInterface
from tqdm import tqdm
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass

import nmdc_schema.nmdc as nmdc
import hashlib


class NOMMetadataGenerator(NMDCMetadataGenerator):
    """
    A class for generating NMDC metadata objects using provided metadata files and configuration
    for Natural Organic Matter (NOM) data.
    Attributes
    ----------
    """

    def __init__(
        self,
        metadata_file: str,
        database_dump_json_path: str,
        raw_data_url: str,
        process_data_url: str,
    ):
        super().__init__(
            metadata_file=metadata_file,
            database_dump_json_path=database_dump_json_path,
            raw_data_url=raw_data_url,
            process_data_url=process_data_url,
        )
        self.raw_data_object_type = "Direct Infusion FT ICR-MS Raw Data"
        self.processed_data_object_type = "FT ICR-MS Analysis Results"
        self.processed_data_category = "processed_data"
        self.execution_resource = "EMSL-RZR"
        self.analyte_category = "nom"
        self.workflow_analysis_name = "NOM Analysis"
        self.workflow_description = (
            "Natural Organic Matter analysis of raw mass spectrometry data."
        )
        self.workflow_param_data_category = "workflow_parameter_data"
        self.workflow_param_data_object_type = "Analysis Tool Parameter File"
        self.unique_columns = ["raw_data_directory", "processed_data_directory"]
        self.grouped_columns = [
            "biosample_id",
            "associated_study",
        ]
        self.mass_spec_desc = "ultra high resolution mass spectrum"
        self.mass_spec_eluent_intro = "direct_infusion_autosampler"
        self.processing_institution = "EMSL"
        self.workflow_git_url = "https://github.com/microbiomedata/enviroMS"
        self.workflow_version = "4.3.1"

    def run(self):
        """
        Execute the metadata generation process.

        This method processes the metadata file, generates biosamples (if needed)
        and metadata, and manages the workflow for generating NOM analysis data.
        """

        nmdc_database_inst = self.start_nmdc_database()
        grouped_data = self.load_metadata()
        metadata_df = grouped_data.apply(lambda x: x.reset_index(drop=True))
        # Initialize parser
        parser = MetadataParser(metadata_file=self.metadata_file)

        tqdm.write("\033[92mStarting metadata processing...\033[0m")
        processed_data = []
        # Iterate through each row in df to generate metadata
        for _, row in tqdm(
            metadata_df.iterrows(),
            total=metadata_df.shape[0],
            desc="Processing NOM rows",
        ):
            # check to see if biosample exists, if not, the function with create and return it
            # this is a bit clunky right now as i slowly shift over to new processes and allow the check_for_biosamples function to be used by multiple subclasses
            # I think eventually parsing the object before generating objects in all subclasses is ultimately more readble and easier to track
            emsl_metadata, biosample_id, _ = self.check_for_biosamples(parser, row)
            if biosample_id is None:
                # if the check comes back as None, this means a biosample exists and we can parse the row into an object
                # TODO: In our next iteration of csv inputs, if the biopsample id exists we will not have the biosample fields filled out in the rows
                # this means that we will need to create a biosample object from the biosample_id using the nmdc Biosample class and the biosample_id
                emsl_metadata, biosample_id = self.handle_biosample(parser, row)
            # Generate MassSpectrometry record
            mass_spec = self.generate_mass_spectrometry(
                file_path=Path(emsl_metadata["data_path"]),
                instrument_name=emsl_metadata["instrument_used"],
                sample_id=biosample_id,
                raw_data_id="nmdc:placeholder",
                study_id=emsl_metadata["associated_study"],
                processing_institution=self.processing_institution,
                mass_spec_config_name=emsl_metadata["mass_spec_config"],
                start_date=row["start_date"],
                end_date=row["end_date"],
            )
            eluent_intro_pretty = self.mass_spec_eluent_intro.replace("_", " ")
            # raw is the zipped .d directory
            raw_data_object_desc = (
                f"Raw {emsl_metadata['instrument_used']} {eluent_intro_pretty} data."
            )
            raw_data_object = self.generate_data_object(
                file_path=Path(row["raw_data_directory"]),
                data_category=self.raw_data_category,
                data_object_type=self.raw_data_object_type,
                description=raw_data_object_desc,
                base_url=self.raw_data_url,
                was_generated_by=mass_spec.id,
            )
            # Generate nom analysis instance, workflow_execution_set (metabolomics analysis), uses the raw data zip file
            started_at_time = row["start_date"] + " " + row["started_at_time"]
            eneded_at_time = row["end_date"] + " " + row["ended_at_time"]
            nom_analysis = self.generate_nom_analysis(
                file_path=Path(row["raw_data_directory"]),
                ref_calibration_path=Path(row["ref_calibration_path"]),
                raw_data_id=raw_data_object.id,
                data_gen_id=mass_spec.id,
                processed_data_id="nmdc:placeholder",
                started_at_time=started_at_time,
                ended_at_time=eneded_at_time,
            )

            ### we will have processed data object AFTER the workflow is ran. Since this is how the lipidomics and gcms work, that is how this will function as well.

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
                        base_url=self.process_data_url,
                        was_generated_by=nom_analysis.id,
                        alternative_id=None,
                    )
                    processed_data.append(processed_data_object.id)
                if file.suffix == ".json":
                    # Generate workflow parameter data object
                    # this is the .json file of processed data
                    workflow_param_data_object_desc = f"CoreMS processing parameters for natural organic matter analysis used to generate {nom_analysis.id}"

                    workflow_data_object = self.generate_data_object(
                        file_path=file,
                        data_category=self.workflow_param_data_category,
                        data_object_type=self.workflow_param_data_object_type,
                        description=workflow_param_data_object_desc,
                        base_url=self.process_data_url,
                        was_generated_by=nom_analysis.id,
                        alternative_id=None,
                    )
            has_input = [workflow_data_object.id, raw_data_object.id]
            # Update the outputs for mass_spectrometry and nom_analysis
            self.update_outputs(
                mass_spec_obj=mass_spec,
                analysis_obj=nom_analysis,
                raw_data_obj=raw_data_object,
                parameter_data_id=has_input,
                processed_data_id_list=processed_data,
            )
            nmdc_database_inst.data_generation_set.append(mass_spec)
            nmdc_database_inst.data_object_set.append(raw_data_object)
            nmdc_database_inst.data_object_set.append(processed_data_object)
            nmdc_database_inst.data_object_set.append(workflow_data_object)
            nmdc_database_inst.workflow_execution_set.append(nom_analysis)
            processed_data = []

        self.dump_nmdc_database(nmdc_database=nmdc_database_inst)
        api_interface = NMDCAPIInterface()
        api_interface.validate_json(self.database_dump_json_path)

    def generate_nom_analysis(
        self,
        file_path: Path,
        ref_calibration_path: str,
        raw_data_id: str,
        data_gen_id: str,
        processed_data_id: str,
        started_at_time: str,
        ended_at_time: str,
    ) -> nmdc.MetabolomicsAnalysis:
        """
        Generate a metabolomics analysis object from the provided file information.

        Parameters
        ----------
        file_path : Path
            The file path of the metabolomics analysis data file.
        ref_calibration_path : str
            The file path of the calibration file used for the analysis
        raw_data_id : str
            The ID of the raw data associated with the analysis.
        data_gen_id : str
            The ID of the data generation process that informed this analysis.
        processed_data_id : str
            The ID of the processed data resulting from this analysis.
        started_at_time : str
            The start time of the analysis.
        ended_at_time : str
            The end time of the analysis.
        Returns
        -------
        nmdc.MetabolomicsAnalysis
            The generated metabolomics analysis object.
        """
        api = NMDCAPIInterface()
        nmdc_id = api.mint_nmdc_id(nmdc_type=NmdcTypes.NomAnalysis)[0]

        # Lookup calibration id by md5 checksum of ref_calibration_path file
        calib_md5 = hashlib.md5(ref_calibration_path.open("rb").read()).hexdigest()
        api_calib_do_getter = ApiInfoRetriever(collection_name="data_object_set")

        try:
            calib_do_id = api_calib_do_getter.get_id_by_slot_from_collection(
                slot_name="md5_checksum", slot_field_value=calib_md5
            )
            api_calibration_getter = ApiInfoRetriever(collection_name="calibration_set")
            calibration_id = api_calibration_getter.get_id_by_slot_from_collection(
                slot_name="calibration_object", slot_field_value=calib_do_id
            )

        except ValueError as e:
            print(f"Calibration object does not exist: {e}")
            calibration_id = None

        except Exception as e:
            print(f"An error occurred: {e}")

        data_dict = {
            "id": f"{nmdc_id}.1",
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
            "started_at_time": started_at_time,
            "ended_at_time": ended_at_time,
            "type": NmdcTypes.NomAnalysis,
        }
        self.clean_dict(data_dict)
        nomAnalysis = nmdc.NomAnalysis(**data_dict)

        return nomAnalysis
