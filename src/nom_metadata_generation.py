from metadata_generator import MetadataGenerator
from metadata_parser import MetadataParser, NmdcTypes
from api_info_retriever import ApiInfoRetriever, NMDCAPIInterface
from tqdm import tqdm
from datetime import datetime
from pathlib import Path

import nmdc_schema.nmdc as nmdc
import hashlib


class NOMMetadataGenerator(MetadataGenerator):
    # TODO: run in 2 modes - you already have biosample ids or you do not have biosample ids. This can be a future feature addition. To start just bring over the instance of already having biosample ids.
    analyte_category="nom"
    workflow_analysis_name="NOM Analysis"
    workflow_description=("Natural Organic Matter analysis of raw mass "
                                    "spectrometry data.")
    workflow_git_url="https://github.com/microbiomedata/enviroMS"

    def __init__(self, metadata_file: str, data_dir: str, ref_calibration_path: str,
                 raw_data_object_type: str, processed_data_object_type: str,
                 database_dump_json_path: str, execution_resource: str,
                 field_strength: int, workflow_version: str,
                 config_path: str):
        super().__init__(metadata_file, data_dir, ref_calibration_path,
                 raw_data_object_type, processed_data_object_type,
                 database_dump_json_path, execution_resource,
                 field_strength, workflow_version,
                 config_path)
        self.mass_spec_description = ""
        self.mass_spec_description = ""
        self.mass_spec_eluent_intro = ""
        self.analyte_category = ""
        
    def run(self):
        """
        Execute the metadata generation process.

        This method processes the metadata file, generates biosamples (if needed) 
        and metadata, and manages the workflow for generating NOM analysis data.
        """

        file_ext = '.d'
        raw_dir_zip, results_dir, registration_dir = self.setup_directories()
        registration_file = registration_dir / self.database_dump_json_path

        # Dictionary to track failures
        failed_metadata = {
            'validation_errors': [],
            'processing_errors': []
        }

        nmdc_database = self.start_nmdc_database()

        # Initialize parser
        parser = MetadataParser(metadata_file=self.metadata_file, config_path=self.config_path)

        # Load metadata spreadsheet with Biosample metadata into dataframe
        metadata_df = parser.load_metadata_file()

        tqdm.write("\033[92mStarting metadata processing...\033[0m")

        # Iterate through each row in df to generate metadata
        for index, row in tqdm(metadata_df.iterrows(), total=metadata_df.shape[0], desc="\033[95mProcessing rows\033[0m"):
            # Do not generate biosamples if biosample_id exists in spreadsheet
            try:
                
                # Check if biosample_id is in metadata_csv. If no biosample_id, then will generate biosamples,
                # if biosample_id exists, will return None for biosample.
                emsl_metadata, biosample_id, biosample = self.handle_biosample(parser, row)

                # Create raw_file_path
                raw_file_path = self.data_dir / emsl_metadata.data_path.with_suffix(file_ext)

            except Exception as e:
                # Record the failed row with its error
                self.record_processing_error(
                    failed_metadata, 
                    index,
                    row.get('LC-MS filename', 'Unknown'),
                    str(e)
                )
                continue

        # At the end of processing, save the failed metadata if there are any errors
        self.save_error_log(failed_metadata, results_dir)

        self.dump_nmdc_database(nmdc_database, registration_file)

        tqdm.write("\033[92mMetadata processing completed.\033[0m")

    def create_nmdc_metadata(self, raw_data_path: Path, data_product_path: Path,
                                emsl_metadata: object, biosample_id: str,
                                toml_workflow_param_path: Path,
                                nom_metadata_db: nmdc.Database):
            """
            Create NMDC metadata entries.

            Parameters
            ----------
            raw_data_path : Path
                The path to the raw data file.
            data_product_path : Path
                The path to the processed data product.
            emsl_metadata : object
                The EMSL metadata object containing information about the sample.
            biosample_id : str
                The ID of the biosample.
            toml_workflow_param_path: Path
                The path to the workflow parameter metadata toml file.
            nom_metadata_db : nmdc.Database
                The database instance to store the generated metadata.
            """
            # Generate mass spectrometry instance
            self.mass_spec_description = f"{emsl_metadata.eluent_intro} ultra high resolution mass spectrum"
            self.mas_spec_eluent_intro = emsl_metadata.eluent_intro
            self.analyte_category = "nom"
            mass_spectrometry = self.generate_mass_spectrometry(raw_data_path=raw_data_path,
                                                                instrument=emsl_metadata.instrument_used,
                                                                metadata_obj=emsl_metadata,
                                                                sample_id=biosample_id,
                                                                processing_institution="EMSL",
                                                                mass_spec_config_name=emsl_metadata.mass_spec_config_name,
                                                                lc_config_name=emsl_metadata.lc_config_name,
                                                                start_date='',
                                                                end_date='',
                                                               )
            

            # Generate raw data object / create a raw data object description.
            eluent_intro_pretty = emsl_metadata.eluent_intro.replace("_", " ")
            raw_data_object_desc = f"Raw {emsl_metadata.instrument_used} {eluent_intro_pretty} data."
            raw_data_object = self.generate_data_object(file_path=raw_data_path,
                                                        data_category=self.raw_data_category,
                                                        data_object_type=self.raw_data_object_type,
                                                        description=raw_data_object_desc,
                                                        was_generated_by=mass_spectrometry.id)

            # Generate nom analysis instance
            nom_analysis = self.generate_nom_analysis(file_path=raw_data_path,
                                                    raw_data_id=raw_data_object.id,
                                                    data_gen_id=mass_spectrometry.id,
                                                    processed_data_id="nmdc:placeholder")

            # Generate processed data object
            processed_data_object_desc = (f"EnviroMS {emsl_metadata.instrument_used} "
                                        "natural organic matter workflow molecular formula assignment output details")
            processed_data_object = self.generate_data_object(file_path=data_product_path,
                                                            data_category=self.processed_data_category,
                                                            data_object_type=self.processed_data_object_type,
                                                            description=processed_data_object_desc,
                                                            was_generated_by=nom_analysis.id)
            
            # Generate workflow parameter data object
            workflow_param_data_object_desc = (f"CoreMS processing parameters for natural organic matter analysis "
                                            "used to generate {processed_data_object.id}")
            parameter_data_object = self.generate_data_object(file_path=toml_workflow_param_path,
                                                            data_category=self.workflow_param_data_category,
                                                            data_object_type=self.workflow_param_data_object_type,
                                                            description=workflow_param_data_object_desc)


            # Update the outputs for mass_spectrometry and nom_analysis
            self.update_outputs(mass_spec_obj=mass_spectrometry,
                                analysis_obj=nom_analysis,
                                raw_data_obj=raw_data_object,
                                processed_data_obj=processed_data_object,
                                workflow_param_obj=parameter_data_object)

            # Add instances to database
            nom_metadata_db.data_object_set.append(raw_data_object)
            nom_metadata_db.workflow_execution_set.append(nom_analysis)
            nom_metadata_db.data_generation_set.append(mass_spectrometry)
            nom_metadata_db.data_object_set.append(processed_data_object)
            nom_metadata_db.data_object_set.append(parameter_data_object)

    def generate_nom_analysis(self, file_path: Path, raw_data_id: str, data_gen_id: str, processed_data_id: str) -> nmdc.MetabolomicsAnalysis:
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

        Returns
        -------
        nmdc.MetabolomicsAnalysis
            The generated metabolomics analysis object.
        """
        api = NMDCAPIInterface()
        nmdc_id = api.mint_nmdc_id(
            nmdc_type=NmdcTypes.NomAnalysis)[0]
        
        # Lookup calibration id by md5 checksum of ref_calibration_path file
        calib_md5 = hashlib.md5(self.ref_calibration_path.open('rb').read()).hexdigest()
        api_calib_do_getter = ApiInfoRetriever(
            collection_name="data_object_set")
        
        try:
            calib_do_id = api_calib_do_getter.get_id_by_slot_from_collection(slot_name="md5_checksum", slot_field_value=calib_md5)
            api_calibration_getter = ApiInfoRetriever(collection_name="calibration_set")
            calibration_id = api_calibration_getter.get_id_by_slot_from_collection(slot_name="calibration_object", slot_field_value=calib_do_id)

        except ValueError as e:
            print(f"Calibration object does not exist: {e}")

        except Exception as e:
            print(f"An error occurred: {e}")

        data_dict = {
            'id': f"{nmdc_id}.1",
            'name': f'{self.workflow_analysis_name} for {file_path.name}',
            'description': self.workflow_description,
            'uses_calibration': calibration_id,
            'processing_institution': self.processing_institution,
            'execution_resource': self.execution_resource,
            'git_url': self.workflow_git_url,
            'version': self.workflow_version,
            'was_informed_by': data_gen_id,
            'has_input': [raw_data_id],
            'has_output': [processed_data_id],
            'started_at_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ended_at_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'type': NmdcTypes.NomAnalysis,
        }

        nomAnalysis = nmdc.NomAnalysis(**data_dict)

        return nomAnalysis
        
