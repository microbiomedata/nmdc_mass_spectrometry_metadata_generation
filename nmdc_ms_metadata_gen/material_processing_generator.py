import re
import sys

import nmdc_schema.nmdc as nmdc
import pandas as pd
from tqdm import tqdm

from nmdc_ms_metadata_gen.metadata_generator import NMDCMetadataGenerator
from nmdc_ms_metadata_gen.metadata_input_check import MetadataSurveyor
from nmdc_ms_metadata_gen.metadata_parser import YamlSpecifier
from nmdc_ms_metadata_gen.schema_bridge import list_material_processing_types
from nmdc_ms_metadata_gen.sheet_generator import (
    ChangeSheetGenerator,
    WorkflowSheetGenerator,
)
from nmdc_ms_metadata_gen.utils import save_to_csv, validate_generated_output


class MaterialProcessingMetadataGenerator(NMDCMetadataGenerator):
    """
    This class provides functionality for generating material processing steps and processed samples based on an adjusted yaml outline and
    tracking the final outputs for changesheets (datageneration records exist in mongo) or workflowsheets (no data generation records in mongo)

    Parameters
    ----------
    study_id : str
        The id of the study the samples are related to.
    yaml_outline_path : str
        YAML file that contains the sample processing steps to be analyzed.
    database_dump_json_path : str
        Path where the output database dump JSON file will be saved.
    sample_to_dg_mapping_path : str
        CSV file mapping biosample ids to their data generation record id.
    test : bool
        Value to determine if extra checks are needed based on it being a test run or real run.
    sample_specific_info_path : str or None
        Path to a CSV file containing sample specific information.
    minting_config_creds : str, optional
        Path to the configuration file containing the client ID and client secret for minting NMDC IDs. It can also include the bio ontology API key if generating biosample ids is needed.
        If not provided, the CLIENT_ID, CLIENT_SECRET, and BIO_API_KEY environment variables will be used.
    """

    def __init__(
        self,
        database_dump_json_path: str,
        study_id: str,
        yaml_outline_path: str,
        sample_to_dg_mapping_path: str,
        test: bool,
        sample_specific_info_path: str = None,
        minting_config_creds: str = None,
    ):
        super().__init__(test=test)
        self.database_dump_json_path = database_dump_json_path
        self.study_id = study_id
        self.yaml_outline_path = yaml_outline_path
        self.sample_to_dg_mapping_path = sample_to_dg_mapping_path
        self.test = test
        self.minting_config_creds = minting_config_creds
        self.sample_specific_info_path = sample_specific_info_path

    def run(self) -> nmdc.Database:
        """
        This main function generates mass spectrometry material processing steps for a given study using a yaml outline and sample specific metadata

        Returns
        -------
        nmdc.Database
            The generated NMDC database instance containing all generated metadata objects.
        """

        ## Setup
        client_id, client_secret = self.load_credentials(
            config_file=self.minting_config_creds
        )
        nmdc_database = self.start_nmdc_database()
        output_changesheet = ChangeSheetGenerator.initialize_empty_df()
        output_workflowsheet = WorkflowSheetGenerator.initialize_empty_df()
        survey = MetadataSurveyor(study=self.study_id)
        data_parser = YamlSpecifier(yaml_outline_path=self.yaml_outline_path)

        ## Load mapping info
        reference_mapping = survey.mapping_info(str(self.sample_to_dg_mapping_path))

        ## Double check data against mongodb if this isn't a test
        if self.test == False:
            survey.metadata_test(reference_mapping)

        ## Determine number of biosamples with each number of final outputs (ex 10 biosamples with 1 final output, 100 biosamples with 2 final outputs)
        pattern_counts = (
            reference_mapping.groupby("biosample_id")["processedsample_placeholder"]
            .apply(lambda x: " + ".join(sorted(x.unique())))
            .value_counts()
        )
        print(f"expected number of biosamples with x final outputs:\n {pattern_counts}")

        ## Get sample specific info for yaml if its provided
        if self.sample_specific_info_path:
            sample_specific_info = survey.additional_info(
                self.sample_specific_info_path,
                reference_mapping["biosample_id"].unique().tolist(),
            )

        ## For each biosample create json of necessary material processing steps and processed samples, as well as output dataframe
        for biosample in tqdm(
            reference_mapping["biosample_id"].unique(),
            desc="Biosamples",
            file=sys.stdout,
            dynamic_ncols=True,
            leave=True,
        ):

            # Get mapping info for this biosample
            sample_mapping = reference_mapping[
                reference_mapping["biosample_id"] == biosample
            ].reset_index(drop=True)

            # Get any additional info for this biosample
            if self.sample_specific_info_path and not sample_specific_info.empty:
                sample_specific_info_subset = sample_specific_info[
                    sample_specific_info["biosample_id"] == biosample
                ].reset_index(drop=True)

            # Get protocols for this biosample
            protocols = (
                sample_mapping["material_processing_protocol_id"].unique().tolist()
            )

            # Iterate through each protocol
            for protocol in protocols:

                yaml_parameters = {}
                yaml_parameters["protocol_id"] = protocol

                # Get mapping info for this protocol
                protocol_mapping = sample_mapping[
                    sample_mapping["material_processing_protocol_id"] == protocol
                ].reset_index(drop=True)

                # Get any additional info for this protocol
                if self.sample_specific_info_path and not sample_specific_info.empty:
                    yaml_parameters["sample_specific_info_protocol_subset"] = (
                        sample_specific_info_subset[
                            sample_specific_info_subset[
                                "material_processing_protocol_id"
                            ]
                            == protocol
                        ].reset_index(drop=True)
                    )

                # Get placeholders for this protocol
                yaml_parameters["target_outputs"] = (
                    protocol_mapping["processedsample_placeholder"].unique().tolist()
                )

                # Use the additional info and target output information to adjust the yaml outline for this biosample and protocol
                full_outline = data_parser.yaml_generation(**yaml_parameters)

                # Create necessary material processing and processed sample ids to link this biosample to a final processed sample that will be the new input to the data generation records
                input_dict = {
                    "Biosample": biosample
                }  # placeholder is key, value is nmdc id
                final_processed_samples = self.json_generation(
                    data=full_outline,
                    placeholder_dict=input_dict,
                    nmdc_database=nmdc_database,
                    CLIENT_ID=client_id,
                    CLIENT_SECRET=client_secret,
                )

                # Match the new final processed sample ids back to raw data files via a changesheet (if dg exists) or a workflowsheet (no dg yet)
                (
                    unmatched_current,
                    output_changesheet,
                    output_workflowsheet,
                ) = self.map_final_samples(
                    biosample,
                    final_processed_samples,
                    protocol_mapping,
                    output_changesheet,
                    output_workflowsheet,
                )

                if not unmatched_current.empty:
                    raise ValueError(
                        f"{biosample} had {unmatched_current.shape[0]} unmatched final processed sample(s):\n {unmatched_current}"
                    )

        # Validate output based on expected counts before saving
        file_path = self.database_dump_json_path.split(".json")[0]
        validation_passed = validate_generated_output(
            reference_mapping,
            nmdc_database,
            self.yaml_outline_path,
            f"{file_path}_validation.txt",
        )
        if not validation_passed:
            print(
                "\n⚠️  WARNING: Validation failed but continuing with output generation"
            )

        # Save database
        self.dump_nmdc_database(
            nmdc_database=nmdc_database, json_path=self.database_dump_json_path
        )

        # Save output sheets
        file_path = self.database_dump_json_path.split(".json")[0]
        if not output_changesheet.empty:
            save_to_csv(output_changesheet, f"{file_path}_changesheet.csv")
        if not output_workflowsheet.empty:
            save_to_csv(
                output_workflowsheet,
                f"{file_path}_workflowreference.csv",
            )
        # change db object to dict
        return self.nmdc_db_to_dict(nmdc_database)

    def map_final_samples(
        self,
        biosample: str,
        final_processed_samples: dict,
        protocol_mapping: pd.DataFrame,
        output_changesheet: pd.DataFrame,
        output_workflowsheet: pd.DataFrame,
    ):
        """
        Use the final processed samples to create changesheets and/or workflowsheets capturing input for the data generation records

        Parameters
        ----------
        biosample: str
            biosample id to map
        final_processed_samples: dict

        protocol_mapping: pd.DataFrame

        output_changesheet: pd.DataFrame
            Dataframe that holds necessary changes to upload to the database.
        output_workflowsheet: pd.DataFrame
            Dataframe that holds

        Returns
        -------
        tuple
        """

        # all raw identifiers for sample
        unmatched_samples = protocol_mapping["raw_data_identifier"].unique().tolist()

        # Get the final processed samples associated with each of the raw files
        for ps_placeholder, ps_id in final_processed_samples.items():
            # Build changesheet or workflowsheet based on raw identifiers for this placeholder
            rawids = (
                protocol_mapping[
                    protocol_mapping["processedsample_placeholder"] == ps_placeholder
                ]["raw_data_identifier"]
                .unique()
                .tolist()
            )

            for rawid in rawids:
                if "nmdc:" in rawid:
                    output_changesheet = ChangeSheetGenerator.add_row(
                        df=output_changesheet,
                        id=rawid,
                        action="update",
                        attribute="has_input",
                        value=ps_id,
                    )
                else:
                    output_workflowsheet = WorkflowSheetGenerator.add_row(
                        df=output_workflowsheet,
                        biosample_id=biosample,
                        raw_data_identifier=rawid,
                        last_processed_sample=ps_id,
                    )

            # Remove mapped raw files from list of unmatched samples
            unmatched_samples = [
                item for item in unmatched_samples if item not in rawids
            ]

        # Raw ids that did not map to a placeholder in final processed samples
        unmatched_samples_df = protocol_mapping[
            protocol_mapping["raw_data_identifier"].isin(unmatched_samples)
        ]

        return unmatched_samples_df, output_changesheet, output_workflowsheet

    def json_generation(
        self,
        data: dict,
        placeholder_dict: dict,
        nmdc_database: nmdc.Database,
        CLIENT_ID: str,
        CLIENT_SECRET: str,
    ):
        """
        Function that creates the json and all minted ids based on the biosample adjusted yaml (tracking ids to make sure they are minted before reference)

        Parameters
        ----------
        data:dict
            The nested dictionary containing the workflow steps (yaml outline)
        placeholder_dict:dict
            Dictionary with yaml placeholder as key and actual NMDC id as value
        CLIENT_ID:str
            The client ID for authentication
        CLIENT_SECRET:str
            The client secret for authentication

        Returns
        -------
        final_output:dict
            Dictionary with final outputs from yaml in placeholder_dict format (for changesheet)

        Json file with generated data saved to specified database_dump_json_path
        """

        all_input = []
        all_output = []

        # for each step in yaml outline
        for step in data["steps"]:

            # Get process/step info from yaml outline
            _, step_data = next(iter(step.items()))
            process_type, process_data = next(iter(step_data.items()))

            ## Validate that all input placeholders for this step have minted NMDC ids in placeholder_dict and
            # save the ids to a list of step inputs, used to fill in the material processing step's has_input slot
            # save the ids to a list of all inputs, used to determine final products of the yaml
            step_input = []
            for input_placeholder in process_data["has_input"]:
                if input_placeholder in placeholder_dict:
                    step_input.append(placeholder_dict[input_placeholder])
                    all_input.append(placeholder_dict[input_placeholder])
                else:
                    raise ValueError(
                        f"ProcessedSample {input_placeholder} referenced before creation"
                    )

            ## Mint the processed samples created by this step using the placeholder's outline in the yaml
            step_output = []
            for output_placeholder in process_data["has_output"]:

                placeholder_outline = None

                # find the template for this placeholder in the yaml outline or error
                for sample_outline in data["processedsamples"]:
                    if output_placeholder in sample_outline:
                        placeholder_outline = list(
                            sample_outline[output_placeholder].values()
                        )[0]

                if placeholder_outline is None:
                    raise ValueError(
                        f"No definition found for placeholder: {output_placeholder}"
                    )

                # replace any placeholders (<>) in the outline with actual nmdc ids from placeholder_dict
                for slot in placeholder_outline:
                    if isinstance(placeholder_outline[slot], str):
                        placeholder_references = re.findall(
                            r"<(.*?)>", placeholder_outline[slot]
                        )
                        for reference in placeholder_references:
                            nmdc_id = placeholder_dict[reference]
                            placeholder_outline[slot] = placeholder_outline[
                                slot
                            ].replace(
                                f"<{reference}>",
                                nmdc_id,
                            )

                # remove id and type from the outline
                placeholder_outline.pop("id", None)
                placeholder_outline.pop("type", None)

                # mint processed sample using outline
                processed_sample = self.generate_processed_sample(
                    **placeholder_outline,
                    CLIENT_ID=CLIENT_ID,
                    CLIENT_SECRET=CLIENT_SECRET,
                )

                # Map the output placeholder to its newly minted id in placeholder_dict
                # info is used to replace the placeholder if its referenced in any future steps (i.e. as an input)
                placeholder_dict[output_placeholder] = processed_sample.id

                # save the new processed sample ids to a list of step outputs, used to fill in the material processing step's has_output slot
                # save the new processed sample ids to a list of all outputs, used to determine final products of the yaml
                step_output.append(processed_sample.id)
                all_output.append(processed_sample.id)

                # add processed sample to Database
                nmdc_database.processed_sample_set.append(processed_sample)

            ## Create the material processing step

            # replace any placeholders (<>) with actual nmdc ids from placeholder_dict
            for slot in process_data:
                if isinstance(process_data[slot], str):
                    placeholder_references = re.findall(r"<(.*?)>", process_data[slot])
                    for reference in placeholder_references:
                        nmdc_id = placeholder_dict[reference]
                        process_data[slot] = process_data[slot].replace(
                            f"<{reference}>", nmdc_id
                        )

            # Replace has_input and has_output with the lists of actual NMDC ids, also remove id and type which will instead be added in the generator method
            for key in ["has_input", "has_output", "id", "type"]:
                process_data.pop(key, None)
            # check that the type is of material processing
            if process_type not in list_material_processing_types():
                raise ValueError(
                    f"Material processing type '{process_type}' is not recognized in the NMDC schema"
                )
            material_processing_metadata = self.generate_material_processing(
                data=process_data,
                type=process_type,
                has_input=step_input,
                has_output=step_output,
                CLIENT_ID=CLIENT_ID,
                CLIENT_SECRET=CLIENT_SECRET,
            )

            # Add material processing step to Database
            nmdc_database.material_processing_set.append(material_processing_metadata)

        ## After generating all the steps and samples, identify outputs that aren't consumed by another step

        # if the placeholder was an output created from any of these steps but not used as an input to another step, it is a final output
        final_output = {}
        for placeholder in placeholder_dict:
            if placeholder_dict[placeholder] in set(all_output) - set(all_input):
                final_output[placeholder] = placeholder_dict[placeholder]

        # Return the dictionary mapping final output placeholders to an nmdc processed sample id (necessary for change sheet)
        return final_output
