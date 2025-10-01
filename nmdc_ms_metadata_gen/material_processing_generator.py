import re
import sys

import nmdc_schema.nmdc as nmdc
import pandas as pd
from tqdm import tqdm

from nmdc_ms_metadata_gen.changesheet_generator import (
    ChangeSheetGenerator,
    WorkflowSheetGenerator,
)
from nmdc_ms_metadata_gen.data_classes import ProcessGeneratorMap
from nmdc_ms_metadata_gen.metadata_generator import NMDCWorkflowMetadataGenerator
from nmdc_ms_metadata_gen.metadata_parser import YamlSpecifier
from nmdc_ms_metadata_gen.study_metadata import MetadataSurveyor
from nmdc_ms_metadata_gen.utils import output_material_processing_summary, save_to_csv


class MaterialProcessingMetadataGenerator(NMDCWorkflowMetadataGenerator):
    """
    This class provides functionality for generating material processing steps and processed samples based on an adjusted yaml outline and tracking the final outputs for changesheets.


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
        super().__init__(
            metadata_file="",
            database_dump_json_path=database_dump_json_path,
            raw_data_url="",
            process_data_url="",
        )
        self.database_dump_json_path = database_dump_json_path
        self.study_id = study_id
        self.yaml_outline_path = yaml_outline_path
        self.sample_to_dg_mapping_path = sample_to_dg_mapping_path
        self.test = test
        self.minting_config_creds = minting_config_creds
        self.sample_specific_info_path = sample_specific_info_path

    def run(self) -> None:
        """
        This main function generates mass spectrometry material processing steps for a given study using provided metadata

        Returns
        -------
        None
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

        ## Determine number of biosamples with each number of expected final outputs (ex 10 biosamples with 1 final output, 100 biosamples with 2 final outputs)
        pattern_counts = (
            reference_mapping.groupby("biosample_id")["processedsample_placeholder"]
            .apply(lambda x: " + ".join(sorted(x.unique())))
            .value_counts()
        )
        print(f"expected number of biosamples with x final outputs:\n {pattern_counts}")

        ## Get sample specific info for yaml if its provided
        sample_specific_info = None
        if self.sample_specific_info_path:
            sample_specific_info = survey.additional_info(
                self.sample_specific_info_path
            )

        ## For each biosample create json of necessary material processing steps and processed samples, as well as output dataframe
        for biosample in tqdm(
            reference_mapping["biosample_id"].unique(),
            desc="Biosamples",
            file=sys.stdout,
            dynamic_ncols=True,
            leave=True,
        ):
            yaml_parameters = {}

            # Get biosample's mapping to results
            sample_mapping = reference_mapping[
                reference_mapping["biosample_id"] == biosample
            ].reset_index(drop=True)

            # Placeholders to match to outline outputs
            yaml_parameters["target_outputs"] = (
                sample_mapping["processedsample_placeholder"].unique().tolist()
            )

            # Are there biosample sample specific values?
            if sample_specific_info:
                yaml_parameters["sample_specific_info_subset"] = sample_specific_info[
                    sample_specific_info["biosample_id"] == biosample
                ].reset_index(drop=True)

            # Use the biosample specific values and target output information to adjust the yaml outline for this biosample
            full_outline = data_parser.yaml_generation(**yaml_parameters)

            # Create necessary material processing and processed sample ids to link each biosample to a final processed sample that will be the new input to the data generation records
            input_dict = {"Biosample": {"id": biosample}}
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
                sample_mapping,
                output_changesheet,
                output_workflowsheet,
            )

            if not unmatched_current.empty:
                raise ValueError(
                    f"{biosample} had {unmatched_current.shape[0]} unmatched final processed sample(s):\n {unmatched_current}"
                )

        # Output summary and save results
        output_material_processing_summary(reference_mapping, nmdc_database)
        self.dump_nmdc_database(nmdc_database=nmdc_database)
        self.validate_nmdc_database(self.database_dump_json_path)
        file_path = self.database_dump_json_path.split(".json")[0]
        if not output_changesheet.empty:
            save_to_csv(output_changesheet, f"{file_path}_changesheet.csv")
        if not output_workflowsheet.empty:
            save_to_csv(
                output_workflowsheet,
                f"{file_path}_workflowreference.csv",
            )

    def map_final_samples(
        self,
        biosample: str,
        final_processed_samples: dict,
        sample_mapping: pd.DataFrame,
        output_changesheet: pd.DataFrame,
        output_workflowsheet: pd.DataFrame,
    ):
        """Use the final processed samples to create necessary changesheets or workflowsheets depicting what goes into data generation records

        Parameters
        ----------
        biosample: str
            biosample id to map
        final_processed_samples: dict

        sample_mapping: pd.DataFrame

        output_changesheet: pd.DataFrame
            Dataframe that holds necessary changes to upload to the database.
        output_workflowsheet: pd.DataFrame
            Dataframe that holds

        Returns
        -------
        tuple
        """

        # all raw identifiers for sample
        unmatched_samples = sample_mapping["raw_data_identifier"].unique().tolist()

        # Get the final processed samples associated with each of the raw files
        for ps_placeholder, ps_id in final_processed_samples.items():
            # Build changesheet or workflowsheet based on raw identifiers for this placeholder
            rawids = (
                sample_mapping[
                    sample_mapping["processedsample_placeholder"] == ps_placeholder
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
        unmatched_samples_df = sample_mapping[
            sample_mapping["raw_data_identifier"].isin(unmatched_samples)
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
            Nested dictionary with yaml placeholder (outer key) to generated NMDC processed sample attributes (inner key is slot and inner value is slot value)
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

        # track all processed samples that are used as an input or output to any of the steps, making sure all ids have been minted before being referenced
        all_input = []
        all_output = []

        # for each step in yaml outline
        for step in data["steps"]:
            # Get process/step specific strings from yaml outline
            _, step_data = next(iter(step.items()))
            process_type, process_data = next(iter(step_data.items()))

            # Check that all placeholders in the has_input slot of the yaml outline exist in placeholder_dict, otherwise error because ID hasn't been minted yet
            step_input = []
            for input_placeholder in process_data["has_input"]:
                if input_placeholder in placeholder_dict:
                    step_input.append(placeholder_dict[input_placeholder]["id"])
                    all_input.append(placeholder_dict[input_placeholder]["id"])
                else:
                    raise ValueError(
                        f"ProcessedSample {input_placeholder} referenced before creation"
                    )

            # Mint new processed samples for this step's has_output using the placeholder's corresponding outline at the bottom of the yaml under 'processedsamples'
            step_output = []
            for output_placeholder in process_data["has_output"]:
                for dictionary in data["processedsamples"]:
                    if output_placeholder in dictionary:
                        placeholder_outline = list(
                            dictionary[output_placeholder].values()
                        )[0]

                        # replace placeholders (<>) in each slot of placeholder_outline with actual nmdc ids if that slot's value is a string (not None which id is until its minted)
                        for slot in placeholder_outline:
                            if isinstance(placeholder_outline[slot], str):
                                placeholder_references = re.findall(
                                    r"<(.*?)>", placeholder_outline[slot]
                                )
                                for reference in placeholder_references:
                                    placeholder_outline[slot] = placeholder_outline[
                                        slot
                                    ].replace(
                                        f"<{reference}>",
                                        placeholder_dict[reference]["id"],
                                    )

                processed_sample = self.generate_processed_sample(
                    placeholder_outline,
                    CLIENT_ID=CLIENT_ID,
                    CLIENT_SECRET=CLIENT_SECRET,
                )

                # Map the output placeholder and its values to the newly generated processed sample id in placeholder_dict. info used for tracking nmdc id generation in future calls of generate_material_processing_metadata
                placeholder_dict[output_placeholder] = {}
                for key, value in processed_sample.__dict__.items():
                    if value is not None and len(value) > 0:
                        placeholder_dict[output_placeholder].update({key: value})

                # add the new processed sample id to list of step outputs, used to generate material processing step's has_output slot
                step_output.append(processed_sample.id)
                all_output.append(placeholder_dict[output_placeholder]["id"])

                # add processed sample to Database
                nmdc_database.processed_sample_set.append(processed_sample)

            ## create new material processing steps

            # replace placeholders (<>) in each slot of process_data with actual nmdc ids if that slot's value is a string (not a list (like has_input) or None (like id until its minted))
            for slot in process_data:
                if isinstance(process_data[slot], str):
                    # print(process_data[slot])
                    placeholder_references = re.findall(r"<(.*?)>", process_data[slot])
                    for reference in placeholder_references:
                        # print(reference)
                        process_data[slot] = process_data[slot].replace(
                            f"<{reference}>", placeholder_dict[reference]["id"]
                        )

            # `generator_method`` is a saved string, corresponding to a function that generates an NMDC id for this stage of material processing (i.e. chemical conversion) by minting ids and filling out name, has_input,has_output, type)
            generator_method = getattr(
                self, getattr(ProcessGeneratorMap(), process_type)
            )

            material_processing_metadata = generator_method(
                process_data,
                input_samp_id=step_input,  # List of actual NMDC IDs
                output_samp_id=step_output,  # List of actual NMDC IDs
                CLIENT_ID=CLIENT_ID,
                CLIENT_SECRET=CLIENT_SECRET,
            )

            # add material processing step to Database
            nmdc_database.material_processing_set.append(material_processing_metadata)

        # if the placeholder was an output created from any of these steps but not used as an input to another step, it is a final output
        final_output = {}
        for placeholder in placeholder_dict:
            if placeholder_dict[placeholder]["id"] in set(all_output) - set(all_input):
                final_output[placeholder] = placeholder_dict[placeholder]["id"]

        # Return the dictionary mapping final outputs of a sampled_portion to an nmdc processed sample id (necessary for change sheet)
        return final_output
