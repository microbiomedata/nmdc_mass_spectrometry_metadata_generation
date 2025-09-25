import re
import sys

import nmdc_schema.nmdc as nmdc
import pandas as pd
from tqdm import tqdm

from nmdc_ms_metadata_gen.changesheet_generator import (
    ChangeSheetGenerator,
    WorkflowSheetGenerator,
    save_to_csv,
)
from nmdc_ms_metadata_gen.data_classes import ProcessGeneratorMap
from nmdc_ms_metadata_gen.metadata_generator import NMDCWorkflowMetadataGenerator
from nmdc_ms_metadata_gen.metadata_parser import YamlSpecifier
from nmdc_ms_metadata_gen.study_metadata import MetadataSurveyor


class MaterialProcessingMetadataGenerator(NMDCWorkflowMetadataGenerator):
    """
    This class provides functionality for generating material processing steps and processed samples based on an adjusted yaml outline and tracking the final outputs for changesheets.


    Parameters
    ----------
    study_id : str
        The id of the study the samples are related to
    yaml_outline_path : str
        YAML file that contains the sample processing steps to be analyzed.
    database_dump_json_path : str
        Path where the output database dump JSON file will be saved.
    sample_to_dg_mapping_path : str
        CSV file mapping biosample ids to their data generation record id.
    test : bool
        Value to determine if extra checks are needed based on it being a test run or real run.
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

    def run(self, sample_specific_info_path=None):
        """
        This main function generates mass spectrometry material processing steps for a given study using provided metadata

        Parameters
        ----------
        sample_specific_info_path : str or None
            The path to the sample specific information csv file, if available

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
        if sample_specific_info_path:
            sample_specific_info = survey.additional_info(sample_specific_info_path)

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
        self.output_summary(reference_mapping, nmdc_database)
        self.dump_nmdc_database(nmdc_database=nmdc_database)
        self.validate_nmdc_database(self.database_dump_json_path)
        if not output_changesheet.empty:
            save_to_csv(
                output_changesheet, f"{self.database_dump_json_path}_changesheet.csv"
            )
        if not output_workflowsheet.empty:
            save_to_csv(
                output_workflowsheet,
                f"{self.database_dump_json_path}_workflowreference.csv",
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

    def output_summary(
        self, reference_mapping: pd.DataFrame, nmdc_database: nmdc.Database
    ) -> None:
        """
        Print summary statistics.

        Parameters
        ----------
        reference_mapping: pd.DataFrame
            Input reference mapping for biosample -> process

        nmdc_database: dict
            The NMDC database instance to which the processed objects will be added

        Returns
        -------
        None

        """
        print(
            f"Total biosample instances: {len(reference_mapping['biosample_id'].unique())}"
        )
        print(
            f"Total material process IDs in database: {len(nmdc_database['material_processing_set'])}"
        )
        print(
            f"Total processed sample IDs in database: {len(nmdc_database['processed_sample_set'])}"
        )

    def json_generation(
        self,
        data: dict,
        placeholder_dict: dict,
        nmdc_database: nmdc.Database,
        CLIENT_ID: str,
        CLIENT_SECRET: str,
    ) -> dict:
        """
        Function that creates the JSON and all minted IDs based on the biosample-adjusted YAML.
        Ensures all placeholders used as inputs have been created as outputs in previous steps.

        Will invoke any of the following methods to add to the material processing set in the nmdc data base (based on material process):
            - generate_subsampling_process
            - generate_extraction
            - generate_chemical_conversion
            - generate_chromatographic_separation
            - generate_dissolving_process

        Parameters
        ----------
        data : dict
            The nested dictionary containing the workflow steps (YAML outline).
        placeholder_dict : dict
            Dictionary mapping YAML placeholders to NMDC processed sample attributes.
        nmdc_database : nmdc.Database
            NMDC database instance.
        CLIENT_ID : str
            Client ID for authentication.
        CLIENT_SECRET : str
            Client secret for authentication.

        Returns
        -------
        final_output : dict
            Dictionary mapping final outputs from the YAML to NMDC processed sample IDs.
        """
        # preprocess `processedsamples` into a dictionary for O(1) lookups
        processedsamples_map = {
            output_placeholder: values
            for dictionary in data["processedsamples"]
            for output_placeholder, values in dictionary.items()
        }

        # track IDs used as inputs or outputs across all steps
        all_input = set()
        all_output = set()

        # iterate through each workflow step in the YAML outline
        for step in data["steps"]:
            # Extract step details
            _, step_data = next(iter(step.items()))
            process_type, process_data = next(iter(step_data.items()))

            # validate 'has_input': all inputs must already exist (previously minted)
            step_input = []
            for input_placeholder in process_data["has_input"]:
                # ensure the input placeholder has been minted
                if input_placeholder not in placeholder_dict:
                    raise ValueError(
                        f"Placeholder '{input_placeholder}' used as an input "
                        f"before being minted as an output in a prior step."
                    )
                # add its corresponding NMDC ID to step inputs
                step_input.append(placeholder_dict[input_placeholder]["id"])
                all_input.add(placeholder_dict[input_placeholder]["id"])

            # process 'has_output': Mint IDs for new outputs
            step_output = []
            for output_placeholder in process_data["has_output"]:
                # ensure the output placeholder exists in the YAML definition
                if output_placeholder not in processedsamples_map:
                    raise ValueError(
                        f"Output placeholder '{output_placeholder}' not found in processedsamples."
                    )

                # get the corresponding outline for the output placeholder
                placeholder_outline = processedsamples_map[output_placeholder]

                # replace placeholders (<>) in outline with actual NMDC IDs
                for slot, value in placeholder_outline.items():
                    if isinstance(value, str):
                        placeholder_references = re.findall(r"<(.*?)>", value)
                        for reference in placeholder_references:
                            if reference not in placeholder_dict:
                                raise ValueError(
                                    f"Referenced placeholder '{reference}' in output "
                                    f"outline has not been minted as an ID yet."
                                )
                            value = value.replace(
                                f"<{reference}>", placeholder_dict[reference]["id"]
                            )
                        placeholder_outline[slot] = value

                # generate the processed sample and update the placeholder dictionary
                processed_sample = self.generate_processed_sample(
                    placeholder_outline,
                    CLIENT_ID=CLIENT_ID,
                    CLIENT_SECRET=CLIENT_SECRET,
                )
                placeholder_dict[output_placeholder] = {
                    key: val for key, val in vars(processed_sample).items() if val
                }

                # add the new processed sample ID to step outputs
                step_output.append(processed_sample.id)
                all_output.add(processed_sample.id)

                # add processed sample to NMDC database
                nmdc_database.processed_sample_set.append(processed_sample)

            # process material processing metadata
            # replace placeholders (<>) in process_data with actual NMDC IDs
            for slot, value in process_data.items():
                if isinstance(value, str):
                    placeholder_references = re.findall(r"<(.*?)>", value)
                    for reference in placeholder_references:
                        if reference not in placeholder_dict:
                            raise ValueError(
                                f"Referenced placeholder '{reference}' in process_data "
                                f"has not been minted as an ID yet."
                            )
                        value = value.replace(
                            f"<{reference}>", placeholder_dict[reference]["id"]
                        )
                    process_data[slot] = value

            generator_method = getattr(
                self, getattr(ProcessGeneratorMap(), process_type)
            )
            material_processing_metadata = generator_method(
                process_data,
                input_samp_id=step_input,
                output_samp_id=step_output,
                CLIENT_ID=CLIENT_ID,
                CLIENT_SECRET=CLIENT_SECRET,
            )

            # add material processing metadata to NMDC database
            nmdc_database.material_processing_set.append(material_processing_metadata)

        # determine final outputs (outputs not used as inputs in subsequent steps)
        final_output = {
            placeholder: placeholder_dict[placeholder]["id"]
            for placeholder, attributes in placeholder_dict.items()
            if attributes["id"] in all_output and attributes["id"] not in all_input
        }

        return final_output
