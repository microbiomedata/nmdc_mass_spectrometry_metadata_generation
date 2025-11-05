import pandas as pd
from nmdc_api_utilities.biosample_search import BiosampleSearch
from nmdc_api_utilities.data_generation_search import DataGenerationSearch


class MetadataSurveyor:
    """
    Tests input files for the necessary columns and provides functionality to obtain all mass spectrometry records for a study from MongoDB, along with relevent metadata
    Uses this information to test:
    1) if the existing metadata includes generation records that already have a processsed sample as input (material processing steps already exist)
    2) if either the biosample or nmdc generation id cannot be found in mongo
    3) if the provided raw data identifier is the name of an existing generation record for that biosample (data generation record exists and should be used instead of filename)

    Parameters
    ----------
    study : str
        The study identifier.
    """

    def __init__(self, study: str, ENV="prod"):
        self.dg_client = DataGenerationSearch(env=ENV)
        self.bsmp_client = BiosampleSearch(env=ENV)
        self.study = study

    def mass_spec_records(self) -> list:
        """
        Gather all data generation records for all omics types produced by mass spectrometry (lipidomics, metabolomics, proteomics, nom)

        Returns
        -------
        list
            List of mass spectrometry data generation records associated with the study
        """
        data_gen_records = self.dg_client.get_record_by_filter(
            filter=f'{{"associated_studies":"{self.study}","analyte_category": {{"$in": ["lipidome", "metabolome", "nom", "metaproteome"]}}}}',
            max_page_size=1000,
            all_pages=True,
        )

        return data_gen_records

    def biosample_records(self) -> list:
        """
        Gather all biosample records for a certain study

        Returns
        -------
        list
            List of biosample records associated with the study
        """
        biosample_records = self.bsmp_client.get_record_by_filter(
            filter=f'{{"associated_studies":"{self.study}"}}',
            max_page_size=1000,
            all_pages=True,
        )

        return biosample_records

    def data_generation_metadata(self, extra_dg_columns=[]) -> pd.DataFrame:
        """
        Gather data generation records for this study into a pandas dataframe

        Parameters
        ----------
        extra_dg_columns: list, optional
            List of additional data generation columns to include in the output dataframe

        Returns
        -------
        pd.DataFrame
            DataFrame of data generation records for this study
        """

        ## get mass spectrometry data generation records for this study
        data_gen_records = self.mass_spec_records()
        data_gen_df = pd.DataFrame(data_gen_records)

        if data_gen_df.empty:
            return None

        else:
            keep_cols = [
                "id",
                "name",
                "has_input",
                "has_output",
                "analyte_category",
            ] + extra_dg_columns
            data_gen_df = data_gen_df[keep_cols]

            # get biosample or processed sample input object
            data_gen_df = data_gen_df.explode("has_input")
            data_gen_df = data_gen_df[
                data_gen_df["has_input"].str.startswith("nmdc:bsm", "nmdc:procsm")
            ]
            data_gen_df = data_gen_df.explode("has_output")
            data_gen_df["analyte"] = data_gen_df["analyte_category"].replace(
                {
                    "metabolome": "metabolite",
                    "lipidome": "lipid",
                    "metaproteome": "protein",
                }
            )
            data_gen_df = data_gen_df.drop("analyte_category", axis=1)

            # rename overlapping columns
            data_gen_df = data_gen_df.rename(
                columns={
                    "id": "raw_data_identifier",
                    "name": "raw_file_name",
                    "has_input": "raw_data_input",
                }
            )

        return data_gen_df

    def biosample_metadata(self) -> pd.DataFrame:
        """
        Gather biosample records for this study into a pandas dataframe

        Returns
        -------
        pd.DataFrame
            DataFrame of biosample records for this study
        """

        ## get biosample records for this study
        biosample_records = self.biosample_records()
        biosample_df = pd.DataFrame(biosample_records)

        if biosample_df.empty:
            return None

        else:
            keep_cols = [
                "id",  # can expand in future
            ]
            biosample_df = biosample_df[keep_cols]

            # rename overlapping columns
            biosample_df = biosample_df.rename(
                columns={
                    "id": "biosample_id",
                }
            )

        return biosample_df

    def additional_info(self, sample_specific_info_path: str) -> pd.DataFrame:
        """
        Read in csv with additional sample information, checking that the required column names and analyte types are present

        Parameters
        ----------
        sample_specific_info_path: pd.DataFrame
            Path to CSV of additional info

        Returns
        -------
        pd.DataFrame
            DataFrame of additional sample information
        """
        addinfo_df = pd.read_csv(sample_specific_info_path)
        columns = set(addinfo_df.columns)
        missing_columns = [
            col
            for col in [
                "biosample_id",
                "stepname",
                "slotname",
                "value",
            ]
            if col not in columns
        ]
        if missing_columns:
            raise ValueError(
                f"Missing required columns in DataFrame: {', '.join(missing_columns)}"
            )

        return addinfo_df

    def mapping_info(self, sample_to_dg_mapping_path: str) -> pd.DataFrame:
        """
        Read in csv with mapping information, checking that the required column names and analyte types are present

        Parameters
        ----------
        sample_to_dg_mapping_path: pd.DataFrame
            Path to CSV of biosample to data generation mappings for this study

        Returns
        -------
        pd.DataFrame
            DataFrame of biosample to data generation mappings for this study
        """
        mapping_df = pd.read_csv(sample_to_dg_mapping_path)
        columns = set(mapping_df.columns)
        required_columns = {
            "biosample_id",
            "raw_data_identifier",
            "processedsample_placeholder",
        }
        missing_columns = required_columns - columns
        if missing_columns:
            raise ValueError(
                f"Missing required columns in DataFrame: {', '.join(missing_columns)}"
            )

        return mapping_df

    def metadata_test(self, mapping_df: pd.DataFrame, extra_dg_columns=[]) -> None:
        """
        Check for potential problems with the input sheet

        Parameters
        ----------
        mapping_df: pd.DataFrame
            Pandas dataframe generated from provided CSV of biosample to filename mappings for this study

        Raises
        ------
        ValueError
            If any of the following conditions are met:
            1) A data generation record already has a processed sample as input, indicating that material
                processing steps likely already exist for this biosample.
            2) Either the biosample ID or the raw data identifier cannot be found in the existing metadata from MongoDB.
            3) The provided raw data identifier is not an NMDC ID, but matches the name of an existing data generation record for that biosample.

        """
        existing_study_dg_metadata = self.data_generation_metadata(extra_dg_columns)
        existing_study_biosamples = self.biosample_metadata()

        # provided biosamples exist
        missing_values = mapping_df[
            ~mapping_df["biosample_id"].isin(existing_study_biosamples["biosample_id"])
        ]["biosample_id"]
        if len(missing_values) > 0:
            raise ValueError(
                f"Provided mapping info contains biosample ids {missing_values} that were not found in mongodb"
            )

        if not mapping_df.empty:
            for __, row in mapping_df.iterrows():
                biosample_id = row["biosample_id"]
                raw_data_identifier = row["raw_data_identifier"]

                # if nmdc id provided as raw identifier, the mongodb records for that raw identifier exist and contain the indicated biosample
                if "nmdc:" in raw_data_identifier:

                    existing_raw_input = existing_study_dg_metadata[
                        (
                            existing_study_dg_metadata["raw_data_identifier"]
                            == raw_data_identifier
                        )
                    ]["raw_data_input"]

                    # raw id in mongo
                    if not existing_raw_input.empty:

                        # raw input is biosample
                        if not existing_raw_input.str.contains(
                            biosample_id, na=False
                        ).any():

                            # raw input not biosample but processed sample
                            if existing_raw_input.str.startswith("nmdc:procsm").any():
                                raise ValueError(
                                    f"Data generation record {raw_data_identifier} already has processed sample as input and likely already has material processing steps created"
                                )

                            # raw input a biosample but not the one indicated
                            else:
                                raise ValueError(
                                    f"Data generation record {raw_data_identifier} does not have {biosample_id} as input, instead its {existing_raw_input}. Provided mapping info out of sync with mongodb"
                                )

                    # raw id not in mongo
                    else:
                        raise ValueError(
                            f"Provided mapping info contains nmdc ids for raw identifier {raw_data_identifier} but that id was not found in search of mongodb's mass spec records"
                        )

                # if the provided raw identifier is not an nmdc id, it does not match any existing record names in mongo
                else:
                    if existing_study_dg_metadata:
                        if "raw_file_name" in existing_study_dg_metadata.columns:
                            existing_dgnames_for_biosample = existing_study_dg_metadata[
                                existing_study_dg_metadata["raw_data_input"]
                                == biosample_id
                            ]["raw_file_name"].tolist()
                            for name in existing_dgnames_for_biosample:
                                if raw_data_identifier in name:
                                    raise ValueError(
                                        f"{biosample_id} doesn't have an nmdc id as the `raw_data_identifier` ({raw_data_identifier}) but there is a data generation record for this biosample in mongo with {raw_data_identifier} in the file name"
                                    )
