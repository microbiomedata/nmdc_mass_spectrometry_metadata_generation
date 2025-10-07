import pandas as pd
from nmdc_api_utilities.collection_search import CollectionSearch
from nmdc_api_utilities.data_object_search import DataObjectSearch


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

    def __init__(self, study: str):
        self.collection_client = CollectionSearch("data_generation_set")
        self.dataobject_client = DataObjectSearch()
        self.study = study

    def mass_spec_records(self) -> list:
        """
        Gather all data generation records for all omics types produced by mass spectrometry (lipidomics, metabolomics, proteomics, nom)

        """
        data_gen_records = self.collection_client.get_record_by_filter(
            filter=f'{{"associated_studies":"{self.study}","analyte_category": {{"$in": ["lipidome", "metabolome", "nom", "metaproteome"]}}}}',
            max_page_size=1000,
            all_pages=True,
        )

        return data_gen_records

    def study_dataobject_records(self) -> list:
        """
        Gather all data objects associated with a study id
        """
        do_records = self.dataobject_client.get_data_objects_for_studies(
            study_id=f"{self.study}", max_page_size=1000
        )

        return do_records

    def study_dataobject_metadata(self) -> pd.DataFrame:
        """
        Gather data objects from this study into a pandas dataframe
        """
        # get data objects for this study
        do_records = self.study_dataobject_records()
        do_records = pd.DataFrame(do_records)

        # reformat data into dataframe (keeping biosample id)
        data_objects = []
        for index, row in do_records.iterrows():
            bio_id = row["biosample_id"]
            row_out = pd.json_normalize(row["data_objects"])
            row_out["biosample_id"] = bio_id
            data_objects.append(row_out)
        data_objects = pd.concat(data_objects)[["id", "biosample_id"]]
        return data_objects

    def data_generation_metadata(self, extra_dg_columns=[]) -> pd.DataFrame:
        """
        Gather data generation records for this study into a pandas dataframe
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
                {"metabolome": "metabolite", "lipidome": "lipid", "metaproteome": "protein"}
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

    def existing_metadata(self, extra_dg_columns=[]) -> pd.DataFrame:
        """
        Connect biosamples to data generation records by matching the output from data generation to the list of data objects associated with each biosample
        """
        try:
            existing_study_dg_metadata = self.data_generation_metadata(extra_dg_columns)
            existing_study_dataobjects = self.study_dataobject_metadata()

            if existing_study_dg_metadata is None:
                existing_study_metadata = existing_study_dataobjects[['biosample_id']]

            else:
                existing_study_metadata = existing_study_dg_metadata.merge(
                    existing_study_dataobjects, left_on="has_output", right_on="id"
                )
                keep_cols = [
                    "biosample_id",
                    "raw_data_identifier",
                    "raw_file_name",
                    "analyte",
                    "raw_data_input",
                ] + extra_dg_columns
                existing_study_metadata = existing_study_metadata[keep_cols]

        except Exception as e:
            raise ValueError("no existing metadata for this study") from e

        return existing_study_metadata

    def additional_info(self, sample_specific_info_path: str) -> pd.DataFrame:
        """
        Read in csv with additional sample information, checking that the required column names and analyte types are present

        Parameters
        ----------
        sample_specific_info_path: pd.DataFrame
            Path to CSV of additional info
        """
        addinfo_df = pd.read_csv(sample_specific_info_path)
        columns = set(addinfo_df.columns)
        missing_columns = [
            col
            for col in [
                "biosample_id",
                "raw_data_identifier",
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

    def metadata_test(self, mapping_df: pd.DataFrame):
        """
        Check for potential problems with the input sheet

        Parameters
        ----------
        mapping_df: pd.DataFrame
            Pandas dataframe generated from provided CSV of biosample to filename mappings for this study
        """
        existing_metadata = self.existing_metadata()
        if not mapping_df.empty:
            for __, row in mapping_df.iterrows():
                biosample_id = row["biosample_id"]
                raw_data_identifier = row["raw_data_identifier"]

                if "nmdc:" in raw_data_identifier:

                    existing_raw_input = existing_metadata[
                        (
                            existing_metadata["biosample_id"]
                            ==biosample_id & 
                            existing_metadata["raw_data_identifier"]
                            == raw_data_identifier
                        )
                    ]["raw_data_input"]

                    if existing_raw_input:
                        if existing_raw_input.str.startswith("nmdc:procsm"):
                            raise ValueError(
                                f"Data generation record {raw_data_identifier} already has processed sample as input and likely already has material processing steps created"
                            )
                    else:
                        raise ValueError(
                            f"Provided mapping info contains nmdc ids for biosample {biosample_id} and raw identifier {raw_data_identifier} but one or both not found in mongodb"
                        )
                else:
                    if 'raw_file_name' in existing_metadata.columns:
                        existing_dgnames_for_biosample = existing_metadata[
                            existing_metadata["biosample_id"] == biosample_id
                        ]["raw_file_name"].tolist()
                        for name in existing_dgnames_for_biosample:
                            if raw_data_identifier in name:
                                raise ValueError(
                                    f"{biosample_id} doesn't have an nmdc id as the `raw_data_identifier` ({raw_data_identifier}) but there is a data generation record for this biosample in mongo with {raw_data_identifier} in the file name"
                                )
