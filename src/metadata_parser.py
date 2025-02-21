# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

from dataclasses import dataclass
from typing import Optional, Dict
from pathlib import Path
from src.api_info_retriever import ApiInfoRetriever
from nmdc_schema.nmdc import Biosample


@dataclass
class NmdcTypes:
    Biosample: str = "nmdc:Biosample"
    MassSpectrometry: str = "nmdc:MassSpectrometry"
    NomAnalysis: str = "nmdc:NomAnalysis"
    DataObject: str = "nmdc:DataObject"
    OntologyClass: str = "nmdc:OntologyClass"
    ControlledIdentifiedTermValue: str = "nmdc:ControlledIdentifiedTermValue"
    TextValue: str = "nmdc:TextValue"
    GeolocationValue: str = "nmdc:GeolocationValue"
    TimeStampValue: str = "nmdc:TimestampValue"
    QuantityValue: str = "nmdc:QuantityValue"
    CalibrationInformation: str = "nmdc:CalibrationInformation"
    MetaboliteIdentification: str = "nmdc:MetaboliteIdentification"


class MetadataParser:
    """Parsers metadata from input metadata spreadsheet."""

    def __init__(self, metadata_file):
        """
        Parameters
        ----------
        metadata_file : str
            Path to the metadata file to be loaded.
        """

        self.metadata_file = metadata_file

    def check_for_valid_configs(sefl, df: pd.DataFrame):
        """
        Get unique values for all columns containing 'config' in their name and verify they exist in API.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to analyze

        Raises
        ------
        ValueError
            If any config value doesn't exist in the API
        """

        # Instantiate configuration_set info retriever
        api_config_getter = ApiInfoRetriever(collection_name="configuration_set")

        config_columns = ["chrom_config_name", "mass_spec_config"]
        invalid_configs = []

        for col in config_columns:
            unique_vals = [val for val in df[col].unique() if pd.notna(val)]

            for val in unique_vals:
                try:
                    # skip empty values
                    if not val:
                        continue
                    api_config_getter.get_id_by_slot_from_collection(
                        slot_name="name", slot_field_value=val
                    )
                except ValueError:
                    invalid_configs.append((col, val))

        # If any invalid conifgs were found, raise error
        if invalid_configs:
            error_msg = "The following configurations were not found in the API:\n"
            for col, val in invalid_configs:
                error_msg += f"  Column '{col}': '{val}'\n"
            raise ValueError(error_msg)

    # Helper function to handle missing or NaN values
    def get_value(self, row: pd.Series, key: str, default=None):
        """
        Retrieve a value from a row, handling missing or NaN values.

        Parameters
        ----------
        row : pd.Series
            A row from the DataFrame.
        key : str
            The key to retrieve the value for.
        default : optional
            Default value to return if the key does not exist or is NaN.

        Returns
        -------
        The value associated with the key, or default if not found.
        """
        value = row.get(key, default)
        if isinstance(value, float) and np.isnan(value):
            return default
        return value

    def parse_biosample_metadata(self, row: pd.Series) -> Dict:
        """
        Parse the metadata row to get non-biosample class information.

        Parameters
        ----------
        row : pd.Series
            A row from the DataFrame containing metadata.

        Returns
        -------
        Dict

        """

        # Initialize the metadata dictionary
        metadata_dict = {
            "raw_data_directory": Path(self.get_value(row, "raw_data_directory")),
            "processed_data_directory": Path(
                self.get_value(row, "processed_data_directory")
            ),
            "data_path": Path(self.get_value(row, "LC-MS filename")),
            "dms_dataset_id": self.get_value(row, "DMS Dataset ID"),
            "myemsl_link": self.get_value(row, "MyEMSL link"),
            "associated_study": [
                study.strip()
                for study in self.get_value(row, "associated_study").split(",")
            ]
            if self.get_value(row, "associated_study")
            else None,
            "biosample_id": self.get_value(row, "biosample_id")
            if self.get_value(row, "biosample_id") or self.get_value(row, "id")
            else None,
            "instrument_used": self.get_value(row, "instrument_used")
            if self.get_value(row, "instrument_used")
            else None,
            "mass_spec_config": self.get_value(row, "mass_spec_config")
            if self.get_value(row, "mass_spec_config")
            else None,
        }

        # Create and return the EmslMetadata instance
        metadata = metadata_dict

        return metadata

    def dynam_parse_biosample_metadata(self, row: pd.Series) -> Biosample:
        """
        Function to parse the metadata row if it includes biosample information.
        This pulls the most recent version of the ontology terms from the API and compares them to the values in the given row.
        """
        metadata = {}
        for field_name, _ in Biosample.__dataclass_fields__.items():
            metadata[field_name] = (
                self.get_value(row, field_name) if field_name in row else None
            )
        return Biosample(**metadata)
