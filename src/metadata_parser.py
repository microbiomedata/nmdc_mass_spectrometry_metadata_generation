# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

from dataclasses import dataclass
from typing import Optional, Dict, List, get_origin, get_args
import typing_inspect
from pathlib import Path
from src.api_info_retriever import BioOntologyInfoRetriever
from nmdc_schema.nmdc import Biosample
import ast


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
            "associated_studies": [
                study.strip()
                for study in self.get_value(row, "associated_studies").split(",")
            ]
            if self.get_value(row, "associated_studies")
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

    def is_list_type(self, type_hint):
        """Recursively check if a type hint is or contains a List type."""
        # Check if the origin of the type hint is a List
        if get_origin(type_hint) == list or (
            typing_inspect.is_union_type(type_hint)
            and any(get_origin(tp) == list for tp in get_args(type_hint))
        ):
            return True
        # If the type is a Union, check the arguments recursively
        if typing_inspect.is_union_type(type_hint):
            return any(
                self.is_list_type(arg)
                for arg in get_args(type_hint)
                if arg is not type(None)
            )
        return False

    def dynam_parse_biosample_metadata(self, row: pd.Series) -> Dict:
        """
        # TODO: handle cases where the field value needs to be a specific data class type ie lat_lon
        Function to parse the metadata row if it includes biosample information.
        This pulls the most recent version of the ontology terms from the API and compares them to the values in the given row.
        params:
            row: pd.Series - A row from the DataFrame containing metadata.
        returns:
            Dict
            The metadata dictionary.
        """
        envo_retriever = BioOntologyInfoRetriever()

        metadata = {}
        for field_name, field_data in Biosample.__dataclass_fields__.items():
            #  check if the field is a list type, we will need to convert the csv row to a list instead of treating it as a string
            if self.is_list_type(field_data.type):
                metadata[field_name] = (
                    ast.literal_eval(self.get_value(row, field_name))
                    if self.get_value(row, field_name)
                    else None
                )
            elif "env_" in field_name and field_name != "env_package":
                # create envo term for env_broad_scale, env_local_scale, env_medium
                metadata[field_name] = (
                    self.create_controlled_identified_term_value(
                        self.get_value(row, field_name),
                        envo_retriever.get_envo_terms(self.get_value(row, field_name)),
                    )
                    if self.get_value(row, field_name)
                    else None
                )
            else:
                metadata[field_name] = (
                    self.get_value(row, field_name)
                    if self.get_value(row, field_name)
                    else None
                )
        return metadata

    def create_controlled_identified_term_value(
        self, row_value: str, slot_enum_dict: dict
    ):
        """
        Create a controlled identified term value.

        Parameters
        ----------
        raw_value : str
            The raw value to be converted.
        control_terms : dict
            A mapping of controlled terms.

        Returns
        -------
        dict
            A dictionary representing the controlled identified term.
        """

        nmdc_controlled_term_slot = {
            "has_raw_value": row_value,
            "term": {
                "id": row_value,
                "name": slot_enum_dict.get(row_value),
                "type": NmdcTypes.OntologyClass,
            },
            "type": NmdcTypes.ControlledIdentifiedTermValue,
        }

        return nmdc_controlled_term_slot
