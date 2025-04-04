# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

from dataclasses import dataclass, is_dataclass
from typing import Union, Dict, List, get_origin, get_args
import typing_inspect
from pathlib import Path
from src.bio_ontology_api import BioOntologyInfoRetriever
from nmdc_schema.nmdc import (
    Biosample,
    ControlledIdentifiedTermValue,
    TextValue,
    QuantityValue,
    GeolocationValue,
    TimestampValue,
)
import ast
from src.data_classes import NmdcTypes


class MetadataParser:
    """Parsers metadata from input metadata spreadsheet."""

    def __init__(self):
        pass

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
        # if the value passed in is a Biosample field, we need to add the biosample prefix
        for field, _ in Biosample.__dataclass_fields__.items():
            if field == key:
                key = "biosample." + key
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
            "associated_studies": ast.literal_eval(
                self.get_value(row, "associated_studies")
            )
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

    def is_type(self, type_hint, type_to_search_for) -> bool:
        """Recursively check if a type hint is or contains input type."""
        if not type_to_search_for:
            return False

        # Check if the type_to_search_for is a dataclass and compare directly
        if is_dataclass(type_to_search_for):
            if is_dataclass(type_hint) and type_hint == type_to_search_for:
                return True

        # Check if the origin of the type hint is the type_to_search_for
        if get_origin(type_hint) == type_to_search_for or (
            typing_inspect.is_union_type(type_hint)
            and any(get_origin(tp) == type_to_search_for for tp in get_args(type_hint))
        ):
            return True

        # If the type is a Union, check the arguments recursively
        if typing_inspect.is_union_type(type_hint):
            return any(
                self.is_type(arg, type_to_search_for)
                for arg in get_args(type_hint)
                if arg is not type(None)
            )
        return False

    def dynam_parse_biosample_metadata(self, row: pd.Series, bio_api_key: str) -> Dict:
        """
        Function to parse the metadata row if it includes biosample information.
        This pulls the most recent version of the ontology terms from the API and compares them to the values in the given row.
        Different parsing is done on different types of fields, such as lists, controlled identified terms, and text values to ensure the correct format is used.
        params:
            row: pd.Series - A row from the DataFrame containing metadata.
            bio_api_key: str - The API key to access the Bio Ontology API
        returns:
            Dict
            The metadata dictionary.
        """
        envo_retriever = BioOntologyInfoRetriever(bio_api_key)

        metadata = {}
        for field_name, field_data in Biosample.__dataclass_fields__.items():
            # check if the field is a list of dataclasses
            if field_name == "type":
                metadata[field_name] = "nmdc:Biosample"
            elif self.is_type(field_data.type, List[Union[dict, dataclass]]):
                # check if a value exists before we begin complex parsing, saves time douing this at the begining
                if self.get_value(row, field_name):
                    # we need to make a dict for each item in the list
                    metadata[field_name] = []
                    # get the list of dicts from the csv row
                    list_of_dicts = ast.literal_eval(self.get_value(row, field_name))
                    # iterate through the list of dicts and format them
                    for item in list_of_dicts:
                        if self.is_type(field_data.type, TextValue):
                            metadata[field_name].append(
                                self.create_text_value(item, field_name)
                            )
                        elif self.is_type(
                            field_data.type, ControlledIdentifiedTermValue
                        ):
                            metadata[field_name].append(
                                self.create_controlled_identified_term_value(
                                    item,
                                    {item: item},
                                )
                            )
                        elif self.is_type(field_data.type, QuantityValue):
                            metadata[field_name].append(
                                self.create_quantity_value(raw_value=item)
                            )
                        else:
                            metadata[field_name].append(item)

            #  check if the field is a list type, we will need to convert the csv row to a list instead of treating it as a string
            elif self.is_type(field_data.type, list):
                metadata[field_name] = (
                    ast.literal_eval(self.get_value(row, field_name))
                    if self.get_value(row, field_name)
                    else None
                )
            # format GeolocationValue dict
            elif self.is_type(field_data.type, GeolocationValue):
                metadata[field_name] = (
                    self.create_geo_loc_value(
                        self.get_value(row, field_name),
                    )
                    if self.get_value(row, field_name)
                    else None
                )
            # format QuantityValue dict
            elif self.is_type(field_data.type, QuantityValue):
                metadata[field_name] = (
                    self.create_quantity_value(
                        raw_value=self.get_value(row, field_name)
                    )
                    if self.get_value(row, field_name)
                    else None
                )
            # format ControlledIdentifiedTermValue dict
            elif self.is_type(
                field_data.type, ControlledIdentifiedTermValue
            ) and field_name not in [
                "env_broad_scale",
                "env_local_scale",
                "env_medium",
            ]:
                metadata[field_name] = (
                    self.create_controlled_identified_term_value(
                        self.get_value(row, field_name),
                        {
                            self.get_value(row, field_name): self.get_value(
                                row, field_name
                            )
                        },
                    )
                    if self.get_value(row, field_name)
                    else None
                )
            # format TextValue dict
            elif self.is_type(field_data.type, TextValue):
                metadata[field_name] = (
                    self.create_text_value(
                        self.get_value(row, field_name), field_name == "env_package"
                    )
                    if self.get_value(row, field_name)
                    else None
                )
            # format and create envo term for env_broad_scale, env_local_scale, env_medium
            elif field_name in ["env_broad_scale", "env_local_scale", "env_medium"]:
                # create envo term for env_broad_scale, env_local_scale, env_medium
                metadata[field_name] = (
                    self.create_controlled_identified_term_value(
                        self.get_value(row, field_name),
                        envo_retriever.get_envo_terms(self.get_value(row, field_name)),
                    )
                    if self.get_value(row, field_name)
                    else None
                )
            # catch all for normal case - strings, ints, etc
            else:
                metadata[field_name] = (
                    self.get_value(row, field_name)
                    if self.get_value(row, field_name)
                    else None
                )
        return metadata

    def create_quantity_value(
        self,
        num_value: str = None,
        min_num_value: str = None,
        max_num_value: str = None,
        unit_value: str = None,
        raw_value: str = None,
    ):
        """
        Create a quantity value representation.

        Parameters
        ----------
        raw_value : str
            The raw value to convert to a quantity.

        Returns
        -------
        dict
            A dictionary representing the quantity value.
        """

        nmdc_quant_value = {"type": NmdcTypes.QuantityValue}

        if num_value:
            nmdc_quant_value["has_numeric_value"] = num_value
        if min_num_value:
            nmdc_quant_value["has_minimum_numeric_value"] = min_num_value
        if max_num_value:
            nmdc_quant_value["has_maximum_numeric_value"] = max_num_value
        if unit_value:
            nmdc_quant_value["has_unit"] = unit_value
        if num_value and unit_value:
            nmdc_quant_value["has_raw_value"] = str(num_value) + str(unit_value)
        if raw_value:
            nmdc_quant_value["has_raw_value"] = raw_value

        return nmdc_quant_value

    def create_geo_loc_value(self, raw_value: str):
        """
        Create a geolocation value representation.

        Parameters
        ----------
        raw_value : str
            The raw value associated with geolocation.
        lat_value : str
            The latitude value.
        long_value : str
            The longitude value.

        Returns
        -------
        dict
            A dictionary representing the geolocation value.
        """
        lat_value, long_value = raw_value.split(" ", 1)
        nmdc_geo_loc_value = {
            "has_raw_value": raw_value,
            "latitude": lat_value,
            "longitude": long_value,
            "type": NmdcTypes.GeolocationValue,
        }

        return nmdc_geo_loc_value

    def create_text_value(self, row_value: str, is_list: bool) -> Dict:
        """
        Create a text value representation.

        Parameters
        ----------
        row_value : str
            The raw value to convert.
        is_list : bool
            Whether to treat the value as a list.

        Returns
        -------
        dict
            A dictionary representing the text value.
        """

        nmdc_text_value = {"has_raw_value": row_value, "type": NmdcTypes.TextValue}

        return nmdc_text_value

    def create_controlled_identified_term_value(
        self, row_value: str, slot_enum_dict: dict
    ) -> Dict:
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
