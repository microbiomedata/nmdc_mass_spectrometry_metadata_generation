import ast
import hashlib

# json validate imports
import importlib.resources
import json
import logging
import os
import pkgutil
import re
from abc import ABC
from copy import deepcopy
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Dict, List

import nmdc_schema.nmdc as nmdc
import numpy as np
import pandas as pd
import requests
import toml
from linkml.validator import Validator
from linkml.validator.plugins import JsonschemaValidationPlugin
from linkml_runtime import SchemaView
from linkml_runtime.dumpers import json_dumper
from nmdc_api_utilities.auth import NMDCAuth
from nmdc_api_utilities.biosample_search import BiosampleSearch
from nmdc_api_utilities.configuration_search import ConfigurationSearch
from nmdc_api_utilities.data_object_search import DataObjectSearch
from nmdc_api_utilities.instrument_search import InstrumentSearch
from nmdc_api_utilities.metadata import Metadata
from nmdc_api_utilities.nmdc_search import NMDCSearch
from nmdc_api_utilities.processed_sample_search import ProcessedSampleSearch
from nmdc_api_utilities.study_search import StudySearch
from nmdc_schema import NmdcSchemaValidationPlugin
from nmdc_schema.nmdc import Database as NMDCDatabase
from tqdm import tqdm

import nmdc_ms_metadata_gen
from nmdc_ms_metadata_gen.data_classes import NmdcTypes, ProcessGeneratorMap
from nmdc_ms_metadata_gen.id_pool import IDPool
from nmdc_ms_metadata_gen.schema_bridge import get_curie_for_class

ENV = os.getenv("NMDC_ENV", "prod")

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# TODO: Update script to for Sample Processing - has_input for MassSpectrometry will have to be changed to be a processed sample id - not biosample id
class NMDCMetadataGenerator:
    """
    Generic base class for generating and validating NMDC metadata

    Parameters
    ----------
    id_pool_size : int
        The size of the ID pool to maintain for minting NMDC IDs. Default is 100.
    id_refill_threshold : int
        The threshold at which to refill the ID pool. Default is 10.

    Attributes
    ----------
    id_pool : IDPool
        An instance of the IDPool class for managing NMDC ID minting.
    provenance_metadata : nmdc.ProvenanceMetadata
        An instance of the ProvenanceMetadata associated with this metadata generation process.
    """

    def __init__(
        self, id_pool_size: int = 100, id_refill_threshold: int = 10, test: bool = False
    ):
        # Initialize ID pool
        self.id_pool = IDPool(
            pool_size=id_pool_size, refill_threshold=id_refill_threshold, test=test
        )
        # Add provenance metadata
        self.provenance_metadata = self._generate_provenance_metadata()

    def _generate_provenance_metadata(self) -> nmdc.ProvenanceMetadata:
        """
        Generate ProvenanceMetadata associated with this metadata generation process.

        This method creates a ProvenanceMetadata instance that captures
        information about the metadata generation process and is subsequently
        associated with generated NMDC instances as appropriate.

        Returns
        -------
        nmdc.ProvenanceMetadata
            The generated ProvenanceMetadata instance.
        """
        type_str = NmdcTypes.get("ProvenanceMetadata")
        git_url = "https://github.com/microbiomedata/nmdc_mass_spectrometry_metadata_generation"
        version = nmdc_ms_metadata_gen.__version__

        # Warn if using development version (package not installed properly)
        if version == "0.0.0-dev":
            logging.warning(
                "Using development version '0.0.0-dev'. Install the package with 'pip install -e .' to use the actual version from pyproject.toml"
            )

        source_system_of_record = "custom"
        provenance_metadata = nmdc.ProvenanceMetadata(
            type=type_str,
            git_url=git_url,
            version=version,
            source_system_of_record=source_system_of_record,
        )

        return provenance_metadata

    def load_credentials(self, config_file: str = None) -> tuple:
        """
        Load the client ID and secret from the environment or a configuration file.

        Parameters
        ----------
        config_file: str
            The path to the configuration file.

        Returns
        -------
        tuple
            A tuple containing the client ID and client secret.

        """
        client_id = os.getenv("CLIENT_ID")
        client_secret = os.getenv("CLIENT_SECRET")

        if not client_id or not client_secret:
            if config_file:
                config_file = Path(config_file)
                try:
                    config = toml.load(config_file)
                    client_id = config.get("CLIENT_ID")
                    client_secret = config.get("CLIENT_SECRET")
                except FileNotFoundError:
                    raise ValueError(f"Config file {config_file} not found.")
                except toml.TomlDecodeError:
                    raise ValueError("Error decoding TOML from the config file.")
                except KeyError:
                    raise ValueError(
                        "Config file must contain CLIENT_ID and CLIENT_SECRET."
                    )

        if not client_id or not client_secret:
            raise ValueError(
                "CLIENT_ID and CLIENT_SECRET must be set either in environment variables or passed in the config file.\nThey must be named CLIENT_ID and CLIENT_SECRET respectively."
            )

        return client_id, client_secret

    def start_nmdc_database(self) -> nmdc.Database:
        """
        Initialize and return a new NMDC Database instance.

        Returns
        -------
        nmdc.Database
            A new instance of an NMDC Database.

        Notes
        -----
        This method simply creates and returns a new instance of the NMDC
        Database. It does not perform any additional initialization or
        configuration.

        """
        return nmdc.Database()

    def find_associated_ids(self, ids: list[str]):
        """
        Given a list of sample ids, find the associated study ids.

        Parameters
        ----------
        ids : list[str]
            The ids to search for.

        Returns
        -------
        """
        search_obj = NMDCSearch(env=ENV)
        resp = search_obj.get_linked_instances_and_associate_ids(
            ids=ids, types="nmdc:Study"
        )
        return resp

    def clean_dict(self, dict: Dict) -> Dict:
        """
        Clean the dictionary by removing keys with empty or None values.

        Parameters
        ----------
        dict : Dict
            The dictionary to be cleaned.
        Returns
        -------
        Dict
            A new dictionary with keys removed where the values are None, an empty string, or a string with only whitespace.

        """
        return {k: v for k, v in dict.items() if v not in [None, "", ""]}

    def generate_data_object(
        self,
        file_path: Path,
        data_category: str,
        data_object_type: str,
        description: str,
        base_url: str,
        CLIENT_ID: str,
        CLIENT_SECRET: str,
        was_generated_by: str = None,
        alternative_id: str = None,
        in_manifest=None,
        url: str = None,
    ) -> nmdc.DataObject:
        """
        Create an NMDC DataObject with metadata from the specified file and details.

        This method generates an NMDC DataObject and assigns it a unique NMDC ID.
        The DataObject is populated with metadata derived from the provided file
        and input parameters.

        Parameters
        ----------
        file_path : Path
            Path to the file representing the data object. The file's name is
            used as the `name` attribute.
        data_category : str
            Category of the data object (e.g., 'instrument_data').
        data_object_type : str
            Type of the data object (e.g., 'LC-DDA-MS/MS Raw Data').
        description : str
            Description of the data object.
        base_url : str
            Base URL for accessing the data object, to which the file name is
            appended to form the complete URL. Ignored if `url` is provided.
        CLIENT_ID : str
            The client ID for the NMDC API.
        CLIENT_SECRET : str
            The client secret for the NMDC API.
        was_generated_by : str, optional
            ID of the process or entity that generated the data object
            (e.g., the DataGeneration id or the MetabolomicsAnalysis id).
        alternative_id : str, optional
            An optional alternative identifier for the data object.
        url : str, optional
            The complete URL for the data object. If provided, this takes
            precedence over constructing the URL from base_url + file name.

        Returns
        -------
        nmdc.DataObject
            An NMDC DataObject instance with the specified metadata.

        Notes
        -----
        This method calculates the MD5 checksum of the file, which may be
        time-consuming for large files.

        """

        nmdc_id = self.id_pool.get_id(
            nmdc_type=NmdcTypes.get("DataObject"),
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )

        data_dict = {
            "id": nmdc_id,
            "data_category": data_category,
            "data_object_type": data_object_type,
            "name": file_path.name,
            "description": description,
            "file_size_bytes": file_path.stat().st_size,
            "md5_checksum": hashlib.md5(file_path.open("rb").read()).hexdigest(),
            "url": url if url is not None else base_url + str(file_path.name),
            "type": NmdcTypes.get("DataObject"),
            "was_generated_by": was_generated_by,
            "alternative_identifiers": alternative_id,
            "in_manifest": in_manifest,
        }

        # If any of the data_dict values are None or empty strings, remove them
        data_dict = self.clean_dict(data_dict)
        data_object = nmdc.DataObject(**data_dict)

        return data_object

    def generate_mass_spectrometry_configuration(
        self,
        mass_spectrometry_acquisition_strategy: str,
        resolution_categories: List[str],
        mass_analyzers: List[str],
        ionization_source: str,
        mass_spectrum_collection_modes: List[str],
        polarity_mode: str,
        name: str,
        description: str,
        CLIENT_ID: str,
        CLIENT_SECRET: str,
        protocol_link: nmdc.Protocol | None = None,
    ) -> nmdc.MassSpectrometryConfiguration:
        """
        Create an NMDC MassSpectrometryConfiguration object with the provided metadata.

        This method generates an NMDC MassSpectrometryConfiguration object,
        populated with the specified metadata.

        Parameters
        ----------
        mass_spectrometry_acquisition_strategy : str
            The acquisition strategy used for mass spectrometry.
        resolution_categories : List[str]
            The resolution categories applicable to the mass spectrometry data.
        mass_analyzers : List[str]
            The mass analyzers used in the experiment.
        ionization_source : str
            The ionization source employed for the mass spectrometry analysis.
        mass_spectrum_collection_modes : List[str]
            The collection modes used for acquiring mass spectra.
        polarity_mode : str
            The polarity mode used in the mass spectrometry analysis.
        name : str
            The name of the mass spectrometry configuration.
        description : str
            A description of the mass spectrometry configuration.
        CLIENT_ID : str
            The client ID for the NMDC API.
        CLIENT_SECRET : str
            The client secret for the NMDC API.
        protocol_link : nmdc.Protocol, optional
            A link to the protocol used for the mass spectrometry configuration.

        Returns
        -------
        nmdc.MassSpectrometryConfiguration
            An NMDC MassSpectrometryConfiguration object with the specified metadata.
        """
        nmdc_id = self.id_pool.get_id(
            nmdc_type=NmdcTypes.get("MassSpectrometryConfiguration"),
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )
        data_dict = {
            "id": nmdc_id,
            "name": name,
            "description": description,
            "mass_spectrometry_acquisition_strategy": mass_spectrometry_acquisition_strategy,
            "resolution_categories": resolution_categories,
            "mass_analyzers": mass_analyzers,
            "ionization_source": ionization_source,
            "mass_spectrum_collection_modes": mass_spectrum_collection_modes,
            "polarity_mode": polarity_mode,
            "type": NmdcTypes.get("MassSpectrometryConfiguration"),
            "protocol_link": protocol_link,
        }

        mass_spectrometry_config = nmdc.MassSpectrometryConfiguration(**data_dict)
        return mass_spectrometry_config

    def generate_portion_of_substance(
        self,
        substance_name: str,
        volume_value: float | None = None,
        volume_unit: str | None = None,
        final_concentration_value: float | None = None,
        source_concentration_value: float | None = None,
        concentration_unit: str | None = None,
        mass_value: float | None = None,
        mass_unit: str | None = None,
        substance_role: str | None = None,
    ) -> nmdc.PortionOfSubstance:
        """
        Create an NMDC PortionOfSubstance object with the provided metadata.

        This method generates an NMDC PortionOfSubstance object, populated with the
        specified metadata.

        Parameters
        ----------
        substance_name : str
            The name of the substance.
        volume_value : float, optional
            The volume of the substance.
        volume_unit : str, optional
            The unit of measurement for the volume.
        final_concentration_value : float, optional
            The final concentration of the substance.
        source_concentration_value : float, optional
            The source concentration of the substance.
        concentration_unit : str, optional
            The unit of measurement for the concentrations (both final and source).
        mass_value : float, optional
            The mass of the substance.
        mass_unit : str, optional
            The unit of measurement for the mass.
        substance_role : str, optional
            The role of the substance in the experiment.
        """
        data_dict = {
            "known_as": substance_name,
            "type": NmdcTypes.get("PortionOfSubstance"),
        }

        if volume_value and volume_unit:
            data_dict["volume"] = nmdc.QuantityValue(
                type=NmdcTypes.get("QuantityValue"),
                has_numeric_value=volume_value,
                has_unit=volume_unit,
            )

        if final_concentration_value and concentration_unit:
            data_dict["final_concentration"] = nmdc.QuantityValue(
                type=NmdcTypes.get("QuantityValue"),
                has_numeric_value=final_concentration_value,
                has_unit=concentration_unit,
            )

        if source_concentration_value and concentration_unit:
            data_dict["source_concentration"] = nmdc.QuantityValue(
                type=NmdcTypes.get("QuantityValue"),
                has_numeric_value=source_concentration_value,
                has_unit=concentration_unit,
            )

        if mass_value and mass_unit:
            data_dict["mass"] = nmdc.QuantityValue(
                type=NmdcTypes.get("QuantityValue"),
                has_numeric_value=mass_value,
                has_unit=mass_unit,
            )

        if substance_role:
            data_dict["substance_role"] = substance_role

        portion_of_substance = nmdc.PortionOfSubstance(**data_dict)
        return portion_of_substance

    def generate_mobile_phase_segment(
        self,
        duration_value: float | None = None,
        duration_unit: str | None = None,
        substances_used: List[nmdc.PortionOfSubstance] | None = None,
    ) -> nmdc.MobilePhaseSegment:
        """
        Generate an NMDC MobilePhaseSegment object with the provided metadata.

        This method creates an NMDC MobilePhaseSegment object, populated with the
        specified metadata.

        Parameters
        ----------
        duration_value : float, optional
            The duration of the mobile phase segment.
        duration_unit : str, optional
            The unit of measurement for the duration.
        substances_used : List[nmdc.PortionOfSubstance], optional
            A list of PortionOfSubstance objects used in the mobile phase segment.

        Returns
        -------
        nmdc.MobilePhaseSegment
            The generated NMDC MobilePhaseSegment object.
        """
        mobile_phase_segment = nmdc.MobilePhaseSegment(
            type=NmdcTypes.get("MobilePhaseSegment")
        )
        if duration_value and duration_unit:
            mobile_phase_segment.duration = nmdc.QuantityValue(
                type=NmdcTypes.get("QuantityValue"),
                has_numeric_value=duration_value,
                has_unit=duration_unit,
            )
        if substances_used:
            mobile_phase_segment.substances_used = substances_used
        return mobile_phase_segment

    def generate_protocol(self, name: str = None, url: str = None) -> nmdc.Protocol:
        """
        Create an NMDC Protocol object with the provided metadata.
        This method generates an NMDC Protocol object, populated with the specified metadata.

        Parameters
        ----------
        name : str, optional
            The name of the protocol.
        url : str, optional
            The URL of the protocol.

        Returns
        -------
        nmdc.Protocol
            An NMDC Protocol object with the specified metadata.

        """

        data_dict = {
            "type": NmdcTypes.get("Protocol"),
            "name": name,
            "url": url,
        }

        return nmdc.Protocol(**data_dict)

    def generate_chromatography_configuration(
        self,
        name: str,
        description: str,
        chromatographic_category: str,
        stationary_phase: str,
        CLIENT_ID: str,
        CLIENT_SECRET: str,
        ordered_mobile_phases: List[nmdc.MobilePhaseSegment] | None = None,
        temperature_value: float | None = None,
        temperature_unit: str | None = None,
        protocol_link: nmdc.Protocol | None = None,
    ) -> nmdc.ChromatographyConfiguration:
        """
        Create an NMDC ChromatographyConfiguration object with the provided metadata.

        This method generates an NMDC ChromatographyConfiguration object,
        populated with the specified metadata.

        Parameters
        ----------
        name : str
            The name of the chromatography configuration.
        description : str
            A description of the chromatography configuration.
        chromatographic_category : str
            The category of chromatography (e.g., 'liquid_chromatography').
        stationary_phase : str
            The stationary phase used in the chromatography (e.g., 'C18').
        CLIENT_ID : str
            The client ID for the NMDC API.
        CLIENT_SECRET : str
            The client secret for the NMDC API.
        ordered_mobile_phases : List[nmdc.MobilePhaseSegment]
            A list of mobile phase segments used in the chromatography.
        temperature_value : float, optional
            The temperature at which the chromatography is performed.
        temperature_unit : str, optional
            The unit of measurement for the temperature (e.g., 'Cel')
        protocol_link : nmdc.Protocol, optional
            A link to the protocol used for the chromatography configuration.

        Returns
        -------
        nmdc.ChromatographyConfiguration
            An NMDC ChromatographyConfiguration object with the specified metadata.
        """

        nmdc_id = self.id_pool.get_id(
            nmdc_type=NmdcTypes.get("ChromatographyConfiguration"),
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )
        data_dict = {
            "id": nmdc_id,
            "name": name,
            "description": description,
            "chromatographic_category": chromatographic_category,
            "ordered_mobile_phases": ordered_mobile_phases,
            "stationary_phase": stationary_phase,
            "type": NmdcTypes.get("ChromatographyConfiguration"),
            "protocol_link": protocol_link,
        }

        if temperature_value and temperature_unit:
            data_dict["temperature"] = nmdc.QuantityValue(
                type=NmdcTypes.get("QuantityValue"),
                has_numeric_value=temperature_value,
                has_unit=temperature_unit,
            )

        chromatography_config = nmdc.ChromatographyConfiguration(**data_dict)
        return chromatography_config

    def generate_processed_sample(
        self,
        name: str,
        description: str,
        CLIENT_ID: str,
        CLIENT_SECRET: str,
        sampled_portion: list = None,
    ) -> nmdc.ProcessedSample:
        """
        Generate a processed sample object from the provided data.

        Parameters
        ----------
        name:str
            Name of the processed sample.
        description:str
            Description of the processed sample.
        sampled_portion:list
            The portion of the sample that is taken for downstream activity.
        CLIENT_ID : str
            The client ID for the NMDC API.
        CLIENT_SECRET : str
            The client secret for the NMDC API.

        Returns
        -------
        nmdc.ProcessedSample
            An NMDC ProcessedSample object with the provided metadata.
        """

        nmdc_id = self.id_pool.get_id(
            nmdc_type=NmdcTypes.get("ProcessedSample"),
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )

        data_dict = {
            "id": nmdc_id,
            "type": NmdcTypes.get("ProcessedSample"),
            "name": name,
            "description": description,
            "sampled_portion": sampled_portion,
        }

        return nmdc.ProcessedSample(**data_dict)

    def generate_material_processing(
        self,
        data: dict,
        type: str,
        has_input: list,
        has_output: list,
        CLIENT_ID: str,
        CLIENT_SECRET: str,
    ):
        """
        Generate a MaterialProcessing object from the provided data.

        Parameters
        ----------
        data : dict
            A dictionary containing the metadata for the material processing.
        type : str
            The type of material processing (e.g., 'PoolingProcess').
        has_input : list
            A list of input sample IDs for the process.
        has_outputs : list
            A list of output sample IDs for the process.
        CLIENT_ID : str
            The client ID for the NMDC API.
        CLIENT_SECRET : str
            The client secret for the NMDC API.

        Returns
        -------
        nmdc MaterialProcessing object
            An NMDC MaterialProcessing object with the provided metadata.
        """

        type_curie = get_curie_for_class(type)

        nmdc_id = self.id_pool.get_id(
            nmdc_type=type_curie,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )

        for key, value in data.items():
            if value is None:
                data.pop(key)
        data.update(
            {
                "id": nmdc_id,
                "has_input": has_input,
                "has_output": has_output,
                "type": type_curie,
            }
        )

        return ProcessGeneratorMap.get(type)(**data)

    def generate_metabolomics_analysis(
        self,
        cluster_name: str,
        raw_data_name: str,
        raw_data_id: str,
        data_gen_id_list: List[str],
        processed_data_id: str,
        parameter_data_id: str,
        processing_institution: str,
        CLIENT_ID: str,
        CLIENT_SECRET: str,
        calibration_id: str = None,
        incremeneted_id: str = None,
        metabolite_identifications: List[nmdc.MetaboliteIdentification] = None,
        type: str = NmdcTypes.get("MetabolomicsAnalysis"),
    ) -> nmdc.MetabolomicsAnalysis:
        """
        Create an NMDC MetabolomicsAnalysis object with metadata for a workflow analysis.

        This method generates an NMDC MetabolomicsAnalysis object, including details
        about the analysis, the processing institution, and relevant workflow information.

        Parameters
        ----------
        cluster_name : str
            Name of the cluster or computing resource used for the analysis.
        raw_data_name : str
            Name of the raw data file that was analyzed.
        raw_data_id : str
            ID of the raw data object that was analyzed.
        data_gen_id_list : List[str]
            ID of the DataGeneration object that generated the raw data.
        processed_data_id : str
            ID of the processed data resulting from the analysis.
        parameter_data_id : str
            ID of the parameter data object used for the analysis.
        processing_institution : str
            Name of the institution where the analysis was performed.
        CLIENT_ID : str
            The client ID for the NMDC API.
        CLIENT_SECRET : str
            The client secret for the NMDC API.
        calibration_id : str, optional
            ID of the calibration information used for the analysis.
            Default is None, indicating no calibration information.
        incremeneted_id : str, optional
            An optional incremented ID for the MetabolomicsAnalysis object.
            If not provided, a new NMDC ID will be minted.
        metabolite_identifications : List[nmdc.MetaboliteIdentification], optional
            List of MetaboliteIdentification objects associated with the analysis.
            Default is None, which indicates no metabolite identifications.
        type : str, optional
            The type of the analysis. Default resolves to the schema type for MetabolomicsAnalysis.

        Returns
        -------
        nmdc.MetabolomicsAnalysis
            An NMDC MetabolomicsAnalysis instance with the provided metadata.

        Notes
        -----
        The 'started_at_time' and 'ended_at_time' fields are initialized with
        placeholder values and should be updated with actual timestamps later
        when the processed files are iterated over in the run method.

        """
        if incremeneted_id is None:
            # If no incremented id is provided, mint a new one

            nmdc_id = (
                self.id_pool.get_id(
                    nmdc_type=NmdcTypes.get("MetabolomicsAnalysis"),
                    client_id=CLIENT_ID,
                    client_secret=CLIENT_SECRET,
                )
                + ".1"
            )

        data_dict = {
            "id": incremeneted_id if incremeneted_id is not None else nmdc_id,
            "name": f"{self.workflow_analysis_name} for {raw_data_name}",
            "description": self.workflow_description,
            "processing_institution": processing_institution,
            "execution_resource": cluster_name,
            "git_url": self.workflow_git_url,
            "version": self.workflow_version,
            "was_informed_by": data_gen_id_list,
            "has_input": [raw_data_id, parameter_data_id],
            "has_output": [processed_data_id],
            "started_at_time": "placeholder",
            "ended_at_time": "placeholder",
            "type": type,
            "metabolomics_analysis_category": self.workflow_category,
        }

        if calibration_id is not None:
            data_dict["uses_calibration"] = calibration_id

        if metabolite_identifications is not None:
            data_dict["has_metabolite_identifications"] = metabolite_identifications
        data_dict = self.clean_dict(data_dict)
        metab_analysis = nmdc.MetabolomicsAnalysis(**data_dict)

        return metab_analysis

    def generate_instrument(
        self,
        name: str,
        description: str,
        CLIENT_ID: str,
        CLIENT_SECRET: str,
        vendor: str = None,
        model: str = None,
    ) -> nmdc.Instrument:
        """
        Create an NMDC Instrument object with the provided metadata.

        This method generates an NMDC Instrument object, populated with the
        specified metadata.

        Parameters
        ----------
        name : str
            The name of the instrument.
        description : str
            A description of the instrument.
        CLIENT_ID : str
            The client ID for the NMDC API.
        CLIENT_SECRET : str
            The client secret for the NMDC API.
        vendor : str, optional
            The vendor/manufacturer of the instrument.
        model : str, optional
            The model of the instrument.

        Returns
        -------
        nmdc.Instrument
            An NMDC Instrument object with the specified metadata.
        """

        nmdc_id = self.id_pool.get_id(
            nmdc_type=NmdcTypes.get("Instrument"),
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )
        data_dict = {
            "id": nmdc_id,
            "name": name,
            "description": description,
            "type": NmdcTypes.get("Instrument"),
        }

        if vendor:
            data_dict["vendor"] = vendor

        if model:
            data_dict["model"] = model

        # Clean the dictionary to remove any None values
        data_dict = self.clean_dict(data_dict)
        instrument = nmdc.Instrument(**data_dict)
        return instrument

    def dump_nmdc_database(self, nmdc_database: nmdc.Database, json_path: Path) -> None:
        """
        Dump the NMDC database to a JSON file.

        This method serializes the NMDC Database instance to a JSON file
        at the specified path.

        Parameters
        ----------
        nmdc_database : nmdc.Database
            The NMDC Database instance to dump.
        json_path : Path
            The file path where the JSON dump will be saved.

        Returns
        -------
        None

        Side Effects
        ------------
        Writes the database content to the file specified by
        `json_path`.
        """
        json_dumper.dump(nmdc_database, json_path)
        logging.info("Database successfully dumped in %s", json_path)

    def validate_nmdc_database(self, json: dict | str, use_api: bool = True) -> dict:
        """
        Validate the NMDC database JSON object against the NMDC schema.

        This method checks if the provided JSON file conforms to the NMDC schema.
        If the validation fails, it returns the errors.


        Parameters
        ----------
        json : str | dict
            The JSON object or path to the JSON file to validate.

        use_api : bool
            Whether to use the NMDC API for validation. If False, uses local validation.

        Returns
        -------
        dict
            A dictionary with the validation result.
            If valid, returns {"result": "All okay!"}.
            If errors, returns {"result": "errors", "detail": [list of errors]}.


        Raises
        ------
        ValueError
            If the JSON file does not conform to the NMDC schema.
        """
        import json as json_lib

        if isinstance(json, str):
            with open(json) as f:
                json = json_lib.load(f)
        if use_api:
            api_metadata = Metadata(env=ENV)
            api_metadata.validate_json(json)
            return {"result": "All okay!"}
        else:

            validation_result = self._validate_json_no_api(metadata=json)
            return validation_result

    def json_submit(self, json: dict | str, CLIENT_ID: str, CLIENT_SECRET: str):
        """
        Submit the generated JSON metadata to the NMDC API.

        Parameters
        ----------
        json : dict | str
            The JSON metadata to submit. Can be a file path or a JSON string.
        CLIENT_ID : str
            The client ID for the NMDC API.
        CLIENT_SECRET : str
            The client secret for the NMDC API.

        Returns
        -------
        None

        """
        import json as json_lib

        if isinstance(json, str):
            with open(json) as f:
                json = json_lib.load(f)
        auth = NMDCAuth(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
        md = Metadata(env=ENV, auth=auth)
        success = md.submit_json(json)
        if success != 200:
            logging.error("Failed to submit JSON metadata: %s", json)
            raise ValueError("Failed to submit JSON metadata")

    @staticmethod
    def get_start_end_times(file) -> tuple:
        """
        Get the start and end times for a given file based on its filesystem metadata.

        This method retrieves the earliest and latest timestamps associated with the file,
        considering creation, modification, and, if available, birth times.

        Parameters
        ----------
        file : Path
            A pathlib.Path object representing the file.

        Returns
        -------
        tuple
            A tuple containing the start time and end time as formatted strings ("%Y-%m-%d %H:%M:%S").
        """
        stat_info = file.stat()
        timestamps = [stat_info.st_mtime, stat_info.st_ctime]
        if hasattr(stat_info, "st_birthtime"):
            timestamps.append(stat_info.st_birthtime)
        earliest_time = min(timestamps)
        start_time = datetime.fromtimestamp(earliest_time).strftime("%Y-%m-%d %H:%M:%S")
        end_time = datetime.fromtimestamp(stat_info.st_mtime).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        assert start_time <= end_time, "Start time must be before end time."
        return start_time, end_time

    @staticmethod
    def _validate_json_no_api(metadata: dict) -> dict:
        """
        Checks whether the input dictionary represents a valid instance of the nmdc `Database` class
        defined in the NMDC Schema.

        This code was grabbed from nmdc-runtime and modified to be a static method here.

        Parameters
        ----------
        in_docs: dict
            The dictionary you want to validate.

        Returns
        -------
        dict
            A dictionary with two keys: `result` and `detail`. The value of `result` is either `"valid"` or `"invalid"`,
              and the value of `detail` is a list of validation error messages (if any).

        Example
        -------
        {
            "biosample_set": [
                {"id": "nmdc:bsm-00-000001", ...},
                {"id": "nmdc:bsm-00-000002", ...}
            ],
            "study_set": [
                {"id": "nmdc:sty-00-000001", ...},
                {"id": "nmdc:sty-00-000002", ...}
            ]
        }
        """

        def get_nmdc_yaml_view() -> SchemaView:
            nmdc_schema_bytes = pkgutil.get_data(
                "nmdc_schema", "nmdc_materialized_patterns.yaml"
            )
            nmdc_schema_yaml_string = str(nmdc_schema_bytes, "utf-8")
            nmdc_view = SchemaView(nmdc_schema_yaml_string)
            return nmdc_view

        def nmdc_database_collection_names():
            """
            This function was designed to return a list of names of all Database slots that represents database
                collections
            """
            names = []
            view = get_nmdc_yaml_view()
            all_classes = set(view.all_classes())
            for slot in view.class_slots("Database"):
                rng = getattr(view.get_slot(slot), "range", None)
                if rng in all_classes:
                    names.append(slot)
            return names

        def get_nmdc_jsonschema_path() -> Path:
            """Get path to NMDC JSON Schema file."""
            with importlib.resources.path(
                "nmdc_schema", "nmdc_materialized_patterns.schema.json"
            ) as p:
                return p

        def get_nmdc_schema_validator() -> Validator:
            schema_view = get_nmdc_yaml_view()
            return Validator(
                schema_view.schema,
                validation_plugins=[
                    JsonschemaValidationPlugin(
                        closed=True,
                        # Since the `nmdc-schema` package exports a pre-built JSON Schema file, use that
                        # instead of relying on the plugin to generate one on the fly.
                        json_schema_path=get_nmdc_jsonschema_path(),
                    ),
                    NmdcSchemaValidationPlugin(),
                ],
            )

        validator = get_nmdc_schema_validator()
        docs = deepcopy(metadata)
        validation_errors = {}

        known_coll_names = set(nmdc_database_collection_names())
        for coll_name, coll_docs in docs.items():
            if coll_name not in known_coll_names:
                # We expect each key in `in_docs` to be a known schema collection name. However, `@type` is a special key
                # for JSON-LD, used for JSON serialization of e.g. LinkML objects. That is, the value of `@type` lets a
                # client know that the JSON object (a dict in Python) should be interpreted as a
                # <https://w3id.org/nmdc/Database>. If `@type` is present as a key, and its value indicates that
                # `metadata` is indeed a nmdc:Database, that's fine, and we don't want to raise an exception.
                #
                # prompted by: https://github.com/microbiomedata/nmdc-runtime/discussions/858
                if coll_name == "@type" and coll_docs in ("Database", "nmdc:Database"):
                    continue
                else:
                    validation_errors[coll_name] = [
                        f"'{coll_name}' is not a known schema collection name"
                    ]
                    continue

            errors = list(
                validator.iter_results({coll_name: coll_docs}, target_class="Database")
            )
            validation_errors[coll_name] = [e.message for e in errors]
            if coll_docs:
                if not isinstance(coll_docs, list):
                    validation_errors[coll_name].append("value must be a list")
                elif not all(isinstance(d, dict) for d in coll_docs):
                    validation_errors[coll_name].append(
                        "all elements of list must be dicts"
                    )

        if all(len(v) == 0 for v in validation_errors.values()):
            # Second pass. Try instantiating linkml-sourced dataclass
            metadata.pop("@type", None)
            try:
                NMDCDatabase(**metadata)
            except Exception as e:
                return {"result": "errors", "detail": str(e)}
            return {"result": "All Okay!"}
        else:
            return {"result": "errors", "detail": validation_errors}

    def nmdc_db_to_dict(self, nmdc_db: nmdc.Database) -> dict:
        """
        Convert an NMDC Database object to a dictionary.

        This method serializes the NMDC Database instance to a dictionary.

        Parameters
        ----------
        nmdc_db : nmdc.Database
            The NMDC Database instance to convert.

        Returns
        -------
        dict
            A dictionary representation of the NMDC Database.
        """
        return json_dumper.to_dict(nmdc_db)


class NMDCWorkflowMetadataGenerator(NMDCMetadataGenerator, ABC):
    """
    Abstract class for generating NMDC metadata objects using provided metadata files and configuration.

    Parameters
    ----------
    metadata_file : str
        Path to the input CSV metadata file.
    database_dump_json_path : str
        Path where the output database dump JSON file will be saved.
    raw_data_url : str
        Base URL for the raw data files.
    process_data_url : str
        Base URL for the processed data files.
    """

    def __init__(
        self,
        metadata_file: str,
        database_dump_json_path: str,
        raw_data_url: str,
        process_data_url: str,
        test: bool = False,
    ):
        super().__init__(test=test)
        self.metadata_file = metadata_file
        self.database_dump_json_path = database_dump_json_path
        self.raw_data_url = raw_data_url
        self.process_data_url = process_data_url
        self.raw_data_category = "instrument_data"

    def load_metadata(self) -> pd.core.frame.DataFrame:
        """
        Load and group workflow metadata from a CSV file.

        This method reads the metadata CSV file, checks for uniqueness in
        specified columns, checks that samples exist, and groups the data by sample ID.

        Returns
        -------
        pd.core.frame.DataFrame
            A DataFrame containing the loaded and grouped metadata.

        Raises
        ------
        FileNotFoundError
            If the `metadata_file` does not exist.
        ValueError
            If values in columns 'Raw Data File',
            and 'Processed Data Directory' are not unique.

        Notes
        -----
        See example_metadata_file.csv in this directory for an example of
        the expected input file format.
        """
        try:
            metadata_df = pd.read_csv(self.metadata_file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Metadata file not found: {self.metadata_file}")

        # Check for uniqueness in specified columns
        columns_to_check = self.unique_columns
        for column in columns_to_check:
            if not metadata_df[column].is_unique:
                raise ValueError(f"Duplicate values found in column '{column}'.")

        # if the run is not a test, check that samples exist and find associated studies
        if self.test == False:
            # Check that all samples exist in the db
            sample_ids = metadata_df["sample_id"].unique()
            # determine sample type
            if pd.isna(sample_ids)[0] == np.False_:
                sample_type = "biosample" if "bsm" in sample_ids[0] else "processed"
            else:
                sample_type = ""

            if sample_type == "biosample":
                sample_client = BiosampleSearch(env=ENV)
            else:
                sample_client = ProcessedSampleSearch(env=ENV)

            if pd.isna(sample_ids)[0] == np.False_:
                if not sample_client.check_ids_exist(list(sample_ids)):
                    raise ValueError("IDs do not exist in the collection.")

            # make a call to find_associated_ids to get the associated studies
            # build the ID list from the input samples
            sample_ids = metadata_df["sample_id"].unique().tolist()
            associations = self.find_associated_ids(ids=sample_ids)
            # map the ids back to the df before returning. associations will be a list of dictionaries with study ids
            for sample_id, studies in associations.items():
                study_str = str(studies)
                metadata_df.loc[
                    metadata_df["sample_id"] == sample_id,
                    "associated_studies",
                ] = study_str
        # if it is a test, plug in associated studies with a placeholder
        else:
            metadata_df["associated_studies"] = "['nmdc:sty-00-000001']"
        return metadata_df

    def generate_mass_spectrometry(
        self,
        file_path: Path,
        instrument_id: str,
        sample_id: str,
        raw_data_id: str,
        study_id: str,
        processing_institution: str,
        mass_spec_configuration_id: str,
        start_date: str,
        end_date: str,
        CLIENT_ID: str,
        CLIENT_SECRET: str,
        instrument_instance_specifier: str = None,
        lc_config_id: str = None,
        calibration_id: str = None,
    ) -> nmdc.DataGeneration:
        """
        Create an NMDC DataGeneration object for mass spectrometry and mint an NMDC ID.

        Parameters
        ----------
        file_path : Path
            File path of the mass spectrometry data.
        instrument_name : str
            Name of the instrument used for data generation.
        sample_id : str
            ID of the input sample associated with the data generation.
        raw_data_id : str
            ID of the raw data object associated with the data generation.
        study_id : str
            ID of the study associated with the data generation.
        processing_institution : str
            Name of the processing institution.
        mass_spec_config_name : str
            Name of the mass spectrometry configuration.
        start_date : str
            Start date of the data generation.
        end_date : str
            End date of the data generation.
        CLIENT_ID : str
            The client ID for the NMDC API.
        CLIENT_SECRET : str
            The client secret for the NMDC API.
        instrument_instance_specifier : str, optional
            Specifier for the instrument instance used in the data generation.
        lc_config_name : str, optional
            Name of the liquid chromatography configuration.
        calibration_id : str, optional
            ID of the calibration information generated with the data.
            Default is None, indicating no calibration information.

        Returns
        -------
        nmdc.DataGeneration
            An NMDC DataGeneration object with the provided metadata.

        Notes
        -----
        This method uses the nmdc_api_utilities package to fetch IDs for the instrument
        and configurations. It also mints a new NMDC ID for the DataGeneration object.

        """
        nmdc_id = self.id_pool.get_id(
            nmdc_type=NmdcTypes.get("MassSpectrometry"),
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )

        data_dict = {
            "id": nmdc_id,
            "name": file_path.stem,
            "description": self.mass_spec_desc,
            "add_date": datetime.now().strftime("%Y-%m-%d"),
            "eluent_introduction_category": self.mass_spec_eluent_intro,
            "has_mass_spectrometry_configuration": mass_spec_configuration_id,
            "has_chromatography_configuration": lc_config_id,
            "analyte_category": self.analyte_category,
            "instrument_used": instrument_id,
            "has_input": [sample_id],
            "has_output": [raw_data_id],
            "associated_studies": study_id,
            "processing_institution": processing_institution,
            "start_date": start_date,
            "end_date": end_date,
            "instrument_instance_specifier": instrument_instance_specifier,
            "type": NmdcTypes.get("MassSpectrometry"),
        }

        if calibration_id is not None:
            data_dict["generates_calibration"] = calibration_id
        data_dict = self.clean_dict(data_dict)
        mass_spectrometry = nmdc.DataGeneration(**data_dict)

        return mass_spectrometry

    def check_doj_urls(self, metadata_df: pd.DataFrame, url_columns: List) -> None:
        """
        Check if the URLs in the input list already exist in the database.

        Parameters
        ----------
        metadata_df : pd.DataFrame
            The DataFrame containing the metadata information.
        url_columns : List
            The list of columns in the DataFrame that contain URLs to check.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If any URL in the metadata DataFrame is invalid or inaccessible.
        FileNotFoundError
            If no files are found in the specified directory columns.

        """
        urls = []
        for col in url_columns:
            # Check if this is a raw data URL column (only handle raw data URLs)
            if col == "raw_data_url":
                # For raw data URL column, use the URLs directly
                column_urls = metadata_df[col].dropna().tolist()
                urls.extend(column_urls)
                # Check if the urls are valid
                for url in column_urls[
                    :5
                ]:  # Check up to 5 URLs, or fewer if the list is shorter
                    try:
                        response = requests.head(url)
                        if response.status_code != 200:
                            raise ValueError(f"URL {url} is not accessible.")
                    except requests.RequestException as e:
                        raise ValueError(f"URL {url} is not accessible. Error: {e}")
            elif "directory" in col:
                # if its a directory, we need to gather all the files in the directory
                file_data_paths = [
                    list(Path(x).glob("**/*")) for x in metadata_df[col].to_list()
                ]
                # Add a check that the processed data directory is not empty
                if not any(file_data_paths):
                    raise FileNotFoundError(
                        f"No files found in {col}: {metadata_df[col]}"
                    )
                # line to flatten the list of lists
                file_data_paths = [
                    file for sublist in file_data_paths for file in sublist
                ]

                # if it is a directory, we need to grab the directory from the metadata file path
                if "process" in col:
                    urls += [
                        self.process_data_url + str(x.parent.name) + "/" + str(x.name)
                        for x in file_data_paths
                    ]
                elif "raw" in col:
                    urls += [
                        self.raw_data_url + str(x.parent.name) + "/" + str(x.name)
                        for x in file_data_paths
                    ]
                # check if the urls are valid
                for url in urls[
                    :5
                ]:  # Check up to 5 URLs, or fewer if the list is shorter
                    try:
                        response = requests.head(url)
                        if response.status_code != 200:
                            raise ValueError(f"URL {url} is not accessible.")
                    except requests.RequestException as e:
                        raise ValueError(f"URL {url} is not accessible. Error: {e}")
            else:
                # if its a file, we need to gather the file paths
                file_data_paths = [Path(x) for x in metadata_df[col].to_list()]
                if "process" in col:
                    urls += [
                        self.process_data_url + str(x.name) for x in file_data_paths
                    ]
                elif "raw" in col:
                    urls += [self.raw_data_url + str(x.name) for x in file_data_paths]
                # check if the urls are valid
                for url in urls[
                    :5
                ]:  # Check up to 5 URLs, or fewer if the list is shorter
                    try:
                        response = requests.head(url)
                        if response.status_code != 200:
                            raise ValueError(f"URL {url} is not accessible.")
                    except requests.RequestException as e:
                        raise ValueError(f"URL {url} is not accessible. Error: {e}")

        doj_client = DataObjectSearch(env=ENV)
        resp = doj_client.get_batch_records(
            id_list=urls, search_field="url", fields="id", chunk_size=10
        )
        if resp:
            raise ValueError(
                f"The following URLs already exist in the database: {', '.join(resp)}"
            )

    def check_manifest(
        self,
        metadata_df: pd.DataFrame,
        nmdc_database_inst: nmdc.Database,
        CLIENT_ID: str,
        CLIENT_SECRET: str,
    ) -> None:
        """
        Check if the manifest id is passed in the metadata DataFrame. If not, generate a new manifest id and add it to the NMDC database instance.

        Parameters
        ----------
        metadata_df : pd.DataFrame
            The DataFrame containing the metadata information.
        nmdc_database_inst : nmdc.Database
            The NMDC Database instance to add the manifest to if one needs to be generated.
        CLIENT_ID : str
            The client ID for the NMDC API. Used to mint a manifest id if one does not exist.
        CLIENT_SECRET : str
            The client secret for the NMDC API. Used to mint a manifest id if one does not exist.

        """
        if (
            "manifest_id" not in metadata_df.columns
            or metadata_df["manifest_id"].isnull().any()
        ):
            self.generate_manifest(
                metadata_df=metadata_df,
                nmdc_database_inst=nmdc_database_inst,
                CLIENT_ID=CLIENT_ID,
                CLIENT_SECRET=CLIENT_SECRET,
            )

    def update_outputs(
        self,
        analysis_obj: object,
        raw_data_obj_id: str,
        parameter_data_id: str,
        processed_data_id_list: list,
        mass_spec_obj: object = None,
        rerun: bool = False,
    ) -> None:
        """
        Update output references for Mass Spectrometry and Workflow Analysis objects.

        This method assigns the output references for a Mass Spectrometry object
        and a Workflow Execution Analysis object. It sets `mass_spec_obj.has_output`
        to the ID of `raw_data_obj` and `analysis_obj.has_output` to a list of
        processed data IDs.

        Parameters
        ----------
        analysis_obj : object
            The Workflow Execution Analysis object to update
            (e.g., MetabolomicsAnalysis).
        raw_data_obj_id : str
            The Raw Data Object associated with the Mass Spectrometry.
        parameter_data_id : str
            ID of the data object representing the parameter data used for the analysis.
        processed_data_id_list : list
            List of IDs representing processed data objects associated with
            the Workflow Execution.
        mass_spec_obj : object , optional
            The Mass Spectrometry object to update. Optional for rerun cases.
        rerun : bool, optional
            If True, this indicates the run is a rerun, and the method will not set `mass_spec_obj.has_output` because there is not one.
            Default is False.

        Returns
        -------
        None

        Notes
        ------
        - Sets `mass_spec_obj.has_output` to [raw_data_obj.id].
        - Sets `analysis_obj.has_output` to `processed_data_id_list`.

        """
        if not rerun:
            # if it is not a rerun, set the mass spec object, otherwise there will not be a mass spec object
            mass_spec_obj.has_output = [raw_data_obj_id]
        analysis_obj.has_input = parameter_data_id
        analysis_obj.has_output = processed_data_id_list

    def generate_mass_spec_fields(
        self,
        metadata_df: pd.DataFrame,
    ) -> None:
        """
        Generate mass_spec_id, lc_config_name, and instrument_id information for each row in the metadata DataFrame. Add fields back to the DataFrame.

        Parameters
        ----------
        metadata_df : pd.DataFrame
            The metadata DataFrame.

        Returns
        -------
        None
        """
        is_client = InstrumentSearch(env=ENV)
        cs_client = ConfigurationSearch(env=ENV)
        instrument_names = metadata_df["instrument_used"].unique()
        if "chromat_configuration_name" in metadata_df.columns:
            # If the column exists, get unique LC configuration names
            lc_config_names = metadata_df["chromat_configuration_name"].unique()
        else:
            # If the column does not exist, set it to an empty array
            lc_config_names = []
        mass_spec_config_names = metadata_df["mass_spec_configuration_name"].unique()

        # loop through each variable and get the id for each name
        # and add it to the metadata_df
        for var in [instrument_names, lc_config_names, mass_spec_config_names]:
            for name in var:
                if var is instrument_names:
                    # Get instrument id
                    instrument_id = is_client.get_record_by_attribute(
                        attribute_name="name",
                        attribute_value=name,
                        fields="id",
                        exact_match=True,
                    )[0]["id"]
                    metadata_df.loc[
                        metadata_df["instrument_used"] == name, "instrument_id"
                    ] = instrument_id
                elif var is lc_config_names:
                    # Get LC configuration id
                    try:
                        lc_config_id = cs_client.get_record_by_attribute(
                            attribute_name="name",
                            attribute_value=name,
                            fields="id",
                            exact_match=True,
                        )[0]["id"]
                    except IndexError:
                        raise ValueError(
                            f"Configuration '{name}' not found in the database."
                        )
                    metadata_df.loc[
                        metadata_df["chromat_configuration_name"] == name,
                        "lc_config_id",
                    ] = lc_config_id

                elif var is mass_spec_config_names:
                    # Get mass spec configuration id
                    mass_spec_id = cs_client.get_record_by_attribute(
                        attribute_name="name",
                        attribute_value=name,
                        fields="id",
                        exact_match=True,
                    )[0]["id"]
                    metadata_df.loc[
                        metadata_df["mass_spec_configuration_name"] == name,
                        "mass_spec_configuration_id",
                    ] = mass_spec_id

    def generate_manifest(
        self,
        metadata_df: pd.DataFrame,
        nmdc_database_inst: nmdc.Database,
        CLIENT_ID: str,
        CLIENT_SECRET: str,
    ) -> None:
        """
        Generate manifest information and data objects for each manifest. Add the manifest id to the metadata DataFrame.

        Parameters
        ----------
        metadata_df : pd.DataFrame
            The metadata DataFrame.
        nmdc_database_inst : nmdc.Database
            The NMDC Database instance.
        CLIENT_ID : str
            The client ID for the NMDC API.
        CLIENT_SECRET : str
            The client secret for the NMDC API.

        Returns
        -------
        None
        """
        # check if manifest_name exists and has non-null values. If not, return
        if (
            "manifest_name" not in metadata_df.columns
            or metadata_df["manifest_name"].isnull().all()
        ):
            print("No manifests will be added.")
            return

        # Get unique combinations of manifest_name and (if existing) manifest_id and create initial mapping
        if "manifest_id" not in metadata_df.columns:
            metadata_df["manifest_id"] = None

        manifest_id_mapping = {}
        manifest_names = metadata_df[["manifest_name", "manifest_id"]].drop_duplicates()

        for _, row in manifest_names.iterrows():
            manifest_id_mapping[row["manifest_name"]] = row["manifest_id"]

        for manifest_name in tqdm(
            manifest_id_mapping.keys(),
            total=len(manifest_names),
            desc="Generating manifest information and data objects",
        ):
            # If there is already an manifest_id associated with a manifest_name
            if manifest_id_mapping[manifest_name] is not None:
                continue
            # mint id
            manifest_id = self.id_pool.get_id(
                nmdc_type=NmdcTypes.get("Manifest"),
                client_id=CLIENT_ID,
                client_secret=CLIENT_SECRET,
            )

            data_dict = {
                "id": manifest_id,
                "name": manifest_name,
                "type": NmdcTypes.get("Manifest"),
                "manifest_category": "instrument_run",
            }

            manifest = nmdc.Manifest(**data_dict)

            nmdc_database_inst.manifest_set.append(manifest)

            # Add manifest_id to the mapping dictionary
            manifest_id_mapping[manifest_name] = manifest.id

        # Update the metadata_df in a single operation
        metadata_df["manifest_id"] = metadata_df["manifest_name"].map(
            manifest_id_mapping
        )

    def get_workflow_version(self, workflow_version_git_url: str) -> str:
        """
        Get the version of the workflow from the git repository.

        Parameters
        ----------
        repo_link : str
            The URL of the git repository containing the workflow version.

        Returns
        -------
        str
            The version of the workflow.
        """
        resp = requests.get(workflow_version_git_url)
        if resp.status_code == 200:
            # Regular expression to find the current_version
            match = re.search(r"current_version\s*=\s*([\d.]+)", resp.text)
            if match:
                current_version = match.group(1)
            return current_version
        else:
            logging.warning(
                f"Failed to fetch the workflow version from the Git repository {workflow_version_git_url}"
            )
        return None
