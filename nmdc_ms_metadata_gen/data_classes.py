from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Dict

from nmdc_ms_metadata_gen.schema_bridge import (
    get_curie_for_class,
    get_material_processing_class,
    get_typecode_for_curie,
    list_material_processing_types,
)


@dataclass
class GCMSMetabWorkflowMetadata:
    """
    Data class for holding GCMS metabolomic workflow metadata information.

    Attributes
    ----------
    sample_id: str
        Identifier for the sample.
    nmdc_study : str
        Identifier for the NMDC study.
    processed_data_file : str
        Path or name of the processed data file.
    raw_data_file : str
        Path or name of the raw data file.
    mass_spec_configuration_id : str
        Identifier for the mass spectrometry configuration used.
    lc_config_id: str
        Identifier for the liquid chromatography configuration used.
    instrument_id: str
        Identifier for the instrument used for analysis.
    calibration_ids: list[str]
        Identifier for the calibration information used.
    instrument_analysis_start_date: str, optional
        Start date of the instrument analysis.
    instrument_analysis_end_date: str, optional
        End date of the instrument analysis.
    processing_institution : str
        Name of the processing institution. Must be a value from ProcessingInstitutionEnum. OPTIONAL IF processing_institution_generation AND processing_institution_workflow ARE PROVIDED
    processing_institution_generation : str
        Name of the processing institution where the data was generated. Must be a value from ProcessingInstitutionEnum. OPTIONAL IF processing_institution IS PROVIDED
    processing_institution_workflow : str
        Name of the processing institution where the workflow was executed. Must be a value from ProcessingInstitutionEnum. OPTIONAL IF processing_institution IS PROVIDED
    execution_resource : str, optional
        Name of the execution resource. Must be a value from ExecutionResourceEnum.
    raw_data_url : str, optional
        Complete URL for the raw data file. If provided, this takes precedence
        over constructing the URL from base_url + filename.
    manifest_id : str
        Identifier for the manifest associated with this workflow metadata.
    instrument_instance_specifier : str, optional
        Specifier for the instrument instance used in the analysis.
    """

    sample_id: str
    nmdc_study: str
    processed_data_file: str
    raw_data_file: str
    mass_spec_configuration_id: str
    lc_config_id: str
    instrument_id: str
    calibration_ids: list[str]
    instrument_analysis_start_date: str = None
    instrument_analysis_end_date: str = None
    processing_institution: str = None
    processing_institution_generation: str = None
    processing_institution_workflow: str = None
    execution_resource: str = None
    raw_data_url: str = None
    manifest_id: str = None
    instrument_instance_specifier: str = None


@dataclass
class LCMSLipidWorkflowMetadata:
    """
    Data class for holding LC-MS lipidomics workflow metadata information.
    Also used for LC-MS Metabolomics workflows.

    Attributes
    ----------
    processed_data_dir : str
        Directory containing processed data files.
    raw_data_file : str
        Path or name of the raw data file.
    mass_spec_config_id : str
        Identifier for the mass spectrometry configuration used.
    lc_config_id : str
        Identifier for the liquid chromatography configuration used.
    instrument_id : str
        Identifier for the instrument used for analysis.
    processing_institution : str
        Name of the processing institution. Must be a value from ProcessingInstitutionEnum. OPTIONAL IF processing_institution_generation AND processing_institution_workflow ARE PROVIDED
    processing_institution_generation : str
        Name of the processing institution where the data was generated. Must be a value from ProcessingInstitutionEnum. OPTIONAL IF processing_institution IS PROVIDED
    processing_institution_workflow : str
        Name of the processing institution where the workflow was executed. Must be a value from ProcessingInstitutionEnum. OPTIONAL IF processing_institution IS PROVIDED
    execution_resource : str, optional
        Name of the execution resource. Must be a value from ExecutionResourceEnum.
    instrument_analysis_start_date : str, optional
        Start date of the instrument analysis.
    instrument_analysis_end_date : str, optional
        End date of the instrument analysis.
    raw_data_url : str, optional
        Complete URL for the raw data file. If provided, this takes precedence
        over constructing the URL from base_url + filename.
    manifest_id : str, optional
        Identifier for the manifest associated with this workflow metadata.
    instrument_instance_specifier : str, optional
        Specifier for the instrument instance used in the analysis.
    """

    processed_data_dir: str
    raw_data_file: str
    mass_spec_configuration_id: str
    lc_config_id: str
    instrument_id: str
    processing_institution: str = None
    processing_institution_generation: str = None
    processing_institution_workflow: str = None
    execution_resource: str = None
    instrument_analysis_start_date: str = None
    instrument_analysis_end_date: str = None
    raw_data_url: str = None
    manifest_id: str = None
    instrument_instance_specifier: str = None


@dataclass
class NOMMetadata:
    """
    Data class for holding NOM workflow metadata information.

    Attributes
    ----------
    raw_data_file : str
        Path or name of the raw data file.
    processed_data_directory : str
        Directory containing processed data files.
    associated_studies : list
        List of associated study identifiers.
    sample_id : str
        Identifier for the sample.
    instrument_id : str
        Identifier for the instrument used for analysis.
    mass_spec_configuration_id : str
        Identifier for the mass spectrometry configuration used.
    lc_config_id : str
        Identifier for the liquid chromatography configuration used.
    manifest_id : str
        Identifier for the manifest associated with this workflow metadata.
    processing_institution : str
        Name of the processing institution. Must be a value from ProcessingInstitutionEnum. OPTIONAL IF processing_institution_generation AND processing_institution_workflow ARE PROVIDED
    processing_institution_generation : str
        Name of the processing institution where the data was generated. Must be a value from ProcessingInstitutionEnum. OPTIONAL IF processing_institution IS PROVIDED
    processing_institution_workflow : str
        Name of the processing institution where the workflow was executed. Must be a value from ProcessingInstitutionEnum. OPTIONAL IF processing_institution IS PROVIDED
    execution_resource : str, optional
        Name of the execution resource. Must be a value from ExecutionResourceEnum.
    instrument_instance_specifier : str, optional
        Specifier for the instrument instance used in the analysis.
    reference_calibration_id : str
        Identifier for the reference mass list used for calibration.
    srfa_calibration_id : str
        Identifier for the SRFA standard raw data used for recalibration. Only used if the workflow was run with batch recalibration.
    """

    raw_data_file: str
    processed_data_directory: str
    associated_studies: list
    sample_id: str
    instrument_id: str
    mass_spec_configuration_id: str
    lc_config_id: str
    manifest_id: str
    processing_institution: str = None
    processing_institution_generation: str = None
    processing_institution_workflow: str = None
    execution_resource: str = None
    instrument_instance_specifier: str = None
    reference_calibration_id: str = None
    srfa_calibration_id: str = None


class ProcessGeneratorMap:
    """Thin shim around dynamic MaterialProcessing class lookups."""

    @staticmethod
    def get(process_type: str):
        """Return the runtime NMDC class for the given material processing name."""

        return get_material_processing_class(process_type)

    @staticmethod
    def available_types():
        """Return all material processing class names known to the schema."""

        return list_material_processing_types()


class NmdcTypes:
    """Resolve CURIEs and ID typecodes on demand."""

    def __init__(self):
        raise NotImplementedError(
            "NmdcTypes is a static class and cannot be instantiated."
        )

    _curie_cache: ClassVar[dict[str, str]] = {}
    _typecode_cache: ClassVar[dict[str, str]] = {}
    _ALIASES: ClassVar[dict[str, str]] = {"TimeStampValue": "TimestampValue"}

    @classmethod
    def get(cls, identifier: str) -> str:
        if identifier not in cls._curie_cache:
            cls._curie_cache[identifier] = cls._resolve_curie(identifier)
        return cls._curie_cache[identifier]

    @classmethod
    def typecode(cls, identifier: str) -> str:
        if identifier not in cls._typecode_cache:
            curie = identifier if ":" in identifier else cls.get(identifier)
            typecode = get_typecode_for_curie(curie)
            if not typecode:
                raise KeyError(
                    f"NMDC class '{identifier}' does not define an ID typecode"
                )
            cls._typecode_cache[identifier] = typecode
        return cls._typecode_cache[identifier].preferred_typecode

    @classmethod
    def _resolve_curie(cls, identifier: str) -> str:
        if ":" in identifier:
            return identifier
        canonical = cls._ALIASES.get(identifier, identifier)
        return get_curie_for_class(canonical)
