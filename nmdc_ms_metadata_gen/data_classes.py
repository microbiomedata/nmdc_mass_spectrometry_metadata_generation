from dataclasses import dataclass

import nmdc_schema.nmdc as nmdc

"""
This module defines data classes for NMDC (National Microbiome Data Collaborative) type constants.
"""


@dataclass
class NmdcTypes:
    """
    Data class holding NMDC type constants.
    Link to documentation https://microbiomedata.github.io/nmdc-schema/typecode-to-class-map/

    Attributes
    ----------
    Biosample : str
        NMDC type for sample.
    MassSpectrometry : str
        NMDC type for Mass Spectrometry.
    MetabolomicsAnalysis : str
        NMDC type for Metabolomics Analysis.
    DataObject : str
        NMDC type for Data Object.
    CalibrationInformation : str
        NMDC type for Calibration Information.
    MetaboliteIdentification : str
        NMDC type for Metabolite Identification.
    NomAnalysis : str
        NMDC type for NOM Analysis.
    OntologyClass : str
        NMDC type for Ontology Class.
    ControlledIdentifiedTermValue : str
        NMDC type for Controlled Identified Term Value.
    TextValue : str
        NMDC type for Text Value.
    GeolocationValue : str
        NMDC type for Geolocation Value.
    TimeStampValue : str
        NMDC type for Timestamp Value.
    QuantityValue : str
        NMDC type for Quantity Value.
    MassSpectrometryConfiguration : str
        NMDC type for Mass Spectrometry Configuration.
    PortionOfSubstance : str
        NMDC type for Portion of Substance.
    MobilePhaseSegment : str
        NMDC type for Mobile Phase Segment.
    ChromatographyConfiguration : str
        NMDC type for Chromatography Configuration.
    Instrument : str
        NMDC type for Instrument.
    Manifest : str
        NMDC type for Manifest.
    Protocol : str
        NMDC type for Protocol.
    ChemicalConversionProcess : str
        NMDC type for Chemical Conversion Process.
    ChromatographicSeparationProcess : str
        NMDC type for Chromatographic Separation Process.
    Pooling : str
        NMDC type for Pooling.
    SubSamplingProcess : str
        NMDC type for Sub Sampling Process.
    Extraction : str
        NMDC type for Extraction.
    ProcessedSample : str
        NMDC type for Processed Sample.
    DissolvingProcess : str
        NMDC type for Dissolving Process.
    FiltrationProcess : str
        NMDC type for Filtration Process.
    ProvenanceMetadata : str
        NMDC type for Provenance Metadata.
    """

    Biosample: str = "nmdc:Biosample"
    MassSpectrometry: str = "nmdc:MassSpectrometry"
    MetabolomicsAnalysis: str = "nmdc:MetabolomicsAnalysis"
    DataObject: str = "nmdc:DataObject"
    CalibrationInformation: str = "nmdc:CalibrationInformation"
    MetaboliteIdentification: str = "nmdc:MetaboliteIdentification"
    NomAnalysis: str = "nmdc:NomAnalysis"
    OntologyClass: str = "nmdc:OntologyClass"
    ControlledIdentifiedTermValue: str = "nmdc:ControlledIdentifiedTermValue"
    TextValue: str = "nmdc:TextValue"
    GeolocationValue: str = "nmdc:GeolocationValue"
    TimeStampValue: str = "nmdc:TimestampValue"
    QuantityValue: str = "nmdc:QuantityValue"
    MassSpectrometryConfiguration: str = "nmdc:MassSpectrometryConfiguration"
    PortionOfSubstance: str = "nmdc:PortionOfSubstance"
    MobilePhaseSegment: str = "nmdc:MobilePhaseSegment"
    ChromatographyConfiguration: str = "nmdc:ChromatographyConfiguration"
    Instrument: str = "nmdc:Instrument"
    Protocol: str = "nmdc:Protocol"
    Manifest: str = "nmdc:Manifest"
    ChemicalConversionProcess: str = "nmdc:ChemicalConversionProcess"
    ChromatographicSeparationProcess: str = "nmdc:ChromatographicSeparationProcess"
    MixingProcess: str = "nmdc:MixingProcess"
    Pooling: str = "nmdc:Pooling"
    SubSamplingProcess: str = "nmdc:SubSamplingProcess"
    Extraction: str = "nmdc:Extraction"
    ProcessedSample: str = "nmdc:ProcessedSample"
    DissolvingProcess: str = "nmdc:DissolvingProcess"
    FiltrationProcess: str = "nmdc:FiltrationProcess"
    ProvenanceMetadata: str = "nmdc:ProvenanceMetadata"
    Pooling: str = "nmdc:Pooling"
    LibraryPreparation: str = "nmdc:LibraryPreparation"
    MixingProcess: str = "nmdc:MixingProcess"


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
    calibration_id : str
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
    calibration_id: str
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


@dataclass
class ProcessGeneratorMap:
    """
    Maps process names from YAML file to their corresponding generator methods.

    This mapping is used to dynamically call the appropriate generator method
    based on the process type found in the YAML file.
    """

    SubSamplingProcess = nmdc.SubSamplingProcess
    Extraction = nmdc.Extraction
    ChemicalConversionProcess = nmdc.ChemicalConversionProcess
    ChromatographicSeparationProcess = nmdc.ChromatographicSeparationProcess
    DissolvingProcess = nmdc.DissolvingProcess
    FiltrationProcess = nmdc.FiltrationProcess
    Pooling = nmdc.Pooling
    LibraryPreparation = nmdc.LibraryPreparation
    MixingProcess = nmdc.MixingProcess
