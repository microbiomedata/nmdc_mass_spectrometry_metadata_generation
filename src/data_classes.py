# -*- coding: utf-8 -*-
from dataclasses import dataclass

"""
This module defines data classes for NMDC (National Microbiome Data Collaborative) type constants.
"""


@dataclass
class NmdcTypes:
    """
    Data class holding NMDC type constants.

    Attributes
    ----------
    Biosample : str
        NMDC type for Biosample.
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


@dataclass
class GCMSMetabWorkflowMetadata:
    """
    Data class for holding GCMS metabolomic workflow metadata information.

    Attributes
    ----------
    biosample_id: str
        Identifier for the biosample.s
    nmdc_study : str
        Identifier for the NMDC study.
    processing_institution : str
        Name of the institution processing the data.
    processed_data_file : str
        Path or name of the processed data file.
    raw_data_file : str
        Path or name of the raw data file.
    mass_spec_config_name : str
        Name of the mass spectrometry configuration used.
    chromat_config_name : str
        Name of the chromatography configuration used.
    instrument_used : str
        Name of the instrument used for analysis.
    instrument_analysis_start_date : str
        Start date of the instrument analysis.
    instrument_analysis_end_date : str
        End date of the instrument analysis.
    execution_resource : float
        Identifier for the execution resource.
    calibration_id : str
        Identifier for the calibration information used.

    """

    biosample_id: str
    nmdc_study: str
    processing_institution: str
    processed_data_file: str
    raw_data_file: str
    mass_spec_config_name: str
    chromat_config_name: str
    instrument_used: str
    instrument_analysis_start_date: str
    instrument_analysis_end_date: str
    execution_resource: float
    calibration_id: str


@dataclass
class LCMSLipidWorkflowMetadata:
    """
    Data class for holding LC-MS lipidomics workflow metadata information.

    Attributes
    ----------
    processed_data_dir : str
        Directory containing processed data files.
    raw_data_file : str
        Path or name of the raw data file.
    mass_spec_config_name : str
        Name of the mass spectrometry configuration used.
    lc_config_name : str
        Name of the liquid chromatography configuration used.
    instrument_used : str
        Name of the instrument used for analysis.
    instrument_analysis_start_date : str
        Start date of the instrument analysis.
    instrument_analysis_end_date : str
        End date of the instrument analysis.
    execution_resource : float
        Identifier for the execution resource.

    """

    processed_data_dir: str
    raw_data_file: str
    mass_spec_config_name: str
    lc_config_name: str
    instrument_used: str
    instrument_analysis_start_date: str
    instrument_analysis_end_date: str
    execution_resource: float
