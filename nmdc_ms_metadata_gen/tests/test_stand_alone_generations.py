# This script will serve as a test for the generation of `MassSpectrometryConfiguration` and `ChromatographyConfiguration` records
import os
from datetime import datetime

from dotenv import load_dotenv

from nmdc_ms_metadata_gen.metadata_generator import NMDCMetadataGenerator

load_dotenv()

python_path = os.getenv("PYTHONPATH")
if python_path:
    os.environ["PYTHONPATH"] = python_path

import json


def test_mass_spec_configuration_generation():
    """
    Test the generation and validation of a Mass Spectrometry Configuration record.
    """
    # Set up an output file with datetime stamp
    output_file = (
        "tests/test_data/test_database_mass_spec_configuration_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )
    generator = NMDCMetadataGenerator()

    # Load credentials from the config file
    client_id, client_secret = generator.load_credentials()

    # Make a database object
    db = generator.start_nmdc_database()

    # Generate a mass spectrometry configuration
    example_ms_config = generator.generate_mass_spectrometry_configuration(
        name="Test Mass Spectrometry Configuration",
        description="Test configuration for mass spectrometry data generation.",
        mass_spectrometry_acquisition_strategy="data_dependent_acquisition",
        resolution_categories=["high"],
        mass_analyzers=["Orbitrap"],
        ionization_source="electrospray_ionization",
        mass_spectrum_collection_modes=["centroid"],
        polarity_mode="positive",
        CLIENT_ID=client_id,
        CLIENT_SECRET=client_secret,
    )
    assert example_ms_config is not None

    # Add to the database
    db.configuration_set.append(example_ms_config)

    # Save the database and run json validation
    generator.dump_nmdc_database(nmdc_database=db, json_path=output_file)
    file = open(output_file)
    working_data = json.load(file)
    file.close()
    validate = generator.validate_nmdc_database(json=working_data, use_api=False)
    assert validate["result"] == "All Okay!"


def test_chromatography_configuration_generation():
    """
    Test the generation and validation of a Chromatography Configuration record.
    """
    # Set up an output file with datetime stamp
    output_file = (
        "tests/test_data/test_database_chromatography_configuration_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )
    generator = NMDCMetadataGenerator()

    # Load credentials from the config file
    client_id, client_secret = generator.load_credentials()

    # Make a database object
    db = generator.start_nmdc_database()

    # Make a configuration record for Chromatography configuration
    ## First, create all the portions of substances
    water_99_9 = generator.generate_portion_of_substance(
        substance_name="water",
        final_concentration_value=99.9,
        concentration_unit="%",
    )
    acetonitrile_99_9 = generator.generate_portion_of_substance(
        substance_name="acetonitrile",
        final_concentration_value=99.9,
        concentration_unit="%",
    )
    formic_acid__1 = generator.generate_portion_of_substance(
        substance_name="formic_acid",
        final_concentration_value=0.1,
        concentration_unit="%",
    )
    ## Then, create the mobile phase segments
    mobile_phase_segment_A = generator.generate_mobile_phase_segment(
        duration_value=11,
        duration_unit="min",
        substances_used=[water_99_9, formic_acid__1],
    )
    mobile_phase_segment_B = generator.generate_mobile_phase_segment(
        duration_value=11,
        duration_unit="min",
        substances_used=[acetonitrile_99_9, formic_acid__1],
    )

    # create protocol
    protocol = generator.generate_protocol(
        name="Test Chromatography Protocol", url="https://example.com/protocol"
    )

    ## Finally, create the chromatography configuration
    emp500_chromat_config = generator.generate_chromatography_configuration(
        name="Test Chromatography Configuration",
        description="Test configuration for chromatography data generation.",
        chromatographic_category="liquid_chromatography",
        ordered_mobile_phases=[mobile_phase_segment_A, mobile_phase_segment_B],
        stationary_phase="C18",
        temperature_value=40,
        temperature_unit="Cel",
        protocol_link=protocol,
        CLIENT_ID=client_id,
        CLIENT_SECRET=client_secret,
    )
    assert emp500_chromat_config is not None

    # Add to the database
    db.configuration_set.append(emp500_chromat_config)

    # Save the database and run json validation
    generator.dump_nmdc_database(nmdc_database=db, json_path=output_file)
    file = open(output_file)
    working_data = json.load(file)
    file.close()
    validate = generator.validate_nmdc_database(json=working_data, use_api=False)
    assert validate["result"] == "All Okay!"


def test_instrument_generation():
    """
    Test the generation and validation of an Instrument record.
    """
    # Set up an output file with datetime stamp
    output_file = (
        "tests/test_data/test_database_instrument_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )
    generator = NMDCMetadataGenerator()

    # Load credentials from the config file
    client_id, client_secret = generator.load_credentials()

    # Make a database object
    db = generator.start_nmdc_database()

    # Generate an instrument
    example_instrument = generator.generate_instrument(
        name="Test Mass Spectrometer",
        description="Test instrument for mass spectrometry data generation.",
        vendor="thermo_fisher",
        model="orbitrap_q_exactive",
        CLIENT_ID=client_id,
        CLIENT_SECRET=client_secret,
    )
    assert example_instrument is not None

    # Add to the database
    db.instrument_set.append(example_instrument)

    # Save the database and run json validation
    generator.dump_nmdc_database(nmdc_database=db, json_path=output_file)
    file = open(output_file)
    working_data = json.load(file)
    file.close()
    validate = generator.validate_nmdc_database(json=working_data, use_api=False)
    assert validate["result"] == "All Okay!"


def test_json_validate_no_api_pass():
    """
    Test the JSON validation function that does not use the API.
    This function validates directly against the JSON schema.
    """
    in_docs = {
        "data_object_set": [
            {
                "id": "nmdc:dobj-12-5d8wce28",
                "type": "nmdc:DataObject",
                "name": "nom_sample_1.png",
                "description": "EnviroMS QC plots representing a Direct Infusion NOM analysis.",
                "data_category": "processed_data",
                "data_object_type": "Direct Infusion FT-ICR MS QC Plots",
                "file_size_bytes": 320594,
                "md5_checksum": "b23430d45421845c62664742a8512fbc",
                "url": "https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_processed_nom/nom_sample_1/nom_sample_1.png",
                "was_generated_by": "nmdc:wfnom-11-65b8j621.2",
            },
            {
                "id": "nmdc:dobj-12-gw3hjz30",
                "type": "nmdc:DataObject",
                "name": "nom_sample_1.csv",
                "description": "EnviroMS natural organic matter workflow molecular formula assignment output details",
                "data_category": "processed_data",
                "data_object_type": "Direct Infusion FT-ICR MS Analysis Results",
                "file_size_bytes": 914178,
                "md5_checksum": "a93ab26c9b8b452f3b6d54c2fb9d1ef7",
                "url": "https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_processed_nom/nom_sample_1/nom_sample_1.csv",
                "was_generated_by": "nmdc:wfnom-11-65b8j621.2",
            },
            {
                "id": "nmdc:dobj-12-r5gam483",
                "type": "nmdc:DataObject",
                "name": "nom_sample_1.json",
                "description": "EnviroMS processing parameters for natural organic matter analysis.",
                "data_category": "workflow_parameter_data",
                "data_object_type": "Analysis Tool Parameter File",
                "file_size_bytes": 5788,
                "md5_checksum": "e38a1a2f4b54dd082e544b7826a57dbb",
                "url": "https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_processed_nom/nom_sample_1/nom_sample_1.json",
                "was_generated_by": "nmdc:wfnom-11-65b8j621.2",
            },
        ],
        "workflow_execution_set": [
            {
                "id": "nmdc:wfnom-11-65b8j621.2",
                "type": "nmdc:NomAnalysis",
                "name": "NOM Analysis for Blanchard_MeOHExt_H-17-AB-M_20Feb18_000001.zip",
                "description": "Processing of raw DI FT-ICR MS data for natural organic matter identification",
                "has_input": ["nmdc:dobj-12-r5gam483", "nmdc:dobj-11-dmqrkq30"],
                "has_output": ["nmdc:dobj-12-5d8wce28", "nmdc:dobj-12-gw3hjz30"],
                "processing_institution": "NMDC",
                "git_url": "https://github.com/microbiomedata/enviroMS/blob/master/wdl/di_fticr_ms.wdl",
                "started_at_time": "2025-08-05 12:14:07",
                "was_informed_by": ["nmdc:dgms-11-n80fvn39"],
                "ended_at_time": "2025-08-05 12:14:07",
                "execution_resource": "EMSL-RZR",
                "version": "5.0.0",
                "uses_calibration": "nmdc:calib-14-hhn3qb47",
            }
        ],
        "@type": "Database",
    }

    gen = NMDCMetadataGenerator()
    results = gen.validate_nmdc_database(json=in_docs, use_api=False)

    assert results["result"] == "All Okay!"


def test_json_validate_no_api_fail():
    """
    Test the JSON validation function that does not use the API.
    This function validates directly against the JSON schema.
    """
    in_docs = {
        "data_object_set": [
            {
                "type": "nmdc:DataObject",
                "name": "nom_sample_1.png",
                "description": "EnviroMS QC plots representing a Direct Infusion NOM analysis.",
                "data_category": "processed_data",
                "data_object_type": "Direct Infusion FT-ICR MS QC Plots",
                "file_size_bytes": 320594,
                "md5_checksum": "b23430d45421845c62664742a8512fbc",
                "url": "https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_processed_nom/nom_sample_1/nom_sample_1.png",
                "was_generated_by": "nmdc:wfnom-11-65b8j621.2",
            }
        ],
        "workflow_execution_set": [
            {
                "id": "nmdc:wfnom-11-65b8j621.2",
                "type": "nmdc:NomAnalysis",
                "name": "NOM Analysis for Blanchard_MeOHExt_H-17-AB-M_20Feb18_000001.zip",
                "description": "Processing of raw DI FT-ICR MS data for natural organic matter identification",
                "has_input": ["nmdc:dobj-12-r5gam483", "nmdc:dobj-11-dmqrkq30"],
                "has_output": ["nmdc:dobj-12-5d8wce28", "nmdc:dobj-12-gw3hjz30"],
                "processing_institution": "NMDC",
                "git_url": "https://github.com/microbiomedata/enviroMS/blob/master/wdl/di_fticr_ms.wdl",
                "started_at_time": "2025-08-05 12:14:07",
                "was_informed_by": ["nmdc:dgms-11-n80fvn39"],
                "ended_at_time": "2025-08-05 12:14:07",
                "execution_resource": "EMSL-RZR",
                "version": "5.0.0",
                "uses_calibration": "nmdc:calib-14-hhn3qb47",
            }
        ],
        "@type": "Database",
    }

    gen = NMDCMetadataGenerator()
    results = gen.validate_nmdc_database(json=in_docs, use_api=False)
    assert results["result"] == "errors"
    assert "'id' is a required property" in results["detail"]["data_object_set"]
