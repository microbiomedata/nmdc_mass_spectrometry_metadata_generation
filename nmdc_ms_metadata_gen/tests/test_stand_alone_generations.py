# This script will serve as a test for the generation of `MassSpectrometryConfiguration` and `ChromatographyConfiguration` records
import os
from datetime import datetime

from dotenv import load_dotenv
from nmdc_api_utilities.biosample_search import BiosampleSearch

from nmdc_ms_metadata_gen.metadata_generator import NMDCMetadataGenerator

load_dotenv()
ENV = os.getenv("NMDC_ENV", "prod")
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
    generator = NMDCMetadataGenerator(test=True)

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
    generator = NMDCMetadataGenerator(test=True)

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
    generator = NMDCMetadataGenerator(test=True)

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
                "uses_calibration": ["nmdc:calib-14-hhn3qb47"],
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
    assert "'id' is a required property" in results["detail"]["data_object_set"][0]


def test_json_validate_units_fail():
    """
    Test the JSON validation function that does not use the API against the unit validator.
    This function validates directly against the JSON schema.
    """

    in_docs = {
        "configuration_set": [
            {
                "id": "nmdc:chrcon-11-trmb8z05",
                "type": "nmdc:ChromatographyConfiguration",
                "name": "JGI/LBNL Metabolomics Standard LC Method - Polar HILIC-Z",
                "description": "HILIC Chromatography configuration for standard JGI/LBNL Metabolomics analysis for polar compounds. This configuration uses a HILIC column (InfinityLab Poroshell 120 HILIC-Z, 2.1x150 mm, 2.7 um, Agilent, #683775-924) held at 40 degC, with mobile phase solvents running at a flow rate of 0.45 mL/min. For each sample, 2 uL were injected onto the column.",
                "protocol_link": {
                    "type": "nmdc:Protocol",
                    "url": "https://www.protocols.io/view/jgi-lbnl-metabolomics-standard-lc-ms-ms-esi-method-kxygxydwkl8j/v1",
                    "name": "JGI/LBNL Metabolomics - Standard LC-MS/MS ESI Method - Polar HILIC-Z",
                },
                "chromatographic_category": "liquid_chromatography",
                "stationary_phase": "HILIC",
                "ordered_mobile_phases": [
                    {
                        "type": "nmdc:MobilePhaseSegment",
                        "duration": {
                            "type": "nmdc:QuantityValue",
                            "has_numeric_value": 11,
                            "has_unit": "minute",
                        },
                    }
                ],
            }
        ],
        "@type": "Database",
    }

    gen = NMDCMetadataGenerator()
    results = gen.validate_nmdc_database(json=in_docs, use_api=False)
    assert results["result"] == "errors"
    assert "'minute' is not one of [" in results["detail"]["configuration_set"][0]


def test_get_associated_ids():
    """
    Test getting multiple associated ids.
    """

    bs = BiosampleSearch(env=ENV)
    ids = bs.get_records(max_page_size=500, fields="id")
    id_list = [x["id"] for x in ids]

    gen = NMDCMetadataGenerator()
    resp = gen.find_associated_ids(ids=id_list)

    for id in id_list:
        assert id in resp.keys()


def test_validate_yaml_outline():
    """
    Test validating a yaml outline.
    """
    from nmdc_ms_metadata_gen.validate_yaml_outline import validate_yaml_outline

    yaml_outline_path = (
        "tests/test_data/test_material_processing/test_yaml_for_output_adjust_test.yaml"
    )
    # protocol_id_list = ["NOM"]

    results = validate_yaml_outline(
        yaml_outline_path=yaml_outline_path,
        test=True,
    )

    for result in results:
        assert result["result"].lower() == "all okay!"


def test_nmdc_types_static():
    """
    Test that NmdcTypes cannot be instantiated.
    """
    from nmdc_ms_metadata_gen.data_classes import NmdcTypes

    try:
        nmdc_types_instance = NmdcTypes()
    except NotImplementedError as e:
        assert str(e) == "NmdcTypes is a static class and cannot be instantiated."
    else:
        assert False, "NmdcTypes instantiation did not raise NotImplementedError"


def test_clean_dict_removes_nan():
    """
    Test that clean_dict removes keys with NaN values (e.g. from empty CSV cells).
    This prevents 'instrument_instance_specifier': 'nan' appearing in generated JSON.
    """
    import numpy as np

    gen = NMDCMetadataGenerator()

    input_dict = {
        "valid_key": "valid_value",
        "nan_key": float("nan"),
        "np_nan_key": np.nan,
        "none_key": None,
        "empty_key": "",
    }
    result = gen.clean_dict(input_dict)

    assert "valid_key" in result
    assert result["valid_key"] == "valid_value"
    assert "nan_key" not in result
    assert "np_nan_key" not in result
    assert "none_key" not in result
    assert "empty_key" not in result
