# -*- coding: utf-8 -*-
# This script will serve as a test for the generation of `MassSpectrometryConfiguration` and `ChromatographyConfiguration` records
from datetime import datetime
from src.metadata_generator import NMDCMetadataGenerator
from dotenv import load_dotenv

load_dotenv()
import os

python_path = os.getenv("PYTHONPATH")
if python_path:
    os.environ["PYTHONPATH"] = python_path


def test_mass_spec_configuration_generation():
    """
    Test the generation and validation of a Mass Spectrometry Configuration record.
    """
    # Set up an output file with datetime stamp
    output_file = (
        "tests/test_data/test_mass_spec_configuration_"
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
    generator.validate_nmdc_database(json_path=output_file)


def test_chromatography_configuration_generation():
    """
    Test the generation and validation of a Chromatography Configuration record.
    """
    # Set up an output file with datetime stamp
    output_file = (
        "tests/test_data/test_chromatography_configuration_"
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
        duration_unit="minute",
        substances_used=[water_99_9, formic_acid__1],
    )
    mobile_phase_segment_B = generator.generate_mobile_phase_segment(
        duration_value=11,
        duration_unit="minute",
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
    generator.validate_nmdc_database(json_path=output_file)


def test_instrument_generation():
    """
    Test the generation and validation of an Instrument record.
    """
    # Set up an output file with datetime stamp
    output_file = (
        "tests/test_data/test_instrument_"
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
    generator.validate_nmdc_database(json_path=output_file)
