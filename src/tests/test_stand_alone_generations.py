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
