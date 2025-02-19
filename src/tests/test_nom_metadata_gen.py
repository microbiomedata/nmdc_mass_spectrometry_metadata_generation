# -*- coding: utf-8 -*-
# This script will serve as a test for the lipdomics metadata generation script.
from datetime import datetime
from src.nom_metadata_generation import NOMMetadataGenerator
from dotenv import load_dotenv

load_dotenv()
import os

python_path = os.getenv("PYTHONPATH")
if python_path:
    os.environ["PYTHONPATH"] = python_path


def test_nom_metadata_gen():
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_nom_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = NOMMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_nom.csv",
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/",
        process_data_url="https://example_processed_data_url/",
    )

    # Run the metadata generation process
    generator.run()
    assert os.path.exists(output_file)
