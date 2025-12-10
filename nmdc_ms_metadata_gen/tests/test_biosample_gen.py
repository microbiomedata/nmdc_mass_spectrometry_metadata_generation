import json
import os
from datetime import datetime

from dotenv import load_dotenv

from nmdc_ms_metadata_gen.biosample_generator import BiosampleGenerator

load_dotenv()

python_path = os.getenv("PYTHONPATH")
if python_path:
    os.environ["PYTHONPATH"] = python_path


def test_di_nom_biosample_gen_more_fields():
    """
    Test the biosample generation process.
    Test case includes generating a biosample with more than the required fields
    """
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_biosample_generation_weird_data_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = BiosampleGenerator(
        metadata_file="tests/test_data/test_metadata_file_biosample_generation_weird_data.csv",
        database_dump_json_path=output_file,
    )

    # Run the metadata generation process
    metadata = generator.run()
    validate = generator.validate_nmdc_database(json=metadata, use_api=False)
    assert validate["result"] == "All Okay!"
    assert os.path.exists(output_file)

    file = open(output_file)
    working_data = json.load(file)
    file.close()
    assert len(working_data["biosample_set"]) == 2


def test_biosample_gen():
    """
    Test the biosample generation process with only the required fields.
    """
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_biosample_generation_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = BiosampleGenerator(
        metadata_file="tests/test_data/test_metadata_file_biosample_generation.csv",
        database_dump_json_path=output_file,
    )

    # Run the metadata generation process
    metadata = generator.run()
    validate = generator.validate_nmdc_database(json=metadata, use_api=False)
    assert validate["result"] == "All Okay!"
    assert os.path.exists(output_file)

    file = open(output_file)
    working_data = json.load(file)
    file.close()
    assert len(working_data["biosample_set"]) == 2
