# This script will serve as a test for the lipdomics metadata generation script.
from datetime import datetime
from metadata_generator import GCMSMetabolomicsMetadataGenerator
from dotenv import load_dotenv
load_dotenv()
import os
python_path = os.getenv('PYTHONPATH')
if python_path:
    os.environ['PYTHONPATH'] = python_path
if __name__ == "__main__":
    # Set up output file with datetime stame
    output_file = (
        "tests/test_data/test_database_gcms_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )

    # Start the metadata generation setup
    generator = GCMSMetabolomicsMetadataGenerator(
        metadata_file="tests/test_data/test_metadata_file_gcms.csv",
        database_dump_json_path=output_file,
        raw_data_url="https://example_raw_data_url/",
        process_data_url="https://example_processed_data_url/",
    )

    # Run the metadata generation process
    generator.run()
