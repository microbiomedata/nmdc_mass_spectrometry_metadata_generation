import argparse
import os
import sys

# clarifying path variable for relative imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from metadata_gen_scripts.data_parser import YamlSpecifier
from metadata_gen_scripts.material_processing_gen import (
    MaterialProcessingMetadataGenerator,
)


def validate_yaml_outline(yaml_outline_path=str):
    """
    test to make sure yaml will generate valid json ()
    command line example : python metadata_gen_scripts/tests/validate_yaml_outline.py --yaml_outline_path='studies/emp500_sty-11-547rwq94/500_metabolites_bulk_feces.yaml'
    """

    generator = MaterialProcessingMetadataGenerator(
        yaml_outline_path=yaml_outline_path,
        config_path="metadata_gen_scripts/config.yaml",
        study_id="sdjklfdjsf",  # doesn't matter, wont be called on
        output_path="sdjfkldsjf",  # doesn't matter, wont be called on
        sample_to_dg_mapping_path="jdksldjfs",  # doesn't matter, won't be called on
        test=True,
    )
    nmdc_database = generator.start_nmdc_database()
    data_parser = YamlSpecifier(yaml_outline_path=yaml_outline_path)

    outline = data_parser.load_material_processing()
    test_biosample = "nmdc:bsm-11-64vz3p24"  # random biosample id
    input_dict = {"Biosample": {"id": test_biosample}}

    generator.json_generation(
        data=outline, placeholder_dict=input_dict, nmdc_database=nmdc_database
    )
    generator.dump_nmdc_database(nmdc_database=nmdc_database)
    response = generator.validate_json()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--yaml_outline_path", required=True)
    args = parser.parse_args()

    validate_yaml_outline(args.yaml_outline_path)


if __name__ == "__main__":
    main()
