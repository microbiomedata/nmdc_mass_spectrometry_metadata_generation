import argparse
import os
import sys

# clarifying path variable for relative imports
#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from nmdc_ms_metadata_gen.material_processing_generator import (
    MaterialProcessingMetadataGenerator,
)
from nmdc_ms_metadata_gen.metadata_parser import YamlSpecifier


def validate_yaml_outline(yaml_outline_path=str):
    """
    test to make sure yaml will generate valid json () if given a random biosample (no adjustments for dg/filename)
    command line example, run in nmdc_mass_spectrometry_metadata_generation : 
    python -m nmdc_ms_metadata_gen.validate_yaml_outline --yaml_outline_path='{path_to_yaml}/GROW_sty-11-5tgfr349/GROW_NOM_sediment.yaml'
    """

    generator = MaterialProcessingMetadataGenerator(
        yaml_outline_path=yaml_outline_path,
        study_id="sdjklfdjsf",  # doesn't matter, wont be called on
        database_dump_json_path="Latest_Tested_Outline_Output",
        sample_to_dg_mapping_path="jdksldjfs",  # doesn't matter, won't be called on
        test=True,
        minting_config_creds='config.toml',
    )
    client_id, client_secret = generator.load_credentials(
            config_file=generator.minting_config_creds
        )
    nmdc_database = generator.start_nmdc_database()
    data_parser = YamlSpecifier(yaml_outline_path=yaml_outline_path)

    outline = data_parser.load_yaml()
    test_biosample = "nmdc:bsm-11-64vz3p24"  # random biosample id
    input_dict = {"Biosample": {"id": test_biosample}}

    generator.json_generation(
        data=outline, placeholder_dict=input_dict, nmdc_database=nmdc_database, CLIENT_ID=client_id, CLIENT_SECRET=client_secret
    )
    generator.dump_nmdc_database(nmdc_database=nmdc_database,json_path=generator.database_dump_json_path)
    response = generator.validate_nmdc_database(generator.database_dump_json_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--yaml_outline_path", required=True)
    args = parser.parse_args()

    validate_yaml_outline(args.yaml_outline_path)


if __name__ == "__main__":
    main()
