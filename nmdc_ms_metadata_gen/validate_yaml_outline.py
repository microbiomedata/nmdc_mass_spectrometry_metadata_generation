import click

from nmdc_ms_metadata_gen.material_processing_generator import (
    MaterialProcessingMetadataGenerator,
)
from nmdc_ms_metadata_gen.metadata_parser import YamlSpecifier


@click.command()
@click.option("--yaml_outline_path", required=True)
@click.option("--protocol_id_list", required=True)
@click.option("--use_api", type=bool, default=False)
def validate_yaml_outline(yaml_outline_path: str, protocol_id_list: str, use_api=False):
    """
    Test to make sure yaml will generate valid json if given a random biosample (no adjustments for dg/filename)

    Parameters
    ----------
    yaml_outline_path: str
        Path to yaml outline to validate

    Examples
    --------
    Command line example
    `python -m nmdc_mass_spectrometry_metadata_generation.nmdc_ms_metadata_gen.validate_yaml_outline --yaml_outline_path 'path_to_yaml/example.yaml' --protocol_id_list 'example_protocol1,example_protocol2' --use_api False`
    """

    protocol_id_list = [p.strip() for p in protocol_id_list.split(",")]

    generator = MaterialProcessingMetadataGenerator(
        yaml_outline_path=yaml_outline_path,
        study_id="sdjklfdjsf",  # doesn't matter, wont be called on
        database_dump_json_path="Validated_Outline_Output",
        sample_to_dg_mapping_path="jdksldjfs",  # doesn't matter, won't be called on
        test=True,
        minting_config_creds="nmdc_mass_spectrometry_metadata_generation/config.toml",
    )
    client_id, client_secret = generator.load_credentials(
        config_file=generator.minting_config_creds
    )
    nmdc_database = generator.start_nmdc_database()
    data_parser = YamlSpecifier(yaml_outline_path=yaml_outline_path)

    for protocol_id in protocol_id_list:

        outline = data_parser.load_yaml(protocol_id)
        test_biosample = "nmdc:bsm-11-64vz3p24"  # random biosample id
        input_dict = {"Biosample": test_biosample}

        generator.json_generation(
            data=outline,
            placeholder_dict=input_dict,
            nmdc_database=nmdc_database,
            CLIENT_ID=client_id,
            CLIENT_SECRET=client_secret,
        )
        generator.dump_nmdc_database(
            nmdc_database=nmdc_database, json_path=generator.database_dump_json_path
        )
        validate = generator.validate_nmdc_database(
            generator.database_dump_json_path, use_api=use_api
        )

        print(validate["result"])


if __name__ == "__main__":
    validate_yaml_outline()
