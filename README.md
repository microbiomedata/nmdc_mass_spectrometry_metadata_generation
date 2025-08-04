# Mass spectrometry metadata generation
A library designed to automate the ingestion of raw data and metadata schema generation.

# Documentation
Documentation about available functions and helpful usage notes can be found at https://microbiomedata.github.io/nmdc_mass_spectrometry_metadata_generation/.

# Installation
To install run the following pip command `pip install git+https://github.com/microbiomedata/nmdc_mass_spectrometry_metadata_generation.git@1.4.0`
This will install the package through the git repository. Each module can be accessed through its name. For example:
`import nmdc_ms_metadata_gen `

You can also add `nmdc_ms_metadata_gen @ git+https://github.com/microbiomedata/nmdc_mass_spectrometry_metadata_generation.git@1.4.0` to a requirements.txt file.

To install and test a specific branch use `pip install git+https://github.com/microbiomedata/nmdc_mass_spectrometry_metadata_generation.git@BRANCH_NAME`

# CLI Usage
To utilize the CLI, first download the source code. Then run the script with the required flags. More details on the flags can be found in the [documentation](https://microbiomedata.github.io/nmdc_mass_spectrometry_metadata_generation/functions.html#main-cli). Credentials to reach the NMDC API for necessary functions can either be passed in a .toml file OR read in from .env variables. Examples on format for these can be found under src/example_data.


```bash
python3 /path/to/main.py --generator lcms_lipid --metadata_file /path/to/csv --database_dump_json_path /path/to/dump --raw_data_url https://example.com/raw/ --process_data_url https://example.com/results/
```

To use the CLI with toml credentials:
```bash
python3 /path/to/main.py --generator lcms_lipid --metadata_file /path/to/csv --database_dump_json_path /path/to/dump --raw_data_url https://example.com/raw/ --process_data_url https://example.com/results/ --minting_config_creds path/to/config_creds.toml
```

# Development Environment
To run scripts in the dev NMDC API environment, set NMDC_ENV='dev' in .env file. Default will run in production.
