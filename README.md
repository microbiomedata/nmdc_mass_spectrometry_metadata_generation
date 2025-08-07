# Mass spectrometry metadata generation
A library designed to automate the ingestion of raw data and metadata schema generation.

# Documentation
Documentation about available functions and helpful usage notes can be found at https://microbiomedata.github.io/nmdc_mass_spectrometry_metadata_generation/.

# Installation
To install run the following pip command
```bash
pip install git+https://github.com/microbiomedata/nmdc_mass_spectrometry_metadata_generation.git@1.4.0`
```

This will install the package through the git repository. Each module can be accessed through its name. For example:
```python
import nmdc_ms_metadata_gen.di_nom_metadata_generator
```

You can also add `nmdc_ms_metadata_gen @ git+https://github.com/microbiomedata/nmdc_mass_spectrometry_metadata_generation.git@1.4.0` to a requirements.txt file.

To install and test a specific branch use `pip install git+https://github.com/microbiomedata/nmdc_mass_spectrometry_metadata_generation.git@BRANCH_NAME`

# CLI Usage
To utilize the CLI, first follow the [Installation](#installation) guide to install the library.

Confirm the installation was successful and see the list of available commands
```bash
nmdc-ms-metadata-gen --help
```

To see more information on each command
```bash
nmdc-ms-metadata-gen command-name --help
# example to see more info for di-nom
nmdc-ms-metadata-gen di-nom --help
```

Run a command to generate metadata for a DI NOM data type

```bash
nmdc-ms-metadata-gen di-nom --database_dump_path tests/test_data/test_database_nom_main \

--process_data_url https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_processed_nom/ \

--raw_data_url https://nmdcdemo.emsl.pnnl.gov/nom/test_data/test_raw_nom/ \

--metadata_file nmdc_ms_metadata_gen/tests/test_data/test_metadata_file_nom.csv
```

Details on the flags can be found in the [documentation](https://microbiomedata.github.io/nmdc_mass_spectrometry_metadata_generation/functions.html#main-cli). Credentials to reach the NMDC API for necessary functions can either be passed in a .toml file OR read in from .env variables. Examples on format for these can be found under [nmdc_ms_metadata_gen/example_data](https://github.com/microbiomedata/nmdc_mass_spectrometry_metadata_generation/tree/main/nmdc_ms_metadata_gen/example_data).

# Development Environment
To run scripts in the dev NMDC API environment, set NMDC_ENV='dev' in .env file. Default will run in production.
