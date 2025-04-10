# -*- coding: utf-8 -*-
from pytest_console_scripts import ScriptRunner
from datetime import datetime
import os


def test_cli():
    sc = ScriptRunner(launch_mode="subprocess", rootdir=".")
    current_directory = os.path.dirname(__file__)
    csv_file_path = os.path.join(
        current_directory, "test_data", "test_metadata_file_lipid.csv"
    )
    # Command to be tested
    output_file = (
        "tests/test_data/test_database_lipid_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )
    ret = sc.run(
        [
            "python",
            "../main.py",
            "--generator",
            "lcms_lipid",
            "--metadata_file",
            csv_file_path,
            "--database_dump_json_path",
            output_file,
            "--raw_data_url",
            "https://nmdcdemo.emsl.pnnl.gov/metabolomics/test_data/test_raw_lcms_lipid/",
            "--process_data_url",
            "https://nmdcdemo.emsl.pnnl.gov/metabolomics/test_data/test_processed_lcms_lipid/",
        ]
    )
    # Verify it exits with a status code of zero
    print("ret.success", ret.success)
    assert ret.success
