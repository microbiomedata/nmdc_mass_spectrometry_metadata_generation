from pathlib import Path
from typing import List

import nmdc_schema.nmdc as nmdc
import pandas as pd

from nmdc_ms_metadata_gen.data_classes import NmdcTypes
from nmdc_ms_metadata_gen.lcms_metadata_generator import LCMSMetadataGenerator


class LCMSMetabolomicsMetadataGenerator(LCMSMetadataGenerator):
    """
    A class for generating NMDC metadata objects using provided metadata files and configuration
    for LC-MS metabolomics data.

    This class processes input metadata files, generates various NMDC objects, and produces
    a database dump in JSON format.
    Parameters
    ----------
    metadata_file : str
        Path to the input CSV metadata file.
    database_dump_json_path : str
        Path where the output database dump JSON file will be saved.
    process_data_url : str
        Base URL for the processed data files.
    raw_data_url : str, optional
        Base URL for the raw data files. If the raw data url is not directly passed in, it will use the raw data urls from the metadata file.
    minting_config_creds : str, optional
        Path to the configuration file containing the client ID and client secret for minting NMDC IDs. It can also include the bio ontology API key if generating biosample ids is needed.
        If not provided, the CLIENT_ID, CLIENT_SECRET, and BIO_API_KEY environment variables will be used.
    test : bool, optional
        Flag indicating whether to run in test mode. If True, will skip biosample ID checks in the database, data object URL check, and will use local IDs (skip API minting). Default is False.

    Attributes
    ----------
    unique_columns : list[str]
        List of unique columns in the metadata file.
    mass_spec_desc : str
        Description of the mass spectrometry analysis.
    mass_spec_eluent_intro : str
        Eluent introduction category for mass spectrometry.
    analyte_category : str
        Category of the analyte.
    raw_data_obj_type : str
        Type of the raw data object.
    raw_data_obj_desc : str
        Description of the raw data object.
    workflow_analysis_name : str
        Name of the workflow analysis.
    workflow_description : str
        Description of the workflow.
    workflow_git_url : str
        URL of the workflow's Git repository.
    workflow_version : str
        Version of the workflow.
    workflow_category : str
        Category of the workflow.
    wf_config_process_data_category : str
        Category of the workflow configuration process data.
    wf_config_process_data_obj_type : str
        Type of the workflow configuration process data object.
    wf_config_process_data_description : str
        Description of the workflow configuration process data.
    no_config_process_data_category : str
        Category for processed data without configuration.
    no_config_process_data_obj_type : str
        Type of processed data object without configuration.
    csv_process_data_description : str
        Description of CSV processed data.
    hdf5_process_data_obj_type : str
        Type of HDF5 processed data object.
    hdf5_process_data_description : str
        Description of HDF5 processed data.
    add_metabolite_ids : bool
        Whether to add metabolite IDs to the metadata.
    add_wf_stats: bool
        Whether to add workflow statistics to the metadata.
    """

    unique_columns: list[str] = ["processed_data_directory"]
    # Data Generation attributes
    mass_spec_desc: str = (
        "Generation of mass spectrometry data for the analysis of metabolomics using liquid chromatography."
    )
    mass_spec_eluent_intro: str = "liquid_chromatography"
    analyte_category: str = "metabolome"
    raw_data_obj_type: str = "LC-DDA-MS/MS Raw Data"
    raw_data_obj_desc: str = "LC-DDA-MS/MS raw data for metabolomics data acquisition."

    # Workflow attributes
    workflow_analysis_name: str = "Metabolomics analysis"
    workflow_description: str = (
        "Analysis of raw mass spectrometry data for the annotation of metabolites."
    )
    workflow_git_url: str = (
        "https://github.com/microbiomedata/metaMS/blob/master/wdl/metaMS_lcms_metabolomics.wdl"
    )
    workflow_version: str
    workflow_category: str = "lc_ms_metabolomics"
    add_metabolite_ids: bool = True
    add_wf_stats: bool = True

    # QC thresholds
    peak_count_threshold: int = 0
    peak_assignment_count_threshold: int = 0
    c13_isotopologue_count_threshold: int = 0

    # Processed data attributes
    wf_config_process_data_category: str = "workflow_parameter_data"
    wf_config_process_data_obj_type: str = "Configuration toml"
    wf_config_process_data_description: str = (
        "CoreMS parameters used for metabolomics workflow."
    )
    no_config_process_data_category: str = "processed_data"
    no_config_process_data_obj_type: str = "LC-MS Metabolomics Results"
    csv_process_data_description: str = (
        "Metabolite annotations as a result of a metabolomics workflow activity."
    )

    hdf5_process_data_obj_type: str = "LC-MS Metabolomics Processed Data"
    hdf5_process_data_description: str = (
        "CoreMS hdf5 file representing a metabolomics data file including annotations."
    )

    def __init__(
        self,
        metadata_file: str,
        database_dump_json_path: str,
        process_data_url: str,
        raw_data_url: str = None,
        minting_config_creds: str = None,
        workflow_version: str = None,
        existing_data_objects: list[str] = [],
        test: bool = False,
        skip_sample_id_check: bool = False,
    ):
        super().__init__(
            metadata_file=metadata_file,
            database_dump_json_path=database_dump_json_path,
            raw_data_url=raw_data_url,
            process_data_url=process_data_url,
            test=test,
            skip_sample_id_check=skip_sample_id_check,
        )
        # Set the workflow version, prioritizing user input, then fetching from the Git URL, and finally using a default.
        self.workflow_version = workflow_version or self.get_workflow_version(
            workflow_version_git_url="https://raw.githubusercontent.com/microbiomedata/metaMS/master/.bumpversion_lcmsmetab.cfg"
        )
        self.minting_config_creds = minting_config_creds
        self.existing_data_objects = existing_data_objects

    def generate_stats(self, processed_data_dir: str) -> List[int]:
        """
        Generate QC Stats from processed data directory.

        Parameters
        ----------
        processed_data_dir : str
            Path to the processed data directory.

        Returns
        -------
        List[int]
            List of QC stats generated from the processed data directory.

        Notes
        -----
        This method reads in the processed data file and generates QC stats.

        """
        # Find the processed data .csv and read in as a pandas dataframe
        processed_data = self._read_processed_csv(processed_data_dir)

        # Calculate peak_count
        peak_count = processed_data["Mass Feature ID"].nunique()

        # Calculate peak_assignment_count
        peak_assignments = processed_data[["Mass Feature ID", "inchikey"]].dropna()
        peak_assignment_count = peak_assignments["Mass Feature ID"].nunique()

        # Calculate c13_isotopologue_count
        c13_isotopologue_count = processed_data[
            "Monoisotopic Mass Feature ID"
        ].nunique()

        return peak_count, peak_assignment_count, c13_isotopologue_count

    def generate_metab_identifications(
        self, processed_data_dir: str
    ) -> List[nmdc.MetaboliteIdentification]:
        """
        Generate MetaboliteIdentification objects from processed data directory.

        Parameters
        ----------
        workflow_metadata : str
            Path to the processed data directory.

        Returns
        -------
        List[nmdc.MetaboliteIdentification]
            List of MetaboliteIdentification objects generated from the processed data directory.

        Notes
        -----
        This method reads in the processed data file and generates MetaboliteIdentification objects,
        pulling out the best hit for each peak based on the highest "Similarity Score".

        """
        # Find the processed data .csv and read in as a pandas dataframe
        processed_data = self._read_processed_csv(processed_data_dir)

        # Drop any rows with missing entropy similarity scores
        processed_data = processed_data.dropna(subset=["Entropy Similarity"])

        # Group by "Mass Feature ID" and find the best hit for each peak based on the highest "Entropy Similarity"
        best_hits = processed_data.groupby("Mass Feature ID").apply(
            lambda x: x.loc[x["Entropy Similarity"].idxmax()], include_groups=False
        )

        metabolite_identifications = []
        for index, best_hit in best_hits.iterrows():
            # Check if the best hit has a Chebi ID, if not, do not create a MetaboliteIdentification object
            if pd.isna(best_hit["chebi"]):
                continue
            chebi_id = "chebi:" + str(int(best_hit["chebi"]))

            # Prepare KEGG Compound ID as an alternative identifier
            alt_ids = []
            if not pd.isna(best_hit["kegg"]):
                # Check for | in Kegg Compound ID and split if necessary
                if "|" in best_hit["kegg"]:
                    alt_ids.extend(
                        ["kegg:" + x.strip() for x in best_hit["kegg"].split("|")]
                    )
                else:
                    alt_ids.append("kegg:" + best_hit["kegg"])
            alt_ids = list(set(alt_ids))

            data_dict = {
                "metabolite_identified": chebi_id,
                "alternative_identifiers": alt_ids,
                "type": NmdcTypes.get("MetaboliteIdentification"),
                "highest_similarity_score": best_hit["Entropy Similarity"],
            }

            metabolite_identification = nmdc.MetaboliteIdentification(**data_dict)
            metabolite_identifications.append(metabolite_identification)

        return metabolite_identifications

    def _get_wf_stats(self, processed_data_dir: str) -> dict:
        """Return workflow statistics for metabolomics data as a dict."""
        peak_count, peak_assignment_count, c13_isotopologue_count = self.generate_stats(
            processed_data_dir=processed_data_dir
        )
        return {
            "peak_count": peak_count,
            "peak_assignment_count": peak_assignment_count,
            "c13_isotopologue_count": c13_isotopologue_count,
        }

    def _resolve_qc_from_stats(self, qc_status, qc_comment, wf_stats: dict):
        """Determine qc_status and qc_comment from metabolomics stats and optional CSV input.

        Threshold checks are always run. Resolution follows these rules:

        1. Stats fail AND CSV says "fail": returns "fail" with both the CSV comment and
           the stat failure message concatenated together.
        2. Stats fail AND CSV says "pass" or no CSV input: stats prevail, returns "fail"
           with the stat failure message only.
        3. Stats pass AND CSV says "fail": CSV override is accepted unconditionally,
           returns "fail" with the CSV comment only.
        4. Stats pass AND no CSV "fail": returns "pass" with the CSV comment if provided,
           otherwise the default pass message.

        Parameters
        ----------
        qc_status : str or None
            QC status value from the CSV, or None if not provided.
        qc_comment : str or None
            QC comment value from the CSV, or None if not provided.
        wf_stats : dict
            Dictionary of workflow statistics (peak_count, peak_assignment_count,
            c13_isotopologue_count).

        Returns
        -------
        tuple
            A tuple of (qc_status, qc_comment) resolved according to the rules above.
        """
        # Always compute stat failures
        failed = []
        if wf_stats.get("peak_count", 0) < self.peak_count_threshold:
            failed.append(
                f"peak_count ({wf_stats.get('peak_count', 0)} < {self.peak_count_threshold})"
            )
        if (
            wf_stats.get("peak_assignment_count", 0)
            < self.peak_assignment_count_threshold
        ):
            failed.append(
                f"peak_assignment_count ({wf_stats.get('peak_assignment_count', 0)} < {self.peak_assignment_count_threshold})"
            )
        if (
            wf_stats.get("c13_isotopologue_count", 0)
            < self.c13_isotopologue_count_threshold
        ):
            failed.append(
                f"c13_isotopologue_count ({wf_stats.get('c13_isotopologue_count', 0)} < {self.c13_isotopologue_count_threshold})"
            )

        stat_comment = f"QC failed on: {', '.join(failed)}." if failed else None

        if failed and qc_status == "fail":
            # Both stats and CSV indicate failure — concatenate comments
            combined_comment = "; ".join(filter(None, [qc_comment, stat_comment]))
            return "fail", combined_comment
        elif failed:
            # Stats fail, CSV says "pass" or nothing — stats prevail
            return "fail", stat_comment
        elif qc_status == "fail":
            # Stats pass, but CSV explicitly forces a fail — accept it
            return qc_status, qc_comment
        else:
            # Stats pass and no CSV override to fail
            return "pass", (
                qc_comment
                if qc_comment is not None
                else "QC passed all computed peak count thresholds."
            )

    def rerun(self):
        return super().rerun()

    def run(self):
        return super().run()
