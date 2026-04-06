from typing import List

import pandas as pd

from nmdc_ms_metadata_gen.nom_metadata_generator import NOMMetadataGenerator


class LCMSNOMMetadataGenerator(NOMMetadataGenerator):
    """
    A class for generating NMDC metadata objects using provided metadata files and configuration
    for LC FT-ICR MS NOM data.

    This class processes input metadata files, generates various NMDC objects, and produces
    a database dump in JSON format.

    Parameters
    ----------
    metadata_file : str
        Path to the input CSV metadata file.
    database_dump_json_path : str
        Path where the output database dump JSON file will be saved.
    raw_data_url : str, optional
        Base URL for the raw data files. If the raw data url is not directly passed in, it will use the raw data urls from the metadata file.
    process_data_url : str
        Base URL for the processed data files.
    minting_config_creds : str, optional
        Path to the configuration file containing the client ID and client secret for minting NMDC IDs. It can also include the bio ontology API key if generating biosample ids is needed.
        If not provided, the CLIENT_ID, CLIENT_SECRET, and BIO_API_KEY environment variables will be used.
    test : bool, optional
        Flag indicating whether to run in test mode. If True, will skip biosample ID checks in the database, data object URL check, and will use local IDs (skip API minting). Default is False.

    Attributes
    ----------
    processed_data_object_desc : str
    qc_process_data_obj_type : str
    qc_process_data_description : str
    raw_data_object_type : str
    raw_data_obj_desc : str
    processed_data_object_type : str
    processed_data_category: str
    analyte_category: str
    workflow_analysis_name: str
    workflow_param_data_object_desc: str
    workflow_param_data_category: str
    workflow_param_data_object_type: str
    unique_columns: list[str]
    mass_spec_eluent_intro: str
    mass_spec_desc: str
    workflow_git_url: str
    workflow_version: str
    workflow_description: str
    """

    processed_data_object_desc: str = (
        "NOM annotations as a result of a NOM workflow activity."
    )
    qc_process_data_obj_type: str = "LC FT-ICR MS QC Plots"
    qc_process_data_description: str = "EnviroMS QC plots representing a NOM analysis."

    raw_data_object_type: str = "LC FT-ICR MS Raw Data"
    raw_data_obj_desc: str = "LC FT-ICR MS Raw Data raw data for NOM data acquisition."
    processed_data_object_type: str = "LC FT-ICR MS Analysis Results"
    processed_data_category: str = "processed_data"
    analyte_category: str = "nom"
    workflow_analysis_name: str = "LC FT-ICR MS NOM Analysis"
    workflow_param_data_object_desc: str = (
        "EnviroMS processing parameters for natural organic matter analysis when acquired using liquid chromatography."
    )
    workflow_param_data_category: str = "workflow_parameter_data"
    workflow_param_data_object_type: str = "Analysis Tool Parameter File"
    unique_columns: list[str] = ["processed_data_directory"]
    mass_spec_eluent_intro: str = "liquid_chromatography"
    mass_spec_desc: str = (
        "Generation of mass spectrometry data for the analysis of NOM when acquired using liquid chromatography."
    )
    workflow_git_url: str = (
        "https://github.com/microbiomedata/enviroMS/blob/master/wdl/lc_fticr_ms.wdl"
    )
    workflow_version: str
    workflow_description: str = (
        "Processing of raw liquid chromatography FT-ICR MS data for natural organic matter identification."
    )
    # on the mass spec records, you will need to add has_chromatograohy_configuration - created in parent metadata gen class

    # QC thresholds
    peak_count_threshold: int = 0
    peak_assignment_count_threshold: int = 0
    peak_assignment_rate_threshold: float = 0.0

    def __init__(
        self,
        metadata_file: str,
        database_dump_json_path: str,
        process_data_url: str,
        raw_data_url: str = None,
        minting_config_creds: str = None,
        workflow_version: str = None,
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
        # Set the workflow version, prioritizing user input, then fetching from the Git URL.
        self.workflow_version = workflow_version or self.get_workflow_version(
            workflow_version_git_url="https://raw.githubusercontent.com/microbiomedata/enviroMS/master/.bumpversion.cfg"
        )
        self.minting_config_creds = minting_config_creds

    def generate_stats(self, processed_data: pd.DataFrame) -> List[int]:
        """
        Generate QC Stats from processed data.

        Parameters
        ----------
        processed_data : pd.DataFrame
            DataFrame containing the processed data.

        Returns
        -------
        List[int]
            List of QC stats generated from the processed data.

        Notes
        -----
        This method reads in the processed data file and generates QC stats.

        """

        # Calculate peak_count
        peak_count = processed_data["Index"].nunique()

        # Calculate peak_assignment_count
        peak_assignments = processed_data[["Index", "Molecular Formula"]].dropna()
        peak_assignment_count = peak_assignments["Index"].nunique()

        return peak_count, peak_assignment_count

    def _get_wf_stats(self, processed_data: pd.DataFrame) -> dict:
        """Return workflow statistics for di NOM data as a dict."""
        peak_count, peak_assignment_count = self.generate_stats(
            processed_data=processed_data
        )
        return {
            "peak_count": peak_count,
            "peak_assignment_count": peak_assignment_count,
        }

    def _resolve_qc_from_stats(self, qc_status, qc_comment, wf_stats: dict):
        """Determine qc_status and qc_comment from di NOM stats and optional CSV input.

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
            Dictionary of workflow statistics (peak_count, peak_assignment_count).

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
        peak_assignment_rate = (
            wf_stats.get("peak_assignment_count", 0) / wf_stats.get("peak_count", 0)
            if wf_stats.get("peak_count", 0) > 0
            else 0
        )
        if peak_assignment_rate < self.peak_assignment_rate_threshold:
            failed.append(
                f"peak_assignment_rate ({peak_assignment_rate} < {self.peak_assignment_rate_threshold})"
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
