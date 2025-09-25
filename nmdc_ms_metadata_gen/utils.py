from pathlib import Path

import nmdc_schema.nmdc as nmdc
import pandas as pd


def save_to_csv(df: pd.DataFrame, output_path: str | Path):
    """
    Save the DataFrame to a CSV file.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to save
    output_path : str or Path
        The path where the CSV should be saved

    Returns
    -------
    None

    """
    df.to_csv(f"{output_path}.csv", index=False)
    print(f"Sheet saved to {output_path}.csv")


def output_material_processing_summary(
    reference_mapping: pd.DataFrame, nmdc_database: nmdc.Database
) -> None:
    """
    Print summary statistics.

    Parameters
    ----------
    reference_mapping: pd.DataFrame
        Input reference mapping for biosample -> process

    nmdc_database: dict
        The NMDC database instance to which the processed objects will be added

    Returns
    -------
    None

    """
    print(
        f"Total biosample instances: {len(reference_mapping['biosample_id'].unique())}"
    )
    print(
        f"Total material process IDs in database: {len(nmdc_database['material_processing_set'])}"
    )
    print(
        f"Total processed sample IDs in database: {len(nmdc_database['processed_sample_set'])}"
    )
