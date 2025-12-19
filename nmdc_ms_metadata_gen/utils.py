from collections import defaultdict
from pathlib import Path

import nmdc_schema.nmdc as nmdc
import pandas as pd

from nmdc_ms_metadata_gen.metadata_parser import YamlSpecifier


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
    df.to_csv(f"{output_path}", index=False)
    print(f"Sheet saved to {output_path}")


def validate_generated_output(
    reference_mapping: pd.DataFrame,
    nmdc_database: nmdc.Database,
    yaml_outline_path: str,
    output_file: str = None,
) -> bool:
    """
    Validate that generated output matches expected counts based on input mapping.

    Parameters
    ----------
    reference_mapping : pd.DataFrame
        DataFrame containing biosample to raw data mapping with columns:
        - biosample_id
        - material_processing_protocol_id
        - processedsample_placeholder
    nmdc_database : nmdc.Database
        The generated NMDC database instance
    yaml_outline_path : str
        Path to the YAML outline file
    output_file : str, optional
        Path to save validation output. If None, only prints to console.

    Returns
    -------
    bool
        True if validation passes, False otherwise
    """
    import sys
    from collections import defaultdict
    from io import StringIO

    from nmdc_ms_metadata_gen.metadata_parser import YamlSpecifier

    # Capture output if file path provided
    if output_file:
        old_stdout = sys.stdout
        sys.stdout = captured_output = StringIO()

    try:
        print("\n" + "=" * 80)
        print("VALIDATING GENERATED OUTPUT")
        print("=" * 80)

        # Calculate expected counts
        yaml_spec = YamlSpecifier(yaml_outline_path)

        expected_step_count = 0
        expected_sample_count = 0

        # Track unique combinations of (protocol, placeholder set)
        combination_stats = defaultdict(
            lambda: {
                "biosample_count": 0,
                "step_count": 0,
                "sample_count": 0,
                "step_names": None,
                "sample_names": None,
            }
        )

        print("Calculating expected counts...")

        # Group by biosample and protocol to handle shared steps correctly
        for biosample_id in reference_mapping["biosample_id"].unique():
            biosample_mappings = reference_mapping[
                reference_mapping["biosample_id"] == biosample_id
            ]

            for protocol_id in biosample_mappings[
                "material_processing_protocol_id"
            ].unique():
                protocol_mappings = biosample_mappings[
                    biosample_mappings["material_processing_protocol_id"] == protocol_id
                ]

                # Get all target placeholders for this biosample + protocol combination
                target_placeholders = (
                    protocol_mappings["processedsample_placeholder"].unique().tolist()
                )

                try:
                    protocol_data = yaml_spec.load_yaml(protocol_id)
                except KeyError:
                    print(f"Warning: Protocol {protocol_id} not found in outline")
                    continue

                # Filter to only the steps/samples needed for ALL target placeholders
                filtered_data = yaml_spec.update_sample_outputs(
                    data=protocol_data.copy(), target_outputs=target_placeholders
                )

                step_names = tuple(
                    [list(step.keys())[0] for step in filtered_data.get("steps", [])]
                )
                sample_names = tuple(
                    [
                        list(sample.keys())[0]
                        for sample in filtered_data.get("processedsamples", [])
                    ]
                )

                num_steps = len(filtered_data.get("steps", []))
                num_samples = len(filtered_data.get("processedsamples", []))

                expected_step_count += num_steps
                expected_sample_count += num_samples

                # Track combination stats (by unique set of placeholders)
                combo_key = (protocol_id, tuple(sorted(target_placeholders)))
                combination_stats[combo_key]["biosample_count"] += 1
                combination_stats[combo_key]["step_count"] = num_steps
                combination_stats[combo_key]["sample_count"] = num_samples
                combination_stats[combo_key]["step_names"] = step_names
                combination_stats[combo_key]["sample_names"] = sample_names

        # Count actual generated objects
        actual_step_count = (
            len(nmdc_database.material_processing_set)
            if nmdc_database.material_processing_set
            else 0
        )
        actual_sample_count = (
            len(nmdc_database.processed_sample_set)
            if nmdc_database.processed_sample_set
            else 0
        )
        actual_biosample_count = len(reference_mapping["biosample_id"].unique())

        # Print combination analysis
        print("\n" + "=" * 80)
        print("PLACEHOLDER COMBINATIONS BY PROTOCOL:")
        print("=" * 80)

        for (protocol_id, target_placeholders), stats in sorted(
            combination_stats.items()
        ):
            print(f"\n{protocol_id}:")
            print(f"  Target placeholders: {list(target_placeholders)}")
            print(f"  Resulting samples: {list(stats['sample_names'])}")
            print(f"  Biosamples with this combination: {stats['biosample_count']}")
            print(f"  Steps required per biosample: {stats['step_count']}")
            print(
                f"  Processed samples required per biosample: {stats['sample_count']}"
            )
            print(
                f"  Total steps for this combination: {stats['step_count'] * stats['biosample_count']}"
            )
            print(
                f"  Total samples for this combination: {stats['sample_count'] * stats['biosample_count']}"
            )

        # Compare totals
        print("\n" + "=" * 80)
        print("VALIDATION SUMMARY:")
        print("=" * 80)

        print(f"\nBiosamples:")
        print(f"  Unique in mapping: {actual_biosample_count}")

        print(f"\nMaterial Processing Steps:")
        print(f"  Expected: {expected_step_count}")
        print(f"  Actual:   {actual_step_count}")
        step_match = expected_step_count == actual_step_count
        print(f"  {'✓ MATCH' if step_match else '✗ MISMATCH'}")
        if not step_match:
            print(f"  Difference: {actual_step_count - expected_step_count:+d}")

        print(f"\nProcessed Samples:")
        print(f"  Expected: {expected_sample_count}")
        print(f"  Actual:   {actual_sample_count}")
        sample_match = expected_sample_count == actual_sample_count
        print(f"  {'✓ MATCH' if sample_match else '✗ MISMATCH'}")
        if not sample_match:
            print(f"  Difference: {actual_sample_count - expected_sample_count:+d}")

        print(f"\nUnique placeholder combinations: {len(combination_stats)}")

        all_match = step_match and sample_match

        print("\n" + "=" * 80)
        if all_match:
            print("✓ VALIDATION PASSED: All counts match!")
        else:
            print("✗ VALIDATION FAILED: Counts do not match")
        print("=" * 80)

    finally:
        if output_file:
            # Restore stdout
            sys.stdout = old_stdout

            # Get captured output
            output_text = captured_output.getvalue()

            # Print to console
            print(output_text)

            # Save to file
            with open(output_file, "w") as f:
                f.write(output_text)
            print(f"Validation output saved to: {output_file}")

    return all_match
