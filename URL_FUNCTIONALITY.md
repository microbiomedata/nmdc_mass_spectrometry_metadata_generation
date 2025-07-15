# URL Support for NMDC Mass Spectrometry Metadata Generation

This document demonstrates the new URL functionality that allows sample-specific URLs for raw data in metadata input files.

## New Feature

The metadata generation scripts now support sample-specific URLs for raw data files. You can include a `raw_data_url` column in your metadata CSV file instead of relying solely on base URLs provided via CLI arguments.

## Supported URL Column

- `raw_data_url` - Complete URL for the raw data file

Note: Processed data objects continue to use the base URL approach for consistency.

## Usage Examples

### Example 1: Using Raw Data URL Column in Metadata

Create a CSV file with a raw data URL column:

```csv
biosample_id,raw_data_file,processed_data_file,raw_data_url
nmdc:bsm-11-123,sample1.raw,sample1.csv,https://project1.data.org/raw/sample1.raw
nmdc:bsm-11-456,sample2.raw,sample2.csv,https://project2.data.org/raw/sample2.raw
```

Run the generator with only the processed data base URL:

```bash
python main.py \
  --generator gcms_metab \
  --metadata_file metadata_with_raw_urls.csv \
  --database_dump_json_path output.json \
  --process_data_url https://data.example.com/processed/
```

### Example 2: Backwards Compatibility

The old approach still works - you can use base URLs without URL columns:

```bash
python main.py \
  --generator gcms_metab \
  --metadata_file metadata_without_urls.csv \
  --database_dump_json_path output.json \
  --raw_data_url https://data.example.com/raw/ \
  --process_data_url https://data.example.com/processed/
```

### Example 3: Mixed Approach

You can provide base URLs as fallbacks and use URL columns for specific samples:

```bash
python main.py \
  --generator gcms_metab \
  --metadata_file metadata_mixed.csv \
  --database_dump_json_path output.json \
  --raw_data_url https://default.example.com/raw/ \
  --process_data_url https://default.example.com/processed/
```

## Benefits

1. **Sample-specific raw data URLs**: Different samples can point to different data repositories or storage locations for raw data
2. **Flexibility**: Mix and match URL approaches as needed for your workflow
3. **Backwards compatibility**: Existing workflows continue to work unchanged
4. **Reduced CLI complexity**: Raw data URL is optional when using the URL column

## Technical Implementation

- The `raw_data_url` column takes precedence over base URL when both are provided
- Missing `raw_data_url` column falls back to base URL + filename construction
- Processed data objects always use the base URL + filename approach
- CLI validation ensures either `--raw_data_url` argument or `raw_data_url` column is provided
- `--process_data_url` argument is always required
- All workflow generators (GCMS, LCMS, NOM) support the new raw data URL functionality