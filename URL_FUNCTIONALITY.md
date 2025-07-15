# URL Support for NMDC Mass Spectrometry Metadata Generation

This document demonstrates the new URL functionality that allows sample-specific URLs in metadata input files.

## New Feature

The metadata generation scripts now support sample-specific URLs for raw and processed data. You can include URL columns in your metadata CSV file instead of relying solely on base URLs provided via CLI arguments.

## Supported URL Columns

- `raw_data_url` - Complete URL for the raw data file
- `processed_data_url` - Complete URL for the processed data file

## Usage Examples

### Example 1: Using URL Columns in Metadata

Create a CSV file with URL columns:

```csv
biosample_id,raw_data_file,processed_data_file,raw_data_url,processed_data_url
nmdc:bsm-11-123,sample1.raw,sample1.csv,https://data.example.com/project1/raw/sample1.raw,https://data.example.com/project1/processed/sample1.csv
nmdc:bsm-11-456,sample2.raw,sample2.csv,https://data.example.com/project2/raw/sample2.raw,https://data.example.com/project2/processed/sample2.csv
```

Run the generator without base URL arguments:

```bash
python main.py \
  --generator gcms_metab \
  --metadata_file metadata_with_urls.csv \
  --database_dump_json_path output.json
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

1. **Sample-specific URLs**: Different samples can point to different data repositories or storage locations
2. **Flexibility**: Mix and match URL approaches as needed for your workflow
3. **Backwards compatibility**: Existing workflows continue to work unchanged
4. **Reduced CLI complexity**: Fewer required command-line arguments when using URL columns

## Technical Implementation

- URL columns take precedence over base URLs when both are provided
- Missing URL columns fall back to base URL + filename construction
- CLI validation ensures either base URLs or URL columns are provided
- All workflow generators (GCMS, LCMS, NOM) support the new URL functionality