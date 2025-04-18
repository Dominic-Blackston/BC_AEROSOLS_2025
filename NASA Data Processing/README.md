# Campaign Data Processing Workflow 
This repository provides a set of scripts to process, standardize, and consolidate raw instrument data from atmospheric campaigns. The processing workflow is divided into three distinct steps:

Raw Data Merging
Column Filtering & Renaming
Bin Consolidation (Binning)

Each step produces an output file that is used as the input for the next step. You can try it out yourself with the NAAMES(2015) Data. The following sections explain each script and the overall workflow.

---

## Raw Data Merging Script 
**Script**: `raw_data_merger.py`

### Purpose:
This script searches the campaign folder for instrument data files (in `.ict` format). It uses the SP2 instrument data to extract unique dates (by scanning file names using an eight-digit pattern). For each date, it reads and merges data from all instrument subdirectories using a common time field. The result is a set of daily merged files that are finally concatenated into one master raw CSV file (e.g., `CAMP2Ex_Raw.csv` or `NAAMES(2017)_Raw.csv`).

### Key Points:
- **Instrument Discovery**: Scans subdirectories (excluding a "datasets" folder) to find all instruments.
- **Date Extraction**: Uses SP2 files and an eight-digit regular expression to determine unique dates.
- **Data Merging**: Merges data from each instrument based on a common time field (after converting it to a numeric `merge_time` value).
- **Output**: Produces a raw merged file that contains all dates' data; this file is used by the next stage.

---

## Column Filtering & Renaming Script
**Script**: `rename_columns.py`

### Purpose:
This script takes the raw merged file (from the raw data merger script) and standardizes the column names. It uses candidate lists to map multiple possible raw names to a consistent set of target names. For example, it can map `merge_time` to `UTC`, `GPS_Altitude` to `Altitude`, and various bin columns to a standard naming format (e.g., `bin1`, `bin2`, … for aerosol measurements).

The script also supports cloud bin columns like `cbin1` or `CDP_Bin01`, and includes them in the comprehensive output if available.

### Key Points:
- **Candidate Matching**: Uses helper functions (`find_candidate` and `build_mapping`) to search for raw column names among multiple possible candidates.
- **Dynamic Detection**: Searches for bin columns by regex (e.g., LAS/UHSAS for aerosols and CDP for cloud bins).
- **Manual Mapping**: Predefines key columns that must be present (e.g., `UTC`, `Date`, `Latitude`, etc.) and throws an error if any required column is missing.
- **Blank Column Handling**: The comprehensive output includes *all expected columns*, and if a column isn't present in the input, it is still added to the output as a blank column.

### Outputs:
- **Restricted file** (e.g., `NAAMES(2017)_Restricted.csv`): Contains only the core variables plus required aerosol bin columns.
- **Comprehensive file** (e.g., `NAAMES(2017)_Comprehensive.csv`): Contains all expected fields, even if some were missing in the raw file (blank columns will be added as placeholders).

---

## Bin Consolidation (Binning) Script
**Script**: `binning.py` or `binning_post_renaming.py`

### Purpose:
This script is run *after* the renaming columns script and further processes the renamed files by consolidating (or "binning") the bin columns. The binning process has two parts:

### Restricted Binning (Aerosol Only):
- Reads the Restricted renamed file.
- Uses the original aerosol bin diameters (from the NASA Diameters.csv file for LAS/UHSAS instruments) and desired new bin diameters.
- Consolidates the aerosol bin columns based on an index-mapping algorithm.
- Outputs the result to a file like `CAMP2Ex_Restricted_renamed_binned.csv`.

### Comprehensive Binning (Aerosol and Cloud):
- Reads the Comprehensive renamed file.
- Consolidates aerosol bins (as above).
- Then uses original cloud bin diameters (e.g., from `CAMP2EX_CDP_` row in NASA Diameters.csv) and desired new diameters.
- Outputs the final file (e.g., `CAMP2Ex_Comprehensive_renamed_binned.csv`).

### Key Functions Included:
- `create_index_bin`: Maps old bin indices to new bin groups.
- `bin_name_list`: Generates new bin names (e.g., `bin1`, `bin2`, or `cbin1`, `cbin2`).
- `consolidate_bins`: Aggregates and replaces original bin columns with the new consolidated bins.

If **no cloud bins are detected**, only aerosol bins will be consolidated. If either aerosol or cloud bins are missing in the file, the function will gracefully skip that type of binning (with a warning if aerosol bins are missing in a restricted dataset).

---

## Workflow Summary

### Step 1: Data Merging
Run `raw_data_merger.py` to merge all `.ict` instrument files into a single raw file like `CAMP2Ex_Raw.csv`.

### Step 2: Column Filtering & Renaming
Run `rename_columns.py`:
- Produces a `Restricted` file (core variables + aerosol bins).
- Produces a `Comprehensive` file (includes all expected columns).
- Ensures missing columns still appear as blanks in the comprehensive version.

### Step 3: Binning
Run `binning.py`:
- Reads the `Restricted` file and consolidates aerosol bins.
- Reads the `Comprehensive` file and consolidates aerosol and cloud bins.
- Produces `_renamed_binned.csv` versions for each.

---

## Setup

### Directory Structure:
Organize your campaign data as follows:

```bash
CampaignName/
├── Instrument1/
│   ├── file1.ict
│   └── ...
├── Instrument2/
├── SP2/
└── ...
```

### Dependencies:
Install Python packages using:
```bash
pip install icartt pandas tqdm numpy
```

### Configuration:
Set the following variables in each script:
- `base_dir`: Path to your campaigns.
- `campaign`: Name of your campaign (e.g., `CAMP2Ex`).
- Check that the bin diameters match the instrument in `NASA Diameters.csv`.

---

## Execution Order
Run the scripts in the following order:
1. `raw_data_merger.py`
2. `rename_columns.py`
3. `binning.py`

Each step saves an output file that feeds into the next.

---

## Notes
- If aerosol bin columns are not detected in `rename_columns.py`, the script will throw an error (they're required).
- The comprehensive output will **always** contain all expected fields; missing columns will be filled with blanks.
- The binning script gracefully skips bin types that are missing, warning when needed.

---

## Final Note
By following the above workflow and using the three scripts in sequence, you transform raw campaign files into standardized, merged, and binned datasets.
