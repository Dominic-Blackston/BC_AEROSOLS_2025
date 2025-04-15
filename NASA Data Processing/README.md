# Campaign Data Processing Workflow 
This repository provides a set of scripts to process, standardize, and consolidate raw instrument data from atmospheric campaigns. The processing workflow is divided into three distinct steps:

Raw Data Merging

Column Filtering & Renaming

Bin Consolidation (Binning)

Each step produces an output file that is used as the input for the next step. The following sections explain each script and the overall workflow.


## Raw Data Merging Script 
Script: raw_data_merger.py

### Purpose:
This script searches the campaign folder for instrument data files (in .ict format). It uses the SP2 instrument data to extract unique dates (by scanning file names using an eight-digit pattern). For each date, it reads and merges data from all instrument subdirectories using a common time field. The result is a set of daily merged files that are finally concatenated into one master raw CSV file (e.g., CAMP2Ex_Raw.csv or NAAMES(2017)_Raw.csv).

### Key Points:

Instrument Discovery: Scans subdirectories (excluding a “datasets” folder) to find all instruments.

Date Extraction: Uses SP2 files and an eight-digit regular expression to determine unique dates.

Data Merging: Merges data from each instrument based on a common time field (after converting it to a numeric “merge_time” value).

Output: Produces a raw merged file that contains all dates' data; this file is used by the next stage.

## Column Filtering & Renaming Script
Script: rename_columns.py

### Purpose:
This script takes the raw merged file (from the raw data merger script) and standardizes the column names. It uses candidate lists to map multiple possible raw names to a consistent set of target names. For example, it can map merge_time to UTC, GPS_Altitude to Altitude, and various bin columns to a standard naming format (e.g., bin1, bin2, … for aerosol measurements). The script also automatically detects and filters scattering (Sc\d{3}_total) and absorption (Abs\d{3}_total) columns.

### Key Points:

Candidate Matching: Uses helper functions (find_candidate and build_mapping) to search for raw column names among multiple possible candidates.

Dynamic Detection: Searches for bin columns by regex (for instance, those matching typical LAS/UHSAS formats).

Manual Mapping: Predefines key columns that must be present (e.g., UTC, Date, Latitude, etc.) and throws an error if any required column is missing.

### Outputs:

A Restricted file (e.g., NAAMES(2017)_Restricted.csv) that contains only the core variables plus the aerosol bin columns.

A Comprehensive file (e.g., NAAMES(2017)_Comprehensive.csv) that includes all of the restricted fields plus extra variables (such as wind speeds, supersaturation, etc.) and may include additional bin types (if available, e.g., cloud bins).

## Bin Consolidation (Binning) Script
Script: binning.py (or binning_post_renaming.py)

### Purpose:
This script is run after the renaming columns script and further processes the renamed files by consolidating (or "binning") the bin columns. The binning process has two parts:

Restricted Binning (Aerosol Only):
It reads in the Restricted renamed file, uses a set of original aerosol bin diameters (from the NASA Diameters.csv file for LAS/UHSAS instruments) and desired new bin diameters, then consolidates the aerosol bin columns based on an index-mapping algorithm. The output file is saved (e.g., CAMP2Ex_Restricted_renamed_binned.csv).

Comprehensive Binning (Aerosol and Cloud):
It reads in the Comprehensive renamed file, and first consolidates the aerosol bins (as above). Then it uses another set of original cloud bin diameters (from the CAMP2EX_CDP_ row of NASA Diameters.csv) and desired new cloud bin diameters to consolidate the cloud bins. The final output file (e.g., CAMP2Ex_Comprehensive_renamed_binned.csv) contains the consolidated aerosol and cloud bin columns.

### Key Functions Included:

create_index_bin: Generates lists of old column indices mapped to new bin groups.

bin_name_list: Generates a list of new bin names (e.g., “bin1”, “bin2” for aerosol or “cbin1”, “cbin2” for cloud bins) based on the bin type.

consolidate_bins: Sums and consolidates original bin columns into fewer bins based on provided index mapping, preserving NA values.


## Overall Workflow

## Data Merging:

Run raw_data_merger.py to automatically merge all raw instrument files (from various subdirectories) into a single merged CSV file (e.g., CAMP2Ex_Raw.csv).

## Column Filtering & Renaming:

Run rename_columns.py to process the raw merged file.

This script standardizes the column names, detects bin columns, and produces two versions: a Restricted file (core fields plus aerosol bins) and a Comprehensive file (Restricted fields plus additional parameters and extra bin columns if available).

## Binning (Bin Consolidation):

Run binning.py (or binning_post_renaming.py) after the renaming step.

For the Restricted dataset, the script consolidates the aerosol bin columns using the original aerosol diameters (from NASA Diameters.csv) and a set of desired new bin diameters.

For the Comprehensive dataset, it consolidates aerosol bins first and then consolidates cloud bins (using original cloud bin diameters provided in the NASA Diameters.csv—such as the CAMP2EX_CDP_ row midpoints).

The output files (e.g., CAMP2Ex_Restricted_renamed_binned.csv and CAMP2Ex_Comprehensive_renamed_binned.csv) are then produced for further analysis.

## Setup

1. Directory Structure:

Organize your campaign data as follows:

markdown
Copy
CampaignName/
├── Instrument1/
│   ├── file1.ict
│   ├── file2.ict
│   └── ... 
├── Instrument2/
│   ├── file1.ict
│   └── ... 
└── Instrument3/
    ├── file1.ict
    └── ...

2. Dependencies:

Ensure you have the following Python packages installed: 
pip install icartt pandas tqdm

3. Configuration:

Edit each of the scripts to set:

base_dir: Base directory where your campaign folder is located.
campaign: The name of your campaign (e.g., "CAMP2Ex").

# How it works 
Instrument Discovery:
The script finds all instrument subdirectories within your campaign folder (excluding the datasets folder).

Date Extraction:
It scans the SP2 instrument files to extract unique eight-digit dates from file names.

Data Merging:
For each date, data from the instruments are loaded and merged based on a time column. Daily merged files are saved in a datasets subdirectory. Finally, all daily merges are combined into a single CSV file.

# Things to Note
Directory Structure:
Place your campaign folder under the base directory, with instrument subdirectories organized as follows:

CampaignName/
├── Instrument1/
├── Instrument2/
├── SP2/
└── ... 
Dependencies:
Ensure you have installed all required packages (icartt, pandas, numpy, tqdm, etc.) via pip.

Configuration:
Edit the base_dir and campaign variables in each script as needed. Also, verify that the bin diameters (for both aerosol and cloud) are correct by cross-checking with NASA Diameters.csv.

Execution Order:
Run the scripts sequentially:

raw_data_merger.py

rename_columns.py

binning.py

Error Checking:
The renaming script will throw errors if required fields (e.g., UTC, Date, Latitude, etc.) are missing. The binning script will also error if no aerosol bin columns are detected, as they are required for Restricted output.

## Final Note
By following the above workflow and using the three scripts in sequence, you transform raw campaign files into standardized, merged, and binned datasets.

