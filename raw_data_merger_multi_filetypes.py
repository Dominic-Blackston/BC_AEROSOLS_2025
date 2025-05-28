import os
import re
import warnings
import icartt
import pandas as pd
from tqdm import tqdm
import numpy as np
from collections import defaultdict
from datetime import datetime

# Suppress all warnings.
warnings.filterwarnings("ignore")

def standardize_date(raw_date):
    # Remove trailing non-digit characters
    cleaned = re.match(r'\d{6,8}', raw_date)
    if not cleaned:
        return None

    date_str = cleaned.group()

    if len(date_str) == 8:
        return date_str  # Already in YYYYMMDD
    elif len(date_str) == 6:
        yy = int(date_str[:2])
        mmdd = date_str[2:]
        # Convert YY to YYYY (assume 2000–2049 range)
        yyyy = 2000 + yy if yy < 50 else 1900 + yy
        return f"{yyyy}{mmdd}"
    return None

def replace_with_second_mean(df):
    df['merge_time_sec'] = df['merge_time'].astype(int)
    df['merge_time_sec'] = (df['merge_time_sec'] / 5).round() * 5
    df['merge_time'] = df['merge_time'].astype(int)
    df['merge_time'] = (df['merge_time'] / 5).round() * 5
    means = df.groupby('merge_time_sec').transform('mean')

    # Replace each value with the mean of its second
    return means

# Helper function to get the time column name in a case-insensitive manner.
def get_time_column(df):
    candidate_list = ["time_mid", "time_start", "time", "utc_mid", "utc_start", "utc", "start_utc", "ut_time", "uhsas_mid_time", "time_utc", "time(utc)","second", "start_time_(utc)"]
    for candidate in candidate_list:
        for col in df.columns:
            if col.lower() == candidate:
                return col
    return None

# Base directory where your instrument files are stored.
base_dir = "C:/Users/dblac/OneDrive/Desktop/OSU/BC_Research_Project/Data/"

# Campaign variable; change this as needed.
campaign = "TCAP"

# Campaign directory (contains instrument subdirectories such as CCN, FCDP, LAS, MetNav, OPTICAL, SP2, WINDS, etc.)
campaign_dir = os.path.join(base_dir, campaign)

# Define the subdirectory to skip (e.g., where merged files are stored).
skip_dir = "datasets"

# Get all immediate subdirectories in the campaign directory (skip files and the skip_dir).
instrument_dirs = [d for d in os.listdir(campaign_dir)
                   if os.path.isdir(os.path.join(campaign_dir, d)) and d.lower() != skip_dir.lower()]

print("Found instrument subdirectories:")
for subdir in instrument_dirs:
    print(f" - {subdir}")

# ----------------------------------------------------------------------
# Extract unique dates from the SP2 subdirectory only (guaranteed to exist).
sp2_dir = os.path.join(campaign_dir, "SP2")
if not os.path.exists(sp2_dir):
    print("SP2 directory not found in the campaign folder. Exiting.")
    exit(1)

# Match 6 or 8 digits followed optionally by a single letter (e.g., '20120709a' or '120709a')
date_pattern = re.compile(r'(\d{6,8}[a-z]?)')
dates_set = set()
allowed_extensions = ('.ict', '.csv', '.txt', '.dat')

print(f"\nScanning files in SP2 directory: {sp2_dir}")
for file in os.listdir(sp2_dir):
    if file.lower().endswith(allowed_extensions):
        match = date_pattern.search(file)
        if match:
            dates_set.add(match.group())

standardized_dates = set()

for raw in dates_set:
    std = standardize_date(raw)
    if std:
        standardized_dates.add(std)

print("Extracted Dates:")
dates_set = standardized_dates
print(dates_set)

# Sort dates in descending order (or change sorting as desired).
dates = sorted(list(dates_set), reverse=True)
print("\nUnique dates (from SP2) found:", dates)
# ----------------------------------------------------------------------

# Create directory for daily merged files (datasets folder within the campaign directory).
datasets_dir = os.path.join(campaign_dir, "datasets")
os.makedirs(datasets_dir, exist_ok=True)

# Define which instrument should be used as the base for merging.
# By default, we use OPTICAL if available.
preferred_base = "SP2"
if preferred_base not in [inst.upper() for inst in instrument_dirs]:
    preferred_base = instrument_dirs[0].upper()
print(f"\nUsing {preferred_base} as the base instrument for merging (if available).")

all_merged = []  # List to store each day's merged DataFrame.

# Process each date with a progress bar.
for date in tqdm(dates, desc="Processing Dates"):
    print(f"\n=== Processing date: {date} ===")
    instrument_data = {}
    
    # Loop through each instrument subdirectory.
    for inst in instrument_dirs:
        inst_path = os.path.join(campaign_dir, inst)
        matching_files = [f for f in os.listdir(inst_path) if f.endswith('.ict') and date in f]
        if matching_files:
            loaded_dfs = []
            for file_name in sorted(matching_files):
                file_path = os.path.join(inst_path, file_name)
                try:
                    df_temp = pd.DataFrame(icartt.Dataset(file_path).data[:])
                except Exception as e:
                    print(f"Error loading {inst} file '{file_name}' for {date}: {e}")
                    continue
                loaded_dfs.append(df_temp)
            if not loaded_dfs:
                continue
            # If multiple files exist for an instrument (e.g., LAS "cold" and "hot"), concatenate them.
            if len(loaded_dfs) > 1:
                df = pd.concat(loaded_dfs, ignore_index=True)
                print(f"Combined {len(loaded_dfs)} files for {inst} for date {date}.")
            else:
                df = loaded_dfs[0]
            # Find the appropriate time column.
            time_col = get_time_column(df)
            if not time_col:
                print(f"Time column not found in {inst} for {date}; skipping this instrument.")
                continue
            # Convert the time column to numeric and rename it to 'merge_time'.
            df[time_col] = pd.to_numeric(df[time_col], errors='coerce')
            df = df.rename(columns={time_col: "merge_time"})
            df = replace_with_second_mean(df)
            df.reset_index(drop=True, inplace= True)
            df.drop_duplicates(inplace=True)
            instrument_data[inst.upper()] = df
            print(f"Loaded {inst.upper()} data for {date} (using time column '{time_col}').")
        else:
            print(f"No file found in {inst} for {date}")
    
    if not instrument_data:
        print(f"No instrument files loaded for {date}, skipping date.")
        continue
    
    # Choose base dataset.
    if preferred_base in instrument_data:
        base_inst = preferred_base
        merged = instrument_data[base_inst]
        merged = merged[['merge_time', 'SP2_rBC_conc']]

    else:
        base_inst = list(instrument_data.keys())[0]
        merged = instrument_data[base_inst]
        print(f"Preferred base instrument not found for {date}; using {base_inst} as base.")
    
    # Merge all other instruments on 'merge_time'.
    for inst, df in instrument_data.items():
        if inst == base_inst:
            continue
        merged = pd.merge(merged, df, on="merge_time", how="left", suffixes=("", f"_{inst}"))
        print(f"Merged {inst} data into base ({base_inst}).")
    
    merged['Date'] = date
    merged_file_name = f"{campaign}_Merged_Data_{date}.csv"
    merged_file_path = os.path.join(datasets_dir, merged_file_name)
    merged.to_csv(merged_file_path, index=False)
    print(f"Saved merged data for {date} to {merged_file_path}")
    
    all_merged.append(merged)

if all_merged:
    print("\nMerging all dates into one DataFrame...")
    final_merged = pd.concat(all_merged, ignore_index=True)
    final_merged_name = f"{campaign}_Raw.csv"
    final_merged_file = os.path.join(campaign_dir, final_merged_name)
    final_merged.to_csv(final_merged_file, index=False)
    
    # Print out the final columns list.
    print("\nFinal merged DataFrame columns:")
    for col in final_merged.columns.tolist():
        print(f" - {col}")
    
    print(f"\nAll dates merged into one file: {final_merged_file}")
else:
    print("No merged data to combine.")



import os
import glob

# Adjust the path and pattern as needed
file_list = glob.glob("path/to/your/files/*.csv")

with open("combined.csv", "w") as outfile:
    for i, fname in enumerate(file_list):
        with open(fname) as infile:
            if i != 0:
                # Skip the header on all but the first file
                next(infile)
            for line in infile:
                outfile.write(line)
