import os
import icartt
import pandas as pd

# Base directory where your instrument files are stored
base_dir = "/Users/aaditshah/Downloads/Developing/BC_Research/"

# List of dates from your SP2 filenames
dates = [
    "20191005", "20191003", "20191001", "20190929", "20190927",
    "20190925", "20190923", "20190921", "20190919", "20190916",
    "20190915", "20190913", "20190908", "20190906", "20190904",
    "20190830", "20190829", "20190827", "20190824"
]

all_merged = []  # List to store each day's merged DataFrame

for date in dates:
    # Build file paths based on the date.
    sp2_file     = os.path.join(base_dir, "CAMP2Ex/SP2", f"CAMP2Ex-LARGE-SP2_P3B_{date}_R1.ict")
    optical_file = os.path.join(base_dir, "CAMP2Ex/OPTICAL", f"CAMP2Ex-LARGE-OPTICAL_P3B_{date}_R0.ict")
    las_file     = os.path.join(base_dir, "CAMP2Ex/LAS", f"CAMP2Ex-LARGE-LAS_P3B_{date}_R0.ict")
    metnav_file  = os.path.join(base_dir, "CAMP2Ex/MetNav", f"CAMP2EX-MetNav_P3B_{date}_R0.ict")
    ccn_file     = os.path.join(base_dir, "CAMP2Ex/CCN", f"CAMP2Ex-CCN_P3B_{date}_R1.ict")
    fcdp_file    = os.path.join(base_dir, "CAMP2Ex/FCDP", f"CAMP2Ex-HawkFCDP_P3B_{date}_R1.ict")
    
    # Load mandatory datasets; if any fail, skip this date.
    try:
        sp2_df = pd.DataFrame(icartt.Dataset(sp2_file).data[:])
        if 'BlackCarbon_STP' in sp2_df.columns:
            sp2_df = sp2_df.dropna(subset=['BlackCarbon_STP'])
    except Exception as e:
        print(f"Error loading SP2 for {date}: {e}")
        continue
        
    try:
        optical_df = pd.DataFrame(icartt.Dataset(optical_file).data[:])
    except Exception as e:
        print(f"Error loading Optical for {date}: {e}")
        continue
        
    try:
        las_df = pd.DataFrame(icartt.Dataset(las_file).data[:])
    except Exception as e:
        print(f"Error loading LAS for {date}: {e}")
        continue
        
    try:
        metnav_df = pd.DataFrame(icartt.Dataset(metnav_file).data[:])
    except Exception as e:
        print(f"Error loading MetNav for {date}: {e}")
        continue

    # Load optional datasets (CCN and FCDP) if they exist.
    ccn_df = None
    if os.path.exists(ccn_file):
        try:
            ccn_df = pd.DataFrame(icartt.Dataset(ccn_file).data[:])
        except Exception as e:
            print(f"Error loading CCN for {date}: {e}")
    fcdp_df = None
    if os.path.exists(fcdp_file):
        try:
            fcdp_df = pd.DataFrame(icartt.Dataset(fcdp_file).data[:])
        except Exception as e:
            print(f"Error loading FCDP for {date}: {e}")
            
    # Convert time columns to numeric for proper merging.
    optical_df['Time_Mid'] = pd.to_numeric(optical_df['Time_Mid'], errors='coerce')
    sp2_df['Time_Start']     = pd.to_numeric(sp2_df['Time_Start'], errors='coerce')
    las_df['Time_Start']     = pd.to_numeric(las_df['Time_Start'], errors='coerce')
    metnav_df['Time_Start']  = pd.to_numeric(metnav_df['Time_Start'], errors='coerce')
    if ccn_df is not None:
        ccn_df['Time_Start'] = pd.to_numeric(ccn_df['Time_Start'], errors='coerce')
    if fcdp_df is not None:
        fcdp_df['Time_Start'] = pd.to_numeric(fcdp_df['Time_Start'], errors='coerce')
    
    # Merge datasets.
    # Use Optical as the base (present every day) and left join others.
    merged = pd.merge(optical_df, sp2_df, left_on='Time_Mid', right_on='Time_Start', how='left',
                      suffixes=('_Optical', '_SP2'))
    merged = pd.merge(merged, las_df, left_on='Time_Mid_Optical', right_on='Time_Start', how='left',
                      suffixes=('', '_LAS'))
    merged = pd.merge(merged, metnav_df, left_on='Time_Mid_Optical', right_on='Time_Start', how='left',
                      suffixes=('', '_MetNav'))
    if ccn_df is not None:
        merged = pd.merge(merged, ccn_df, left_on='Time_Mid_Optical', right_on='Time_Start', how='left',
                          suffixes=('', '_CCN'))
    if fcdp_df is not None:
        merged = pd.merge(merged, fcdp_df, left_on='Time_Mid_Optical', right_on='Time_Start', how='left',
                          suffixes=('', '_FCDP'))
    
    # Optionally, add a column to track the date.
    merged['Date'] = date
    
    # Save this day's merged data to a CSV file (if desired).
    merged_file_path = os.path.join(base_dir, f"Merged_{date}.csv")
    merged.to_csv(merged_file_path, index=False)
    print(f"Merged data for {date}")
    
    # Append this merged DataFrame to our list.
    all_merged.append(merged)

# After processing all dates, merge them all into one DataFrame.
final_merged = pd.concat(all_merged, ignore_index=True)
final_merged_file = os.path.join(base_dir, "Merged_AllDates.csv")
final_merged.to_csv(final_merged_file, index=False)
print("All dates merged into one file.")
