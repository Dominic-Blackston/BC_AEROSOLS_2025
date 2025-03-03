import os, glob
import icartt
import pandas as pd

base_dir = "/Users/aaditshah/Downloads/Developing/BC_Research/"

# Define glob patterns for each instrument (assuming similar file naming)
patterns = {
    "SP2":    "CAMP2Ex/SP2/CAMP2Ex-LARGE-SP2_P3B_*_R1.ict",
    "OPTICAL": "CAMP2Ex/OPTICAL/CAMP2Ex-LARGE-OPTICAL_P3B_*_R0.ict",
    "LAS":    "CAMP2Ex/LAS/CAMP2Ex-LARGE-LAS_P3B_*_R0.ict",
    "MetNav": "CAMP2Ex/MetNav/CAMP2EX-MetNav_P3B_*_R0.ict",
    "CCN":    "CAMP2Ex/CCN/CAMP2Ex-CCN_P3B_*_R1.ict",
    "FCDP":   "CAMP2Ex/FCDP/CAMP2Ex-HawkFCDP_P3B_*_R1.ict"
}

dataframes = {}

# Load and concatenate files for each instrument
for key, pattern in patterns.items():
    files = glob.glob(os.path.join(base_dir, pattern))
    df_list = []
    for file in files:
        ict = icartt.Dataset(file)
        df = pd.DataFrame(ict.data[:])
        # Filter SP2 data if needed
        # if key == "SP2" and 'BlackCarbon_STP' in df.columns:
        #     df = df.dropna(subset=['BlackCarbon_STP'])
        df_list.append(df)
    if df_list:
        dataframes[key] = pd.concat(df_list, ignore_index=True)
    else:
        dataframes[key] = None

# Convert time columns to numeric
dataframes["OPTICAL"]['Time_Mid'] = pd.to_numeric(dataframes["OPTICAL"]['Time_Mid'], errors='coerce')
dataframes["SP2"]['Time_Start']     = pd.to_numeric(dataframes["SP2"]['Time_Start'], errors='coerce')
dataframes["LAS"]['Time_Start']     = pd.to_numeric(dataframes["LAS"]['Time_Start'], errors='coerce')
dataframes["MetNav"]['Time_Start']  = pd.to_numeric(dataframes["MetNav"]['Time_Start'], errors='coerce')
if dataframes["CCN"] is not None:
    dataframes["CCN"]['Time_Start'] = pd.to_numeric(dataframes["CCN"]['Time_Start'], errors='coerce')
if dataframes["FCDP"] is not None:
    dataframes["FCDP"]['Time_Start'] = pd.to_numeric(dataframes["FCDP"]['Time_Start'], errors='coerce')

# Now merge datasets using Optical as the base, and left join optional ones
merged_df = pd.merge(dataframes["OPTICAL"], dataframes["SP2"], left_on='Time_Mid', right_on='Time_Start', 
                     how='left', suffixes=('_Optical', '_SP2'))
merged_df = pd.merge(merged_df, dataframes["LAS"], left_on='Time_Mid_Optical', right_on='Time_Start', 
                     how='left', suffixes=('', '_LAS'))
merged_df = pd.merge(merged_df, dataframes["MetNav"], left_on='Time_Mid_Optical', right_on='Time_Start', 
                     how='left', suffixes=('', '_MetNav'))
if dataframes["CCN"] is not None:
    merged_df = pd.merge(merged_df, dataframes["CCN"], left_on='Time_Mid_Optical', right_on='Time_Start', 
                         how='left', suffixes=('', '_CCN'))
if dataframes["FCDP"] is not None:
    merged_df = pd.merge(merged_df, dataframes["FCDP"], left_on='Time_Mid_Optical', right_on='Time_Start', 
                         how='left', suffixes=('', '_FCDP'))

merged_file_path = os.path.join(base_dir, "Merged_AllDates.csv")
merged_df.to_csv(merged_file_path, index=False)
print("All dates merged into one file.")
