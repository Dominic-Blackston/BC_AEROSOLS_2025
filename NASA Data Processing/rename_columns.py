import os
import pandas as pd
import re

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def find_candidate(column_list, candidates):
    lower_columns = {col.lower(): col for col in column_list}
    for candidate in candidates:
        cand_low = candidate.lower()
        if cand_low in lower_columns:
            return lower_columns[cand_low]
    return None

def build_mapping(expected_dict, raw_columns):
    mapping = {}
    for target, candidates in expected_dict.items():
        found = find_candidate(raw_columns, candidates)
        if found:
            mapping[found] = target
    return mapping

def detect_columns(raw_columns, pattern_funcs):
    detected = []
    for col in raw_columns:
        if any(func(col) for func in pattern_funcs):
            detected.append(col)
    return sorted(detected)

# =============================================================================
# SETUP
# =============================================================================
base_dir = "/Users/aaditshah/Downloads/Developing/BC_Research/"
campaign = "NAAMES(2017)"
campaign_dir = os.path.join(base_dir, campaign)
raw_file = os.path.join(campaign_dir, f"{campaign}_Raw.csv")
df_raw = pd.read_csv(raw_file)
print("Loaded raw file with columns:")
print(df_raw.columns.tolist())

df_raw["Organization"] = "NASA"
df_raw["Campaign"] = campaign
raw_cols = df_raw.columns.tolist()

# =============================================================================
# BIN DETECTION
# =============================================================================
aerosol_bin_candidates = [
    lambda col: col.startswith("dNdlogDp_PSL_") and col.endswith("_LAS"),
    lambda col: col.startswith("LAS_Bin") and re.match(r'\d+', col[8:]) is not None,
]
aerosol_bin_cols = detect_columns(raw_cols, aerosol_bin_candidates)
if not aerosol_bin_cols:
    raise ValueError("Aerosol bin columns (LAS/UHSAS) are required but none were detected!")
aerosol_rename_map = {col: f"bin{i+1}" for i, col in enumerate(aerosol_bin_cols)}

cloud_bin_candidates = [
    lambda col: re.match(r"^cbin\d+", col, re.IGNORECASE) is not None,
    lambda col: col.startswith("CDP_Bin") and re.search(r'\d+', col) is not None,
]
cloud_bin_cols = detect_columns(raw_cols, cloud_bin_candidates)
cloud_rename_map = {col: f"cbin{i+1}" for i, col in enumerate(cloud_bin_cols)}

# =============================================================================
# EXPECTED COLUMNS
# =============================================================================
restricted_expected = {
    "UTC": ["merge_time", "UTC", "START_UTC", "Time", "Time_Start", "Time_Mid"],
    "Date": ["Date", "date"],
    "Latitude": ["LATITUDE", "GPS_LAT", "Lat", "FMS_LAT"],
    "Longitude": ["LONGITUDE", "GPS_LON", "Lon", "FMS_LON"],
    "Altitude": ["GPS_ALT", "Alt", "FMS_ALT_PRES", "GPS_Altitude", "MSL_GPS_Altitude"],
    "Temperature": ["Static_Air_Temp", "T"],
    "Rel_humidity": ["RH_amb", "Relative_Humidity"],
    "Pressure": ["P", "Static_Pressure", "FMS_ALT_PRES"],
    "BC_Mass": ["BlackCarbonMassConcentration", "BC_Mass", "mBC", "BC_AccumMode_mass_HDSP2", "BC_mass_90_550_nm_HDSP2", "BlackCarbon_STP", "BC_mass_90_550_nm"],
    "Sc450_total": ["totSc450_stdPT", "Sc450_total", "Scat450tot", "drySc450_stdPT"],
    "Sc550_total": ["totSc550_stdPT", "Sc550_total", "Scat550tot", "drySc550_stdPT"],
    "Sc700_total": ["totSc700_stdPT", "Sc700_total", "Scat700tot", "drySc700_stdPT"],
    "Abs470_total": ["absSc470_stdPT", "Abs470_total", "Abs470tot", "Abs470_stdPT"],
    "Abs532_total": ["absSc532_stdPT", "Abs532_total", "Abs532tot", "Abs532_stdPT"],
    "Abs660_total": ["absSc660_stdPT", "Abs660_total", "Abs660tot", "Abs660_stdPT"],
}

comprehensive_expected = {
    **restricted_expected,
    "U": ["U", "E/W Wind Speed", "U_ms-1"],
    "V": ["V", "N/S Wind Speed", "V_ms-1"],
    "W": ["W", "Vertical Wind Speed", "w_ms-1"],
    "Supersaturation": ["Supersaturation", "CCN_Supersaturation"],
    "Number_Concentration": ["Number_Concentration"],
    "CNgt3nm": ["CNgt3nm", "CN>3nm"],
    "CNgt10nm": ["CNgt10nm", "CN>10nm"],
    "LWC": ["lwc", "Liquid Water Content"],
}

# =============================================================================
# RESTRICTED OUTPUT
# =============================================================================
restricted_mapping = build_mapping(restricted_expected, raw_cols)
missing_restricted = [target for target in restricted_expected if target not in restricted_mapping.values()]
if missing_restricted:
    raise ValueError("Required restricted columns not found: " + ", ".join(missing_restricted))

restricted_manual = {"Organization": "Organization", "Campaign": "Campaign"}
restricted_manual.update(restricted_mapping)
restricted_manual.update(aerosol_rename_map)
for target in ["Sc450_total", "Sc550_total", "Sc700_total", "Abs470_total", "Abs532_total", "Abs660_total"]:
    found = find_candidate(raw_cols, restricted_expected.get(target, []))
    if found:
        restricted_manual[found] = target

restricted_columns = list(restricted_manual.keys())
df_restricted = df_raw.reindex(columns=restricted_columns).rename(columns=restricted_manual)

restricted_order = [
    "Organization", "Campaign", "UTC", "Date", "Latitude", "Longitude",
    "Altitude", "Temperature", "Rel_humidity", "Pressure", "BC_Mass"
] + [f"bin{i+1}" for i in range(len(aerosol_bin_cols))] + [
    "Sc450_total", "Sc550_total", "Sc700_total",
    "Abs470_total", "Abs532_total", "Abs660_total"
]
restricted_order_final = [col for col in restricted_order if col in df_restricted.columns]
df_restricted = df_restricted[restricted_order_final]

# =============================================================================
# COMPREHENSIVE OUTPUT
# =============================================================================
comprehensive_mapping = build_mapping(comprehensive_expected, raw_cols)
comprehensive_manual = {"Organization": "Organization", "Campaign": "Campaign"}
comprehensive_manual.update(comprehensive_mapping)
comprehensive_manual.update(aerosol_rename_map)
comprehensive_manual.update(cloud_rename_map)
for target in ["Sc450_total", "Sc550_total", "Sc700_total", "Abs470_total", "Abs532_total", "Abs660_total"]:
    found = find_candidate(raw_cols, restricted_expected.get(target, []))
    if found:
        comprehensive_manual[found] = target

comprehensive_columns = list(comprehensive_manual.keys())
df_comprehensive = df_raw.reindex(columns=comprehensive_columns).rename(columns=comprehensive_manual)

comprehensive_order = [
    "Organization", "Campaign", "UTC", "Date", "Latitude", "Longitude",
    "Altitude", "Temperature", "Rel_humidity", "Pressure",
    "U", "V", "W", "Supersaturation", "Number_Concentration", "CNgt3nm", "CNgt10nm",
    "BC_Mass", "LWC"
] + [f"cbin{i+1}" for i in range(len(cloud_bin_cols))] + [f"bin{i+1}" for i in range(len(aerosol_bin_cols))] + [
    "Sc450_total", "Sc550_total", "Sc700_total",
    "Abs470_total", "Abs532_total", "Abs660_total"
]
comprehensive_order_final = comprehensive_order
df_comprehensive = df_comprehensive.reindex(columns=comprehensive_order_final)

# =============================================================================
# OUTPUT FILES
# =============================================================================
restricted_output_file = os.path.join(campaign_dir, f"{campaign}_Restricted.csv")
comprehensive_output_file = os.path.join(campaign_dir, f"{campaign}_Comprehensive.csv")

df_restricted.to_csv(restricted_output_file, index=False)
print("\n=== Restricted File Columns ===")
print(df_restricted.columns.tolist())
print("Restricted file saved to:", restricted_output_file)

print("\n=== Comprehensive File Columns ===")
print([col for col in df_comprehensive.columns if col not in df_restricted.columns])
print("Comprehensive file saved to:", comprehensive_output_file)
df_comprehensive.to_csv(comprehensive_output_file, index=False)
