#!/usr/bin/env python3
"""
binning_post_renaming.py

This script is designed to run after your renaming columns script.
It consolidates (bins) the aerosol and cloud bin columns from your campaign data.
It processes two files: a Restricted file (aerosol-only) and a Comprehensive file 
(aerosol plus cloud measurements). Adjust file paths and bin diameter lists as needed.
"""

import os
import pandas as pd
import numpy as np
import re

# =============================================================================
# SECTION 1: Helper Functions for Binning
# =============================================================================
def create_index_bin(old_bin_diams, new_bin_diams):
    """
    Create index bins for new bin diameters based on old bin diameters,
    with each bin containing only indices exclusive to it.
    
    Parameters:
      old_bin_diams (list): List of original bin diameters.
      new_bin_diams (list): List of desired new bin diameters.
      
    Returns:
      list of lists: Each sub-list contains indices (from the old bins) to be consolidated.
    """
    max_indices = []
    for diameter in new_bin_diams:
        # Find the highest index where old_bin_diams is less than the current diameter
        indices = [i for i, old_diam in enumerate(old_bin_diams) if old_diam < diameter]
        max_index = max(indices) if indices else -1
        max_indices.append(max_index)
    
    index_bins = []
    prev_max = -1
    for current_max in max_indices:
        exclusive_indices = list(range(prev_max + 1, current_max + 1))
        index_bins.append(exclusive_indices)
        prev_max = current_max
    # Add remaining indices if any
    remaining_indices = list(range(prev_max + 1, len(old_bin_diams)))
    index_bins.append(remaining_indices)
    
    return index_bins

def bin_name_list(num_bins, bin_type="Aerosol"):
    """
    Generate a list of bin names.
    
    Args:
      num_bins (int): Number of bin columns.
      bin_type (str): "Aerosol" for aerosol bins or anything else for cloud bins.
      
    Returns:
      list of str: Names like "bin1", "bin2", ... for Aerosol or "cbin1", "cbin2", ... for Cloud.
    """
    if bin_type == "Aerosol":
        return [f"bin{i+1}" for i in range(num_bins)]
    else:
        return [f"cbin{i+1}" for i in range(num_bins)]

def consolidate_bins(df, index_bins, original_bin_names, new_bin_count, bin_type="Aerosol"):
    """
    Consolidate original bin columns into new bins by summing the corresponding columns.
    Preserves NA values where all the source columns are NA.
    
    Parameters:
      df (DataFrame): The input DataFrame containing bin columns.
      index_bins (list of lists): Mapping from old bin indices to new bins.
      original_bin_names (list): List of original bin column names in order.
      new_bin_count (int): Number of new bins to create.
      bin_type (str): "Aerosol" or "Cloud" (used for naming).
      
    Returns:
      DataFrame: Updated DataFrame with the original bin columns replaced by new consolidated bins.
    """
    # Work on a copy
    result_df = df.copy()
    
    # Determine the first bin column position to preserve ordering.
    all_columns = list(df.columns)
    if original_bin_names and original_bin_names[0] in all_columns:
        first_bin_position = all_columns.index(original_bin_names[0])
    else:
        first_bin_position = len(all_columns)
    
    new_bin_names = bin_name_list(new_bin_count, bin_type)
    new_bin_values = {}
    
    for new_idx, indices in enumerate(index_bins):
        # Filter valid indices
        valid_indices = [i for i in indices if i < len(original_bin_names)]
        old_columns = [original_bin_names[i] for i in valid_indices]
        if old_columns:
            # Sum across the columns; if all values are NA, the result is set to NA.
            summed = df[old_columns].sum(axis=1, skipna=True)
            # Determine rows where all are NA:
            all_na = df[old_columns].isna().all(axis=1)
            summed[all_na] = np.nan
            new_bin_values[new_bin_names[new_idx]] = summed
        else:
            # No columns to sum; fill with NA.
            new_bin_values[new_bin_names[new_idx]] = pd.Series([np.nan]*len(df), index=df.index)
    
    # Drop original bin columns
    result_df.drop(columns=original_bin_names, inplace=True)
    
    # Insert new bin columns at the original position
    for i, new_name in enumerate(new_bin_names):
        insert_pos = first_bin_position + i
        result_df.insert(insert_pos, new_name, new_bin_values[new_name])
    
    return result_df

# =============================================================================
# SECTION 2: Binning for Restricted and Comprehensive Files
# =============================================================================
def main():
    # --- Configuration ---
    base_dir = "/Users/aaditshah/Downloads/Developing/BC_Research/"
    campaign = "NAAMES(2015)"  # Adjust campaign name if needed.
    campaign_dir = os.path.join(base_dir, campaign)
    
    # --------------------
    # Restricted Binning (Aerosol only)
    # --------------------
    # Read the Restricted renamed file from your renaming script.
    restricted_file = os.path.join(campaign_dir, f"{campaign}_Restricted.csv")
    df_restricted = pd.read_csv(restricted_file)
    
    # Define your original aerosol bin diameters (from your campaign) 
    old_aerosol_diams = [100.0, 112.2, 125.9, 141.3, 158.5, 177.8, 199.5, 223.9, 251.2, 281.8, 316.2, 354.8, 398.1, 446.7, 501.2, 562.3, 631.0, 707.9, 794.3, 891.3, 1000.0, 1258.9, 1584.9, 1995.3, 2511.9, 3162.3
    ]

    # Define desired new aerosol bin diameters.
    new_aerosol_diams = [150, 169.8, 192.1, 217.5, 246.1, 278.6, 315.3, 356.8, 403.9, 457.1, 
                         517.3, 585.5, 662.7, 750]
    
    # Determine the number of original aerosol bins (assumed to be in the restricted file).
    # Here we assume that in the restricted file the aerosol bin columns are named as you set by renaming.
    # We'll re-create a list of original aerosol bin names based on the number of old aerosol diameters.
    NUM_AEROSOL_BINS = len(old_aerosol_diams)
    original_aerosol_bin_names = bin_name_list(NUM_AEROSOL_BINS, "Aerosol")
    
    # Create index bins mapping old to new aerosol bins.
    aerosol_index_bins = create_index_bin(old_aerosol_diams, new_aerosol_diams)
    # Consolidate aerosol bins in the restricted file.
    df_restricted_binned = consolidate_bins(df_restricted, aerosol_index_bins, original_aerosol_bin_names,
                                            len(aerosol_index_bins), "Aerosol")
    
    # Write the restricted binned output.
    restricted_binned_file = os.path.join(campaign_dir, f"{campaign}_Restricted_renamed_binned.csv")
    df_restricted_binned.to_csv(restricted_binned_file, index=False)
    print("\nRestricted binned file saved to:", restricted_binned_file)
    
    # --------------------
    # Comprehensive Binning (Aerosol and Cloud)
    # --------------------
    # Read the Comprehensive renamed file from your renaming script.
    comprehensive_file = os.path.join(campaign_dir, f"{campaign}_Comprehensive.csv")
    df_comprehensive = pd.read_csv(comprehensive_file)
    
    # --- First, consolidate aerosol bins ---
    # Use aerosol diameters as defined above.
    # We assume the comprehensive file has aerosol bin columns with the same names as in restricted.
    df_comprehensive_binned = consolidate_bins(df_comprehensive, aerosol_index_bins, original_aerosol_bin_names,
                                               len(aerosol_index_bins), "Aerosol")
    
    # --- Next, consolidate cloud bins ---
    # Define your original cloud bin diameters (from your campaign for cloud measurements)
    old_cloud_diams = [2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5,  10.5, 11.5, 12.5, 13.5, 15.0, 17.0, 19.0, 21.0, 23.0, 25.0, 27.0, 29.0, 31.0, 33.0, 35.0, 37.0, 39.0, 41.0, 43.0, 45.0, 47.0, 49.0]
    # Define desired new cloud bin diameters.
    new_cloud_diams = [3, 5.3, 7.7, 10, 12.3, 14.7, 17, 19.3, 21.7, 24, 26.3, 28.7,
                       31, 33.3, 35.7, 38, 40.3, 42.7, 45]
    
    NUM_CLOUD_BINS = len(old_cloud_diams)
    original_cloud_bin_names = bin_name_list(NUM_CLOUD_BINS, "Cloud")
    
    # Create index bins mapping for cloud bins.
    cloud_index_bins = create_index_bin(old_cloud_diams, new_cloud_diams)
    # Consolidate cloud bins in the comprehensive file.
    df_comprehensive_binned = consolidate_bins(df_comprehensive_binned, cloud_index_bins, original_cloud_bin_names,
                                               len(cloud_index_bins), "Cloud")
    
    # Write the comprehensive binned output.
    comprehensive_binned_file = os.path.join(campaign_dir, f"{campaign}_Comprehensive_renamed_binned.csv")
    df_comprehensive_binned.to_csv(comprehensive_binned_file, index=False)
    print("\nComprehensive binned file saved to:", comprehensive_binned_file)


if __name__ == '__main__':
    main()
