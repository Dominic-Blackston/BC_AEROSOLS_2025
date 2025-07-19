This part of the code is needed to prepare the MAC dataset that will be used when running models on the may dataset.

STEPS:

1) Run prep_MAC_Modules.ipynb
PURPOSE: prepares the necesarry files to run Modules A, C, D, and the Module_result_combiner code.
OUTPUT:
-master_restricted_datetimefixed.csv: the original dataset but with consistent datetime columns.
-size_dist_number.csv and size_dist_diameter.csv: needed info to run Module A
-Bscat_raw.csv and Babs_raw.csv: needed to run Module C
-BC_mass.csv: needed for Module D
VARIABLES TO CHANGE:
IN PART 1
-FILE_PATH: to master_restricted.csv in your machine
-output_path: to master_restricted_datetimefixed.csv anywhere on your machine
IN PART 2
-AEROSOL DIAMS: to the already defined bin midpoints in the may dataset for aerosol.
-change all paths that are called by .read_csv and .to_csv into wanted paths, KEEP THE NAME OF FILE THE SAME THOUGH: datetime.csv, size_dist_diameter_input.csv, size_dist_input.csv
IN PART 3
-change all paths that are called by .read_csv and .to_csv into wanted paths, KEEP THE NAME OF FILE THE SAME THOUGH: Bscat_raw.csv and Babs_raw.csv
IN PART 4
-change all paths that are called by .read_csv and .to_csv into wanted paths, KEEP THE NAME OF FILE THE SAME THOUGH: BC_mass.csv


2) Run Module_A_size_dist_w_merge_linux_Integrate_Cora.ipynb
PURPOSE: create the new size and volume distributions to be run by model
OUTPUT: Output_Module_A_w_merge_Cora_integrate.csv: new size and volume distributions
VARIABLES TO CHANGE:
-size_dist_input, size_dist_diameter_input, datetime_input: change to paths that were produced by STEP 1
-d_Nx_list, d_Vx_list: change to desired bin and volume bins
-export_csv: change to path desired, keep the name the same though: Output_Module_A_w_merge_Cora_integrate.csv

3) Run Module_C_convert_wavelength_linux_Integrate_Cora.ipynb
PURPOSE: creates the corrected wavelength data before being fed into the model
OUTPUT:
-Output_Module_C_Babs_Bscat_SSA_HANYANG+CORA.csv: correct babs and bscat values with SSA values
-Output_Module_C_Babs_CORA.csv: correct babs values with AAE
-Output_Module_C_Bscat_CORA.csv: correct bscat values with SAE
VARIABLES TO CHANGE:
-NEPH_Bscat_before_data: to path output by STEP1 for Bscat_raw.csv
-Filter_based_instrument_data: to path output by STEP1 for Babs_raw.csv
