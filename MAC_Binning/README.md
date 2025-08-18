The MAC Binning component bins the dataset by d_Nx (number distribution) and d_Vx (volume distribution). 
 
The MAC_binning.ipynb does two things that you can run separately: 
 

FUNCTION 1: Bins the dataset by user-defined bins 

1. In STEP 0, change AEROSOL_DIAMS to the original may dataset bins. Change df_may to the dataset path. 

2. In STEP 1, change df_MAC to the dataset path. Change d_Nx_list and d_Vx_list to user-desired bin diameters. 

3. In STEP 1, change result_df.to_csv to output path that you want. 

4. Run import cell, STEP 0, then STEP 1. 

 

FUNCTION 2: Automatically bins dataset by bin number input. 

Note: The algorithm optimizes which bin diameters to choose by maximizing equal distribution between bins and minimizing the number of empty bins. 

1. In STEP 0, change AEROSOL_DIAMS to the original may dataset bins. Change df_may to the dataset path. 

2. IN BOTH ADDITIONAL CELLS, change NUM_BINS_DESIRED to number of bins you want. Then change df_MAC to the dataset path. Change output_filename to desired output path. 

3. Run import cell, STEP 0, then ADDITIONAL cells. 
