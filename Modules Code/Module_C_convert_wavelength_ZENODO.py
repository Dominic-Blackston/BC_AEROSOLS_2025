import os
import sys
import numpy as np
import pandas as pd
from scipy.optimize import curve_fit
import scipy.odr as odr
import time
from tqdm import tqdm

datetimedf = pd.read_csv(rf"C:\Users\haika\Desktop\May_Research\MAC Model for may dataset\ModuleA\datetime.csv")
B_ABS_PATH = rf"C:\Users\haika\Desktop\May_Research\MAC Model for may dataset\ModuleC\Babs_raw.csv"
B_SCAT_PATH = rf"C:\Users\haika\Desktop\May_Research\MAC Model for may dataset\ModuleC\Bscat_raw.csv"
OUTPUT_BSCAT_PATH = rf'C:\Users\haika\Desktop\May_Research\MAC Model for may dataset\ModuleC\Output_Module_3_Bscat.csv'
OUTPUT_BABS_PATH = rf'C:\Users\haika\Desktop\May_Research\MAC Model for may dataset\ModuleC\Output_Module_3_Babs.csv'
OUTPUT_COMBINED_PATH = rf"C:\Users\haika\Desktop\May_Research\MAC Model for may dataset\ModuleC\Output_Module_3_Combined.csv"

def SAE_fit_one_measurement( wavelength, SAE_fit, SAE_constant):
    return  SAE_constant*np.power(wavelength, -SAE_fit)

def AAE_fit_one_measurement( wavelength, AAE_fit, AAE_constant):
    return  AAE_constant*np.power(wavelength, -AAE_fit)

def SAE_power_fit(bscat_wvl_red,bscat_wvl_green,bscat_wvl_blue,w1,w2,w3,w_global_1,w_global_2,w_global_3):

    wavelength_instrument=np.array([w1,w2,w3])  # modify if necessary

    temp_SAE = np.nan; temp_SAE_constant = np.nan
    temp_sigma = np.nan; temp_sigma_constant = np.nan  #uncertainty of fit parameters

    SAE_three_lambda = np.empty(len(bscat_wvl_red)); SAE_constant = np.empty(len(bscat_wvl_red))
    sigma_SAE = np.empty(len(bscat_wvl_red)); sigma_constant = np.empty(len(bscat_wvl_red))

    #compute SAE_two_lambda, which are used later when initializing SAE_three_lambda
    SAE_R_G = -np.log(bscat_wvl_red/bscat_wvl_green)/np.log(wavelength_instrument[0]/wavelength_instrument[1])
    SAE_R_B = -np.log(bscat_wvl_red/bscat_wvl_blue)/np.log(wavelength_instrument[0]/wavelength_instrument[2])
    SAE_G_B = -np.log(bscat_wvl_green/bscat_wvl_blue)/np.log(wavelength_instrument[1]/wavelength_instrument[2])

    # Progress tracking
    start_time = time.time()
    print("Processing SAE (Scattering) data...")
    
    # Progress bar for the main loop
    for m in tqdm(range(len(bscat_wvl_red)), desc="SAE Processing", unit="measurements"):
        if (np.isnan(bscat_wvl_red[m])==0 and np.isnan(bscat_wvl_green[m])==0 and np.isnan(bscat_wvl_blue[m])==0):

            one_babs_at_three_lambda=[bscat_wvl_red[m],bscat_wvl_green[m],bscat_wvl_blue[m]]
                
            #use SAE_G_B as initial input
            W_coef = np.array([SAE_G_B.values[m], bscat_wvl_green[m]/ (np.power(wavelength_instrument[1], -SAE_G_B.values[m]))] )
            try:
                W_coef, w_sigma = curve_fit(SAE_fit_one_measurement, wavelength_instrument, one_babs_at_three_lambda,p0=W_coef, maxfev=1000)
                w_sigma = np.sqrt(np.diag(w_sigma))
            except RuntimeError:
                pass
            temp_SAE = W_coef[0];temp_SAE_constant = W_coef[1];temp_sigma= w_sigma[0];temp_sigma_constant = w_sigma[1]
            #use SAE_R_G as initial input
            W_coef = np.array([SAE_R_G.values[m], bscat_wvl_red[m]/ (np.power(wavelength_instrument[0], -SAE_R_G.values[m]))]  ) 
            try:          
                W_coef, w_sigma = curve_fit(SAE_fit_one_measurement, wavelength_instrument, one_babs_at_three_lambda,p0=W_coef, maxfev=1000)
                w_sigma = np.sqrt(np.diag(w_sigma))
            except RuntimeError:
                pass
            
            #check if the current inital guess (SAE_R_G) is better than the previous one (SAE_G_B): ? w_sigma is smaller
            if (w_sigma[0]<temp_sigma):  #Yes, the current w_sigma is smaller                
                temp_SAE = W_coef[0];temp_SAE_constant = W_coef[1];temp_sigma= w_sigma[0];temp_sigma_constant = w_sigma[1]
            else:
                pass       #do not replace the previous results
            
            #use SAE_R_B as initial input
            W_coef = np.array([SAE_R_B.values[m],bscat_wvl_blue[m]/ (np.power(wavelength_instrument[2], -SAE_R_B.values[m]))])
            try:   
                W_coef, w_sigma = curve_fit(SAE_fit_one_measurement, wavelength_instrument, one_babs_at_three_lambda,p0=W_coef, maxfev=1000)
                w_sigma = np.sqrt(np.diag(w_sigma))
            except RuntimeError:
                pass               
            #check if the current initial guess (SAE_R_B) is better than the previous one (SAE_G_B or SAE_R_G): ? w_sigma is smaller
            if (w_sigma[0]<temp_sigma):  #Yes, the current w_sigma is smaller                        
                temp_SAE = W_coef[0];temp_SAE_constant = W_coef[1];temp_sigma= w_sigma[0];temp_sigma_constant = w_sigma[1]
            else: 
                pass #do not replace the previous results   
            
        #Output the results
        SAE_three_lambda[m]=temp_SAE; SAE_constant[m]=temp_SAE_constant
        sigma_SAE[m]=temp_sigma; sigma_constant[m]=temp_sigma_constant
        #clear temp_variables
        temp_SAE=np.nan;temp_SAE_constant=np.nan
        temp_sigma=np.nan;temp_sigma_constant=np.nan

    # Time tracking
    end_time = time.time()
    processing_time = end_time - start_time
    print(f"SAE processing completed in {processing_time:.2f} seconds")
    print(f"Average time per measurement: {processing_time/len(bscat_wvl_red):.4f} seconds")

    b_scat_red=SAE_constant*(np.power(w_global_1, -SAE_three_lambda))
    b_scat_green=SAE_constant*(np.power(w_global_2, -SAE_three_lambda))
    b_scat_blue=SAE_constant*(np.power(w_global_3, -SAE_three_lambda))

    file_M_3_Bscat = pd.DataFrame() 
    
    # Length validation
    if len(datetimedf) != len(bscat_wvl_red):
        print(f"WARNING: Length mismatch! datetime data: {len(datetimedf)}, measurement data: {len(bscat_wvl_red)}")
        print("Using minimum length for safety...")
        min_length = min(len(datetimedf), len(bscat_wvl_red))
        file_M_3_Bscat['datetime'] = datetimedf['datetime_all'].iloc[:min_length]
        file_M_3_Bscat['SAE'] = SAE_three_lambda[:min_length]
        file_M_3_Bscat['SAE_constant'] = SAE_constant[:min_length]
        file_M_3_Bscat['b_scat_red'] = b_scat_red[:min_length]
        file_M_3_Bscat['b_scat_green'] = b_scat_green[:min_length]
        file_M_3_Bscat['b_scat_blue'] = b_scat_blue[:min_length]
    else:
        print(f"Length validation passed: {len(datetimedf)} measurements")
        file_M_3_Bscat['datetime'] = datetimedf['datetime_all']
        file_M_3_Bscat['SAE'] = SAE_three_lambda
        file_M_3_Bscat['SAE_constant'] = SAE_constant
        file_M_3_Bscat['b_scat_red'] = b_scat_red
        file_M_3_Bscat['b_scat_green'] = b_scat_green
        file_M_3_Bscat['b_scat_blue'] = b_scat_blue

    print("Saving SAE results to CSV...")
    export_csv = file_M_3_Bscat.to_csv (OUTPUT_BSCAT_PATH, index = None, header=True) 
    print("SAE results saved successfully!")


def AAE_power_fit(babs_wvl_red,babs_wvl_green,babs_wvl_blue,w1,w2,w3,w_global_1,w_global_2,w_global_3):

    wavelength_instrument=np.array([w1,w2,w3])  # modify if necessary

    temp_AAE = np.nan; temp_AAE_constant = np.nan
    temp_sigma = np.nan; temp_sigma_constant = np.nan  #uncertainty of fit parameters

    AAE_three_lambda = np.empty(len(babs_wvl_red)); AAE_constant = np.empty(len(babs_wvl_red))
    sigma_AAE = np.empty(len(babs_wvl_red)); sigma_constant = np.empty(len(babs_wvl_red))

    #compute AAE_two_lambda, which are used later when initializing AAE_three_lambda
    AAE_R_G = -np.log(babs_wvl_red/babs_wvl_green)/np.log(wavelength_instrument[0]/wavelength_instrument[1])
    AAE_R_B = -np.log(babs_wvl_red/babs_wvl_blue)/np.log(wavelength_instrument[0]/wavelength_instrument[2])
    AAE_G_B = -np.log(babs_wvl_green/babs_wvl_blue)/np.log(wavelength_instrument[1]/wavelength_instrument[2])

    # Progress tracking
    start_time = time.time()
    print("Processing AAE (Absorption) data...")
    
    # Progress bar for the main loop
    for m in tqdm(range(len(babs_wvl_red)), desc="AAE Processing", unit="measurements"):
        if (np.isnan(babs_wvl_red[m])==0 and np.isnan(babs_wvl_green[m])==0 and np.isnan(babs_wvl_blue[m])==0):

            one_babs_at_three_lambda=[babs_wvl_red[m],babs_wvl_green[m],babs_wvl_blue[m]]
                
            #use AAE_G_B as initial input
            W_coef = np.array([AAE_G_B.values[m], babs_wvl_green[m]/ (np.power(wavelength_instrument[1], -AAE_G_B.values[m]))] )
            try:
                W_coef, w_sigma = curve_fit(AAE_fit_one_measurement, wavelength_instrument, one_babs_at_three_lambda,p0=W_coef, maxfev=1000)
                w_sigma = np.sqrt(np.diag(w_sigma))
            except RuntimeError:
                pass
            temp_AAE = W_coef[0];temp_AAE_constant = W_coef[1];temp_sigma= w_sigma[0];temp_sigma_constant = w_sigma[1]
            #use AAE_R_G as initial input
            W_coef = np.array([AAE_R_G.values[m], babs_wvl_red[m]/ (np.power(wavelength_instrument[0], -AAE_R_G.values[m]))]  ) 
            try:          
                W_coef, w_sigma = curve_fit(AAE_fit_one_measurement, wavelength_instrument, one_babs_at_three_lambda,p0=W_coef, maxfev=1000)
                w_sigma = np.sqrt(np.diag(w_sigma))
            except RuntimeError:
                pass
            
            #check if the current inital guess (AAE_R_G) is better than the previous one (AAE_G_B): ? w_sigma is smaller
            if (w_sigma[0]<temp_sigma):  #Yes, the current w_sigma is smaller                
                temp_AAE = W_coef[0];temp_AAE_constant = W_coef[1];temp_sigma= w_sigma[0];temp_sigma_constant = w_sigma[1]
            else:
                pass       #do not replace the previous results
            
            #use AAE_R_B as initial input
            W_coef = np.array([AAE_R_B.values[m],babs_wvl_blue[m]/ (np.power(wavelength_instrument[2], -AAE_R_B.values[m]))])
            try:   
                W_coef, w_sigma = curve_fit(AAE_fit_one_measurement, wavelength_instrument, one_babs_at_three_lambda,p0=W_coef, maxfev=1000)
                w_sigma = np.sqrt(np.diag(w_sigma))
            except RuntimeError:
                pass               
            #check if the current initial guess (AAE_R_B) is better than the previous one (AAE_G_B or AAE_R_G): ? w_sigma is smaller
            if (w_sigma[0]<temp_sigma):  #Yes, the current w_sigma is smaller                        
                temp_AAE = W_coef[0];temp_AAE_constant = W_coef[1];temp_sigma= w_sigma[0];temp_sigma_constant = w_sigma[1]
            else: 
                pass #do not replace the previous results   
            
        #Output the results
        AAE_three_lambda[m]=temp_AAE; AAE_constant[m]=temp_AAE_constant
        sigma_AAE[m]=temp_sigma; sigma_constant[m]=temp_sigma_constant
        #clear temp_variables
        temp_AAE=np.nan;temp_AAE_constant=np.nan
        temp_sigma=np.nan;temp_sigma_constant=np.nan

    # Time tracking
    end_time = time.time()
    processing_time = end_time - start_time
    print(f"AAE processing completed in {processing_time:.2f} seconds")
    print(f"Average time per measurement: {processing_time/len(babs_wvl_red):.4f} seconds")

    b_abs_red=AAE_constant*(np.power(w_global_1, -AAE_three_lambda))
    b_abs_green=AAE_constant*(np.power(w_global_2, -AAE_three_lambda))
    b_abs_blue=AAE_constant*(np.power(w_global_3, -AAE_three_lambda))

    file_M_3_Babs = pd.DataFrame() 
    
    # Length validation
    if len(datetimedf) != len(babs_wvl_red):
        print(f"WARNING: Length mismatch! datetime data: {len(datetimedf)}, measurement data: {len(babs_wvl_red)}")
        print("Using minimum length for safety...")
        min_length = min(len(datetimedf), len(babs_wvl_red))
        file_M_3_Babs['datetime'] = datetimedf['datetime_all'].iloc[:min_length]
        file_M_3_Babs['AAE'] = AAE_three_lambda[:min_length]
        file_M_3_Babs['AAE_constant'] = AAE_constant[:min_length]
        file_M_3_Babs['b_abs_red'] = b_abs_red[:min_length]
        file_M_3_Babs['b_abs_green'] = b_abs_green[:min_length]
        file_M_3_Babs['b_abs_blue'] = b_abs_blue[:min_length]
    else:
        print(f"Length validation passed: {len(datetimedf)} measurements")
        file_M_3_Babs['datetime'] = datetimedf['datetime_all']
        file_M_3_Babs['AAE'] = AAE_three_lambda
        file_M_3_Babs['AAE_constant'] = AAE_constant
        file_M_3_Babs['b_abs_red'] = b_abs_red
        file_M_3_Babs['b_abs_green'] = b_abs_green
        file_M_3_Babs['b_abs_blue'] = b_abs_blue

    print("Saving AAE results to CSV...")
    export_csv = file_M_3_Babs.to_csv (OUTPUT_BABS_PATH, index = None, header=True) 
    print("AAE results saved successfully!")


#Implement Module C
def implement_module_C(wl_1,wl_2,wl_3,wl_global_1,wl_global_2,wl_global_3, abs_or_scat = "scat"):

    total_start_time = time.time()
    
    if (abs_or_scat == "scat"):
        print("=" * 60)
        print("STARTING SCATTERING (SAE) ANALYSIS")
        print("=" * 60)
        
        #load filter-based absorption photometer data
        print("Loading scattering data...")
        NEPH_Bscat_before_data  = pd.read_csv(B_SCAT_PATH)
        bscat_red=NEPH_Bscat_before_data['scat_red']
        bscat_green=NEPH_Bscat_before_data['scat_green']
        bscat_blue=NEPH_Bscat_before_data['scat_blue']
        
        print(f"Loaded {len(bscat_red)} scattering measurements")
        print(f"Input wavelengths: {wl_1}nm (red), {wl_2}nm (green), {wl_3}nm (blue)")
        print(f"Target wavelengths: {wl_global_1}nm (red), {wl_global_2}nm (green), {wl_global_3}nm (blue)")
        
        SAE_power_fit(bscat_red,bscat_green,bscat_blue,wl_1,wl_2,wl_3,wl_global_1,wl_global_2,wl_global_3)

    elif (abs_or_scat == "abs"):
        print("=" * 60)
        print("STARTING ABSORPTION (AAE) ANALYSIS") 
        print("=" * 60)

        #load filter-based absorption photometer data
        print("Loading absorption data...")
        Filter_based_instrument_data  = pd.read_csv(B_ABS_PATH)
        babs_red=Filter_based_instrument_data['abs_red']
        babs_green=Filter_based_instrument_data['abs_green']
        babs_blue=Filter_based_instrument_data['abs_blue']
        
        print(f"Loaded {len(babs_red)} absorption measurements")
        print(f"Input wavelengths: {wl_1}nm (red), {wl_2}nm (green), {wl_3}nm (blue)")
        print(f"Target wavelengths: {wl_global_1}nm (red), {wl_global_2}nm (green), {wl_global_3}nm (blue)")
        
        AAE_power_fit(babs_red,babs_green,babs_blue, wl_1,wl_2,wl_3,wl_global_1,wl_global_2,wl_global_3)
    
    # Total time tracking
    total_end_time = time.time()
    total_processing_time = total_end_time - total_start_time
    print("=" * 60)
    print(f"TOTAL PROCESSING TIME: {total_processing_time:.2f} seconds")
    print("=" * 60)


def combine_output_files():
    """Combine SAE and AAE output files into a single comprehensive file"""
    print("=" * 60)
    print("COMBINING OUTPUT FILES")
    print("=" * 60)
    
    try:
        # Load both output files
        print("Loading SAE results...")
        scat_df = pd.read_csv(OUTPUT_BSCAT_PATH)
        
        print("Loading AAE results...")
        abs_df = pd.read_csv(OUTPUT_BABS_PATH)
        
        # Verify both files have the same number of rows
        if len(scat_df) != len(abs_df):
            print(f"WARNING: Row count mismatch! SAE: {len(scat_df)}, AAE: {len(abs_df)}")
            min_rows = min(len(scat_df), len(abs_df))
            print(f"Using first {min_rows} rows from both files")
            scat_df = scat_df.iloc[:min_rows]
            abs_df = abs_df.iloc[:min_rows]
        else:
            print(f"Row count validation passed: {len(scat_df)} measurements")
        
        # Create combined dataframe
        # Start with scattering data (includes datetime)
        combined_df = scat_df.copy()
        
        # Add absorption data (excluding datetime column to avoid duplication)
        abs_columns_to_add = ['AAE', 'AAE_constant', 'b_abs_red', 'b_abs_green', 'b_abs_blue']
        for col in abs_columns_to_add:
            combined_df[col] = abs_df[col]
        
        # Verify final column order
        expected_columns = ['datetime', 'SAE', 'SAE_constant', 'b_scat_red', 'b_scat_green', 'b_scat_blue', 
                          'AAE', 'AAE_constant', 'b_abs_red', 'b_abs_green', 'b_abs_blue']
        
        print("Final column order:")
        for i, col in enumerate(combined_df.columns, 1):
            print(f"  {i}. {col}")
        
        # DO SSA CALCULATIONS BEFORE OUTPUTTING
        #replace values that could cause calculation errors with another flag (-3333)
        combined_df.loc[combined_df['b_abs_red']+combined_df['b_scat_red'] == 0, 'b_abs_red'] = -3333
        combined_df.loc[combined_df['b_abs_green']+combined_df['b_scat_green'] == 0, 'b_abs_green'] = -3333
        combined_df.loc[combined_df['b_abs_blue']+combined_df['b_scat_blue'] == 0, 'b_abs_blue'] = -3333

        combined_df.loc[combined_df['b_abs_red']== 0, 'b_scat_red'] = -3333
        combined_df.loc[combined_df['b_abs_green']== 0, 'b_scat_green'] = -3333
        combined_df.loc[combined_df['b_abs_blue']== 0, 'b_scat_blue'] = -3333

        #calculate SSA
        combined_df['SSA_red'] = combined_df.apply(lambda x: x['b_scat_red'] / (x['b_scat_red'] + x['b_abs_red']), axis=1)
        combined_df['SSA_blue'] = combined_df.apply(lambda x: x['b_scat_blue'] / (x['b_scat_blue'] + x['b_abs_blue']), axis=1)
        combined_df['SSA_green'] = combined_df.apply(lambda x: x['b_scat_green'] / (x['b_scat_green'] + x['b_abs_green']), axis=1)

        #replace values where SSA <0 or >1 with a flag (-2222)
        combined_df.loc[combined_df['SSA_red'] < 0, 'SSA_red'] = -2222
        combined_df.loc[combined_df['SSA_green'] < 0, 'SSA_green'] = -2222
        combined_df.loc[combined_df['SSA_blue'] < 0, 'SSA_blue'] = -2222
        combined_df.loc[combined_df['SSA_red'] > 1, 'SSA_red'] = -2222
        combined_df.loc[combined_df['SSA_green'] > 1, 'SSA_green'] = -2222
        combined_df.loc[combined_df['SSA_blue'] > 1, 'SSA_blue'] = -2222

        # Save combined file
        output_path = OUTPUT_COMBINED_PATH
        combined_df.to_csv(output_path, index=None, header=True)
        
        print(f"Combined file saved successfully!")
        print(f"File location: {output_path}")
        print(f"Total measurements: {len(combined_df)}")
        
    except Exception as e:
        print(f"Error combining files: {str(e)}")
        print("Make sure both SAE and AAE analyses have been completed first.")


# para_wl_1 = int(sys.argv[1])
# para_wl_2 = int(sys.argv[2])
# para_wl_3 = int(sys.argv[3])
# para_wl_4 = int(sys.argv[4])
# para_wl_5 = int(sys.argv[5])
# para_wl_6 = int(sys.argv[6])
# para_abs_or_scat = str(sys.argv[7])

#implement_module_C(para_wl_1,para_wl_2,para_wl_3,para_wl_4,para_wl_5,para_wl_6, abs_or_scat = para_abs_or_scat)

ori_scat_red = 700
ori_scat_green = 550
ori_scat_blue = 450

ori_abs_red = 660
ori_abs_green = 532
ori_abs_blue = 470

new_wl_red = 660
new_wl_green = 532
new_wl_blue = 470

print("Starting Module C Analysis...")
print(f"Analysis started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

#scat run
implement_module_C(ori_scat_red, ori_scat_green, ori_scat_blue, new_wl_red, new_wl_green, new_wl_blue, abs_or_scat = "scat")

#abs run
implement_module_C(ori_abs_red, ori_abs_green, ori_abs_blue, new_wl_red, new_wl_green, new_wl_blue, abs_or_scat = "abs")

# Combine both outputs into a single file
combine_output_files()

print(f"Analysis completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")