Implements the data splitting methods found in: 

[DATA STRAT 1]
https://buckeyemailosu.sharepoint.com/:w:/r/sites/NOAADataAnalysisforBlackCarbonAerosols/_layouts/15/Doc2.aspx?action=edit&sourcedoc=%7Bb1644ef0-b534-4413-98ce-aa4e82e3de2b%7D&wdOrigin=TEAMS-MAGLEV.teamsSdk_ns.rwc&wdExp=TEAMS-TREATMENT&wdhostclicktime=1753724613781&web=1

[DATA STRAT 2]
https://buckeyemailosu.sharepoint.com/:w:/r/sites/NOAADataAnalysisforBlackCarbonAerosols/_layouts/15/Doc.aspx?sourcedoc=%7B032CCE40-14E1-4C64-969E-E14A09CB7CE0%7D&file=Data_Splitting_Strategey_v2.docx&action=default&mobileredirect=true

Steps to run:
FOR create_data_split.ipynb

IN STEP 0:
-change paths for variables: dataset_path, output_path, kfold_info_path, campaign_based_info_path, day_based_info_path
-all info paths point to info.csvs. Examples found in the info_list in this folder.
-dataset_path: your dataset csv with all the MAC columns.

IN STEP 1, 2, 3, 4:
-No variable changes, run as normal



FOR create_data_split_automated.ipynb

IN STEP 0: run normally to initialize functions
IN STEP 1:
-change bins and data_strats to a list of which bins and data strategies we want to run split methods 1-4 on.
-change dataset_path and output_path to where the dataset and output path is
-change kfold_info_path, campaign_based_info_path, day_based_info_path , random_based_info_path to info lists for the four split methods
-Then run like normal
