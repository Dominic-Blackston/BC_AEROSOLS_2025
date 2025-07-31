Implements the data splitting methods found in: https://buckeyemailosu.sharepoint.com/:w:/r/sites/NOAADataAnalysisforBlackCarbonAerosols/_layouts/15/Doc2.aspx?action=edit&sourcedoc=%7Bb1644ef0-b534-4413-98ce-aa4e82e3de2b%7D&wdOrigin=TEAMS-MAGLEV.teamsSdk_ns.rwc&wdExp=TEAMS-TREATMENT&wdhostclicktime=1753724613781&web=1

Steps to run:

All code to run is in create_data_split.ipynb

IN STEP 0:
-change paths for variables: dataset_path, output_path, kfold_info_path, campaign_based_info_path, day_based_info_path
-all info paths point to info.csvs. Examples found in the info_list in this folder.
-dataset_path: your dataset csv with all the MAC columns.

IN STEP 1, 2, 3:
-No variable changes, run as normal
