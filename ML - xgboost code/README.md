The XGBoost code runs the XGBoost model on the dataset through different combinations of # of bins (seen in Section 11. MAC Binning) and # of split strategies (seen in Section 12. MAC Dataset Data Splitting). 
There are four files: xgboost_campaign, xgboost_day, xgboost_kfold, xgboost_random each implementing the training structure seen in Section 12. MAC Dataset Data Splitting. Each file needs the same steps before running. 

STEPS: 

1. Change bins and split_strats to desired combinations to test the XGBoost model on. 
2. Change DATASET_PATH and OUTPUT_PATH to desired input dataset and output directories. Right now, we have it automate to choose files based on bins and split_strats file names. 
3. Run the entire file 
