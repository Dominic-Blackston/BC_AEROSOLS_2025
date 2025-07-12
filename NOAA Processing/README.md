NOAA Processing code

Campaigns used to process: 
1) CALNEX

Steps:

STEP 1: Run NOAA_combiner_raw.ipynb
Purpose: Combine individual instrument files from a NOAA campaign.

Output:
-campaign.csv

Variables to Configure:
-campaign_path : path to raw instrument campaign files
Expected directory tree look is like this:

CAMPAIGN_NAME
-> instrument_1
  -> var_1
  -> var_2
  -> others...
-> instrument_2
  -> var_1
  -> var_2
  -> others..
-> others..

STEPS OTHER: Refer to DOE Processing code, we're using the same code from there. Mainly, from there:
Steps 2, 6, and 7

Notes:
-Expected directory tree is hugely different from the directories seen in NASA, DOE, and NSF processing.
